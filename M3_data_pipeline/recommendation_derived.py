"""추천에 사용할 파생 데이터를 생성하고 조회 기능을 제공합니다."""

from datetime import datetime
import hashlib
import json
from types import FunctionType, SimpleNamespace

from sqlalchemy import text

try:
    from .environment_features import derive_exercise_features, read_processed_environment
    from .facility_derived import KST, derive_recommendation
except ImportError:
    from environment_features import derive_exercise_features, read_processed_environment
    from facility_derived import KST, derive_recommendation


TABLE = "derived.facility_recommendation"
DDL = f"""CREATE TABLE IF NOT EXISTS {TABLE} (
    source_table text NOT NULL,
    source_key text NOT NULL,
    facility_name text NOT NULL,
    facility_type text NOT NULL,
    address text NOT NULL,
    city text NOT NULL,
    district text NOT NULL,
    latitude double precision,
    longitude double precision,
    source_row jsonb NOT NULL,
    features jsonb NOT NULL,
    evaluated_at timestamptz NOT NULL,
    PRIMARY KEY (source_table, source_key)
)"""


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, allow_nan=False)


def _coordinate(value, bound):
    try:
        value = float(value)
        return value if abs(value) <= bound else None
    except (ValueError, TypeError):
        return None


def build_record(row, source_table, *, now, environment):
    """Keep the two facility populations separate; never join by name/institution."""
    row = dict(row)
    if source_table == "processed.facility":
        normalized = dict(row, latitude=row.get("faci_lat"), longitude=row.get("faci_lot"),
                          sido_standard=row.get("cp_nm"))
        name = row.get("faci_nm") or "시설명 미등록"
        kind = row.get("ftype_nm") or row.get("fcob_nm") or "운동 시설"
        address = row.get("faci_road_addr") or row.get("faci_addr") or ""
        city, district = row.get("cp_nm") or "", row.get("cpb_nm") or ""
        exposure = {"실내": "INDOOR", "실외": "OUTDOOR"}.get(row.get("inout_gbn_nm"))
    elif source_table == "processed.public_open_facility":
        normalized = row
        name, kind = row.get("openFcltyNm") or "", row.get("openFcltyType") or ""
        address = row.get("rdnmadr") or row.get("lnmadr") or ""
        city, district, exposure = row.get("sido_standard") or "", "", None
    else:
        raise ValueError("Unsupported processed source")
    features = derive_exercise_features(
        normalized, now=now, **environment,
        verified_exposure=({"state": exposure, "evidence": "processed.facility.inout_gbn_nm"}
                           if exposure else None),
    )
    features["weather_link"].update(source_table=source_table,
        source_columns=["faci_lat", "faci_lot"] if source_table == "processed.facility"
        else ["latitude", "longitude"])
    # Content identity is snapshot-scoped, not a fabricated cross-source facility ID.
    key = hashlib.sha256(_json(row).encode("utf-8")).hexdigest()
    return dict(source_table=source_table, source_key=key, facility_name=name,
                facility_type=kind, address=address, city=city, district=district,
                latitude=_coordinate(normalized.get("latitude"), 90),
                longitude=_coordinate(normalized.get("longitude"), 180),
                source_row=_json(row), features=_json(features), evaluated_at=now)


def refresh(engine, *, now=None):
    """Read a consistent source snapshot, release it, then write only derived.

    No source DDL, locks for writes, pipeline metadata, or external services.
    Short lock timeouts avoid waiting behind an E2E table replacement.
    """
    if engine.url.host not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("Only local PostgreSQL is allowed")
    now = now or datetime.now(KST)
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            conn.execute(text("SET LOCAL lock_timeout = '500ms'"))
            # read_processed_environment accepts an engine-like context provider.
            from contextlib import nullcontext
            environment = read_processed_environment(SimpleNamespace(connect=lambda: nullcontext(conn)), now=now)
            sources = {table: conn.execute(text(f"SELECT * FROM processed.{table}")).mappings().all()
                       for table in ("facility", "public_open_facility")}
    records = {}
    for table, rows in sources.items():
        for row in rows:
            record = build_record(row, f"processed.{table}", now=now, environment=environment)
            records[(record["source_table"], record["source_key"])] = record
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '500ms'"))
        locked = conn.execute(text("SELECT pg_try_advisory_xact_lock(731905291)")).scalar_one()
        if not locked:
            raise RuntimeError("Another derived refresh is running")
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS derived"))
        conn.execute(text(DDL))
        conn.execute(text(f"DELETE FROM {TABLE}"))
        values = list(records.values())
        columns = list(values[0]) if values else []
        if values:
            binds = [f"CAST(:{c} AS jsonb)" if c in {"source_row", "features"} else f":{c}" for c in columns]
            insert = text(f"INSERT INTO {TABLE} ({', '.join(columns)}) VALUES ({', '.join(binds)})")
            for start in range(0, len(values), 1000):
                conn.execute(insert, values[start:start + 1000])
    return {"source_rows": {k: len(v) for k, v in sources.items()}, "stored_rows": len(records)}


def facilities_for_region(engine, sido, district, limit=1000, origin=None, nearby_only=False):
    params = {"sido": sido, "district": district, "limit": max(limit, 2000) if origin and nearby_only else limit}
    where = "source_table = 'processed.facility'"
    distance = "NULL::double precision"
    if not (origin and nearby_only):
        where += " AND trim(city) = trim(:sido) AND trim(district) = trim(:district)"
    if origin:
        params.update(lat=origin[0], lon=origin[1])
        where += " AND latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180"
        distance = "ST_Distance(ST_SetSRID(ST_MakePoint(:lon, :lat),4326)::geography, ST_SetSRID(ST_MakePoint(longitude,latitude),4326)::geography)/1000.0"
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT *, {distance} AS db_distance_km FROM {TABLE} WHERE {where} "
                                 "ORDER BY db_distance_km NULLS LAST, facility_name, source_key LIMIT :limit"), params).mappings().all()
    return [dict(r, source=f"DB:{TABLE}", homepage="", hours="", operation_notice="") for r in rows]


def make_recommendations(service, engine, *args, now=None, **kwargs):
    """Run the reference service's actual ranking with derived facility candidates.

    The caller passes frontend.recommendation_service. Isolated function globals
    avoid modifying reference files or the live module, even across requests.
    Existing environment lookup/scoring remains intact; verified derived exposure
    replaces name guesses when known. Snapshot features are
    explicitly timestamped; time/travel feasibility is recomputed for this request.
    """
    now = now or datetime.now(KST)
    namespace = dict(vars(service))
    for name, function in vars(service).items():
        if isinstance(function, FunctionType) and function.__globals__ is vars(service):
            clone = FunctionType(function.__code__, namespace, name, function.__defaults__, function.__closure__)
            clone.__kwdefaults__ = function.__kwdefaults__
            namespace[name] = clone
    candidates = []

    def load(*a, **kw):
        rows = facilities_for_region(engine, *a, **kw)
        candidates.extend(rows)
        return rows

    namespace["facilities_for_region"] = load
    original_outdoor = namespace["_is_outdoor"]

    def is_outdoor(item):
        exposure = (item.get("features") or {}).get("facility_exposure")
        if exposure in {"INDOOR", "OUTDOOR"}:
            return exposure == "OUTDOOR"
        return original_outdoor(item)

    namespace["_is_outdoor"] = is_outdoor
    result = namespace["make_recommendations"](*args, **kwargs)
    def identity(row, recommendation=False):
        return (row.get("name" if recommendation else "facility_name"), row.get("facility_type"),
                row.get("address"), row.get("province" if recommendation else "city"),
                row.get("district"), row.get("latitude"), row.get("longitude"))
    lookup = {}
    for candidate in candidates:
        lookup.setdefault(identity(candidate), []).append(candidate)
    for recommendation in result["recommendations"]:
        matches = lookup.get(identity(recommendation, True), [])
        if recommendation.get("source") != f"DB:{TABLE}" or len(matches) != 1:
            continue
        candidate = matches[0]
        recommendation["derived_source_key"] = candidate["source_key"]
        recommendation["derived_snapshot"] = candidate["features"]
        recommendation["derived_evaluated_at"] = candidate["evaluated_at"]
        recommendation["request_features"] = derive_recommendation(
            recommendation, now=now, verified_facility_row=candidate["source_row"])
    return result


if __name__ == "__main__":
    from pipeline_elt import create_db_engine
    engine = create_db_engine()
    try:
        print(_json(refresh(engine)))
    finally:
        engine.dispose()
