"""추천에 사용할 날씨·대기질 환경 정보를 정제 데이터에서 조회합니다.

시설 위치를 기준으로 날씨 정보를 조회하고, 대기질은 지역 단위 정보를 활용합니다.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
import math
import re
from collections.abc import Mapping, Sequence

if __package__:
    from .facility_derived import KST, DerivedConfig, derive_facility
    from .kma_grid import facility_weather_link
else:
    from facility_derived import KST, DerivedConfig, derive_facility
    from kma_grid import facility_weather_link


@dataclass(frozen=True)
class EnvironmentConfig:
    observation_max_age_minutes: float = 90
    forecast_max_issue_age_minutes: float = 180
    forecast_target_tolerance_minutes: float = 60
    air_max_age_minutes: float = 120
    temperature_min_c: float = 5
    temperature_max_c: float = 30
    pm10_caution_at: float = 81
    pm25_caution_at: float = 36

    def __post_init__(self):
        if any(not math.isfinite(v) for v in vars(self).values()):
            raise ValueError("Environment settings must be finite")
        if any(v <= 0 for k, v in vars(self).items() if k not in {"temperature_min_c", "temperature_max_c"}):
            raise ValueError("Age limits, tolerances and PM thresholds must be positive")
        if self.temperature_min_c >= self.temperature_max_c:
            raise ValueError("Invalid temperature interval")


REGIONS = {
    "서울": "서울특별시", "부산": "부산광역시", "대구": "대구광역시",
    "인천": "인천광역시", "광주": "광주광역시", "대전": "대전광역시",
    "울산": "울산광역시", "세종": "세종특별자치시", "경기": "경기도",
    "강원": "강원특별자치도", "강원도": "강원특별자치도",
    "충북": "충청북도", "충남": "충청남도", "전북": "전북특별자치도",
    "전라북도": "전북특별자치도", "전남": "전라남도", "경북": "경상북도",
    "경남": "경상남도", "제주": "제주특별자치도", "제주도": "제주특별자치도",
}


def _text(value):
    value = str(value).strip() if value is not None else ""
    return "" if value.lower() in {"none", "nan", "nat", "<na>", "null", "-"} else value


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError):
        return None


def _timestamp(value):
    value = _text(value)
    if not value:
        return None
    try:
        result = datetime.fromisoformat(value)
        return result.replace(tzinfo=KST) if result.tzinfo is None else result.astimezone(KST)
    except ValueError:
        return None


def _weather_time(row, prefix):
    day = _text(row.get(prefix + "Date"))
    clock = _text(row.get(prefix + "Time"))
    if not re.fullmatch(r"\d{4}", clock):
        return None
    try:
        parsed = datetime.strptime(day, "%Y%m%d") if re.fullmatch(r"\d{8}", day) else datetime.fromisoformat(day)
        return datetime(parsed.year, parsed.month, parsed.day, int(clock[:2]), int(clock[2:]), tzinfo=KST)
    except ValueError:
        return None


def _minutes(later, earlier):
    return (later - earlier).total_seconds() / 60


def _region(value):
    value = _text(value)
    canonical = REGIONS.get(value, value)
    return canonical if canonical in REGIONS.values() else None


def _combine(states):
    if "CAUTION" in states:
        return "CAUTION"
    return "FAVORABLE" if states and all(s == "FAVORABLE" for s in states) else "UNKNOWN"


def weather_features(observations, forecasts, *, grid, now, target, config):
    if grid is None:
        return {"suitability": "UNKNOWN", "availability": "UNLINKED", "grid": None, "components": {}}
    if (not isinstance(grid, (tuple, list)) or len(grid) != 2
            or any(isinstance(v, bool) or not isinstance(v, int) or v <= 0 for v in grid)):
        raise ValueError("A verified weather grid must be a positive integer (nx, ny)")
    chosen, rejected = {}, {}
    for category in ("T1H", "PTY", "RN1"):
        candidates = []
        reasons = []
        for kind, rows in (("OBSERVATION", observations), ("FORECAST", forecasts)):
            for row in rows:
                if row.get("category") != category or (_number(row.get("nx")), _number(row.get("ny"))) != tuple(grid):
                    continue
                issued = _weather_time(row, "base")
                valid = _weather_time(row, "fcst") if kind == "FORECAST" else issued
                collected = _timestamp(row.get("collected_at"))
                if not issued or not valid or not collected:
                    reasons.append("INVALID_TIME")
                    continue
                if issued > now or collected > now or (kind == "OBSERVATION" and valid > now):
                    reasons.append("FUTURE_DATA")
                    continue
                age_limit = config.forecast_max_issue_age_minutes if kind == "FORECAST" else config.observation_max_age_minutes
                if _minutes(now, issued) > age_limit:
                    reasons.append("STALE")
                    continue
                if kind == "FORECAST":
                    aligned = valid >= issued and abs(_minutes(target, valid)) <= config.forecast_target_tolerance_minutes
                else:
                    aligned = 0 <= _minutes(target, valid) <= config.observation_max_age_minutes
                if not aligned:
                    reasons.append("TIME_MISMATCH")
                    continue
                # Prefer closest valid time, then most recent issue/revision.
                # At an equal valid time, current requests prefer observations.
                preferred = "FORECAST" if target > now else "OBSERVATION"
                rank = (abs(_minutes(target, valid)), kind != preferred, -issued.timestamp(), -collected.timestamp())
                candidates.append((rank, {
                    "value": row.get("fcstValue" if kind == "FORECAST" else "obsrValue"),
                    "source_table": "processed.weather_ultra_fcst" if kind == "FORECAST" else "processed.weather_ultra_ncst",
                    "issued_at": issued, "valid_at": valid, "collected_at": collected,
                    "age_minutes": _minutes(now, issued), "target_offset_minutes": _minutes(target, valid),
                }))
        if candidates:
            chosen[category] = min(candidates, key=lambda c: c[0])[1]
        rejected[category] = sorted(set(reasons)) or (["MISSING"] if category not in chosen else [])
    temperature = _number(chosen.get("T1H", {}).get("value"))
    temperature_state = "UNKNOWN" if temperature is None else (
        "CAUTION" if temperature < config.temperature_min_c or temperature > config.temperature_max_c else "FAVORABLE")
    pty = _number(chosen.get("PTY", {}).get("value"))
    rain_value = chosen.get("RN1", {}).get("value")
    rain = 0.0 if _text(rain_value) == "강수없음" else _number(rain_value)
    precipitation = []
    if pty is not None and pty in (0, 1, 2, 3, 5, 6, 7):
        precipitation.append(pty != 0)
    if rain is not None and rain >= 0:
        precipitation.append(rain > 0)
    wet = any(precipitation) if precipitation else None
    precip_state = "UNKNOWN" if wet is None else "CAUTION" if wet else "FAVORABLE"
    known = temperature is not None and wet is not None
    unavailable = {r for values in rejected.values() for r in values}
    availability = "AVAILABLE" if known else "PARTIAL" if chosen else "STALE" if "STALE" in unavailable else "TIME_MISMATCH" if "TIME_MISMATCH" in unavailable else "MISSING"
    return {
        "suitability": _combine([temperature_state, precip_state]), "availability": availability,
        "grid": tuple(grid), "target_time": target, "temperature_c": temperature,
        "temperature_state": temperature_state, "precipitation_present": wet,
        "precipitation_state": precip_state, "rain_amount_mm": rain,
        "components": chosen, "unavailable_reasons": rejected,
        "policy": {"temperature_min_c": config.temperature_min_c, "temperature_max_c": config.temperature_max_c},
    }


def air_features(rows, *, region, station, now, target, config):
    region = _region(region)
    scope = "VERIFIED_STATION" if station else "REGIONAL_PROXY"
    provenance = {
        "source_table": "processed.air_quality",
        "spatial_basis": "CALLER_VERIFIED_STATION_IN_SAME_SIDO" if station else "SAME_SIDO_REGIONAL_REFERENCE",
        "requested_station": station,
        "is_facility_measurement": False,
        "is_nearest_station": False,
        "is_regional_reference": not bool(station),
        "aggregation": "LATEST_AT_VERIFIED_STATION" if station else "MAX_OF_LATEST_PER_STATION",
    }
    if not region:
        return {"suitability": "UNKNOWN", "availability": "UNLINKED", "scope": scope,
                "provenance": provenance, "stations": []}
    latest, rejected = {}, set()
    for row in rows:
        name = _text(row.get("stationName"))
        if _region(row.get("sidoName")) != region or not name or (station and name != station):
            continue
        measured, collected = _timestamp(row.get("dataTime")), _timestamp(row.get("collected_at"))
        # An undated capture is not a pollution measurement, regardless of its
        # newly generated observation_time_key or recent collection time.
        if not measured or not collected:
            rejected.add("UNKNOWN_MEASUREMENT_TIME")
            continue
        if measured > now or collected > now:
            rejected.add("FUTURE_DATA")
            continue
        rank = (measured, collected)
        if name not in latest or rank > latest[name][0]:
            latest[name] = (rank, row)
    stations = []
    for name, ((measured, collected), row) in sorted(latest.items()):
        age = _minutes(target, measured)
        if age < 0 or age > config.air_max_age_minutes:
            rejected.add("STALE")
            continue
        pm10, pm25 = _number(row.get("pm10Value")), _number(row.get("pm25Value"))
        pm10 = pm10 if pm10 is not None and pm10 >= 0 else None
        pm25 = pm25 if pm25 is not None and pm25 >= 0 else None
        stations.append({"station_name": name, "measured_at": measured, "collected_at": collected,
                         "age_at_target_minutes": age, "pm10": pm10, "pm25": pm25})
    # A provincial maximum is an explicit regional caution indicator. It does
    # not claim these readings occurred at the facility or predict arrival air.
    def maximum(column):
        values = [s[column] for s in stations if s[column] is not None]
        return max(values) if values else None
    pm10, pm25 = maximum("pm10"), maximum("pm25")
    complete = bool(stations) and all(s["pm10"] is not None and s["pm25"] is not None for s in stations) and not rejected
    caution = ((pm10 is not None and pm10 >= config.pm10_caution_at)
               or (pm25 is not None and pm25 >= config.pm25_caution_at))
    return {
        "suitability": "CAUTION" if caution else "FAVORABLE" if complete else "UNKNOWN",
        "availability": "AVAILABLE" if complete else "PARTIAL" if stations else "STALE" if "STALE" in rejected else "MISSING",
        "scope": scope, "region": region, "target_time": target, "pm10": pm10, "pm25": pm25,
        "aggregation": provenance["aggregation"], "provenance": provenance, "stations": stations,
        "unavailable_reasons": sorted(rejected), "is_arrival_forecast": False,
        "policy": {"pm10_caution_at": config.pm10_caution_at, "pm25_caution_at": config.pm25_caution_at},
    }


def derive_exercise_features(facility: Mapping, *, now: datetime, estimated_travel_minutes=None,
                             weather_observations: Sequence[Mapping] = (), weather_forecasts: Sequence[Mapping] = (),
                             air_measurements: Sequence[Mapping] = (), verified_weather_grid=None,
                             verified_air_station=None, verified_exposure=None,
                             facility_config=DerivedConfig(), environment_config=EnvironmentConfig(),
                             holiday_calendar=None) -> dict:
    """Rows originate in processed tables; facility coordinates determine grid.

    Exposure, if provided, must be an independently verified mapping with
    state INDOOR/OUTDOOR and nonempty evidence. Names/types/web `indoor` flags
    alone are not accepted as a reliable source.
    """
    base = derive_facility(facility, now=now, estimated_travel_minutes=estimated_travel_minutes,
                           config=facility_config, holiday_calendar=holiday_calendar)
    now = base["evaluated_at"]
    target = base["estimated_arrival_time"] or now
    exposure, evidence = "UNKNOWN", None
    if verified_exposure is not None:
        if (not isinstance(verified_exposure, Mapping) or verified_exposure.get("state") not in {"INDOOR", "OUTDOOR"}
                or not _text(verified_exposure.get("evidence"))):
            raise ValueError("Exposure requires a verified state and evidence")
        exposure, evidence = verified_exposure["state"], verified_exposure["evidence"]
    weather_link = facility_weather_link(facility, verified_weather_grid)
    weather = weather_features(weather_observations, weather_forecasts, grid=weather_link["grid"],
                               now=now, target=target, config=environment_config)
    air = air_features(air_measurements, region=facility.get("sido_standard"), station=verified_air_station,
                       now=now, target=target, config=environment_config)
    outdoor = "NOT_APPLICABLE" if exposure == "INDOOR" else (
        _combine([weather["suitability"], air["suitability"]]) if exposure == "OUTDOOR" else "UNKNOWN")
    return {**base, "environment_target_time": target,
            "environment_target_basis": "ARRIVAL" if base["estimated_arrival_time"] else "NOW_NO_TRAVEL_ESTIMATE",
            "facility_exposure": exposure, "facility_exposure_evidence": evidence,
            "weather_suitability": weather["suitability"], "air_quality_suitability": air["suitability"],
            "outdoor_suitability": outdoor, "weather_link": weather_link,
            "weather_features": weather, "air_quality_features": air,
            "environment_information_availability": {"weather": weather["availability"], "air_quality": air["availability"]},
            "information_availability": {**base["information_availability"], "weather": weather["availability"], "air_quality": air["availability"]}}


def derive_recommendation_features(recommendation, *, verified_facility_row=None, now, **kwargs):
    """Attach the returned dict downstream of travel_time calculation; no score changes."""
    return derive_exercise_features(verified_facility_row if verified_facility_row is not None else {},
                                    now=now, estimated_travel_minutes=recommendation.get("travel_time"), **kwargs)


def read_processed_environment(engine, *, now, config=EnvironmentConfig()):
    """One request-scoped processed read; no API fallback or persistence.

    Optional all-NULL columns can be absent after processing. Whole records
    retain their actual schema and missing values are handled by .get().
    Call once per request, then reuse rows for all candidate facilities.
    """
    from sqlalchemy import text
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(KST)
    since = now - timedelta(minutes=max(config.observation_max_age_minutes,
                                       config.forecast_max_issue_age_minutes, config.air_max_age_minutes))
    result = {}
    with engine.connect() as connection:
        for table in ("weather_ultra_ncst", "weather_ultra_fcst", "air_quality"):
            exists = connection.execute(text("SELECT to_regclass(:table)"), {"table": f"processed.{table}"}).scalar_one()
            result[table] = ([] if exists is None else [dict(r) for r in connection.execute(
                # JSON lookup permits a missing all-NULL collected_at column.
                # Processors store local KST timestamps; do not use DB timezone.
                text(f'SELECT t.* FROM "processed"."{table}" AS t '
                     "WHERE (to_jsonb(t)->>'collected_at')::timestamp BETWEEN :since AND :now"),
                {"since": since.replace(tzinfo=None), "now": now.replace(tzinfo=None)},
            ).mappings().all()])
    return {"weather_observations": result["weather_ultra_ncst"],
            "weather_forecasts": result["weather_ultra_fcst"], "air_measurements": result["air_quality"]}
