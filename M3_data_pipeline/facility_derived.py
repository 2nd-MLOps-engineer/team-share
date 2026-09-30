"""정제된 시설 데이터를 기반으로 운영시간 등 추천에 사용할 시설 이용 가능성 정보를 생성합니다."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import math
import re
from collections.abc import Callable, Mapping


KST = timezone(timedelta(hours=9))
SOURCE_TABLE = "processed.public_open_facility"
EARTH_RADIUS_KM = 6371
REFERENCE_MINUTES_PER_KM = 20
SOURCE_COLUMNS = (
    "openFcltyNm", "openLcNm", "openFcltyType", "insttCode",
    "latitude", "longitude", "rstde", "weekdayOperOpenHhmm",
    "weekdayOperColseHhmm", "wkendOperOpenHhmm", "wkendOperCloseHhmm",
)


@dataclass(frozen=True)
class DerivedConfig:
    minimum_usable_minutes: float = 30
    closing_soon_minutes: float = 60

    def __post_init__(self):
        if not (0 < self.minimum_usable_minutes <= self.closing_soon_minutes
                and math.isfinite(self.closing_soon_minutes)):
            raise ValueError("Require 0 < minimum_usable_minutes <= closing_soon_minutes")


def _text(value) -> str:
    if value is None:
        return ""
    value = str(value).strip()
    return "" if value.lower() in {"", "none", "null", "nan", "<na>", "nat", "-"} else value


def _clock(value) -> int | None:
    value = _text(value)
    if not re.fullmatch(r"\d{2}:?\d{2}", value):
        return None
    digits = value.replace(":", "")
    hour, minute = int(digits[:2]), int(digits[2:])
    if hour == 24 and minute == 0:
        return 1440
    return hour * 60 + minute if hour < 24 and minute < 60 else None


def _holiday(value, day: date, holiday_calendar) -> bool | None:
    """Only fully understood rules imply a known working day.

    A calendar callback, when supplied, must answer for each requested date.
    Lunar/ordinal/free-text closures remain unknown in this MVP.
    """
    value = re.sub(r"\s+", "", _text(value))
    if value == "연중무휴":
        return False
    if not value:
        return None
    outcomes = []
    for token in re.split(r"[+,/·]|및", value):
        token = token.removesuffix("요일")
        if token in tuple("월화수목금토일"):
            outcomes.append(day.weekday() == "월화수목금토일".index(token))
        elif token == "주말":
            outcomes.append(day.weekday() >= 5)
        elif token in {"공휴일", "법정공휴일", "법정휴일", "법적공휴일"}:
            outcomes.append(holiday_calendar(day) if holiday_calendar else None)
        else:
            outcomes.append(None)
    if any(result is True for result in outcomes):
        return True
    return None if any(result is None for result in outcomes) else False


def _interval(row, day, holiday_calendar):
    closed = _holiday(row.get("rstde"), day, holiday_calendar)
    if closed is True:
        return "closed", None
    keys = (("weekdayOperOpenHhmm", "weekdayOperColseHhmm")
            if day.weekday() < 5 else ("wkendOperOpenHhmm", "wkendOperCloseHhmm"))
    start, end = (_clock(row.get(key)) for key in keys)
    if closed is None or start is None or end is None or start == end or start == 1440:
        return "unknown", None
    midnight = datetime.combine(day, time(), KST)
    if end < start:
        end += 1440
    return "known", (midnight + timedelta(minutes=start), midnight + timedelta(minutes=end))


def derive_facility(row: Mapping, *, now: datetime,
                    estimated_travel_minutes=None,
                    config: DerivedConfig = DerivedConfig(),
                    holiday_calendar: Callable[[date], bool | None] | None = None) -> dict:
    """Return the seven derived fields plus provenance/availability.

    CLOSED means no current operating interval; pre-opening is CLOSED too.
    TOO_LATE means arrival leaves less than the configured minimum duration.
    Urgency uses remaining usable time after arrival; missing travel => UNKNOWN.
    Equal start/end times are ambiguous, never assumed to mean 24-hour access.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(KST)
    try:
        travel = float(estimated_travel_minutes)
        if isinstance(estimated_travel_minutes, bool) or not math.isfinite(travel) or travel < 0:
            travel = None
    except (TypeError, ValueError):
        travel = None
    try:
        arrival = now + timedelta(minutes=travel) if travel is not None else None
    except OverflowError:
        travel, arrival = None, None

    today_status, today = _interval(row, now.date(), holiday_calendar)
    previous_status, previous = _interval(row, now.date() - timedelta(days=1), holiday_calendar)
    active = next((interval for interval in (previous, today)
                   if interval and interval[0] <= now < interval[1]), None)
    # A confirmed rest day takes precedence over a previous overnight session.
    if today_status == "closed":
        active = None
        is_open = False
    elif today_status == "unknown":
        active = None
        is_open = None
    elif active:
        is_open = True
    elif previous_status == "unknown" and today and now < today[0]:
        is_open = None
    else:
        is_open = False

    closes_at = active[1] if active else None
    # Do not carry an overnight interval through a known/unknown next-day closure.
    if closes_at and closes_at.date() > now.date() and closes_at.time() != time():
        next_closed = _holiday(row.get("rstde"), closes_at.date(), holiday_calendar)
        if next_closed is True:
            closes_at = datetime.combine(closes_at.date(), time(), KST)
        elif next_closed is None:
            closes_at = None
    remaining = (closes_at - now).total_seconds() / 60 if closes_at else None
    usable = max(0.0, (closes_at - arrival).total_seconds() / 60) if closes_at and arrival else None
    if is_open is False:
        feasibility = urgency = "CLOSED"
    elif usable is None:
        feasibility = urgency = "UNKNOWN"
    elif usable < config.minimum_usable_minutes:
        feasibility = urgency = "TOO_LATE"
    else:
        feasibility = "AVAILABLE"
        urgency = "CLOSING_SOON" if usable <= config.closing_soon_minutes else "AVAILABLE"
    return {
        "is_open_now": is_open,
        "minutes_until_close": remaining,
        "estimated_travel_minutes": travel,
        "estimated_arrival_time": arrival,
        "usable_minutes_after_arrival": usable,
        "feasibility_state": feasibility,
        "urgency_state": urgency,
        "evaluated_at": now,
        "closing_time": closes_at,
        "information_availability": {
            "schedule": "KNOWN" if is_open is not None and (not is_open or closes_at) else "UNKNOWN",
            "travel": "KNOWN" if travel is not None else "UNKNOWN",
        },
    }


def _coordinate(value, bound):
    try:
        if isinstance(value, bool):
            return None
        number = float(value)
        return number if math.isfinite(number) and abs(number) <= bound else None
    except (TypeError, ValueError):
        return None


def derive_from_test_origin(row: Mapping, *, origin: tuple[float, float],
                            now: datetime, config: DerivedConfig = DerivedConfig(),
                            holiday_calendar=None) -> dict:
    """Explicit test-only estimate, following the read-only web reference.

    Reference: frontend/recommendation_service.py at
    0ccecbaf2d4241c0b892a991731fa4de827c68a4: _distance_km and
    make_recommendations. Haversine distance, then round(distance * 20).
    This standalone path avoids importing Django or executing the web service.
    No route, traffic, waiting, reservation or live user location is inferred.
    Invalid origin is an input error; missing facility coordinates stay UNKNOWN.
    """
    if not isinstance(origin, (tuple, list)) or len(origin) != 2:
        raise ValueError("origin must be an explicit (latitude, longitude) pair")
    origin_lat, origin_lon = _coordinate(origin[0], 90), _coordinate(origin[1], 180)
    if origin_lat is None or origin_lon is None:
        raise ValueError("origin must contain finite valid latitude/longitude")
    lat, lon = _coordinate(row.get("latitude"), 90), _coordinate(row.get("longitude"), 180)
    distance = None
    if lat is not None and lon is not None:
        p1, p2 = math.radians(origin_lat), math.radians(lat)
        dp, dl = math.radians(lat - origin_lat), math.radians(lon - origin_lon)
        value = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        value = max(0.0, min(1.0, value))
        distance = EARTH_RADIUS_KM * 2 * math.atan2(math.sqrt(value), math.sqrt(1 - value))
    travel = round(distance * REFERENCE_MINUTES_PER_KM) if distance is not None else None
    return {
        **derive_facility(row, now=now, estimated_travel_minutes=travel,
                          config=config, holiday_calendar=holiday_calendar),
        "distance_km": distance,
        "travel_assumptions": {
            "source": "EXPLICIT_TEST_ORIGIN",
            "origin": (origin_lat, origin_lon),
            "distance_method": "HAVERSINE_STRAIGHT_LINE",
            "earth_radius_km": EARTH_RADIUS_KM,
            "mode": "ASSUMED_WALKING",
            "minutes_per_km": REFERENCE_MINUTES_PER_KM,
            "speed_kmh": 60 / REFERENCE_MINUTES_PER_KM,
            "rounding": "Python round(distance_km * minutes_per_km)",
            "routing_or_traffic_included": False,
        },
    }


def derive_recommendation(recommendation: Mapping, *, now: datetime,
                          verified_facility_row: Mapping | None = None,
                          config: DerivedConfig = DerivedConfig(),
                          holiday_calendar=None) -> dict:
    """Reuse frontend.recommendation_service.make_recommendations travel_time.

    Verified against woosimwoonkka-web commit
    0ccecbaf2d4241c0b892a991731fa4de827c68a4. The service already computes
    distance and travel_time; do not recompute from rounded distance_km.
    Its id is a result ordinal, not a facility key. The caller must supply a
    separately verified same-facility processed row to use schedule data.
    Without that linkage the schedule stays UNKNOWN. '확인 필요' travel_time
    is also UNKNOWN. Return only derived fields, preserving score and ranking.
    """
    return derive_facility(
        verified_facility_row if verified_facility_row is not None else {},
        now=now, estimated_travel_minutes=recommendation.get("travel_time"),
        config=config, holiday_calendar=holiday_calendar,
    )


def derive_processed_facilities(engine, *, now: datetime,
                                travel_minutes_for: Callable[[Mapping], float | None] | None = None,
                                test_origin: tuple[float, float] | None = None,
                                config: DerivedConfig = DerivedConfig(),
                                holiday_calendar=None) -> list[dict]:
    """Read actual processed rows without persisting time/user-dependent results.

    Rows retain source columns; insttCode is NOT treated as a facility ID.
    The optional resolver reuses the caller's recommendation/routing result.
    Alternatively test_origin explicitly opts into the reference estimate.
    No name-based joining is performed.
    """
    from sqlalchemy import text

    if test_origin is not None and travel_minutes_for is not None:
        raise ValueError("Choose either test_origin or travel_minutes_for")
    if test_origin is not None:
        derive_from_test_origin({}, origin=test_origin, now=now, config=config)
    columns = ", ".join(f'"{name}"' for name in SOURCE_COLUMNS)
    with engine.connect() as connection:
        rows = connection.execute(text(f"SELECT {columns} FROM {SOURCE_TABLE}")).mappings().all()
    if test_origin is not None:
        return [dict(row, **derive_from_test_origin(
            row, origin=test_origin, now=now, config=config,
            holiday_calendar=holiday_calendar,
        )) for row in rows]
    return [dict(row, **derive_facility(
        row, now=now,
        estimated_travel_minutes=travel_minutes_for(row) if travel_minutes_for else None,
        config=config, holiday_calendar=holiday_calendar,
    )) for row in rows]


def derive_processed_exercise_features(engine, *, now: datetime,
                                      travel_minutes_for=None, weather_grid_for=None,
                                      air_station_for=None, exposure_for=None,
                                      config: DerivedConfig = DerivedConfig(),
                                      environment_config=None, holiday_calendar=None) -> list[dict]:
    """Read processed-only features, preserving every facility and its source row.

    Weather grid defaults to the source coordinates; an optional resolver is
    cross-checked against them. Other callbacks supply verified links. Missing
    environment data does not filter facilities. Existing CLI stays unchanged.
    """
    from sqlalchemy import text
    if __package__:
        from .environment_features import EnvironmentConfig, derive_exercise_features, read_processed_environment
    else:
        from environment_features import EnvironmentConfig, derive_exercise_features, read_processed_environment
    environment_config = environment_config or EnvironmentConfig()
    environment = read_processed_environment(engine, now=now, config=environment_config)
    with engine.connect() as connection:
        rows = connection.execute(text(f"SELECT * FROM {SOURCE_TABLE}")).mappings().all()
    return [dict(row, **derive_exercise_features(
        row, now=now,
        estimated_travel_minutes=travel_minutes_for(row) if travel_minutes_for else None,
        verified_weather_grid=weather_grid_for(row) if weather_grid_for else None,
        verified_air_station=air_station_for(row) if air_station_for else None,
        verified_exposure=exposure_for(row) if exposure_for else None,
        facility_config=config, environment_config=environment_config,
        holiday_calendar=holiday_calendar, **environment,
    )) for row in rows]


if __name__ == "__main__":
    import argparse
    import json
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-origin", nargs=2, type=float, metavar=("LAT", "LON"),
                        help="Explicit synthetic origin; Haversine km * 20 minutes/km")
    args = parser.parse_args()
    if args.test_origin is not None:
        derive_from_test_origin({}, origin=args.test_origin, now=datetime.now(KST))
    if __package__:
        from .pipeline_elt import create_db_engine
    else:
        from pipeline_elt import create_db_engine
    engine = create_db_engine()
    try:
        print(json.dumps(derive_processed_facilities(engine, now=datetime.now(KST),
                                                     test_origin=args.test_origin),
                         ensure_ascii=False, default=str))
    finally:
        engine.dispose()
