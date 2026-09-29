"""Schedule arithmetic and missing-information regression tests."""
from datetime import datetime, timezone
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from facility_derived import (
    DerivedConfig, KST, SOURCE_COLUMNS, derive_facility, derive_processed_facilities,
    derive_recommendation,
    derive_from_test_origin,
)


def row(**changes):
    # Actual published schedule observed in processed.public_open_facility.
    result = {
        "openFcltyNm": "용유동행정복지센터 남북동 다목적 경기장",
        "rstde": "연중무휴",
        "weekdayOperOpenHhmm": "06:00", "weekdayOperColseHhmm": "23:59",
        "wkendOperOpenHhmm": "06:00", "wkendOperCloseHhmm": "23:59",
    }
    return dict(result, **changes)


def at(value):
    return datetime.fromisoformat(value).replace(tzinfo=KST)


@pytest.mark.parametrize("clock,travel,usable,feasibility,urgency", [
    ("20:00", 20, 219, "AVAILABLE", "AVAILABLE"),
    ("23:00", 20, 39, "AVAILABLE", "CLOSING_SOON"),
    ("23:30", 20, 9, "TOO_LATE", "TOO_LATE"),
    ("23:40", 19, 0, "TOO_LATE", "TOO_LATE"),
    ("23:40", 30, 0, "TOO_LATE", "TOO_LATE"),
    ("23:09", 20, 30, "AVAILABLE", "CLOSING_SOON"),
    ("22:39", 20, 60, "AVAILABLE", "CLOSING_SOON"),
])
def test_published_schedule_with_explicit_scenario_travel(clock, travel, usable, feasibility, urgency):
    now = at("2026-09-29T" + clock)
    result = derive_facility(row(), now=now, estimated_travel_minutes=travel)
    assert result["is_open_now"] is True
    assert result["usable_minutes_after_arrival"] == usable
    assert result["minutes_until_close"] == (result["closing_time"] - now).total_seconds() / 60
    assert (result["estimated_arrival_time"] - now).total_seconds() / 60 == travel
    assert result["feasibility_state"] == feasibility
    assert result["urgency_state"] == urgency


@pytest.mark.parametrize("travel", [None, float("nan"), float("inf"), -1, True, "invalid"])
def test_missing_or_invalid_travel_is_not_friction(travel):
    result = derive_facility(row(), now=at("2026-09-29T20:00"), estimated_travel_minutes=travel)
    assert result["is_open_now"] is True
    assert result["minutes_until_close"] == 239
    assert result["estimated_arrival_time"] is None
    assert result["usable_minutes_after_arrival"] is None
    assert result["feasibility_state"] == result["urgency_state"] == "UNKNOWN"


@pytest.mark.parametrize("changes", [
    {"rstde": None}, {"rstde": "매월 둘째 화요일"},
    {"rstde": "설 연휴+추석 연휴"}, {"weekdayOperOpenHhmm": None},
    {"weekdayOperColseHhmm": None}, {"weekdayOperColseHhmm": "25:00"},
    {"weekdayOperOpenHhmm": "00:00", "weekdayOperColseHhmm": "00:00"},
])
def test_unknown_schedule_preserves_known_arrival(changes):
    result = derive_facility(row(**changes), now=at("2026-09-29T20:00"), estimated_travel_minutes=15)
    assert result["is_open_now"] is None
    assert result["estimated_arrival_time"] == at("2026-09-29T20:15")
    assert result["feasibility_state"] == "UNKNOWN"
    assert result["usable_minutes_after_arrival"] is None


def test_rest_day_and_holiday_calendar():
    source = row(rstde="토+일+공휴일")
    assert derive_facility(source, now=at("2026-10-03T12:00"))["feasibility_state"] == "CLOSED"
    now = at("2026-09-29T12:00")
    assert derive_facility(source, now=now)["is_open_now"] is None
    assert derive_facility(source, now=now, holiday_calendar=lambda day: False)["is_open_now"] is True
    assert derive_facility(source, now=now, holiday_calendar=lambda day: True)["is_open_now"] is False


@pytest.mark.parametrize("clock,opened", [("05:59", False), ("06:00", True), ("23:59", False)])
def test_operating_boundaries(clock, opened):
    assert derive_facility(row(), now=at("2026-09-29T" + clock))["is_open_now"] is opened


def test_weekend_and_previous_weekday_overnight():
    source = row(weekdayOperOpenHhmm="22:00", weekdayOperColseHhmm="02:00",
                 wkendOperOpenHhmm="10:00", wkendOperCloseHhmm="18:00")
    result = derive_facility(source, now=at("2026-10-03T01:00"), estimated_travel_minutes=10)
    assert result["closing_time"] == at("2026-10-03T02:00")
    assert result["usable_minutes_after_arrival"] == 50
    result = derive_facility(source, now=at("2026-10-03T17:00"), estimated_travel_minutes=10)
    assert result["usable_minutes_after_arrival"] == 50


def test_overnight_stops_at_known_next_rest_day():
    source = row(rstde="수", weekdayOperOpenHhmm="22:00", weekdayOperColseHhmm="02:00")
    result = derive_facility(source, now=at("2026-09-29T23:30"), estimated_travel_minutes=20)
    assert result["closing_time"] == at("2026-09-30T00:00")
    assert result["usable_minutes_after_arrival"] == 10


def test_timezone_config_zero_travel_and_no_mutation():
    source = row()
    original = source.copy()
    result = derive_facility(source, now=at("2026-09-29T23:30").astimezone(timezone.utc),
                             estimated_travel_minutes=0,
                             config=DerivedConfig(minimum_usable_minutes=10, closing_soon_minutes=20))
    assert result["feasibility_state"] == "AVAILABLE"
    assert result["urgency_state"] == "AVAILABLE"
    assert source == original
    with pytest.raises(ValueError):
        derive_facility(source, now=datetime(2026, 9, 29))
    with pytest.raises(ValueError):
        DerivedConfig(minimum_usable_minutes=60, closing_soon_minutes=30)


@pytest.mark.parametrize("use_origin", [False, True])
def test_processed_reader_reuses_resolver_and_preserves_duplicate_names(use_origin):
    from sqlalchemy import create_engine
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.exec_driver_sql("ATTACH DATABASE ':memory:' AS processed")
        columns = ", ".join(f'"{name}" TEXT' for name in SOURCE_COLUMNS)
        connection.exec_driver_sql(f"CREATE TABLE processed.public_open_facility ({columns})")
        for name in ("same", "same"):
            source = row(openFcltyNm=name, latitude=0.01, longitude=0)
            marks = ", ".join("?" for _ in SOURCE_COLUMNS)
            connection.exec_driver_sql(f"INSERT INTO processed.public_open_facility VALUES ({marks})",
                                       tuple(source.get(key) for key in SOURCE_COLUMNS))
    calls = []
    def resolver(source):
        calls.append(source["openFcltyNm"])
        return 20
    kwargs = {"test_origin": (0, 0)} if use_origin else {"travel_minutes_for": resolver}
    result = derive_processed_facilities(engine, now=at("2026-09-29T23:00"), **kwargs)
    assert len(result) == 2
    assert calls == ([] if use_origin else ["same", "same"])
    assert all(r["usable_minutes_after_arrival"] == (37 if use_origin else 39) for r in result)
    if use_origin:
        assert all(r["distance_km"] == pytest.approx(1.1119492664) for r in result)
        assert all(r["estimated_travel_minutes"] == 22 for r in result)
        assert all(r["estimated_arrival_time"] == at("2026-09-29T23:22") for r in result)
        assert all(r["feasibility_state"] == "AVAILABLE" for r in result)
        assert all(r["urgency_state"] == "CLOSING_SOON" for r in result)
    assert "facility_id" not in result[0]
    engine.dispose()


def test_web_recommendation_reuses_travel_time_without_recalculating_score():
    recommendation = {"id": 1, "travel_time": 20, "distance_km": 1.234, "score": 87}
    original = recommendation.copy()
    result = derive_recommendation(recommendation, now=at("2026-09-29T23:00"),
                                   verified_facility_row=row())
    assert result["estimated_travel_minutes"] == 20
    assert result["usable_minutes_after_arrival"] == 39
    assert "score" not in result
    assert recommendation == original


def test_web_result_ordinal_is_not_used_to_infer_schedule():
    result = derive_recommendation({"id": 1, "travel_time": 20}, now=at("2026-09-29T23:00"))
    assert result["estimated_arrival_time"] == at("2026-09-29T23:20")
    assert result["is_open_now"] is None
    assert result["feasibility_state"] == "UNKNOWN"


@pytest.mark.parametrize("recommendation", [{"travel_time": "확인 필요"}, {}, {"travel_time": None}])
def test_web_missing_travel_remains_unknown(recommendation):
    result = derive_recommendation(recommendation, now=at("2026-09-29T23:00"),
                                   verified_facility_row=row())
    assert result["estimated_travel_minutes"] is None
    assert result["is_open_now"] is True
    assert result["feasibility_state"] == "UNKNOWN"


@pytest.mark.parametrize("clock,usable,state,urgency", [
    ("20:00", 217, "AVAILABLE", "AVAILABLE"),
    ("23:00", 37, "AVAILABLE", "CLOSING_SOON"),
    ("23:30", 7, "TOO_LATE", "TOO_LATE"),
])
def test_origin_full_flow(clock, usable, state, urgency):
    source = row(latitude=0.01, longitude=0)
    result = derive_from_test_origin(source, origin=(0, 0), now=at("2026-09-29T" + clock))
    assert result["distance_km"] == pytest.approx(1.1119492664)
    assert result["estimated_travel_minutes"] == 22
    assert result["usable_minutes_after_arrival"] == usable
    assert result["feasibility_state"] == state
    assert result["urgency_state"] == urgency
    assert result["travel_assumptions"]["speed_kmh"] == 3


@pytest.mark.parametrize("origin", [None, (0,), (91, 0), (0, 181), (float("nan"), 0), (True, 0)])
def test_invalid_test_origin_rejected(origin):
    with pytest.raises(ValueError):
        derive_from_test_origin(row(), origin=origin, now=at("2026-09-29T12:00"))


@pytest.mark.parametrize("lat,lon", [(None, 127), (37, None), (float("inf"), 0), (91, 127)])
def test_invalid_facility_coordinate_does_not_create_friction(lat, lon):
    result = derive_from_test_origin(row(latitude=lat, longitude=lon), origin=(37, 127),
                                     now=at("2026-09-29T12:00"))
    assert result["distance_km"] is None
    assert result["estimated_travel_minutes"] is None
    assert result["feasibility_state"] == "UNKNOWN"


def test_same_origin_and_mutually_exclusive_travel_sources():
    result = derive_from_test_origin(row(latitude=37, longitude=127), origin=(37, 127),
                                     now=at("2026-09-29T12:00"))
    assert result["distance_km"] == result["estimated_travel_minutes"] == 0
    with pytest.raises(ValueError):
        derive_processed_facilities(None, now=at("2026-09-29T12:00"),
                                    test_origin=(37, 127), travel_minutes_for=lambda row: 20)
