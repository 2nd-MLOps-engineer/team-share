"""정제된 테스트 fixture와 mock SELECT를 사용하며, 실제 DB/API는 호출하지 않습니다."""

from datetime import datetime, timedelta
from pathlib import Path
import copy
import sys
from unittest.mock import MagicMock, Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from facility_derived import KST, derive_facility, derive_processed_exercise_features
from environment_features import EnvironmentConfig, derive_exercise_features, derive_recommendation_features, read_processed_environment


NOW = datetime(2026, 9, 29, 12, tzinfo=KST)


def facility(**updates):
    return dict({"openFcltyNm": "체육관", "openFcltyType": "체육시설", "insttCode": "A",
                 "sido_standard": "서울특별시", "latitude": 37.56, "longitude": 126.97,
                 "rstde": "연중무휴", "weekdayOperOpenHhmm": "06:00", "weekdayOperColseHhmm": "23:00",
                 "wkendOperOpenHhmm": "06:00", "wkendOperCloseHhmm": "23:00"}, **updates)


def weather(category="T1H", value=20, *, issue="1130", valid="1200", collected="2026-09-29 11:45:00", forecast=True):
    row = dict(nx=60, ny=127, baseDate=datetime(2026, 9, 29), baseTime=issue,
               category=category, collected_at=collected)
    if forecast:
        row.update(fcstDate=datetime(2026, 9, 29), fcstTime=valid, fcstValue=value)
    else:
        row.update(obsrValue=value)
    return row


def air(**updates):
    return dict({"sidoName": "서울", "stationName": "중구", "dataTime": "2026-09-29 11:00:00",
                 "collected_at": "2026-09-29 11:30:00", "pm10Value": 20, "pm25Value": 10}, **updates)


def derive(row=None, **updates):
    kwargs = dict(now=NOW, estimated_travel_minutes=20, verified_weather_grid=(60, 127),
                  weather_forecasts=[weather(), weather("PTY", 0)], air_measurements=[air()])
    kwargs.update(updates)
    return derive_exercise_features(facility() if row is None else row, **kwargs)


def test_healthy_environment_preserves_facility_features_and_input():
    row = facility()
    original = copy.deepcopy(row)
    result = derive(row)
    base = derive_facility(row, now=NOW, estimated_travel_minutes=20)
    for field in base:
        if field != "information_availability":
            assert result[field] == base[field]
    assert result["weather_suitability"] == result["air_quality_suitability"] == "FAVORABLE"
    assert result["environment_target_time"] == NOW + timedelta(minutes=20)
    assert result["air_quality_features"]["scope"] == "REGIONAL_PROXY"
    assert result["facility_exposure"] == result["outdoor_suitability"] == "UNKNOWN"
    assert row == original
    assert "score" not in result


@pytest.mark.parametrize("closing,feasibility,urgency", [
    ("11:00", "CLOSED", "CLOSED"), ("12:30", "TOO_LATE", "TOO_LATE"),
    ("13:00", "AVAILABLE", "CLOSING_SOON"),
])
def test_schedule_states_remain_independent(closing, feasibility, urgency):
    result = derive(facility(weekdayOperColseHhmm=closing))
    assert result["feasibility_state"] == feasibility
    assert result["urgency_state"] == urgency
    assert result["weather_suitability"] == "FAVORABLE"


@pytest.mark.parametrize("missing", ["weather", "air", "both"])
def test_missing_environment_does_not_disqualify_facility(missing):
    kwargs = {}
    if missing in {"weather", "both"}:
        kwargs["weather_forecasts"] = []
    if missing in {"air", "both"}:
        kwargs["air_measurements"] = []
    result = derive(**kwargs)
    assert result["feasibility_state"] == "AVAILABLE"
    if missing in {"weather", "both"}:
        assert result["weather_suitability"] == "UNKNOWN"
    if missing in {"air", "both"}:
        assert result["air_quality_suitability"] == "UNKNOWN"


def test_stale_times_not_refreshed_by_collected_at():
    result = derive(weather_forecasts=[weather(issue="0700"), weather("PTY", 0, issue="0700")],
                    air_measurements=[air(dataTime="2026-09-29 07:00:00")])
    assert result["environment_information_availability"] == {"weather": "STALE", "air_quality": "STALE"}
    assert result["weather_suitability"] == result["air_quality_suitability"] == "UNKNOWN"
    assert result["feasibility_state"] == "AVAILABLE"


def test_arrival_forecast_and_latest_issue_selected_without_future_leakage():
    rows = [weather(value=10, valid="1200"), weather(value=21, valid="1300"),
            weather(value=19, issue="1030", valid="1300"),
            weather(value=99, issue="1230", valid="1300", collected="2026-09-29 12:45:00"),
            weather("PTY", 0, valid="1300")]
    result = derive(estimated_travel_minutes=50, weather_forecasts=rows)
    assert result["weather_features"]["temperature_c"] == 21
    assert result["weather_features"]["components"]["T1H"]["valid_at"].hour == 13


def test_recent_observation_selected_for_now_and_forecast_outside_target_rejected():
    result = derive(estimated_travel_minutes=0,
                    weather_observations=[weather(value=22, issue="1200", collected="2026-09-29 12:00:00", forecast=False),
                                          weather("PTY", 0, issue="1200", collected="2026-09-29 12:00:00", forecast=False)],
                    weather_forecasts=[weather(value=99, valid="1500")])
    assert result["weather_features"]["temperature_c"] == 22
    assert result["weather_features"]["components"]["T1H"]["source_table"].endswith("ncst")


def test_coordinate_grid_default_and_conflicting_override():
    assert derive(verified_weather_grid=None)["weather_suitability"] == "FAVORABLE"
    result = derive(verified_weather_grid=(61, 127))
    assert result["weather_suitability"] == "UNKNOWN"
    assert result["weather_link"]["reason"] == "CALLER_GRID_COORDINATE_MISMATCH"


def test_rain_and_temperature_are_independent_flags():
    result = derive(weather_forecasts=[weather(value=35), weather("RN1", "강수없음")])
    assert result["weather_features"]["temperature_state"] == "CAUTION"
    assert result["weather_features"]["precipitation_present"] is False
    result = derive(weather_forecasts=[weather(), weather("PTY", 1)])
    assert result["weather_suitability"] == "CAUTION"
    assert result["feasibility_state"] == "AVAILABLE"


def test_unknown_rain_string_is_not_assumed_dry():
    result = derive(weather_forecasts=[weather(), weather("RN1", "알 수 없음")])
    assert result["weather_features"]["precipitation_present"] is None
    assert result["weather_suitability"] == "UNKNOWN"


def test_air_latest_per_station_and_regional_scope_not_nearest_guess():
    rows = [air(pm10Value=999, dataTime="2026-09-29 10:00:00"), air(),
            air(stationName="종로", pm25Value=40), air(sidoName="경기", pm10Value=900)]
    result = derive(air_measurements=rows)
    evidence = result["air_quality_features"]
    assert evidence["pm10"] == 20
    assert evidence["pm25"] == 40
    assert len(evidence["stations"]) == 2
    assert result["air_quality_suitability"] == "CAUTION"
    linked = derive(air_measurements=rows, verified_air_station="중구")
    assert linked["air_quality_features"]["scope"] == "VERIFIED_STATION"
    assert linked["air_quality_suitability"] == "FAVORABLE"


def test_undated_capture_and_future_air_never_count_as_measurement():
    for row in [air(dataTime=None, observation_time_key="collected:2026-09-29T11:30:00"),
                air(dataTime="2026-09-29 13:00:00"), air(collected_at="2026-09-29 13:00:00")]:
        result = derive(air_measurements=[row])
        assert result["air_quality_suitability"] == "UNKNOWN"
        assert result["air_quality_features"]["stations"] == []


def test_air_stale_at_long_arrival_and_missing_values_not_caution():
    assert derive(estimated_travel_minutes=121)["air_quality_suitability"] == "UNKNOWN"
    result = derive(air_measurements=[air(pm10Value=None, pm25Value=None)])
    assert result["air_quality_suitability"] == "UNKNOWN"
    assert result["feasibility_state"] == "AVAILABLE"


def test_exposure_requires_verified_evidence_not_name_or_web_flag():
    assert derive(facility(openFcltyNm="야외 운동장", indoor=False))["facility_exposure"] == "UNKNOWN"
    assert derive(verified_exposure={"state": "INDOOR", "evidence": "verified record"})["outdoor_suitability"] == "NOT_APPLICABLE"
    assert derive(verified_exposure={"state": "OUTDOOR", "evidence": "verified record"})["outdoor_suitability"] == "FAVORABLE"
    with pytest.raises(ValueError):
        derive(verified_exposure={"state": "OUTDOOR"})


def test_missing_travel_evaluates_now_and_does_not_invent_arrival():
    result = derive(estimated_travel_minutes=None)
    assert result["environment_target_basis"] == "NOW_NO_TRAVEL_ESTIMATE"
    assert result["estimated_arrival_time"] is None
    assert result["feasibility_state"] == "UNKNOWN"


def test_recommendation_adapter_preserves_score_and_requires_verified_row():
    recommendation = {"id": 1, "travel_time": 20, "score": 90, "indoor": True}
    before = recommendation.copy()
    result = derive_recommendation_features(recommendation, now=NOW)
    assert result["feasibility_state"] == "UNKNOWN"
    assert result["facility_exposure"] == "UNKNOWN"
    assert recommendation == before and "score" not in result


def test_processed_reader_and_full_wrapper_only_issue_selects_keep_all_facilities():
    connection = MagicMock()
    queries = []
    def execute(statement, params=None):
        sql = str(statement)
        queries.append(sql)
        result = Mock()
        result.scalar_one.return_value = "exists"
        rows = ([facility(), facility(weekdayOperColseHhmm="11:00")]
                if "public_open_facility" in sql else
                [weather(), weather("PTY", 0)] if "weather_ultra_fcst" in sql else
                [air()] if '"air_quality"' in sql else [])
        result.mappings.return_value.all.return_value = rows
        return result
    connection.execute.side_effect = execute
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    result = derive_processed_exercise_features(engine, now=NOW, travel_minutes_for=lambda row: 20,
                                                weather_grid_for=lambda row: (60, 127))
    assert len(result) == 2
    assert [r["feasibility_state"] for r in result] == ["AVAILABLE", "CLOSED"]
    assert all(q.startswith("SELECT") and "raw." not in q for q in queries)
    assert sum('FROM "processed"."air_quality"' in q for q in queries) == 1
    engine.begin.assert_not_called()


def test_missing_processed_environment_tables_return_empty_without_fallback():
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.scalar_one.return_value = None
    assert read_processed_environment(engine, now=NOW) == {
        "weather_observations": [], "weather_forecasts": [], "air_measurements": []}


def test_settings_and_timestamps_validated():
    with pytest.raises(ValueError):
        EnvironmentConfig(air_max_age_minutes=-1)
    with pytest.raises(ValueError):
        derive(now=NOW.replace(tzinfo=None))
