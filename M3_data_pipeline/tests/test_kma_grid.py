"""Offline KMA projection control points and environment linkage regressions."""
import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kma_grid import latlon_to_kma_grid, facility_weather_link
from test_environment_features import derive, facility, weather, air


@pytest.mark.parametrize("latitude,longitude,expected", [
    (37.5665, 126.9780, (60, 127)),  # Seoul city hall
    (35.1796, 129.0756, (98, 76)),   # Busan city hall
    (33.4996, 126.5312, (53, 38)),   # Jeju city
    (38.0, 126.0, (43, 136)),       # KMA reference point
    # Independent corner coordinates from the KMA grid-area specification.
    (31.7944, 123.7613, (1, 1)),
    (43.3935, 123.3102, (1, 253)),
    (31.6518, 131.6423, (149, 1)),
    (43.2175, 132.7750, (149, 253)),
])
def test_real_positions_and_official_domain_control_points(latitude, longitude, expected):
    assert latlon_to_kma_grid(latitude, longitude) == expected
    assert latlon_to_kma_grid(str(latitude), str(longitude)) == expected
    for _ in range(3):
        assert latlon_to_kma_grid(latitude, longitude) == expected


@pytest.mark.parametrize("latitude,longitude", [
    (None, 127), (37, None), ("", 127), ("-", 127), ("bad", 127),
    (float("nan"), 127), (37, float("inf")), (True, 127), (37, False),
    (91, 127), (-90, 127), (-89.99999999999999, 127), (90, 127), (37, 181),
    (126.978, 37.5665), (0, 0), (51.5074, -0.1278),
])
def test_missing_invalid_swapped_and_outside_domain_coordinates(latitude, longitude):
    assert latlon_to_kma_grid(latitude, longitude) is None
    result = derive(facility(latitude=latitude, longitude=longitude), verified_weather_grid=None)
    assert result["weather_link"]["availability"] == "UNLINKED"
    assert result["weather_suitability"] == "UNKNOWN"
    assert result["feasibility_state"] == "AVAILABLE"


def test_busan_uses_only_its_own_grid_not_seoul():
    row = facility(latitude=35.1796, longitude=129.0756, sido_standard="부산광역시")
    before = copy.deepcopy(row)
    result = derive(row, verified_weather_grid=None)
    assert result["weather_link"]["grid"] == (98, 76)
    assert result["weather_suitability"] == "UNKNOWN"
    forecasts = [dict(weather(), nx=98, ny=76), dict(weather("PTY", 0), nx=98, ny=76)]
    result = derive(row, verified_weather_grid=None, weather_forecasts=forecasts)
    assert result["weather_suitability"] == "FAVORABLE"
    assert row == before


def test_link_provenance_and_invalid_coordinates_cannot_be_overridden():
    row = facility()
    link = facility_weather_link(row)
    assert link["method"] == "KMA_DFS_LCC_5KM"
    assert link["source_table"] == "processed.public_open_facility"
    assert link["source_columns"] == ("latitude", "longitude")
    assert facility_weather_link({}, (60, 127))["availability"] == "UNLINKED"


def test_air_station_and_regional_reference_have_explicit_distinct_provenance():
    rows = [air(), air(stationName="종로", pm10Value=100)]
    regional = derive(air_measurements=rows)["air_quality_features"]
    linked = derive(air_measurements=rows, verified_air_station="중구")["air_quality_features"]
    assert regional["scope"] == "REGIONAL_PROXY"
    assert regional["provenance"]["spatial_basis"] == "SAME_SIDO_REGIONAL_REFERENCE"
    assert regional["provenance"]["is_regional_reference"] is True
    assert regional["aggregation"] == "MAX_OF_LATEST_PER_STATION"
    assert regional["pm10"] == 100
    assert linked["scope"] == "VERIFIED_STATION"
    assert linked["provenance"]["requested_station"] == "중구"
    assert linked["provenance"]["is_regional_reference"] is False
    assert linked["aggregation"] == "LATEST_AT_VERIFIED_STATION"
    assert linked["pm10"] == 20
    for result in (regional, linked):
        assert result["provenance"]["is_facility_measurement"] is False
        assert result["provenance"]["is_nearest_station"] is False
        assert result["provenance"]["source_table"] == "processed.air_quality"
        assert all(s["measured_at"] is not None for s in result["stations"])


def test_no_region_or_wrong_station_does_not_silently_change_air_scope():
    result = derive(facility(sido_standard=None))["air_quality_features"]
    assert result["availability"] == "UNLINKED"
    assert result["provenance"]["is_facility_measurement"] is False
    result = derive(verified_air_station="not-present")["air_quality_features"]
    assert result["scope"] == "VERIFIED_STATION"
    assert result["availability"] == "MISSING"
    assert result["stations"] == []
