import ast
from datetime import datetime
import json
from pathlib import Path
from types import ModuleType

import pytest

import recommendation_derived as derived
from facility_derived import KST


NOW = datetime(2026, 9, 29, 12, tzinfo=KST)


def record(**changes):
    row = dict(faci_cd="123", faci_nm="테스트 체육관", ftype_nm="체육관", cp_nm="서울특별시",
               cpb_nm="종로구", faci_lat=37.57, faci_lot=126.98, inout_gbn_nm="실내")
    row.update(changes)
    return derived.build_record(row, "processed.facility", now=NOW, environment={})


def reference_service():
    path = Path(__file__).resolve().parents[2] / ".reference/woosimwoonkka-web/frontend/recommendation_service.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    tree.body = [node for node in tree.body if not (
        isinstance(node, ast.ImportFrom) and (node.level or (node.module or "").startswith("django")))]
    module = ModuleType("reference_contract")
    exec(compile(tree, str(path), "exec"), vars(module))
    module._load_env = lambda: None
    module._region_for_coordinate = lambda origin: None
    module.environment_for_region = lambda *a: {"weather": {}, "air": {}}
    module._attach_live_operation_evidence = lambda *a, **kw: None
    module._kakao_fallback_facilities = lambda *a: []
    return module


def candidate(**changes):
    row = record(**changes)
    row["features"] = json.loads(row["features"])
    row["source_row"] = json.loads(row["source_row"])
    row.update(source=f"DB:{derived.TABLE}", db_distance_km=0.5, operation_notice="")
    return row


def test_actual_reference_ranking_uses_derived_and_request_travel(monkeypatch):
    service = reference_service()
    original = service.facilities_for_region
    monkeypatch.setattr(derived, "facilities_for_region", lambda *a, **kw: [candidate()])
    result = derived.make_recommendations(service, None, "서울특별시 종로구", {"fitness"}, 60, 30,
                                          origin=(37.57, 126.98), now=NOW)
    item = result["recommendations"][0]
    assert item["score"] == 76  # Original 45 + (40 - 0.5 * 18).
    assert item["travel_time"] == 10
    assert item["request_features"]["estimated_travel_minutes"] == 10
    assert item["request_features"]["feasibility_state"] == "UNKNOWN"
    assert item["derived_snapshot"]["facility_exposure"] == "INDOOR"
    assert item["source"] == f"DB:{derived.TABLE}"
    assert service.facilities_for_region is original


def test_reference_filters_sport_and_travel(monkeypatch):
    monkeypatch.setattr(derived, "facilities_for_region", lambda *a, **kw: [candidate()])
    for sport, travel in [({"swimming"}, 30), ({"fitness"}, 5)]:
        result = derived.make_recommendations(reference_service(), None, "서울특별시 종로구", sport, 60, travel,
                                              origin=(37.57, 126.98), now=NOW)
        assert result["recommendations"] == []


def test_stored_exposure_actually_changes_reference_weather_scoring(monkeypatch):
    service = reference_service()
    service.environment_for_region = lambda *a: {"weather": {"PTY": "1"}, "air": {}}
    # The name heuristic calls this outdoor, but the source explicitly says indoor.
    indoor = candidate(faci_nm="공원 체육관", inout_gbn_nm="실내")
    assert service._is_outdoor(indoor) is True
    monkeypatch.setattr(derived, "facilities_for_region", lambda *a, **kw: [indoor])
    result = derived.make_recommendations(service, None, "서울특별시 종로구", {"running"}, 60, 30,
                                          origin=(37.57, 126.98), now=NOW)
    item = result["recommendations"][0]
    assert item["indoor"] is True
    assert item["score"] == 84  # Same policy: indoor rain bonus +8, not outdoor -24.


def test_separate_sources_no_schedule_invention():
    row = record()
    features = json.loads(row["features"])
    assert features["is_open_now"] is None
    assert features["weather_link"]["source_table"] == "processed.facility"
    assert features["weather_link"]["source_columns"] == ["faci_lat", "faci_lot"]
    assert row["source_key"] == record()["source_key"]
    assert row["source_key"] != record(faci_cd="456")["source_key"]


def test_public_schedule_reuses_existing_features():
    row = derived.build_record(dict(openFcltyNm="개방시설", insttCode="same-institution",
        rstde="연중무휴", weekdayOperOpenHhmm="09:00", weekdayOperColseHhmm="18:00"),
        "processed.public_open_facility", now=NOW, environment={})
    features = json.loads(row["features"])
    assert features["is_open_now"] is True
    assert features["minutes_until_close"] == 360
    assert features["estimated_travel_minutes"] is None


@pytest.mark.parametrize("value", [None, "bad", float("inf"), float("nan"), 91])
def test_invalid_coordinates(value):
    assert derived._coordinate(value, 90) is None


def test_remote_refresh_rejected_before_connect():
    from types import SimpleNamespace

    engine = SimpleNamespace(
        url=SimpleNamespace(host="remote.invalid")
    )

    with pytest.raises(ValueError, match="local"):
        derived.refresh(engine)


def test_ambiguous_result_identity_never_attaches_wrong_features(monkeypatch):
    monkeypatch.setattr(derived, "facilities_for_region", lambda *a, **kw: [candidate(), candidate(faci_cd="456")])
    result = derived.make_recommendations(reference_service(), None, "서울특별시 종로구", {"fitness"}, 60, 30,
                                          origin=(37.57, 126.98), now=NOW)
    assert len(result["recommendations"]) == 2
    assert all("derived_source_key" not in r for r in result["recommendations"])
