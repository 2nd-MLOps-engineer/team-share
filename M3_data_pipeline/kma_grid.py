"""위도·경도 좌표를 기상청 단기예보 API의 격자 좌표(nx, ny)로 변환합니다.

기상청 단기예보 격자 사양을 기준으로 5 km 격자 좌표를 계산합니다.
"""


import math


def _coordinate(value, limit):
    if isinstance(value, bool):
        return None
    try:
        value = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return value if math.isfinite(value) and abs(value) <= limit else None


def latlon_to_kma_grid(latitude, longitude) -> tuple[int, int] | None:
    """Return (nx, ny), or None for missing/invalid/out-of-domain positions."""
    lat, lon = _coordinate(latitude, 90), _coordinate(longitude, 180)
    if lat is None or lon is None or not 0 < lat < 90:
        return None
    radius = 6371.00877 / 5.0
    phi1, phi2 = math.radians(30), math.radians(60)
    phi0, lambda0 = math.radians(38), math.radians(126)
    tangent1 = math.tan(math.pi / 4 + phi1 / 2)
    n = math.log(math.cos(phi1) / math.cos(phi2)) / math.log(
        math.tan(math.pi / 4 + phi2 / 2) / tangent1
    )
    factor = tangent1 ** n * math.cos(phi1) / n
    rho0 = radius * factor / math.tan(math.pi / 4 + phi0 / 2) ** n
    rho = radius * factor / math.tan(math.pi / 4 + math.radians(lat) / 2) ** n
    delta = math.radians(lon) - lambda0
    delta = (delta + math.pi) % (2 * math.pi) - math.pi
    theta = n * delta
    nx = math.floor(rho * math.sin(theta) + 43 + 0.5)
    ny = math.floor(rho0 - rho * math.cos(theta) + 136 + 0.5)
    return (nx, ny) if 1 <= nx <= 149 and 1 <= ny <= 253 else None


def facility_weather_link(facility, verified_grid=None) -> dict:
    """Expose coordinate provenance; optional legacy grid must agree with it."""
    grid = latlon_to_kma_grid(facility.get("latitude"), facility.get("longitude"))
    reason = None if grid else "MISSING_INVALID_OR_OUTSIDE_GRID_COORDINATES"
    if grid is not None and verified_grid is not None:
        if (not isinstance(verified_grid, (tuple, list)) or len(verified_grid) != 2
                or any(isinstance(v, bool) or not isinstance(v, int) for v in verified_grid)
                or tuple(verified_grid) != grid):
            grid, reason = None, "CALLER_GRID_COORDINATE_MISMATCH"
    return {
        "availability": "LINKED" if grid else "UNLINKED", "grid": grid,
        "method": "KMA_DFS_LCC_5KM", "source_table": "processed.public_open_facility",
        "source_columns": ("latitude", "longitude"),
        "latitude": _coordinate(facility.get("latitude"), 90),
        "longitude": _coordinate(facility.get("longitude"), 180),
        "grid_spacing_km": 5, "reason": reason,
    }
