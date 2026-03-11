"""Reverse geocoding module for BYD Flash Charge stations.

Dual mode:
  1. Primary: Amap (高德地图) reverse geocoding API
  2. Fallback: Offline point-in-polygon via shapely + province GeoJSON

When Amap quota is exhausted (infocode 10003/10004/10044), automatically
switches to offline PiP for the remainder of the batch.
"""

import json
import os
import time
import requests
import logging

from config import AMAP_API_KEY

log = logging.getLogger(__name__)

AMAP_REGEO_URL = "https://restapi.amap.com/v3/geocode/regeo"

# Amap infocodes that indicate quota exhaustion
_QUOTA_CODES = {"10003", "10004", "10044"}

# Lazy-loaded shapely PiP data
_pip_provinces = None


def _load_pip_data():
    """Load province GeoJSON files for offline point-in-polygon fallback."""
    global _pip_provinces
    if _pip_provinces is not None:
        return

    try:
        from shapely.geometry import shape, Point  # noqa: F401
    except ImportError:
        log.warning("shapely not installed — offline PiP fallback unavailable")
        _pip_provinces = []
        return

    map_dir = os.path.join("public", "static", "maps")
    map_index = os.path.join(map_dir, "province_map.json")

    if not os.path.exists(map_index):
        log.warning(f"{map_index} not found — run download_maps.py first")
        _pip_provinces = []
        return

    with open(map_index, "r", encoding="utf-8") as f:
        province_map = json.load(f)

    _pip_provinces = []
    for prov_name, filename in province_map.items():
        geo_path = os.path.join(map_dir, filename)
        if not os.path.exists(geo_path):
            continue
        with open(geo_path, "r", encoding="utf-8") as f:
            geojson = json.load(f)
        for feature in geojson.get("features", []):
            geom = shape(feature["geometry"])
            city_name = feature.get("properties", {}).get("name", "")
            _pip_provinces.append({
                "province": prov_name,
                "city": city_name,
                "geometry": geom,
            })

    log.info(f"Loaded {len(_pip_provinces)} PiP regions from {len(province_map)} provinces")


def _geocode_pip(lat: float, lng: float) -> dict:
    """Offline point-in-polygon geocoding using shapely."""
    from shapely.geometry import Point

    _load_pip_data()
    if not _pip_provinces:
        return {}

    pt = Point(lng, lat)
    for region in _pip_provinces:
        if region["geometry"].contains(pt):
            return {
                "province": region["province"],
                "city": region["city"] or region["province"],
            }
    return {}


def _geocode_amap(lat: float, lng: float) -> dict:
    """Call Amap reverse geocoding API. Returns dict or raises on quota exhaustion."""
    params = {
        "key": AMAP_API_KEY,
        "location": f"{lng},{lat}",  # Amap uses lng,lat order
        "output": "json",
    }

    resp = requests.get(AMAP_REGEO_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    infocode = data.get("infocode", "")
    if infocode in _QUOTA_CODES:
        raise QuotaExhausted(f"Amap quota exhausted (infocode={infocode})")

    if data.get("status") != "1":
        log.warning(f"Amap API error: {data.get('info')} (infocode={infocode})")
        return {}

    regeo = data.get("regeocode", {})
    addr_comp = regeo.get("addressComponent", {})

    province = addr_comp.get("province", "")
    city = addr_comp.get("city", "")
    # For municipalities (直辖市), city is [] — use province instead
    if not city or isinstance(city, list):
        city = province

    return {"province": province, "city": city}


class QuotaExhausted(Exception):
    pass


def geocode_station(lat: float, lng: float, use_pip: bool = False) -> dict:
    """Geocode a single station coordinate.

    Returns: {"province": "广东省", "city": "深圳市"} or {}
    """
    if use_pip:
        return _geocode_pip(lat, lng)

    try:
        return _geocode_amap(lat, lng)
    except QuotaExhausted:
        raise
    except Exception as e:
        log.error(f"Amap geocode failed for ({lat}, {lng}): {e}")
        return {}


def geocode_pending_stations(conn, delay: float = 0.1):
    """Batch geocode all stations where geocoded=0.

    Uses Amap API primarily. On quota exhaustion, switches to shapely PiP
    for remaining stations. Commits every 50 stations.
    """
    stations = conn.execute(
        "SELECT id, lat, lng FROM stations WHERE geocoded = 0"
    ).fetchall()

    if not stations:
        log.info("No stations pending geocoding")
        return

    log.info(f"Geocoding {len(stations)} pending stations...")

    use_pip = False
    success = 0

    for i, s in enumerate(stations):
        try:
            result = geocode_station(s["lat"], s["lng"], use_pip=use_pip)
        except QuotaExhausted:
            log.warning("Amap quota exhausted — switching to offline PiP")
            use_pip = True
            result = geocode_station(s["lat"], s["lng"], use_pip=True)

        if not result and not use_pip:
            # Amap returned empty — try with PiP as one-off
            try:
                result = _geocode_pip(s["lat"], s["lng"])
            except Exception:
                pass

        if not result:
            # Still nothing — skip but don't mark as geocoded
            if not use_pip:
                time.sleep(delay)
            continue

        conn.execute("""
            UPDATE stations SET province = ?, city = ?, geocoded = 1
            WHERE id = ?
        """, (result.get("province", ""), result.get("city", ""), s["id"]))

        success += 1

        if (i + 1) % 50 == 0:
            conn.commit()
            log.info(f"  Geocoded {i+1}/{len(stations)} ({success} success, pip={'on' if use_pip else 'off'})")

        if not use_pip:
            time.sleep(delay)

    conn.commit()
    log.info(f"Geocoding done: {success}/{len(stations)} successful (pip={'on' if use_pip else 'off'})")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if not AMAP_API_KEY:
        print("AMAP_API_KEY not set in config.py")
    else:
        import sqlite3
        conn = sqlite3.connect("data/stations.db")
        conn.row_factory = sqlite3.Row
        geocode_pending_stations(conn)
        conn.close()
