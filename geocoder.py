"""Reverse geocoding module for BYD Flash Charge stations.

Uses Amap (高德地图) reverse geocoding API to determine:
- Province / City / District
- Nearby road info (highway vs city road)
- Station type classification (高速站 / 市区站)

Set AMAP_API_KEY in config or environment before use.
"""

import os
import time
import sqlite3
import requests
import logging

log = logging.getLogger(__name__)

# Will be set by user
AMAP_API_KEY = os.environ.get("AMAP_API_KEY", "")

AMAP_REGEO_URL = "https://restapi.amap.com/v3/geocode/regeo"


def reverse_geocode(lat: float, lng: float) -> dict:
    """Call Amap reverse geocoding API for a single coordinate.

    Returns dict with: province, city, district, road_name, road_type, formatted_address
    """
    if not AMAP_API_KEY:
        raise ValueError("AMAP_API_KEY not set. Set it in environment or config.")

    params = {
        "key": AMAP_API_KEY,
        "location": f"{lng},{lat}",  # Amap uses lng,lat order
        "extensions": "all",
        "output": "json",
    }

    try:
        resp = requests.get(AMAP_REGEO_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "1":
            log.warning(f"Amap API error: {data.get('info')}")
            return {}

        regeo = data.get("regeocode", {})
        addr_comp = regeo.get("addressComponent", {})

        # Extract road info
        roads = regeo.get("roads", [])
        nearest_road = roads[0] if roads else {}

        # Determine road type
        road_name = nearest_road.get("name", "")
        road_type = nearest_road.get("type", "")
        road_distance = float(nearest_road.get("distance", 9999))

        return {
            "province": addr_comp.get("province", ""),
            "city": addr_comp.get("city", "") or addr_comp.get("province", ""),
            "district": addr_comp.get("district", ""),
            "road_name": road_name,
            "road_type": road_type,
            "road_distance": road_distance,
            "formatted_address": regeo.get("formatted_address", ""),
        }

    except Exception as e:
        log.error(f"Reverse geocode failed for ({lat}, {lng}): {e}")
        return {}


def classify_station_type(geocode_result: dict, station_name: str = "") -> str:
    """Classify station as 高速站 / 市区站 / 站中站 based on geocoding + name.

    Rules:
    1. Station name contains highway keywords → 高速站
    2. Nearby road type indicates highway (高速/快速/国道) → 高速站
    3. Station name contains 站中站 → 站中站
    4. Otherwise → 市区站
    """
    highway_name_keywords = ["高速", "服务区", "收费站", "枢纽"]
    highway_road_types = ["高速", "快速路", "国道"]

    # Check station name
    for kw in highway_name_keywords:
        if kw in station_name:
            return "高速站"

    if "站中站" in station_name:
        return "站中站"

    # Check road type from geocoding
    road_type = geocode_result.get("road_type", "")
    road_name = geocode_result.get("road_name", "")
    road_distance = geocode_result.get("road_distance", 9999)

    # Only consider nearby roads (within 200m)
    if road_distance < 200:
        for rt in highway_road_types:
            if rt in road_type or rt in road_name:
                return "高速站"

    return "市区站"


def geocode_all_stations(db_path: str = "data/stations.db", delay: float = 0.1):
    """Geocode all un-geocoded stations in the database.

    Amap free tier: 5000 requests/day. We do ~0.1s delay between calls.
    """
    if not AMAP_API_KEY:
        log.error("AMAP_API_KEY not set! Set environment variable or update config.")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Get un-geocoded stations
    stations = conn.execute(
        "SELECT id, station_name, lat, lng FROM stations WHERE geocoded = 0"
    ).fetchall()

    log.info(f"Geocoding {len(stations)} stations...")

    success = 0
    for i, s in enumerate(stations):
        result = reverse_geocode(s["lat"], s["lng"])
        if not result:
            time.sleep(delay)
            continue

        station_type = classify_station_type(result, s["station_name"])

        conn.execute("""
            UPDATE stations SET
                province = ?,
                city = ?,
                district = ?,
                nearby_road = ?,
                road_type = ?,
                station_type = ?,
                geocoded = 1
            WHERE id = ?
        """, (
            result.get("province", ""),
            result.get("city", ""),
            result.get("district", ""),
            result.get("road_name", ""),
            result.get("road_type", ""),
            station_type,
            s["id"],
        ))

        success += 1
        if (i + 1) % 100 == 0:
            conn.commit()
            log.info(f"Geocoded {i+1}/{len(stations)}, {success} success")

        time.sleep(delay)

    conn.commit()
    conn.close()
    log.info(f"Geocoding done: {success}/{len(stations)} successful")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if not AMAP_API_KEY:
        print("请设置高德地图 API Key:")
        print("  export AMAP_API_KEY=your_key_here")
        print("  python geocoder.py")
    else:
        geocode_all_stations()
