"""Scraper for BYD Flash Charge stations - scans all of China via grid."""

import json
import os
import time
import requests
import logging
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import API_URL, REQUEST_HEADERS, REQUEST_TEMPLATE, SCAN_GRID, CONCURRENT_WORKERS
from cities import MAJOR_CITIES
from database import init_db, get_db, upsert_station, insert_daily_snapshot, update_daily_summary

os.makedirs("data", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("data/scraper.log"),
    ],
)
log = logging.getLogger(__name__)

# Tier 1: Mega cities - 8 directions × 2 radii (0.3° + 0.5°) = 16 offset scans
TIER1_CITIES = {
    "北京", "上海", "广州", "深圳", "成都", "重庆", "武汉", "西安", "天津", "杭州",
}
# Tier 2: Large cities - 8 directions × 0.3° = 8 offset scans
TIER2_CITIES = {
    "南京", "郑州", "长沙", "苏州", "东莞", "佛山", "合肥", "青岛", "沈阳", "济南",
    "昆明", "南宁", "福州", "石家庄", "太原", "南昌", "哈尔滨", "长春", "贵阳", "兰州",
    "无锡", "常州", "宁波", "温州", "厦门", "惠州", "中山", "珠海", "清远", "南充",
    "洛阳", "新乡", "银川", "乌鲁木齐", "呼和浩特", "大连", "廊坊",
}
# 8-direction unit vectors
_8DIR = [(1, 0), (-1, 0), (0, 1), (0, -1), (1, 1), (1, -1), (-1, 1), (-1, -1)]


def fetch_stations(lat: float, lng: float) -> list:
    """Fetch stations near a given coordinate."""
    req_data = REQUEST_TEMPLATE.copy()
    req_data["lat"] = lat
    req_data["lng"] = lng
    req_data["reqTimestamp"] = int(time.time() * 1000)

    payload = {"request": json.dumps(req_data)}

    try:
        resp = requests.post(API_URL, json=payload, headers=REQUEST_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        inner = json.loads(data.get("response", "{}"))
        if inner.get("code") != "0":
            log.warning(f"API error at ({lat}, {lng}): {inner.get('message')}")
            return []

        respond_data = json.loads(inner.get("respondData", "{}"))
        return respond_data.get("rows", [])

    except Exception as e:
        log.error(f"Request failed at ({lat}, {lng}): {e}")
        return []


def batch_fetch(coords, label=""):
    """Fetch stations for a list of coordinates concurrently. Returns {id: station}."""
    results = {}
    total = len(coords)

    with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
        future_to_coord = {
            executor.submit(fetch_stations, lat, lng): (lat, lng)
            for lat, lng in coords
        }
        done = 0
        for future in as_completed(future_to_coord):
            done += 1
            stations = future.result()
            for s in stations:
                results[s["id"]] = s
            if done % 100 == 0 and label:
                log.info(f"  {label}: {done}/{total} done, {len(results)} unique so far")

    return results


def run_full_scan():
    """Scan major cities first, then grid for full coverage."""
    init_db()
    today = date.today().isoformat()
    all_stations = {}
    t_start = time.time()

    # Phase 1: Major cities (fast, covers most stations)
    log.info(f"Phase 1: Scanning {len(MAJOR_CITIES)} major cities ({CONCURRENT_WORKERS} workers)")
    city_coords = [(lat, lng) for lat, lng, _ in MAJOR_CITIES]
    city_results = batch_fetch(city_coords, "Cities")
    all_stations.update(city_results)
    log.info(f"Phase 1 done: {len(all_stations)} unique stations ({time.time()-t_start:.0f}s)")

    # Phase 1b: Dense offset scans for large cities (cover suburbs)
    offset_coords = []
    for lat, lng, name in MAJOR_CITIES:
        if name in TIER1_CITIES:
            radii, dirs = [0.3, 0.5], _8DIR
        elif name in TIER2_CITIES:
            radii, dirs = [0.3], _8DIR
        else:
            radii, dirs = [0.2], [(1, 0), (-1, 0), (0, 1), (0, -1)]
        for r in radii:
            for dlat, dlng in dirs:
                offset_coords.append((lat + dlat * r, lng + dlng * r))

    log.info(f"Phase 1b: {len(offset_coords)} offset scans for tier-1/tier-2 cities")
    t1b = time.time()
    offset_results = batch_fetch(offset_coords, "Offsets")
    new_from_offset = sum(1 for sid in offset_results if sid not in all_stations)
    all_stations.update(offset_results)
    log.info(f"Phase 1b done: +{new_from_offset} new stations ({time.time()-t1b:.0f}s) | Total: {len(all_stations)}")

    # Phase 2: Grid scan for full coverage
    grid_coords = list(SCAN_GRID)

    log.info(f"Phase 2: {len(grid_coords)} grid points")
    t2 = time.time()
    grid_results = batch_fetch(grid_coords, "Grid")
    new_from_grid = sum(1 for sid in grid_results if sid not in all_stations)
    all_stations.update(grid_results)
    log.info(f"Phase 2 done: +{new_from_grid} new stations ({time.time()-t2:.0f}s) | Total: {len(all_stations)}")

    # Save to database
    elapsed = time.time() - t_start
    log.info(f"Scan complete: {len(all_stations)} stations in {elapsed:.0f}s")
    log.info("Saving to database...")

    conn = get_db()
    try:
        for station in all_stations.values():
            upsert_station(conn, station, today)
            insert_daily_snapshot(conn, station, today)
        update_daily_summary(conn, today)
        conn.commit()
        log.info("Database updated successfully.")
    finally:
        conn.close()

    # Also save raw JSON
    raw_path = f"data/raw_{today}.json"
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(list(all_stations.values()), f, ensure_ascii=False, indent=2)
    log.info(f"Raw data saved to {raw_path}")

    return all_stations


if __name__ == "__main__":
    stations = run_full_scan()
    print(f"\nDone! Found {len(stations)} unique stations across China.")
