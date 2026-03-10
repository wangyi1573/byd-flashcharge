"""Scraper for BYD Flash Charge stations - scans all of China via grid."""

import json
import time
import requests
import logging
from datetime import date
from config import API_URL, REQUEST_HEADERS, REQUEST_TEMPLATE, SCAN_GRID, REQUEST_DELAY
from cities import MAJOR_CITIES
from database import init_db, get_db, upsert_station, insert_daily_snapshot, update_daily_summary

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

        # Log pagination info on first call to help diagnose coverage gaps
        extra_keys = {k for k in respond_data if k != "rows"}
        if extra_keys:
            log.info(f"API respondData extra keys: {extra_keys} (total={respond_data.get('total', '?')}, "
                     f"hasMore={respond_data.get('hasMore', '?')}, pageSize={respond_data.get('pageSize', '?')})")

        return respond_data.get("rows", [])

    except Exception as e:
        log.error(f"Request failed at ({lat}, {lng}): {e}")
        return []


def run_full_scan():
    """Scan major cities first, then grid for full coverage."""
    import os
    os.makedirs("data", exist_ok=True)

    init_db()
    today = date.today().isoformat()
    all_stations = {}

    # Phase 1: Major cities (fast, covers most stations)
    log.info(f"Phase 1: Scanning {len(MAJOR_CITIES)} major cities, date={today}")
    for i, (lat, lng, name) in enumerate(MAJOR_CITIES):
        stations = fetch_stations(lat, lng)
        new_count = sum(1 for s in stations if s["id"] not in all_stations)
        for s in stations:
            all_stations[s["id"]] = s
        if stations:
            log.info(f"[{i+1}/{len(MAJOR_CITIES)}] {name} ({lat:.2f}, {lng:.2f}) -> {len(stations)} found, {new_count} new | Total: {len(all_stations)}")
        time.sleep(REQUEST_DELAY)

    log.info(f"Phase 1 done: {len(all_stations)} unique stations from major cities")

    # Phase 1b: Dense offset scans for large cities (cover suburbs)
    tier1_count = sum(1 for _, _, n in MAJOR_CITIES if n in TIER1_CITIES)
    tier2_count = sum(1 for _, _, n in MAJOR_CITIES if n in TIER2_CITIES)
    log.info(f"Phase 1b: Offset scans for {tier1_count} tier-1 + {tier2_count} tier-2 cities")
    offset_new = 0
    for lat, lng, name in MAJOR_CITIES:
        if name in TIER1_CITIES:
            radii = [0.3, 0.5]
        elif name in TIER2_CITIES:
            radii = [0.3]
        else:
            continue
        for r in radii:
            for dlat, dlng in _8DIR:
                stations = fetch_stations(lat + dlat * r, lng + dlng * r)
                new_count = sum(1 for s in stations if s["id"] not in all_stations)
                for s in stations:
                    all_stations[s["id"]] = s
                offset_new += new_count
                time.sleep(REQUEST_DELAY)
        if name in TIER1_CITIES:
            log.info(f"  {name}: offset scans done | Total: {len(all_stations)}")
    log.info(f"Phase 1b done: +{offset_new} new stations from offset scans | Total: {len(all_stations)}")

    # Phase 2: Grid scan for remaining areas (skip points near already-scanned cities)
    log.info(f"Phase 2: Grid scan ({len(SCAN_GRID)} points) for coverage gaps")
    scanned = 0
    skipped = 0
    for i, (lat, lng) in enumerate(SCAN_GRID):
        # Skip if close to a major city already scanned
        too_close = False
        for clat, clng, _ in MAJOR_CITIES:
            if abs(lat - clat) < 0.5 and abs(lng - clng) < 0.7:
                too_close = True
                break
        if too_close:
            skipped += 1
            continue

        stations = fetch_stations(lat, lng)
        scanned += 1
        new_count = sum(1 for s in stations if s["id"] not in all_stations)
        for s in stations:
            all_stations[s["id"]] = s

        if new_count > 0:
            log.info(f"[Grid {scanned}] ({lat:.2f}, {lng:.2f}) -> {new_count} new stations | Total: {len(all_stations)}")

        if scanned % 100 == 0:
            log.info(f"Grid progress: {scanned} scanned, {skipped} skipped, total stations: {len(all_stations)}")

        time.sleep(REQUEST_DELAY)

    # Save to database
    log.info(f"Scan complete. Total unique stations: {len(all_stations)}")
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
