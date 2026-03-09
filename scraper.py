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

    # Geocode new stations (if API key available)
    try:
        from geocoder import AMAP_API_KEY, geocode_all_stations
        if AMAP_API_KEY:
            log.info("Running geocoding for new stations...")
            geocode_all_stations()
        else:
            log.info("AMAP_API_KEY not set, skipping geocoding")
    except Exception as e:
        log.warning(f"Geocoding skipped: {e}")

    return all_stations


if __name__ == "__main__":
    stations = run_full_scan()
    print(f"\nDone! Found {len(stations)} unique stations across China.")
