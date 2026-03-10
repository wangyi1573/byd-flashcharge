"""Database module for BYD Flash Charge Station Tracker"""

import re
import sqlite3
import os
from datetime import datetime, date
from config import DB_PATH, DATA_DIR
from cities import CITY_NAMES, DISTRICT_TO_CITY


def get_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# Brand prefixes to strip before city matching
_BRAND_PREFIXES = ("比亚迪", "腾势", "方程豹汽车", "方程豹", "仰望", "领汇")
# Pre-compiled pattern: extract content inside 闪充(...) with full/half-width parens
_PAREN_RE = re.compile(r"闪充[（(](.+?)[）)]")


def extract_city_from_name(station_name: str) -> str:
    """Extract city name from station_name like '闪充(比亚迪深圳宝安4S店)充电站'.

    Algorithm:
    1. Extract content inside 闪充(...) parentheses
    2. Strip brand prefix (比亚迪/腾势/方程豹/仰望/领汇)
    3. Longest-match against CITY_NAMES from the start of remaining text
    4. Fallback: try DISTRICT_TO_CITY mapping on leading 2-3 chars
    """
    if not station_name:
        return ""

    m = _PAREN_RE.search(station_name)
    if not m:
        return ""

    inner = m.group(1)

    # Strip brand prefix
    for prefix in _BRAND_PREFIXES:
        if inner.startswith(prefix):
            inner = inner[len(prefix):]
            break

    # Longest-match city from start of text (CITY_NAMES is sorted longest-first)
    for city in CITY_NAMES:
        if inner.startswith(city):
            return city

    # Fallback: district → city mapping (try 5-char down to 2-char)
    for length in (5, 4, 3, 2):
        if len(inner) >= length:
            district = inner[:length]
            if district in DISTRICT_TO_CITY:
                return DISTRICT_TO_CITY[district]

    return ""


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS stations (
            id INTEGER PRIMARY KEY,
            station_name TEXT,
            address TEXT,
            lat REAL,
            lng REAL,
            operator_name TEXT,
            operator_id TEXT,
            operator_station_id TEXT,
            flash_charge_num INTEGER DEFAULT 0,
            fast_charge_num INTEGER DEFAULT 0,
            slow_charge_num INTEGER DEFAULT 0,
            super_charge_num INTEGER DEFAULT 0,
            flash_charge INTEGER DEFAULT 0,
            byd_self_support INTEGER DEFAULT 0,
            service_tags TEXT,
            attribute_tags TEXT,
            first_seen DATE,
            last_seen DATE
        );

        CREATE TABLE IF NOT EXISTS daily_snapshots (
            snapshot_date DATE,
            station_id INTEGER,
            flash_charge_num INTEGER DEFAULT 0,
            fast_charge_num INTEGER DEFAULT 0,
            slow_charge_num INTEGER DEFAULT 0,
            super_charge_num INTEGER DEFAULT 0,
            flash_idle_num INTEGER DEFAULT 0,
            fast_idle_num INTEGER DEFAULT 0,
            slow_idle_num INTEGER DEFAULT 0,
            super_idle_num INTEGER DEFAULT 0,
            electricity_fee REAL,
            service_fee REAL,
            PRIMARY KEY (snapshot_date, station_id)
        );

        CREATE TABLE IF NOT EXISTS daily_summary (
            snapshot_date DATE PRIMARY KEY,
            total_stations INTEGER,
            total_flash_connectors INTEGER,
            total_fast_connectors INTEGER,
            total_slow_connectors INTEGER,
            total_super_connectors INTEGER,
            new_stations_today INTEGER DEFAULT 0,
            city_count INTEGER DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_stations_last_seen ON stations(last_seen);
        CREATE INDEX IF NOT EXISTS idx_snapshot_date ON daily_snapshots(snapshot_date);
    """)
    conn.commit()
    conn.close()


def upsert_station(conn, station: dict, today: str):
    """Insert or update a station record."""
    existing = conn.execute("SELECT first_seen FROM stations WHERE id = ?", (station["id"],)).fetchone()
    first_seen = existing["first_seen"] if existing else today

    # Auto-extract city from station name if not already set
    station_name = station.get("stationName", "")
    city = extract_city_from_name(station_name)

    conn.execute("""
        INSERT INTO stations (id, station_name, address, city, lat, lng, operator_name, operator_id,
            operator_station_id, flash_charge_num, fast_charge_num, slow_charge_num, super_charge_num,
            flash_charge, byd_self_support, service_tags, attribute_tags, first_seen, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            station_name=excluded.station_name,
            address=excluded.address,
            city=COALESCE(NULLIF(excluded.city, ''), stations.city),
            lat=excluded.lat,
            lng=excluded.lng,
            flash_charge_num=excluded.flash_charge_num,
            fast_charge_num=excluded.fast_charge_num,
            slow_charge_num=excluded.slow_charge_num,
            super_charge_num=excluded.super_charge_num,
            flash_charge=excluded.flash_charge,
            service_tags=excluded.service_tags,
            attribute_tags=excluded.attribute_tags,
            last_seen=excluded.last_seen
    """, (
        station["id"],
        station.get("stationName", ""),
        station.get("address", ""),
        city,
        station.get("stationLat", 0),
        station.get("stationLng", 0),
        station.get("operatorName", ""),
        station.get("operatorId", ""),
        station.get("operatorStationId", ""),
        station.get("flashChargeConnectorNum", 0),
        station.get("fastChargeConnectorNum", 0),
        station.get("slowChargeConnectorNum", 0),
        station.get("superChargeConnectorNum", 0),
        station.get("flashCharge", 0),
        station.get("bydSelfSupport", 0),
        station.get("serviceTags", ""),
        station.get("attributeTags", ""),
        first_seen,
        today,
    ))


def insert_daily_snapshot(conn, station: dict, today: str):
    """Insert daily snapshot for a station."""
    conn.execute("""
        INSERT OR REPLACE INTO daily_snapshots
        (snapshot_date, station_id, flash_charge_num, fast_charge_num, slow_charge_num, super_charge_num,
         flash_idle_num, fast_idle_num, slow_idle_num, super_idle_num, electricity_fee, service_fee)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        today,
        station["id"],
        station.get("flashChargeConnectorNum", 0),
        station.get("fastChargeConnectorNum", 0),
        station.get("slowChargeConnectorNum", 0),
        station.get("superChargeConnectorNum", 0),
        station.get("flashIdleChargeConnectorNum", 0),
        station.get("fastIdleChargeConnectorNum", 0),
        station.get("slowIdleChargeConnectorNum", 0),
        station.get("superIdleChargeConnectorNum", 0),
        station.get("currentPeriodElectricityFee", 0),
        station.get("currentPeriodServiceFee", 0),
    ))


def update_daily_summary(conn, today: str):
    """Calculate and store daily summary."""
    row = conn.execute("""
        SELECT
            COUNT(*) as total_stations,
            SUM(flash_charge_num) as total_flash,
            SUM(fast_charge_num) as total_fast,
            SUM(slow_charge_num) as total_slow,
            SUM(super_charge_num) as total_super
        FROM stations WHERE last_seen = ?
    """, (today,)).fetchone()

    new_count = conn.execute(
        "SELECT COUNT(*) as c FROM stations WHERE first_seen = ?", (today,)
    ).fetchone()["c"]

    # Count unique cities
    city_count = conn.execute("""
        SELECT COUNT(DISTINCT city) FROM stations
        WHERE last_seen = ? AND city IS NOT NULL AND city != ''
    """, (today,)).fetchone()[0]

    conn.execute("""
        INSERT OR REPLACE INTO daily_summary
        (snapshot_date, total_stations, total_flash_connectors, total_fast_connectors,
         total_slow_connectors, total_super_connectors, new_stations_today, city_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        today,
        row["total_stations"],
        row["total_flash"] or 0,
        row["total_fast"] or 0,
        row["total_slow"] or 0,
        row["total_super"] or 0,
        new_count,
        city_count,
    ))


def get_city_stats(conn, snapshot_date: str = None):
    """Get station counts grouped by city."""
    if not snapshot_date:
        snapshot_date = date.today().isoformat()
    return conn.execute("""
        SELECT
            COALESCE(NULLIF(city, ''), '未知') as city,
            COUNT(*) as station_count,
            SUM(flash_charge_num) as flash_connectors,
            SUM(fast_charge_num) as fast_connectors,
            SUM(slow_charge_num) as slow_connectors,
            SUM(super_charge_num) as super_connectors
        FROM stations
        WHERE last_seen = ?
        GROUP BY city
        ORDER BY station_count DESC
    """, (snapshot_date,)).fetchall()


def get_summary_history(conn, days: int = 30):
    """Get daily summary for the last N days."""
    return conn.execute("""
        SELECT * FROM daily_summary
        ORDER BY snapshot_date DESC
        LIMIT ?
    """, (days,)).fetchall()


if __name__ == "__main__":
    init_db()
    print("Database initialized.")
