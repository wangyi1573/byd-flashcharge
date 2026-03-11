"""Database module for BYD Flash Charge Station Tracker"""

import sqlite3
import os
from datetime import date
from config import DB_PATH, DATA_DIR


def get_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS stations (
            id INTEGER PRIMARY KEY,
            station_name TEXT,
            address TEXT,
            city TEXT,
            province TEXT,
            geocoded INTEGER DEFAULT 0,
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

    # Migrate existing DBs: add province and geocoded columns if missing
    cols = {row[1] for row in conn.execute("PRAGMA table_info(stations)").fetchall()}
    if "province" not in cols:
        conn.execute("ALTER TABLE stations ADD COLUMN province TEXT")
    if "geocoded" not in cols:
        conn.execute("ALTER TABLE stations ADD COLUMN geocoded INTEGER DEFAULT 0")
    conn.commit()
    conn.close()


def upsert_station(conn, station: dict, today: str):
    """Insert or update a station record. City/province set by geocoder later."""
    existing = conn.execute("SELECT first_seen FROM stations WHERE id = ?", (station["id"],)).fetchone()
    first_seen = existing["first_seen"] if existing else today

    conn.execute("""
        INSERT INTO stations (id, station_name, address, lat, lng, operator_name, operator_id,
            operator_station_id, flash_charge_num, fast_charge_num, slow_charge_num, super_charge_num,
            flash_charge, byd_self_support, service_tags, attribute_tags, first_seen, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            station_name=excluded.station_name,
            address=excluded.address,
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
    """Calculate and store daily summary (cumulative, never decreasing)."""
    row = conn.execute("""
        SELECT
            COUNT(*) as total_stations,
            SUM(flash_charge_num) as total_flash,
            SUM(fast_charge_num) as total_fast,
            SUM(slow_charge_num) as total_slow,
            SUM(super_charge_num) as total_super
        FROM stations
    """).fetchone()

    # New = today's total - yesterday's total
    prev = conn.execute("""
        SELECT total_stations FROM daily_summary
        WHERE snapshot_date < ? ORDER BY snapshot_date DESC LIMIT 1
    """, (today,)).fetchone()
    new_count = row["total_stations"] - (prev["total_stations"] if prev else 0)
    if new_count < 0:
        new_count = 0

    city_count = conn.execute("""
        SELECT COUNT(DISTINCT city) FROM stations
        WHERE city IS NOT NULL AND city != ''
    """).fetchone()[0]

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
    """Get station counts grouped by city (cumulative, all stations ever seen)."""
    return conn.execute("""
        SELECT
            COALESCE(NULLIF(city, ''), '未知') as city,
            COUNT(*) as station_count,
            SUM(flash_charge_num) as flash_connectors,
            SUM(fast_charge_num) as fast_connectors,
            SUM(slow_charge_num) as slow_connectors,
            SUM(super_charge_num) as super_connectors
        FROM stations
        GROUP BY city
        ORDER BY station_count DESC
    """).fetchall()


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
