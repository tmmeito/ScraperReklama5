import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import scraperReklama5 as scraper
from storage import sqlite_store


def make_connection():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    sqlite_store.init_schema(conn, scraper.CSV_FIELDNAMES)
    return conn


def iso(dt):
    return dt.isoformat(timespec="seconds")


def base_listing(overrides=None):
    data = {
        "id": "abc",
        "link": "https://example.com/abc",
        "make": "VW",
        "model": "Golf",
        "year": 2019,
        "price": 15000,
        "km": 120000,
        "kw": 110,
        "ps": 150,
        "fuel": "Diesel",
        "gearbox": "Manual",
        "body": "Hatch",
        "color": "Blue",
        "registration": "SK",
        "reg_until": "2024",
        "emission_class": "EU6",
        "date": "01 јан 12:00",
        "city": "Skopje",
        "promoted": 0,
    }
    if overrides:
        data.update(overrides)
    return data


def test_upsert_many_inserts_listing_and_hash():
    conn = make_connection()
    ts = datetime(2024, 1, 5, 13, 0, 0)

    sqlite_store.upsert_many(conn, [base_listing()], scraper.CSV_FIELDNAMES, timestamp=ts)

    rows = conn.execute("SELECT * FROM listings").fetchall()
    assert len(rows) == 1
    row = dict(rows[0])
    assert row["price"] == 15000
    assert row["km"] == 120000
    assert row["hash"]
    assert row["created_at"] == iso(ts)
    assert row["updated_at"] == iso(ts)
    assert row["last_seen"] == iso(ts)


def test_upsert_many_tracks_changes_and_updates_last_seen():
    conn = make_connection()
    ts1 = datetime(2024, 1, 5, 13, 0, 0)
    ts2 = datetime(2024, 1, 7, 14, 30, 0)

    sqlite_store.upsert_many(conn, [base_listing()], scraper.CSV_FIELDNAMES, timestamp=ts1)
    sqlite_store.upsert_many(
        conn,
        [base_listing({"price": 15500, "km": 125000})],
        scraper.CSV_FIELDNAMES,
        timestamp=ts2,
    )

    row = dict(conn.execute("SELECT * FROM listings WHERE id = ?", ("abc",)).fetchone())
    assert row["price"] == 15500
    assert row["km"] == 125000
    assert row["created_at"] == iso(ts1)
    assert row["updated_at"] == iso(ts2)
    assert row["last_seen"] == iso(ts2)

    changes = conn.execute(
        "SELECT field, old_value, new_value, change_type, changed_at FROM listing_changes WHERE listing_id = ? ORDER BY id",
        ("abc",),
    ).fetchall()
    assert {c[0] for c in changes} == {"price", "km"}
    assert {c[3] for c in changes} == {"price", "km"}
    assert all(c[4] == iso(ts2) for c in changes)
    price_change = next(c for c in changes if c[0] == "price")
    assert price_change[1] == "15000"
    assert price_change[2] == "15500"


def test_upsert_many_skips_null_overwrites_and_keeps_updated_at():
    conn = make_connection()
    ts1 = datetime(2024, 1, 5, 13, 0, 0)
    ts2 = datetime(2024, 1, 6, 15, 0, 0)

    sqlite_store.upsert_many(conn, [base_listing()], scraper.CSV_FIELDNAMES, timestamp=ts1)
    sqlite_store.upsert_many(
        conn,
        [base_listing({"km": None, "price": 15000})],
        scraper.CSV_FIELDNAMES,
        timestamp=ts2,
    )

    row = dict(conn.execute("SELECT * FROM listings WHERE id = ?", ("abc",)).fetchone())
    assert row["km"] == 120000
    assert row["price"] == 15000
    assert row["updated_at"] == iso(ts1)
    assert row["last_seen"] == iso(ts2)

    change_rows = conn.execute("SELECT COUNT(*) FROM listing_changes").fetchone()[0]
    assert change_rows == 0


def test_fetch_make_model_stats_respects_filters(monkeypatch):
    conn = make_connection()
    now = datetime(2024, 1, 10, 12, 0, 0)

    class FixedDateTime(datetime):
        @classmethod
        def utcnow(cls):
            return now

    monkeypatch.setattr(sqlite_store, "datetime", FixedDateTime)

    recent = now - timedelta(days=1)
    older = now - timedelta(days=9)

    sqlite_store.upsert_many(
        conn,
        [
            base_listing({"id": "a", "make": "VW", "model": "Golf", "price": 15000}),
            base_listing({"id": "b", "make": "VW", "model": "Golf", "price": 800}),
            base_listing({"id": "c", "make": "Toyota", "model": "Aygo", "price": 6500}),
        ],
        scraper.CSV_FIELDNAMES,
        timestamp=recent,
    )
    sqlite_store.upsert_many(
        conn,
        [base_listing({"id": "d", "make": "VW", "model": "Polo", "price": 4000})],
        scraper.CSV_FIELDNAMES,
        timestamp=older,
    )

    stats = sqlite_store.fetch_make_model_stats(
        conn,
        min_price=1000,
        days=5,
        search="golf",
    )

    assert ("VW", "Golf", "Diesel") in stats
    golf_stats = stats[("VW", "Golf", "Diesel")]
    assert golf_stats["count_total"] == 2
    assert golf_stats["count_for_avg"] == 1
    assert golf_stats["sum"] == 15000
    assert ("VW", "Polo", "Diesel") not in stats


def test_fetch_model_year_stats_ignores_missing_years(monkeypatch):
    conn = make_connection()
    now = datetime(2024, 1, 5, 10, 0, 0)

    class FixedDateTime(datetime):
        @classmethod
        def utcnow(cls):
            return now

    monkeypatch.setattr(sqlite_store, "datetime", FixedDateTime)

    sqlite_store.upsert_many(
        conn,
        [
            base_listing({"id": "a", "year": 2020, "price": 9000}),
            base_listing({"id": "b", "year": None, "price": 9500}),
            base_listing({"id": "c", "year": 2020, "price": 500}),
        ],
        scraper.CSV_FIELDNAMES,
        timestamp=now,
    )

    stats = sqlite_store.fetch_model_year_stats(conn, min_price=1000)
    key = ("VW", "Golf", "Diesel", 2020)
    assert key in stats
    entry = stats[key]
    assert entry["count_total"] == 2
    assert entry["count_for_avg"] == 1
    assert entry["sum"] == 9000


def test_fetch_recent_price_changes_returns_deserialized_values():
    conn = make_connection()
    ts1 = datetime(2024, 1, 5, 13, 0, 0)
    ts2 = datetime(2024, 1, 6, 13, 0, 0)
    sqlite_store.upsert_many(conn, [base_listing()], scraper.CSV_FIELDNAMES, timestamp=ts1)
    sqlite_store.upsert_many(
        conn,
        [base_listing({"price": 14900})],
        scraper.CSV_FIELDNAMES,
        timestamp=ts2,
    )

    changes = sqlite_store.fetch_recent_price_changes(conn, limit=1)
    assert len(changes) == 1
    assert changes[0]["old_price"] == 15000
    assert changes[0]["new_price"] == 14900
