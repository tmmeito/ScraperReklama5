import sqlite3
import sys
from datetime import datetime
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
