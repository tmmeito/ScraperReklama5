import sqlite3
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from scraperReklama5 import (
    CSV_FIELDNAMES,
    DETAIL_ONLY_FIELDS,
    STATUS_UNCHANGED,
    classify_listing_status,
)
from storage import sqlite_store


def build_listing(**overrides):
    base = {name: None for name in CSV_FIELDNAMES}
    base.update(
        {
            "id": "listing-1",
            "link": "https://www.reklama5.mk/AdDetails?ad=listing-1",
            "make": "VW",
            "model": "Golf",
            "year": 2018,
            "price": 9500,
            "km": 120000,
            "kw": 85,
            "ps": 115,
            "date": "2024-04-01 12:00",
            "city": "Skopje",
            "promoted": False,
        }
    )
    base.update(overrides)
    return base


def test_classify_listing_status_ignores_detail_only_fields():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    sqlite_store.init_schema(conn, CSV_FIELDNAMES)

    stored_listing = build_listing(
        fuel="Diesel",
        gearbox="Automatik",
        body="Kombi",
        color="Schwarz",
        registration="MK",
        reg_until="2025-01",
        emission_class="Euro 6",
    )
    sqlite_store.upsert_many(conn, [stored_listing], CSV_FIELDNAMES)

    followup_listing = build_listing()
    for field in DETAIL_ONLY_FIELDS:
        followup_listing[field] = None

    listings = [followup_listing]
    classify_listing_status(listings, conn)

    assert listings[0]["_status"] == STATUS_UNCHANGED
    assert listings[0]["_status_changes"] == {}


def test_classify_listing_status_keeps_numeric_details_when_missing_in_overview():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    sqlite_store.init_schema(conn, CSV_FIELDNAMES)

    stored_listing = build_listing(km=120_000, kw=85, ps=115)
    sqlite_store.upsert_many(conn, [stored_listing], CSV_FIELDNAMES)

    followup_listing = build_listing(km=None, kw=None, ps=None)

    classify_listing_status([followup_listing], conn)

    assert followup_listing["_status"] == STATUS_UNCHANGED
    assert followup_listing["_status_changes"] == {}


def test_classify_listing_status_detects_real_numeric_changes():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    sqlite_store.init_schema(conn, CSV_FIELDNAMES)

    stored_listing = build_listing(km=120_000, kw=85, ps=115)
    sqlite_store.upsert_many(conn, [stored_listing], CSV_FIELDNAMES)

    followup_listing = build_listing(km=121_000)

    classify_listing_status([followup_listing], conn)

    assert followup_listing["_status"] != STATUS_UNCHANGED
    assert followup_listing["_status_changes"].get("km") == {
        "old": 120_000,
        "new": 121_000,
    }


def test_classify_listing_status_ignores_minor_date_drift():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    sqlite_store.init_schema(conn, CSV_FIELDNAMES)

    stored_listing = build_listing(date="2024-05-01 12:00")
    sqlite_store.upsert_many(conn, [stored_listing], CSV_FIELDNAMES)

    followup_listing = build_listing(date="2024-05-01 12:30")

    classify_listing_status([followup_listing], conn)

    assert followup_listing["_status"] == STATUS_UNCHANGED
    assert followup_listing["_status_changes"] == {}


def test_classify_listing_status_flags_large_date_changes():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    sqlite_store.init_schema(conn, CSV_FIELDNAMES)

    stored_listing = build_listing(date="2024-05-01 12:00")
    sqlite_store.upsert_many(conn, [stored_listing], CSV_FIELDNAMES)

    followup_listing = build_listing(date="2024-05-03 13:00")

    classify_listing_status([followup_listing], conn)

    assert followup_listing["_status"] != STATUS_UNCHANGED
    assert followup_listing["_status_changes"].get("date") == {
        "old": "2024-05-01 12:00",
        "new": "2024-05-03 13:00",
    }


def test_classify_listing_status_detects_city_changes():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    sqlite_store.init_schema(conn, CSV_FIELDNAMES)

    stored_listing = build_listing(city="Skopje")
    sqlite_store.upsert_many(conn, [stored_listing], CSV_FIELDNAMES)

    followup_listing = build_listing(city="Bitola")

    classify_listing_status([followup_listing], conn)

    assert followup_listing["_status"] != STATUS_UNCHANGED
    assert followup_listing["_status_changes"].get("city") == {
        "old": "Skopje",
        "new": "Bitola",
    }
