import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import scraperReklama5 as scraper
from storage import sqlite_store


def test_aggregate_data_from_db_respects_filters(tmp_path):
    db_path = tmp_path / "cars.db"
    conn = sqlite_store.open_database(str(db_path))
    sqlite_store.init_schema(conn, scraper.CSV_FIELDNAMES)
    sqlite_store.upsert_many(
        conn,
        [
            {
                "id": "1",
                "link": "http://example.com/1",
                "make": "VW",
                "model": "Golf",
                "fuel": "Diesel",
                "price": 15000,
                "year": 2020,
                "date": "01 јан 12:00",
                "city": "Skopje",
                "promoted": 0,
            },
            {
                "id": "2",
                "link": "http://example.com/2",
                "make": "Toyota",
                "model": "Aygo",
                "fuel": "Benzin",
                "price": 7000,
                "year": 2019,
                "date": "01 јан 12:00",
                "city": "Skopje",
                "promoted": 0,
            },
        ],
        scraper.CSV_FIELDNAMES,
    )
    agg_path = tmp_path / "agg.json"

    result = scraper.aggregate_data(
        db_connection=conn,
        output_json=str(agg_path),
        search_term="aygo",
    )

    assert "Toyota Aygo" in result
    assert "VW Golf" not in result
    conn.close()


def test_display_summary_counts_empty_price_as_low_price(capfd):
    stats = {
        ("Test", "Car", "Diesel"): {
            "count_total": 2,
            "count_for_avg": 1,
            "sum": 2000,
            "excluded_low_price": 1,
        }
    }

    scraper.display_make_model_summary(stats, min_price_for_avg=500, top_n=1)

    captured = capfd.readouterr().out
    assert "2 (1)" in captured
    assert "2 000" in captured


def test_display_avg_price_by_model_year_from_sqlite_stats(capfd):
    stats = {
        ("Test", "Car", "Diesel", 2020): {
            "count_total": 3,
            "count_for_avg": 2,
            "sum": 5000,
            "excluded_low_price": 1,
        }
    }

    scraper.display_avg_price_by_model_year(
        stats, min_listings=1, min_price_for_avg=500
    )

    captured = capfd.readouterr().out
    assert "Test" in captured
    assert "2020" in captured
    assert "3 (1)" in captured
