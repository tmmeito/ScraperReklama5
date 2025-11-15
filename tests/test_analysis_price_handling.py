import csv
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import scraperReklama5 as scraper
from storage import sqlite_store


def _write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=scraper.CSV_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _base_row():
    return {field: "" for field in scraper.CSV_FIELDNAMES}


def test_load_rows_from_csv_sets_empty_price_to_zero(tmp_path, monkeypatch):
    csv_path = tmp_path / "cars.csv"
    empty_price_row = _base_row()
    empty_price_row.update({"id": "1", "make": "Test", "model": "Car", "price": ""})
    price_row = _base_row()
    price_row.update({"id": "2", "make": "Test", "model": "Car", "price": "1800"})
    _write_csv(csv_path, [empty_price_row, price_row])

    monkeypatch.setattr(scraper, "OUTPUT_CSV", str(csv_path))

    rows = scraper.load_rows_from_csv()

    assert rows[0]["price"] == 0
    assert rows[1]["price"] == 1800


def test_aggregate_data_ignores_empty_price_entries(tmp_path, monkeypatch):
    csv_path = tmp_path / "cars.csv"
    agg_path = tmp_path / "agg.json"
    empty_price_row = _base_row()
    empty_price_row.update({"id": "1", "make": "Test", "model": "Car", "price": ""})
    price_row = _base_row()
    price_row.update({"id": "2", "make": "Test", "model": "Car", "price": "2000"})
    _write_csv(csv_path, [empty_price_row, price_row])

    monkeypatch.setattr(scraper, "OUTPUT_CSV", str(csv_path))
    monkeypatch.setattr(scraper, "OUTPUT_AGG", str(agg_path))

    result = scraper.aggregate_data()

    entry = result["Test Car"]
    assert entry["count_total"] == 2
    assert entry["count_with_price"] == 1
    assert entry["avg_price"] == 2000.0


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


def test_display_summary_counts_empty_price_as_low_price(capfd):
    rows = [
        {
            "make": "Test",
            "model": "Car",
            "fuel": "Diesel",
            "price": 0,
        },
        {
            "make": "Test",
            "model": "Car",
            "fuel": "Diesel",
            "price": 2000,
        },
    ]

    analysis = scraper.AnalysisData(rows)
    scraper.display_make_model_summary(analysis, min_price_for_avg=500, top_n=1)

    captured = capfd.readouterr().out
    assert "2 (1)" in captured


def test_analysis_data_avoids_multiple_scans_for_repeated_calls(capfd):
    class CountingList(list):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.iterations = 0

        def __iter__(self):
            self.iterations += 1
            return super().__iter__()

    rows = CountingList(
        [
            {"make": "Test", "model": "Car", "fuel": "Diesel", "price": 1000, "year": 2020},
            {"make": "Test", "model": "Car", "fuel": "Diesel", "price": 1500, "year": 2021},
            {"make": "Test", "model": "Car", "fuel": "Diesel", "price": 300, "year": 2020},
        ]
    )

    analysis = scraper.AnalysisData(rows)
    assert rows.iterations == 1

    scraper.display_make_model_summary(analysis, min_price_for_avg=0, top_n=1)
    scraper.display_avg_price_by_model_year(analysis, min_listings=1, min_price_for_avg=0)
    scraper.display_make_model_summary(analysis, min_price_for_avg=1000, top_n=1)

    assert rows.iterations == 1
