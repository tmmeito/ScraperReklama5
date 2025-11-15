import csv
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import scraperReklama5 as scraper


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
    assert "2 (1 / -)" in captured


def test_display_summary_counts_missing_price_entries(capfd):
    rows = [
        {
            "make": "Test",
            "model": "Car",
            "fuel": "Diesel",
            "price": None,
        },
        {
            "make": "Test",
            "model": "Car",
            "fuel": "Diesel",
            "price": None,
        },
        {
            "make": "Test",
            "model": "Car",
            "fuel": "Diesel",
            "price": 1500,
        },
    ]

    scraper.display_make_model_summary(rows, min_price_for_avg=500, top_n=1)

    captured = capfd.readouterr().out
    assert "2 (1 / -)" in captured


def test_display_summary_counts_missing_price_entries(capfd):
    rows = [
        {
            "make": "Test",
            "model": "Car",
            "fuel": "Diesel",
            "price": None,
        },
        {
            "make": "Test",
            "model": "Car",
            "fuel": "Diesel",
            "price": None,
        },
        {
            "make": "Test",
            "model": "Car",
            "fuel": "Diesel",
            "price": 1500,
        },
    ]

    scraper.display_make_model_summary(rows, min_price_for_avg=500, top_n=1)

    captured = capfd.readouterr().out
    assert "3 (- / 2)" in captured


def test_display_avg_price_counts_missing_values(capfd):
    rows = [
        {
            "make": "Test",
            "model": "Car",
            "fuel": "Diesel",
            "year": 2010,
            "price": None,
        },
        {
            "make": "Test",
            "model": "Car",
            "fuel": "Diesel",
            "year": 2010,
            "price": 900,
        },
        {
            "make": "Test",
            "model": "Car",
            "fuel": "Diesel",
            "year": 2011,
            "price": 2200,
        },
        {
            "make": "Test",
            "model": "Car",
            "fuel": "Diesel",
            "year": None,
            "price": 2300,
        },
    ]

    scraper.display_avg_price_by_model_year(
        rows,
        min_listings=0,
        min_price_for_avg=1500,
    )

    captured = capfd.readouterr().out
    assert "2010" in captured
    assert "2 (1 / 1 / -)" in captured
    assert "1 (- / - / 1)" in captured
