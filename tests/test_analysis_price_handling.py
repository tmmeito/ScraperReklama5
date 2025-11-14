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


def test_load_rows_from_csv_treats_empty_price_as_missing(tmp_path, monkeypatch):
    csv_path = tmp_path / "cars.csv"
    empty_price_row = _base_row()
    empty_price_row.update({"id": "1", "make": "Test", "model": "Car", "price": ""})
    price_row = _base_row()
    price_row.update({"id": "2", "make": "Test", "model": "Car", "price": "1800"})
    _write_csv(csv_path, [empty_price_row, price_row])

    monkeypatch.setattr(scraper, "OUTPUT_CSV", str(csv_path))

    rows = scraper.load_rows_from_csv()

    assert rows[0]["price"] is None
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
