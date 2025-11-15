import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from scraperReklama5 import clean_price


def test_clean_price_handles_thousand_separator_with_comma():
    assert clean_price("6,500 €") == 6500


def test_clean_price_handles_nbsp_and_decimal_comma():
    price_text = "12\u00a0345,00 €"
    assert clean_price(price_text) == 12345


def test_clean_price_returns_none_for_text_values():
    assert clean_price("По договор") is None
