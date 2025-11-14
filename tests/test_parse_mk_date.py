import sys
from datetime import datetime
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import scraperReklama5 as sr


class FixedDateTime(datetime):
    """Helper that allows patching scraperReklama5.datetime.now()."""

    @classmethod
    def now(cls, tz=None):  # pragma: no cover - deterministic helper
        return cls(2024, 1, 5, 12, 0, 0, tzinfo=tz)


class ParseMkDateTests(TestCase):
    def test_rolls_back_year_when_future_date_detected(self):
        with patch.object(sr, "datetime", FixedDateTime):
            result = sr.parse_mk_date("31 дек 23:45")
        self.assertEqual(result, datetime(2023, 12, 31, 23, 45))

    def test_keeps_current_year_for_recent_dates(self):
        with patch.object(sr, "datetime", FixedDateTime):
            result = sr.parse_mk_date("5 јан 11:15")
        self.assertEqual(result, datetime(2024, 1, 5, 11, 15))

    def test_handles_yesterday_keyword_with_time(self):
        with patch.object(sr, "datetime", FixedDateTime):
            result = sr.parse_mk_date("вчера 08:30")
        self.assertEqual(result, datetime(2024, 1, 4, 8, 30))


if __name__ == "__main__":
    import unittest

    unittest.main()
