"""Unit tests for pure helpers in poll_contact_updates_sync."""
from datetime import datetime, timezone
from unittest import TestCase

from vp_xero_integration_v2.poll_contact_updates_sync.utils.python_callable_method import (
    _parse_iso,
)


class TestPollContactUpdatesHelpers(TestCase):

    def test_parse_iso_with_z(self):
        result = _parse_iso('2024-01-15T10:30:00Z')
        self.assertEqual(result, datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc))

    def test_parse_iso_without_z(self):
        result = _parse_iso('2024-01-15T10:30:00')
        self.assertEqual(result, datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc))

    def test_parse_iso_with_ms(self):
        result = _parse_iso('2024-01-15T10:30:00.000')
        self.assertEqual(result, datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc))

    def test_parse_iso_bare_date(self):
        result = _parse_iso('2024-01-15')
        self.assertEqual(result, datetime(2024, 1, 15, tzinfo=timezone.utc))

    def test_parse_iso_invalid_raises(self):
        with self.assertRaises(ValueError):
            _parse_iso('not-a-date')

    def test_parse_iso_newer_than_older(self):
        older = _parse_iso('2024-01-01T00:00:00Z')
        newer = _parse_iso('2024-06-01T00:00:00Z')
        self.assertGreater(newer, older)
