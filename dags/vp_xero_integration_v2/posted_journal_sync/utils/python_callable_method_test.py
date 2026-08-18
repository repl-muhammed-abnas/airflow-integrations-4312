"""Unit tests for pure helpers in posted_journal_sync."""
from unittest import TestCase

from vp_xero_integration_v2.posted_journal_sync.utils.python_callable_method import (
    _format_xero_date,
    _line_from_row,
)


class TestPostedJournalSyncHelpers(TestCase):

    def test_format_xero_date_strips_time(self):
        self.assertEqual(_format_xero_date('2024-03-15T10:00:00'), '2024-03-15')

    def test_format_xero_date_bare_date(self):
        self.assertEqual(_format_xero_date('2024-03-15'), '2024-03-15')

    def test_format_xero_date_none(self):
        self.assertIsNone(_format_xero_date(None))
        self.assertIsNone(_format_xero_date(''))

    def test_line_from_row_basic(self):
        row = {'XeroCode': '400', 'Amount': 100.0, 'Desc2': 'Labour'}
        result = _line_from_row(row)
        self.assertEqual(result['AccountCode'], '400')
        self.assertEqual(result['LineAmount'], 100.0)
        self.assertEqual(result['Description'], 'Labour')

    def test_line_from_row_falls_back_to_desc1(self):
        row = {'XeroCode': '400', 'Amount': 50.0, 'Desc1': 'Fallback'}
        result = _line_from_row(row)
        self.assertEqual(result['Description'], 'Fallback')

    def test_line_from_row_bad_amount_defaults_zero(self):
        row = {'XeroCode': '400', 'Amount': 'bad'}
        result = _line_from_row(row)
        self.assertEqual(result['LineAmount'], 0.0)

    def test_line_from_row_negative_amount_preserved(self):
        row = {'XeroCode': '200', 'Amount': -75.5}
        result = _line_from_row(row)
        self.assertEqual(result['LineAmount'], -75.5)
