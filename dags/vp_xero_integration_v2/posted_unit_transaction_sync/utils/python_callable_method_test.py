"""Unit tests for pure helpers in posted_unit_transaction_sync."""
from unittest import TestCase

from vp_xero_integration_v2.posted_unit_transaction_sync.utils.python_callable_method import (
    _format_xero_date,
    _line_from_row,
)


class TestPostedUnitTransactionSyncHelpers(TestCase):

    def test_format_xero_date_strips_time(self):
        self.assertEqual(_format_xero_date('2024-03-15T10:00:00'), '2024-03-15')

    def test_format_xero_date_bare_date(self):
        self.assertEqual(_format_xero_date('2024-03-15'), '2024-03-15')

    def test_format_xero_date_none(self):
        self.assertIsNone(_format_xero_date(None))
        self.assertIsNone(_format_xero_date(''))

    def test_line_from_row_basic(self):
        row = {'XeroCode': '500', 'Amount': 200.0, 'Desc2': 'Units'}
        result = _line_from_row(row)
        self.assertEqual(result['AccountCode'], '500')
        self.assertEqual(result['LineAmount'], 200.0)
        self.assertEqual(result['Description'], 'Units')

    def test_line_from_row_falls_back_to_desc1(self):
        row = {'XeroCode': '500', 'Amount': 10.0, 'Desc1': 'Backup'}
        result = _line_from_row(row)
        self.assertEqual(result['Description'], 'Backup')

    def test_line_from_row_bad_amount_defaults_zero(self):
        row = {'XeroCode': '500', 'Amount': None}
        result = _line_from_row(row)
        self.assertEqual(result['LineAmount'], 0.0)
