"""Unit tests for pure helpers in ap_voucher_sync."""
from unittest import TestCase

from vp_xero_integration_v2.ap_voucher_sync.utils.python_callable_method import (
    _amount_str,
    _normalize_date,
)


class TestApVoucherHelpers(TestCase):

    def test_amount_str_none_returns_zero(self):
        self.assertEqual(_amount_str(None), '0')
        self.assertEqual(_amount_str(''), '0')

    def test_amount_str_integer(self):
        self.assertEqual(_amount_str(100), '100')

    def test_amount_str_float(self):
        self.assertEqual(_amount_str(1.5), '1.5')

    def test_amount_str_string_decimal(self):
        self.assertEqual(_amount_str('19.99'), '19.99')

    def test_amount_str_invalid_returns_zero(self):
        self.assertEqual(_amount_str('abc'), '0')
        self.assertEqual(_amount_str('1.2.3'), '0')

    def test_normalize_date_iso_datetime(self):
        self.assertEqual(_normalize_date('2024-03-15T10:00:00'), '2024-03-15')

    def test_normalize_date_bare_date(self):
        self.assertEqual(_normalize_date('2024-03-15'), '2024-03-15')

    def test_normalize_date_empty(self):
        self.assertEqual(_normalize_date(''), '')
        self.assertEqual(_normalize_date(None), '')
