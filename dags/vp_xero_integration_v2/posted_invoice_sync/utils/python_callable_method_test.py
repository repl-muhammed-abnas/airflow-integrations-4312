"""Unit tests for pure helpers in posted_invoice_sync."""
from unittest import TestCase

from vp_xero_integration_v2.posted_invoice_sync.utils.python_callable_method import (
    _normalise_account,
    _build_invoice_number,
    _build_vp_tax_codes_map,
    _compute_effective_rate,
)


class TestPostedInvoiceHelpers(TestCase):

    # _normalise_account
    def test_normalise_account_strips_dot_zero_zero(self):
        self.assertEqual(_normalise_account('121.00'), '121')

    def test_normalise_account_preserves_real_decimal(self):
        self.assertEqual(_normalise_account('122.05'), '122.05')

    def test_normalise_account_plain_string(self):
        self.assertEqual(_normalise_account('REV'), 'REV')

    def test_normalise_account_none(self):
        self.assertEqual(_normalise_account(None), '')

    # _build_invoice_number
    def test_build_invoice_number(self):
        self.assertEqual(_build_invoice_number('INV001', '2024-01', '5'), 'INV001.2024-01.5')

    # _build_vp_tax_codes_map
    def test_build_vp_tax_codes_map_basic(self):
        rows = [{'Code': 'GST', 'Rate': 10, 'CompoundOnTaxCode': ''}]
        result = _build_vp_tax_codes_map(rows)
        self.assertIn('GST', result)
        self.assertEqual(result['GST']['Rate'], 10.0)

    def test_build_vp_tax_codes_map_empty(self):
        self.assertEqual(_build_vp_tax_codes_map([]), {})
        self.assertEqual(_build_vp_tax_codes_map(None), {})

    def test_build_vp_tax_codes_map_skips_no_code(self):
        rows = [{'Code': '', 'Rate': 5}]
        self.assertEqual(_build_vp_tax_codes_map(rows), {})

    # _compute_effective_rate
    def test_compute_effective_rate_simple(self):
        tc_map = {'GST': {'Rate': 10.0, 'CompoundOnTaxCode': ''}}
        self.assertAlmostEqual(_compute_effective_rate('GST', tc_map), 0.1)

    def test_compute_effective_rate_compound(self):
        tc_map = {
            'PST': {'Rate': 8.0, 'CompoundOnTaxCode': 'GST'},
            'GST': {'Rate': 5.0, 'CompoundOnTaxCode': ''},
        }
        # (1.08) * (1.05) - 1 = 0.134
        result = _compute_effective_rate('PST', tc_map)
        self.assertAlmostEqual(result, 0.134, places=4)

    def test_compute_effective_rate_unknown_code(self):
        self.assertEqual(_compute_effective_rate('UNKNOWN', {}), 0.0)

    def test_compute_effective_rate_blank_code(self):
        self.assertEqual(_compute_effective_rate('', {}), 0.0)
        self.assertEqual(_compute_effective_rate(None, {}), 0.0)
