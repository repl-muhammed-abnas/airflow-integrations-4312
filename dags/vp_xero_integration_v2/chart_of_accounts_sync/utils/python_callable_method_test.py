"""Unit tests for pure helpers in chart_of_accounts_sync."""
from unittest import TestCase

from vp_xero_integration_v2.chart_of_accounts_sync.utils.python_callable_method import (
    _s,
    _truncate_name,
    map_xero_type_to_vp,
)


class TestChartOfAccountsHelpers(TestCase):

    def test_s_none_returns_empty(self):
        self.assertEqual(_s(None), '')

    def test_s_coerces_numeric(self):
        self.assertEqual(_s(9), '9')
        self.assertEqual(_s(3.14), '3.14')

    def test_s_strips_whitespace(self):
        self.assertEqual(_s('  hello  '), 'hello')

    def test_truncate_name_caps_at_39(self):
        self.assertEqual(len(_truncate_name('X' * 50)), 39)
        self.assertEqual(_truncate_name('short'), 'short')
        self.assertEqual(_truncate_name(None), '')

    def test_map_xero_type_to_vp_known(self):
        result = map_xero_type_to_vp('REVENUE')
        self.assertIsNotNone(result)
        self.assertNotEqual(result, '')

    def test_map_xero_type_to_vp_case_insensitive(self):
        self.assertEqual(map_xero_type_to_vp('revenue'), map_xero_type_to_vp('REVENUE'))

    def test_map_xero_type_to_vp_unknown_defaults_to_1(self):
        self.assertEqual(map_xero_type_to_vp('UNKNOWN_XERO_TYPE'), '1')

    def test_map_xero_type_to_vp_none_defaults(self):
        self.assertEqual(map_xero_type_to_vp(None), '1')
