"""
Unit tests for the pure helper functions in `_account_sync` (Xero → VP chart of
accounts). Covers the deterministic body/row/SQL logic without Airflow or VP
round-trips; the full engine (`sync_xero_accounts_to_vp`) is exercised against
dev-airflow.
"""
from unittest import TestCase

from vp_xero_integration_v2.mapping_sync.utils._account_sync import (
    _truncate_name,
    _build_map_row,
    build_vp_account_create_body,
    build_vp_account_update_body,
    COMPILE_ACCOUNT_CODES_SQL,
    CHART_OF_ACCOUNTS_MAP_STAGING_COLUMNS,
)
from vp_xero_integration_v2.common.tables import MAP_CHART_OF_ACCOUNTS_COLUMNS


class TestAccountSyncHelpers(TestCase):

    def test_truncate_name_caps_at_39(self):
        long = 'X' * 50
        self.assertEqual(len(_truncate_name(long)), 39)
        self.assertEqual(_truncate_name('short'), 'short')
        self.assertEqual(_truncate_name(None), '')

    def test_create_body_fields(self):
        body = build_vp_account_create_body('200', 'Sales Revenue Account', '4')
        self.assertEqual(body['Account'], '200')
        self.assertEqual(body['Name'], 'Sales Revenue Account')
        self.assertEqual(body['Type'], '4')
        self.assertEqual(body['Status'], 'A')
        self.assertEqual(body['Detail'], '1')
        # balancing + QBOAccountID columns sent blank (present, not omitted).
        for col in ('CashBasisAccount', 'UnrealizedLossAccount',
                    'UnrealizedGainAccount', 'CashBasisRevaluation', 'QBOAccountID'):
            self.assertIn(col, body)
            self.assertEqual(body[col], '')

    def test_create_body_truncates_name(self):
        body = build_vp_account_create_body('200', 'N' * 60, '1')
        self.assertEqual(len(body['Name']), 39)

    def test_update_body_status_active(self):
        body = build_vp_account_update_body('Acct', '4', 'ACTIVE')
        self.assertEqual(body['Status'], 'A')
        self.assertNotIn('Account', body)  # carried by the URL on PUT

    def test_update_body_status_archived(self):
        self.assertEqual(
            build_vp_account_update_body('Acct', '4', 'ARCHIVED')['Status'], 'I')
        self.assertEqual(
            build_vp_account_update_body('Acct', '4', '')['Status'], 'I')

    def test_build_map_row_has_all_columns(self):
        row = _build_map_row(
            xero_code='200', xero_name='Sales', xero_type='REVENUE',
            vp_code='200', vp_name='Sales', vp_type='4',
            xero_id='abc-1', messages='',
        )
        self.assertEqual(sorted(row.keys()), sorted(MAP_CHART_OF_ACCOUNTS_COLUMNS))
        self.assertEqual(row['XeroID'], 'abc-1')

    def test_compile_sql_excludes_bank_and_keys_on_xero(self):
        sql = COMPILE_ACCOUNT_CODES_SQL.upper()
        self.assertIn("!= 'BANK'", sql)
        self.assertIn('FROM XERO_ACCOUNTS', sql)
        self.assertIn('LEFT JOIN VP_ACCOUNTS', sql)
        self.assertIn('LEFT JOIN CHART_OF_ACCOUNTS_MAP', sql)

    def test_map_staging_columns_carry_entry_id(self):
        self.assertIn('EntryID', CHART_OF_ACCOUNTS_MAP_STAGING_COLUMNS)
        self.assertIn('XeroID', CHART_OF_ACCOUNTS_MAP_STAGING_COLUMNS)
