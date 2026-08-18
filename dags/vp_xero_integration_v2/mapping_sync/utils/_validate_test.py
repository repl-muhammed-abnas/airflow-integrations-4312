"""
Unit tests for the cursor-level referential validators in `_validate`. Each
validator takes an open sqlite cursor + live id-sets, so we drive them against
an in-memory sqlite DB (no Airflow / S3). The orchestration wrappers
(run_all_mapping_validations / summarize_mapping_validations) are exercised
against dev-airflow.
"""
import sqlite3
from unittest import TestCase

from vp_xero_integration_v2.mapping_sync.utils._validate import (
    _validate_map_firm_with_cursor,
    _validate_map_chart_of_accounts_with_cursor,
    _validate_map_tax_code_with_cursor,
)
from vp_xero_integration_v2.common.tables import (
    MAP_FIRM_TABLE_NAME, MAP_FIRM_COLUMNS,
    MAP_CHART_OF_ACCOUNTS_TABLE_NAME, MAP_CHART_OF_ACCOUNTS_COLUMNS,
    MAP_TAX_CODE_TABLE_NAME, MAP_TAX_CODE_COLUMNS,
)


def _make_table(cur, name, columns, rows):
    cur.execute(f"CREATE TABLE {name} ({', '.join(columns)})")
    if rows:
        placeholders = ', '.join('?' * len(columns))
        cur.executemany(
            f"INSERT INTO {name} ({', '.join(columns)}) VALUES ({placeholders})",
            rows)


def _has_hard_fail(result, check):
    return any(i['check'] == check and i['severity'] == 'hard_fail'
               for i in result['issues'])


class TestValidateFirm(TestCase):
    def _cur(self, rows):
        conn = sqlite3.connect(':memory:')
        _make_table(conn.cursor(), MAP_FIRM_TABLE_NAME, MAP_FIRM_COLUMNS, rows)
        return conn.cursor()

    def test_empty_table_is_warning_not_hard_fail(self):
        # Workato Q-V1: firm validator always returned blank (never blocked the
        # pipeline). Empty firm map after a successful sync is acceptable.
        result = _validate_map_firm_with_cursor(self._cur([]), set(), set())
        self.assertEqual(result['total'], 0)
        self.assertEqual(len(result['issues']), 1)
        self.assertEqual(result['issues'][0]['check'], 'empty_table')
        self.assertEqual(result['issues'][0]['severity'], 'warning')
        self.assertFalse(_has_hard_fail(result, 'empty_table'))

    def test_all_resolve_clean(self):
        # FirmID, ContactID, Status, Vendor, Client, XeroName, VantagepointName, ModDate
        rows = [('100', 'c1', 'ACTIVE', '', '', 'Acme', 'Acme', '')]
        result = _validate_map_firm_with_cursor(
            self._cur(rows), {'c1'}, {'100'})
        self.assertEqual(result['issues'], [])
        self.assertEqual(result['valid'], 1)

    def test_dangling_contact_and_firm(self):
        rows = [('100', 'c1', 'ACTIVE', '', '', 'Acme', 'Acme', '')]
        result = _validate_map_firm_with_cursor(self._cur(rows), set(), set())
        self.assertTrue(_has_hard_fail(result, 'dangling_xero_contact'))
        self.assertTrue(_has_hard_fail(result, 'dangling_vp_firm'))

    def test_archived_rows_skipped(self):
        rows = [('100', 'c1', 'ARCHIVED', '', '', 'Acme', 'Acme', '')]
        # Even with empty live sets, ARCHIVED rows aren't flagged as dangling.
        result = _validate_map_firm_with_cursor(self._cur(rows), set(), set())
        self.assertEqual(result['issues'], [])


class TestValidateAccounts(TestCase):
    def _cur(self, rows):
        conn = sqlite3.connect(':memory:')
        _make_table(conn.cursor(), MAP_CHART_OF_ACCOUNTS_TABLE_NAME,
                    MAP_CHART_OF_ACCOUNTS_COLUMNS, rows)
        return conn.cursor()

    def test_clean_and_dangling(self):
        # XeroCode, XeroName, XeroType, VPCode, VPName, VPType, XeroID, Messages
        rows = [('200', 'Sales', 'REVENUE', '200', 'Sales', '4', 'x1', '')]
        clean = _validate_map_chart_of_accounts_with_cursor(
            self._cur(rows), {'200'}, {'x1'})
        self.assertEqual(clean['issues'], [])

        dangling = _validate_map_chart_of_accounts_with_cursor(
            self._cur(rows), set(), set())
        self.assertTrue(_has_hard_fail(dangling, 'dangling_vp_account'))
        self.assertTrue(_has_hard_fail(dangling, 'dangling_xero_account'))


class TestValidateTax(TestCase):
    def _cur(self, rows):
        conn = sqlite3.connect(':memory:')
        _make_table(conn.cursor(), MAP_TAX_CODE_TABLE_NAME,
                    MAP_TAX_CODE_COLUMNS, rows)
        return conn.cursor()

    def test_clean_and_dangling(self):
        # XeroName, XeroCode, VPCode, Rate, CompoundOnCode, Sequence, Messages
        rows = [('GST on Income', 'GST', 'X0001', '15', '', '1', '')]
        clean = _validate_map_tax_code_with_cursor(
            self._cur(rows), {'X0001'}, {('GST on Income', 'GST')})
        self.assertEqual(clean['issues'], [])

        dangling = _validate_map_tax_code_with_cursor(
            self._cur(rows), set(), set())
        self.assertTrue(_has_hard_fail(dangling, 'dangling_vp_tax_code'))
        self.assertTrue(_has_hard_fail(dangling, 'dangling_xero_rate_component'))
