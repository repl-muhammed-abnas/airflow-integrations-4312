"""
Unit tests for `_tax_code_sync` (Xero → VP tax codes).

Covers the load-bearing flatten transform, the VP-code generator, the
ReverseCharge / rate-diff logic, body/row builders, and the compile-SQL fix —
without Airflow or VP round-trips.

`TestSyncAlreadyExistsAdoption` drives the already-exists adoption branch in
`sync_xero_tax_codes_to_vp` via mocked operators. The happy-path multi-tenant
end-to-end flow is exercised against dev-airflow.
"""
import logging
from unittest import TestCase
from unittest.mock import MagicMock, patch

from vp_xero_integration_v2.mapping_sync.utils._tax_code_sync import (
    flatten_xero_tax_rates,
    _reverse_charge,
    _generate_vp_code,
    _rates_differ,
    _max_existing_sequence,
    _build_map_row,
    build_vp_tax_code_create_body,
    build_vp_tax_code_rate_update_body,
    sync_xero_tax_codes_to_vp,
    COMPILE_TAX_CODES_SQL,
)
from vp_xero_integration_v2.common.tables import MAP_TAX_CODE_COLUMNS


class TestTaxCodeSyncHelpers(TestCase):

    # --- flatten_xero_tax_rates (the load-bearing transform) ---

    def test_flatten_fans_out_per_component(self):
        tax_rates = [{
            'Name': 'GST on Income', 'Status': 'ACTIVE', 'TaxType': 'OUTPUT',
            'ReportTaxType': 'OUTPUT',
            'TaxComponents': [
                {'Name': 'GST', 'Rate': 15.0, 'IsCompound': False},
                {'Name': 'PROV', 'Rate': 5.0, 'IsCompound': True},
            ],
        }]
        rows = flatten_xero_tax_rates(tax_rates)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['RateName'], 'GST on Income')
        self.assertEqual(rows[0]['ComponentName'], 'GST')
        self.assertEqual(rows[0]['IsCompound'], 'f')
        self.assertEqual(rows[1]['IsCompound'], 't')

    def test_flatten_skips_non_active(self):
        tax_rates = [{
            'Name': 'Old', 'Status': 'DELETED',
            'TaxComponents': [{'Name': 'X', 'Rate': 1, 'IsCompound': False}],
        }]
        self.assertEqual(flatten_xero_tax_rates(tax_rates), [])

    def test_flatten_defaults_report_tax_type_none(self):
        tax_rates = [{
            'Name': 'NoReport', 'Status': 'ACTIVE',
            'TaxComponents': [{'Name': 'C', 'Rate': 0, 'IsCompound': False}],
        }]
        self.assertEqual(flatten_xero_tax_rates(tax_rates)[0]['ReportTaxType'], 'none')

    def test_flatten_handles_empty(self):
        self.assertEqual(flatten_xero_tax_rates(None), [])
        self.assertEqual(flatten_xero_tax_rates([]), [])

    # --- _reverse_charge ---

    def test_reverse_charge_by_report_type(self):
        self.assertEqual(_reverse_charge('REVERSECHARGES', 'Anything'), 'Y')

    def test_reverse_charge_by_name(self):
        self.assertEqual(_reverse_charge('OUTPUT', 'EC Reverse Charge'), 'Y')

    def test_reverse_charge_default_n(self):
        self.assertEqual(_reverse_charge('OUTPUT', 'GST on Income'), 'N')
        self.assertEqual(_reverse_charge(None, None), 'N')

    # --- _generate_vp_code ---

    def test_generate_vp_code_pads_to_4(self):
        self.assertEqual(_generate_vp_code(7), 'X0007')
        self.assertEqual(_generate_vp_code(1234), 'X1234')

    # --- _rates_differ ---

    def test_rates_differ_numeric(self):
        self.assertFalse(_rates_differ('10', 10.0))
        self.assertTrue(_rates_differ(15, 10))

    def test_rates_differ_string_fallback(self):
        self.assertTrue(_rates_differ('abc', 'def'))
        self.assertFalse(_rates_differ('same', 'same'))

    # --- _max_existing_sequence ---

    def test_max_existing_sequence(self):
        rows = [{'MappedSequence': '3'}, {'MappedSequence': '7'},
                {'MappedSequence': None}, {'MappedSequence': 'x'}]
        self.assertEqual(_max_existing_sequence(rows), 7)

    def test_max_existing_sequence_empty(self):
        self.assertEqual(_max_existing_sequence([]), 0)

    # --- body / row builders ---

    def test_create_body_fields(self):
        body = build_vp_tax_code_create_body('X0007', 'GST on Income', 15.0, 'OUTPUT')
        self.assertEqual(body['Code'], 'X0007')
        self.assertEqual(body['Description'], 'GST on Income')
        self.assertEqual(body['Rate'], 15.0)
        self.assertEqual(body['ReverseCharge'], 'N')

    def test_rate_update_body_no_code(self):
        body = build_vp_tax_code_rate_update_body(20.0, 'EC Reverse Charge', 'OUTPUT')
        self.assertNotIn('Code', body)
        self.assertEqual(body['Rate'], 20.0)
        self.assertEqual(body['ReverseCharge'], 'Y')

    def test_build_map_row_has_all_columns(self):
        row = _build_map_row(
            xero_name='GST on Income', xero_code='GST', vp_code='X0007',
            rate=15.0, compound_on='', sequence='7', messages='',
        )
        self.assertEqual(sorted(row.keys()), sorted(MAP_TAX_CODE_COLUMNS))

    # --- _build_map_row: already-exists adoption leaves empty Messages ---

    def test_already_exists_adoption_produces_empty_messages(self):
        """When POST returns 'already exists' we adopt the code and PUT the current
        Xero rate (no mapped_rate baseline). Messages must be empty so the map row
        is clean and the next run treats it as a normal reuse."""
        row = _build_map_row(
            xero_name='Sales Tax on Imports', xero_code='TAX', vp_code='X0001',
            rate=0.0, compound_on='', sequence='1', messages='',
        )
        self.assertEqual(row['Messages'], '')
        self.assertEqual(row['VantagepointCode'], 'X0001')

    # --- compile SQL: OR-precedence fix + compound subquery ---

    def test_compile_sql_parenthesizes_or_join(self):
        sql = COMPILE_TAX_CODES_SQL
        self.assertIn('(xtc.RateName = vtc.Description AND xtc.ComponentName = vtc.Code)', sql)
        self.assertIn('OR (tcm.VantagepointCode = vtc.Code)', sql)
        # compound base subquery present
        self.assertIn("'#'", sql)
        self.assertIn("xtcsub.IsCompound = 'f'", sql)


class TestSyncAlreadyExistsAdoption(TestCase):
    """Drives the already-exists adoption branch in sync_xero_tax_codes_to_vp.

    Patches `rail.VantagepointTaxCodesOperator` and `rail.S3UpsertCollectionOperator`
    so the engine runs in-process without a VP connection or S3 bucket.
    """

    # Minimal compiled row: both MappedVantagepointCode and VantagepointCode are
    # empty, so the engine takes the "brand-new component → POST" path.
    _COMPILED_ROW = {
        'XeroRateName': 'GST on Income',
        'XeroComponentName': 'GST',
        'XeroRate': 15.0,
        'XeroIsCompound': 'f',
        'ReportTaxType': 'OUTPUT',
        'MappedVantagepointCode': '',
        'MappedRate': None,
        'MappedSequence': None,
        'MappedCompoundOnCode': '',
        'VantagepointCode': '',
        'CompoundOnCode': '',
    }

    @staticmethod
    def _fake_context():
        dag_run = MagicMock()
        dag_run.conf = {}
        return {
            'task_instance': MagicMock(log=logging.getLogger('test_tax_code_sync')),
            'dag_run': dag_run,
        }

    def _run(self, vp_op_factory):
        """Patch rail operators, run the engine, return (summary, upserted_rows)."""
        upserted_rows = []

        def capture_upsert(*_args, **kwargs):
            upserted_rows.extend(kwargs.get('rows', []))
            return MagicMock()

        with patch('rail.get_current_context', return_value=self._fake_context()), \
             patch(
                 'vp_xero_integration_v2.mapping_sync.utils._tax_code_sync'
                 '._read_compiled_tax_codes',
                 return_value=[self._COMPILED_ROW.copy()],
             ), \
             patch('rail.VantagepointTaxCodesOperator', side_effect=vp_op_factory), \
             patch('rail.S3UpsertCollectionOperator', side_effect=capture_upsert):
            summary = sync_xero_tax_codes_to_vp('test')

        return summary, upserted_rows

    def test_already_exists_adopts_puts_rate_and_writes_clean_map_row(self):
        """POST raises 'already exists' → adoption path:
          - reused_existing is incremented, not created
          - a rate-align PUT is issued exactly once
          - errors list remains empty (non-error path)
          - one map row is upserted with empty Messages and the adopted VP code
        """
        post_op = MagicMock()
        post_op.execute.side_effect = Exception('Tax Code X0001 already exists')
        put_op = MagicMock()

        def factory(*_args, **kwargs):
            return post_op if kwargs.get('request_method') == 'POST' else put_op

        summary, upserted_rows = self._run(factory)

        self.assertEqual(summary['reused_existing'], 1)
        self.assertEqual(summary['created'], 0)
        self.assertEqual(summary['errors'], [])
        put_op.execute.assert_called_once()
        self.assertEqual(len(upserted_rows), 1)
        self.assertEqual(upserted_rows[0]['Messages'], '')
        self.assertEqual(upserted_rows[0]['VantagepointCode'], 'X0001')

    def test_already_exists_put_failure_is_non_fatal(self):
        """A PUT failure during rate alignment is logged as a warning but does
        not add to summary['errors'] and does not raise RuntimeError."""
        post_op = MagicMock()
        post_op.execute.side_effect = Exception('Tax Code X0001 already exists')
        put_op = MagicMock()
        put_op.execute.side_effect = Exception('Connection timeout')

        def factory(*_args, **kwargs):
            return post_op if kwargs.get('request_method') == 'POST' else put_op

        summary, _ = self._run(factory)

        self.assertEqual(summary['reused_existing'], 1)
        self.assertEqual(summary['errors'], [])
