"""
Shim re-exporting the mapping_sync helper API.

This module was once a 3760-line monolith — it now stands as a thin
re-export façade so existing DAG-file imports (e.g. `from
vp_quickbooks_integration.mapping_sync.utils.python_callable_method
import sync_qbo_firms_to_vp`) keep working unchanged after the C0302
split.

Where each public symbol actually lives:

    _shared.py          — S3 access, init Variable, mapping_table_state
                          lifecycle, child DAG conf builder, skip
                          gates, error capture, response normalisers
    _firm_sync.py       — firm body builders + sync engine
    _employee_sync.py   — employee body builders + sync engine
    _account_sync.py    — account body builders + sync engine
    _tax_code_sync.py   — tax-rate flatten + body builders + sync engine
    _validate.py        — Phase-5 validators + summariser

Direct imports of the underscore-prefixed module names are
discouraged — they're considered package-internal. New code should
import from `python_callable_method` for the public symbols listed in
`__all__` below.
"""
from vp_quickbooks_integration.mapping_sync.utils._shared import (
    # S3 collection access
    open_mapping_collection,
    # Per-customer init Variable gate
    is_mapping_init_complete,
    mark_mapping_init_complete,
    # mapping_table_state lifecycle
    seed_mapping_state_rows,
    apply_premapping_state,
    mark_step_status,
    check_step_status,
    mark_all_steps_ready,
    # Child DAG conf builder
    build_child_dag_conf,
    # Skip-gate helpers
    count_collection_rows,
    is_table_populated,
    # Error capture
    capture_dag_error,
    # QBO/VP response normalisers (private — re-exported for the
    # transitional period while per-table sync modules still pull from
    # this façade. Will be tightened to non-public once they import
    # from `_shared` directly.)
    _resolve_s3_locator,
    _resolve_cfg_then_variable,
    _extract_qbo_records,
    _extract_vp_client_id,
    _extract_qbo_entity_id,
    _read_mapping_state_row,
)
from vp_quickbooks_integration.mapping_sync.utils._firm_sync import (
    sync_qbo_firms_to_vp,
)
from vp_quickbooks_integration.mapping_sync.utils._employee_sync import (
    sync_qbo_employees_to_vp,
)
from vp_quickbooks_integration.mapping_sync.utils._account_sync import (
    sync_qbo_accounts_to_vp,
    build_qbo_accounts_staging,
    prepare_vp_accounts_staging,
    read_account_code_map_for_staging,
    COMPILE_ACCOUNT_CODES_SQL,
)
from vp_quickbooks_integration.mapping_sync.utils._tax_code_sync import (
    sync_qbo_tax_codes_to_vp,
    build_qbo_tax_rates_staging,
    prepare_vp_tax_codes_staging,
    read_map_tax_code_for_staging,
    TAX_GROUP_IDS_SQL,
    COMPILE_TAX_CODES_SQL,
)
from vp_quickbooks_integration.mapping_sync.utils._validate import (
    run_all_mapping_validations,
    summarize_mapping_validations,
)


__all__ = [
    # _shared
    'open_mapping_collection',
    'is_mapping_init_complete',
    'mark_mapping_init_complete',
    'seed_mapping_state_rows',
    'apply_premapping_state',
    'mark_step_status',
    'check_step_status',
    'mark_all_steps_ready',
    'build_child_dag_conf',
    'count_collection_rows',
    'is_table_populated',
    'capture_dag_error',
    # _firm_sync
    'sync_qbo_firms_to_vp',
    # _employee_sync
    'sync_qbo_employees_to_vp',
    # _account_sync
    'sync_qbo_accounts_to_vp',
    'build_qbo_accounts_staging',
    'prepare_vp_accounts_staging',
    'read_account_code_map_for_staging',
    'COMPILE_ACCOUNT_CODES_SQL',
    # _tax_code_sync
    'sync_qbo_tax_codes_to_vp',
    'build_qbo_tax_rates_staging',
    'prepare_vp_tax_codes_staging',
    'read_map_tax_code_for_staging',
    'TAX_GROUP_IDS_SQL',
    'COMPILE_TAX_CODES_SQL',
    # _validate
    'run_all_mapping_validations',
    'summarize_mapping_validations',
]
