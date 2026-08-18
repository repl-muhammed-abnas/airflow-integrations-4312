"""
Shim re-exporting the mapping_sync helper API.

A thin re-export façade so DAG-file imports (e.g. `from
vp_xero_integration.mapping_sync.utils.python_callable_method import
build_child_dag_conf`) stay stable as the helper modules grow.

Where each public symbol actually lives:

    _shared.py          — S3 access, init Variable, mapping_table_state
                          lifecycle, child DAG conf builder, skip gates,
                          error capture, response normalisers
    _firm_sync.py       — firm body builders + sync engine            (U2)
    _account_sync.py    — account body builders + sync engine         (U3)
    _tax_code_sync.py   — tax-rate flatten + body builders + engine   (U4)
    _validate.py        — Phase-5 validators + summariser             (U5)

Direct imports of the underscore-prefixed module names are discouraged —
they're considered package-internal. New code should import from
`python_callable_method` for the public symbols listed in `__all__` below.

All engine modules now exist (U2 firm, U3 account, U4 tax, U5 validate); the
façade re-exports the full public surface.
"""
from vp_xero_integration.mapping_sync.utils._shared import (
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
    # Xero/VP response normalisers (private — re-exported for the per-table
    # sync engines until they import from `_shared` directly).
    _resolve_s3_locator,
    _resolve_cfg_then_variable,
    _extract_xero_records,
    _extract_vp_client_id,
    _extract_xero_entity_id,
    _read_mapping_state_row,
)

# --- Engine re-exports ---
from vp_xero_integration.mapping_sync.utils._firm_sync import (  # U2
    sync_xero_firms_to_vp,
)
from vp_xero_integration.mapping_sync.utils._account_sync import (  # U3
    sync_xero_accounts_to_vp,
    build_xero_accounts_staging,
    prepare_vp_accounts_staging,
    read_chart_of_accounts_map_for_staging,
    COMPILE_ACCOUNT_CODES_SQL,
)
from vp_xero_integration.mapping_sync.utils._tax_code_sync import (  # U4
    sync_xero_tax_codes_to_vp,
    flatten_xero_tax_rates,
    build_xero_tax_rates_staging,
    prepare_vp_tax_codes_staging,
    read_map_tax_code_for_staging,
    COMPILE_TAX_CODES_SQL,
)
from vp_xero_integration.mapping_sync.utils._validate import (  # U5
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
    # _firm_sync (U2)
    'sync_xero_firms_to_vp',
    # _account_sync (U3)
    'sync_xero_accounts_to_vp',
    'build_xero_accounts_staging',
    'prepare_vp_accounts_staging',
    'read_chart_of_accounts_map_for_staging',
    'COMPILE_ACCOUNT_CODES_SQL',
    # _tax_code_sync (U4)
    'sync_xero_tax_codes_to_vp',
    'flatten_xero_tax_rates',
    'build_xero_tax_rates_staging',
    'prepare_vp_tax_codes_staging',
    'read_map_tax_code_for_staging',
    'COMPILE_TAX_CODES_SQL',
    # _validate (U5)
    'run_all_mapping_validations',
    'summarize_mapping_validations',
]
