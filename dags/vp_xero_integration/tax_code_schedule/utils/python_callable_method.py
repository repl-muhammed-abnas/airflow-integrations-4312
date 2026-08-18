"""
Public helper API for the tax_code_schedule integration.
"""
import sqlite3

from vp_xero_integration.mapping_sync.utils._tax_code_sync import (
    build_xero_tax_rates_staging,
    prepare_vp_tax_codes_staging,
    read_map_tax_code_for_staging,
    sync_xero_tax_codes_to_vp,
    COMPILE_TAX_CODES_SQL,
    XERO_TAX_COMPONENTS_COLLECTION,
    XERO_TAX_COMPONENTS_STAGING_COLUMNS,
    VP_TAX_CODES_COLLECTION,
    VP_TAX_CODES_STAGING_COLUMNS,
    TAX_CODE_MAP_COLLECTION,
    TAX_CODE_MAP_STAGING_COLUMNS,
    COMPILED_TAX_CODES_COLLECTION,
)
from vp_xero_integration.mapping_sync.utils._shared import capture_dag_error

__all__ = [
    'build_xero_tax_rates_staging',
    'prepare_vp_tax_codes_staging',
    'read_map_tax_code_for_staging',
    'read_map_tax_code_safe',
    'sync_xero_tax_codes_to_vp',
    'capture_dag_error',
    'COMPILE_TAX_CODES_SQL',
    'XERO_TAX_COMPONENTS_COLLECTION',
    'XERO_TAX_COMPONENTS_STAGING_COLUMNS',
    'VP_TAX_CODES_COLLECTION',
    'VP_TAX_CODES_STAGING_COLUMNS',
    'TAX_CODE_MAP_COLLECTION',
    'TAX_CODE_MAP_STAGING_COLUMNS',
    'COMPILED_TAX_CODES_COLLECTION',
]


def read_map_tax_code_safe(**context):
    """Wraps read_map_tax_code_for_staging to handle first-sync customers.

    Returns [] when map_tax_code doesn't yet exist in the S3 DB (customer
    hasn't completed mapping_sync init). The engine then creates all VP tax
    codes from scratch and writes the full map on that run.
    """
    try:
        return read_map_tax_code_for_staging(**context)
    except sqlite3.OperationalError as exc:
        if 'no such table' in str(exc).lower():
            return []
        raise
