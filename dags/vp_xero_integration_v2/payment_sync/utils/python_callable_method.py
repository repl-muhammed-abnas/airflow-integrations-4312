# dags/vp_xero_integration_v2/payment_sync/utils/python_callable_method.py
"""Dispatcher-level callables for Xero -> VP Payment Sync (V2).

Re-exports the three dispatcher callables from the V1 implementation.
Processor callables stay in V1 — they read connections/customerId from
dag_run.conf, which the V2 dispatcher injects via get_connections(config).
"""
from vp_xero_integration.xero_to_vp_payment_sync.utils.python_callable_method import (  # noqa: F401
    prepare_sync_timestamps_method,
    update_last_sync_time_method,
    prepare_payment_items_method,
)
