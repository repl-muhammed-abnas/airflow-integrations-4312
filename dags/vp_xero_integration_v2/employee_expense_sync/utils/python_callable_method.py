# dags/vp_xero_integration_v2/employee_expense_sync/utils/python_callable_method.py
"""Utility callables for VP -> Xero Employee Expense Sync (V2).

Re-exports dispatcher and processor callables from the V1 implementation.
Business logic is unchanged; V2 connections are resolved by the dispatcher
via get_connections(config) rather than dag_run.conf, so processor callables
that read dag_run.conf['connections'] still work (the dispatcher passes
get_connections(config) in the child conf).
"""
from vp_xero_integration.employee_expense_sync.utils.python_callable_method import (  # noqa: F401
    build_vp_expense_poll_filter_method,
    extract_expense_vouchers_method,
    check_if_vouchers_exist_method,
    build_vp_expense_lines_filter_method,
    check_already_exported_method,
    should_skip_if_exported_method,
    build_xero_bill_body_method,
    check_has_payable_lines_method,
    record_expense_result_method,
    capture_processor_error,
)
