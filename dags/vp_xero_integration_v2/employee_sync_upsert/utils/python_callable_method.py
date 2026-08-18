# dags/vp_xero_integration_v2/employee_sync_upsert/utils/python_callable_method.py
"""Utility callables for VP -> Xero Employee Sync Upsert (V2).

Re-exports dispatcher and processor callables from the V1 implementation.
Business logic is unchanged; V2 connections are resolved by the dispatcher
via get_connections(config) rather than dag_run.conf, so processor callables
that read dag_run.conf['connections'] still work (the dispatcher passes
get_connections(config) in the child conf).
"""
from vp_xero_integration.employee_sync_upsert.utils.python_callable_method import (  # noqa: F401
    build_vp_employee_filter_method,
    extract_employee_list_method,
    check_if_employees_exist_method,
    build_vp_single_employee_filter_method,
    get_employee_from_map_method,
    check_employee_exists_in_map_method,
    update_employee_in_map_method,
    add_employee_to_map_method,
    check_vp_returned_employee_method,
    should_skip_employee_method,
    map_row_needs_xero_create_method,
    map_row_is_active_for_update_method,
    map_row_present_for_archive_method,
    build_xero_create_contact_body_method,
    build_xero_update_contact_body_method,
    build_xero_archive_contact_body_method,
    write_map_row_after_create_method,
    refresh_map_row_after_update_method,
    mark_map_row_archived_method,
    log_result_method,
    capture_processor_error,
)
