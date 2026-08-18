"""
BambooHR shared utilities for Airflow DAGs.
Contains common functions used across multiple BambooHR integration DAGs.
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Callable


def extract_changed_employee_ids(response: Any, action_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Extract employee IDs from /employees/changed response with optional action filtering.
    Returns list of employee IDs that have been changed, optionally filtered by action.
    Handles both dictionary and list response formats.

    Args:
        response: API response from BambooHR /employees/changed endpoint
        action_filter: Optional action to filter by ('Inserted', 'Updated', 'Deleted').
                      Case-insensitive. If None, returns all employees.

    Returns:
        List of dictionaries with employee data: [{"id": "123", "action": "Updated", "lastChanged": "..."}]
    """
    if not response:
        return []

    # Handle both possible response formats
    employees_data = None

    if isinstance(response, dict) and 'employees' in response:
        employees_data = response['employees']
    elif isinstance(response, list):
        # If response is directly a list
        employees_data = response
    elif isinstance(response, dict):
        # If response is a dict but without 'employees' key, might be the employees data itself
        employees_data = response

    if not employees_data:
        return []

    employee_ids = []

    # Normalize action filter for case-insensitive comparison
    normalized_action_filter = action_filter.lower() if action_filter else None

    # Handle dictionary format (emp_id as key)
    if isinstance(employees_data, dict):
        for emp_id, emp_data in employees_data.items():
            action = emp_data.get("action") if isinstance(emp_data, dict) else "Unknown"

            # Apply action filter if specified
            if normalized_action_filter and action.lower() != normalized_action_filter:
                continue

            employee_ids.append({
                "id": emp_id,
                "action": action,
                "lastChanged": emp_data.get("lastChanged") if isinstance(emp_data, dict) else None
            })
    # Handle list format
    elif isinstance(employees_data, list):
        for emp_data in employees_data:
            if isinstance(emp_data, dict):
                action = emp_data.get("action", "Unknown")

                # Apply action filter if specified
                if normalized_action_filter and action.lower() != normalized_action_filter:
                    continue

                employee_ids.append({
                    "id": emp_data.get("id"),
                    "action": action,
                    "lastChanged": emp_data.get("lastChanged")
                })
            else:
                # If list contains simple values (like IDs)
                action = "Unknown"

                # Apply action filter if specified
                if normalized_action_filter and action.lower() != normalized_action_filter:
                    continue

                employee_ids.append({
                    "id": str(emp_data),
                    "action": action,
                    "lastChanged": None
                })

    return employee_ids


def create_employee_data_handler(
    status_filter: Optional[str] = None,
    field_mapping: Optional[Dict[str, str]] = None,
    include_filter: Optional[Callable[[Dict], bool]] = None
) -> Callable:
    """
    Factory function to create employee data handlers with different configurations.

    Args:
        status_filter: Filter employees by status (e.g., 'active', 'inactive')
        field_mapping: Map BambooHR fields to output fields
        include_filter: Custom filter function for employees

    Returns:
        Data handler function for use with BambooHROperator
    """
    def handler(response):
        if not response:
            return []

        # Handle both old format (response['employees']) and new format (direct array)
        employees_data = response.get('employees', response) if isinstance(
            response, dict) else response

        if not employees_data:
            return []

        # Transform employees data
        transformed_employees = []
        for item in employees_data:
            if not isinstance(item, dict):
                continue

            # Apply status filter if specified
            if status_filter and item.get('status', '').lower() != status_filter.lower():
                continue

            # Apply custom filter if specified
            if include_filter and not include_filter(item):
                continue

            # Apply field mapping or use default mapping
            if field_mapping:
                employee = {}
                for output_field, input_field in field_mapping.items():
                    employee[output_field] = item.get(input_field)
                transformed_employees.append(employee)
            else:
                # Default mapping (pass through)
                transformed_employees.append(item)

        return transformed_employees

    return handler


def create_user_import_employee_handler() -> Callable:
    """
    Creates a standardized employee data handler for user import DAGs.
    Filters for active employees and maps fields to expected format.
    """
    field_mapping = {
        "id": "id",
        "firstname": "firstName",
        "lastname": "lastName",
        "employeenumber": "employeeNumber",
        "startdate": "hireDate",
        "workemail": "workEmail",
        "status": "status",
        "jobtitle": "jobTitle",
        "location": "location"
    }

    return create_employee_data_handler(
        status_filter='active',
        field_mapping=field_mapping
    )


def create_disable_user_employee_handler() -> Callable:
    """
    Creates a standardized employee data handler for disable user DAGs.
    Filters for inactive employees and maps fields to expected format.
    """
    field_mapping = {
        "workemail": "workEmail",
        "enddate": "terminationDate",
        "status": "status"
    }

    return create_employee_data_handler(
        status_filter='inactive',
        field_mapping=field_mapping
    )


def process_single_employee_response(response: Dict, field_mapping: Dict[str, str]) -> Optional[Dict]:
    """
    Process individual employee response and format for downstream use.

    Args:
        response: Single employee data from BambooHR API
        field_mapping: Mapping of output fields to input fields

    Returns:
        Transformed employee data or None if invalid
    """
    if not response:
        return None

    # Transform to match expected downstream format
    employee = {}
    for output_field, input_field in field_mapping.items():
        employee[output_field] = response.get(input_field)

    return employee


def format_bamboohr_timestamp(dt: datetime) -> str:
    """
    Format datetime for BambooHR API timestamp parameters.

    Args:
        dt: Datetime object

    Returns:
        Formatted timestamp string for BambooHR API
    """
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def get_initial_sync_time(minutes_back: int = 60) -> str:
    """
    Get initial sync time for first run of DAG.

    Args:
        minutes_back: How many minutes back from current time

    Returns:
        Formatted timestamp string
    """
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes_back)).strftime('%Y-%m-%d %H:%M:%S')


# Field definitions for common BambooHR API calls
USER_IMPORT_FIELDS = [
    "id",
    "firstName",
    "lastName",
    "employeeNumber",
    "hireDate",
    "workEmail",
    "status",
    "jobTitle",
    "location"
]

DISABLE_USER_FIELDS = [
    "workEmail",
    "terminationDate",
    "status"
]

# Common field mappings
USER_IMPORT_FIELD_MAPPING = {
    "id": "id",
    "firstname": "firstName",
    "lastname": "lastName",
    "employeenumber": "employeeNumber",
    "startdate": "hireDate",
    "workemail": "workEmail",
    "status": "status",
    "jobtitle": "jobTitle",
    "location": "location"
}

DISABLE_USER_FIELD_MAPPING = {
    "workemail": "workEmail",
    "enddate": "terminationDate",
    "status": "status"
}
