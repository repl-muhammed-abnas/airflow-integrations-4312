def validate_project(project_code, project_details):
    if not project_details:
        return False, None, f"Project '{project_code}' was not found in the system."

    project_status = project_details.get('status', {})
    status_key = project_status.get('displayText', '') if isinstance(project_status, dict) else str(project_status)

    if status_key.lower() != 'in progress':
        return False, project_details.get('uri'), f"Project '{project_code}' is not available for time entry (Status: {status_key})."

    return True, project_details.get('uri'), ""
