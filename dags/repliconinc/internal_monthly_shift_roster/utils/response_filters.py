# In Workato, only assignmentUri was utilitzed from the response for the further processes.
# Hence, the response filter is designed to extract only assignment URIs from the
# API response and pass it to the subsequent task.
def filter_shift_schedule_summary_details_response(response):
    if not response:
        return []
    return [item["assignmentUri"] for item in response]