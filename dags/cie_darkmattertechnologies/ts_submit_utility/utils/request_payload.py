from airflow.models import Variable

def get_timesheet_submit_batch(dag_run):
    if dag_run:
        return {
            "timesheetUris": dag_run.conf["timesheet_uris"],
            "comments": dag_run.conf['timesheet_submit_remarks'],
            "submitOptions": []
        }
    return None

def get_environment_variables(config):
    def_val = {
        "date_format": "%m/%d/%Y",
        "period_in_months": 3,
        "chunk_size": 20,
        "report_date_format": "%m/%d/%Y",
        "max_child_run": 3,
        "execution_timeout_days": 14,
        "timesheet_submit_remarks": "Timesheet Submitted by CIE Utility."
    }

    my_value = Variable.get(config.params, default_var=def_val,  deserialize_json=True)
    return my_value


def execute_batch_timesheet_data(item):
    if item:
        return {
            "timesheetApprovalBatchUri": item,
        }
    return None


def get_processed_ts_uri(dag_run):
    if dag_run:
        return {
            "timesheetUris": dag_run.conf["timesheet_uris"],
        }
    return None