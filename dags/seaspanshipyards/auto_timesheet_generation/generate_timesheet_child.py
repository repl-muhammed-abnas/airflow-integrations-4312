import json
import rail
import pendulum
from rail.lib.ecid import get_dagrun_ecid
from seaspanshipyards.auto_timesheet_generation.utils import request_payload
from seaspanshipyards.auto_timesheet_generation.utils import custom_methods
null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"seaspanshipyards_generate_timesheets_child_dag_{config.instance}",
        description=f"SeaspanShipyards Generate Timesheets {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        def get_valid_timesheets_generated(response):
            data = response.json()['d']
            return json.dumps(list(map(lambda timesheet: timesheet, data)))

        generate_timesheets_for_date = rail.RepliconServiceOperator(
            task_id='generate_timesheets_for_date',
            endpoint='/services/TimesheetService1.svc/BulkGetTimesheetForDate',
            data=request_payload.get_generate_timesheets_payload,
            response_filter=get_valid_timesheets_generated
        )

        log_timesheets_generated = rail.WriteLogOperator(
            task_id='log_timesheets_generated',
            items='{{ result("generate_timesheets_for_date") }}',
            message=custom_methods.get_log_details,
            severity=custom_methods.get_log_severity_or_status,
            properties=lambda item, dag_run: {
                'loginname': item["timesheetOwner"]["loginName"],
                'timesheeturi': item["timesheetForDate"]["timesheet"]["uri"] if item["timesheetForDate"] is not null
                and item["timesheetForDate"]["timesheet"]["uri"] is not null
                and item["timesheetForDate"]["timesheet"]["uri"] != '' else null,
                'timesheetdate': ((pendulum.now(config.time_zone)).add(days=1)).strftime("%Y-%m-%d"),
                'status': custom_methods.get_log_severity_or_status(item),
                'details': custom_methods.get_log_details(item),
                'jobid': '{{ dag_run.conf.dag_run_ecid }}',
                'childjobid': get_dagrun_ecid(dag_run)
            },
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            items='{{ dag_run.conf.user_data | to_json }}',
            message='{{ get_error_message() }}',
            severity="Error",
            properties=lambda item, dag_run: {
                'loginname': item["loginname"],
                'timesheeturi': "NA",
                'timesheetdate': ((pendulum.now(config.time_zone)).add(days=1)).strftime("%Y-%m-%d"),
                'status': "Error",
                'details': '{{ get_error_message() }}',
                'jobid': '{{ dag_run.conf.dag_run_ecid }}',
                'childjobid': get_dagrun_ecid(dag_run)
            },
        )

        generate_timesheets_for_date >> log_timesheets_generated >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
