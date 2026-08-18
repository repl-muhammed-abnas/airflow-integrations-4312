import rail
import pendulum
from victoriashipyards.timesheet_auto_submission_v1.utils import custom_methods
from victoriashipyards.timesheet_auto_submission_v1.utils import request_payload
from victoriashipyards.timesheet_auto_submission_v1.utils import python_callable

# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.automatic_timesheet_submission_child_dagid,
        description=f"victoriashipyards Automatic Timesheets Submission {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_time_punch_log = rail.CreateLogOperator(
            task_id='create_time_punch_log')
        create_shift_change_log = rail.CreateLogOperator(
            task_id='create_shift_change_log')

        get_timesheet_approval_details = rail.RepliconServiceOperator(
            task_id='get_timesheet_approval_details',
            endpoint='/services/TimesheetApprovalService1.svc/GetTimesheetApprovalDetails2',
            data=lambda: {
                "timesheetUri": rail.get_current_context()['dag_run'].conf['timesheeturi']
            }
        )

        approval_history_list = rail.PythonOperator(
            task_id='approval_history_list',
            python_callable=custom_methods.create_approval_history_list
        )

        check_status_reopen = rail.IfOperator(
            task_id='check_status_reopen',
            test='{{ result("approval_history_list")[-1]["status"] == "Reopen" }}',
            yes_task='submit_timesheets',
            no_task='catch_and_log_errors'
        )

        submit_timesheets = rail.RepliconServiceOperator(
            task_id='submit_timesheets',
            endpoint='/services/TimesheetApprovalService1.svc/Submit2',
            data=request_payload.get_submit_timesheet_payload
        )

        check_action_type_3 = rail.IfOperator(
            task_id='check_action_type_3',
            test=lambda: bool(rail.get_current_context()[
                              'dag_run'].conf['type'] == "Time Punch"),
            yes_task='log_time_punch_successful_submit',
            no_task='log_shift_change_successful_submit'
        )

        log_time_punch_successful_submit = rail.WriteLogOperator(
            task_id='log_time_punch_successful_submit',
            log='{{ result("create_time_punch_log") }}',
            message='Submitted Successfully',
            severity="Success",
            properties={
                    'username': '{{ dag_run.conf.username}}',
                    'timesheetperiod': '{{ dag_run.conf.timesheetperiod }}',
                    'datetime': pendulum.now(config.time_zone).strftime("%m/%d/%Y"),
                    'status': "Success",
                    'remarks': "Submitted Successfully",
                    'jobid': '{{ dag_run.conf.jobid}}',
                    'childjobid': '{{ dag_run_ecid() }}'
            },
        )

        log_shift_change_successful_submit = rail.WriteLogOperator(
            task_id='log_shift_change_successful_submit',
            log='{{ result("create_shift_change_log") }}',
            message='Submitted Successfully',
            severity="Success",
            properties={
                    'username': '{{ dag_run.conf.username}}',
                    'timesheetperiod': '{{ dag_run.conf.timesheetperiod }}',
                    'datetime': pendulum.now(config.time_zone).strftime("%m/%d/%Y"),
                    'status': "Success",
                    'remarks': "Submitted Successfully",
                    'jobid': '{{ dag_run.conf.jobid}}',
                    'childjobid': '{{ dag_run_ecid() }}'
            },
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ result("create_time_punch_log") }}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                    'username': '{{ dag_run.conf.username}}',
                    'timesheetperiod': '{{ dag_run.conf.timesheetperiod }}',
                    'datetime': pendulum.now(config.time_zone).strftime("%m/%d/%Y"),
                    'status': "Error",
                    'remarks': '{{ get_error_message() }}',
                    'jobid': '{{ dag_run.conf.jobid}}',
                    'childjobid': '{{ dag_run_ecid() }}'
            },
        )

        create_time_punch_log >> create_shift_change_log >> get_timesheet_approval_details \
            >> approval_history_list >> check_status_reopen
        check_status_reopen >> rail.Label(
            "Yes") >> submit_timesheets >> check_action_type_3
        check_status_reopen >> rail.Label(
            "No") >> catch_and_log_errors
        check_action_type_3 >> rail.Label(
            "Yes") >> log_time_punch_successful_submit >> catch_and_log_errors
        check_action_type_3 >> rail.Label(
            "No") >> log_shift_change_successful_submit >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
