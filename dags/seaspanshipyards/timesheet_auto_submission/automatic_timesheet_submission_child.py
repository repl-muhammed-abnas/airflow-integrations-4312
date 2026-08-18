import rail
import pendulum
from airflow.models import Variable
from seaspanshipyards.timesheet_auto_submission.utils import custom_methods
from seaspanshipyards.timesheet_auto_submission.utils import request_payload
from seaspanshipyards.timesheet_auto_submission.utils import python_callable

# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"seaspanshipyards_automatic_timesheets_submission_child_dag_{config.instance}",
        description=f"SeaspanShipyards Automatic Timesheets Submission {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.timesheet_submission_child_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_process_batch_task, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="create_time_punch_log"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="create_time_punch_log",
            end_task="catch_and_log_errors"
        )

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

        get_expected_approvers = rail.RepliconServiceOperator(
            task_id='get_expected_approvers',
            endpoint='/services/TimesheetApprovalService1.svc/GetExpectedApprovers',
            data=lambda: {
                "timesheetUri": rail.get_current_context()['dag_run'].conf['timesheeturi']
            }
        )

        approval_history_list = rail.PythonOperator(
            task_id='approval_history_list',
            python_callable=custom_methods.create_approval_history_list
        )

        final_approver_check_list = rail.PythonOperator(
            task_id='final_approver_check_list',
            python_callable=custom_methods.get_final_approver_list
        )

        check_status_reopen = rail.IfOperator(
            task_id='check_status_reopen',
            test='{{ result("approval_history_list")[-1]["status"] == "Reopen" }}',
            yes_task='previous_action',
            no_task='status'
        )

        previous_action = rail.PythonOperator(
            task_id='previous_action',
            python_callable=custom_methods.get_previous_action
        )

        last_approver_approved = rail.PythonOperator(
            task_id='last_approver_approved',
            python_callable=custom_methods.get_last_approval
        )

        check_approval_status = rail.IfOperator(
            task_id='check_approval_status',
            test='{{result("previous_action") == "Approve" and result("last_approver_approved") == "Yes"}}',
            yes_task='status_approve',
            no_task='status'
        )

        status_approve = rail.PythonOperator(
            task_id='status_approve',
            python_callable=custom_methods.get_status_approve
        )

        status = rail.PythonOperator(
            task_id='status',
            python_callable=custom_methods.get_status
        )

        is_force_approvable_by_system = rail.IfOperator(
            task_id='is_force_approvable_by_system',
            test=python_callable.check_force_approvable,
            yes_task='force_approve_timesheets_by_system',
            no_task='is_force_approvable'
        )

        force_approve_timesheets_by_system = rail.RepliconServiceOperator(
            task_id='force_approve_timesheets_by_system',
            endpoint='/services/TimesheetApprovalService1.svc/ForceApprove',
            data=request_payload.get_force_approve_timesheet_payload
        )

        check_action_type = rail.IfOperator(
            task_id='check_action_type',
            test=lambda: bool(rail.get_current_context()[
                              'dag_run'].conf['type'] == "Time Punch"),
            yes_task='log_time_punch_successful_approval_by_system',
            no_task='log_shift_change_successful_approval_by_system'
        )

        log_time_punch_successful_approval_by_system = rail.WriteLogOperator(
            task_id='log_time_punch_successful_approval_by_system',
            log='{{ result("create_time_punch_log") }}',
            message='Approved Successfully',
            severity="Success",
            properties={
                    'username': '{{ dag_run.conf.username}}',
                    'timesheetperiod': '{{ dag_run.conf.timesheetperiod }}',
                    'datetime': pendulum.now(config.time_zone).strftime("%m/%d/%Y"),
                    'status': "Success",
                    'remarks': "Approved Successfully",
                    'jobid': '{{ dag_run.conf.jobid}}',
                    'childjobid': '{{ dag_run_ecid() }}'
            },
        )

        log_shift_change_successful_approval_by_system = rail.WriteLogOperator(
            task_id='log_shift_change_successful_approval_by_system',
            log='{{ result("create_shift_change_log") }}',
            message='Approved Successfully',
            severity="Success",
            properties={
                    'username': '{{ dag_run.conf.username}}',
                    'timesheetperiod': '{{ dag_run.conf.timesheetperiod }}',
                    'datetime': pendulum.now(config.time_zone).strftime("%m/%d/%Y"),
                    'status': "Success",
                    'remarks': "Approved Successfully",
                    'jobid': '{{ dag_run.conf.jobid}}',
                    'childjobid': '{{ dag_run_ecid() }}'
            },
        )

        is_force_approvable = rail.IfOperator(
            task_id='is_force_approvable',
            test=python_callable.check_force_approvable_by_anyone,
            yes_task='force_approve_timesheets',
            no_task='is_submittable'
        )

        force_approve_timesheets = rail.RepliconServiceOperator(
            task_id='force_approve_timesheets',
            endpoint='/services/TimesheetApprovalService1.svc/ForceApprove',
            data=request_payload.get_force_approve_timesheet_payload
        )

        check_action_type_2 = rail.IfOperator(
            task_id='check_action_type_2',
            test=lambda: bool(rail.get_current_context()[
                              'dag_run'].conf['type'] == "Time Punch"),
            yes_task='log_time_punch_successful_approval',
            no_task='log_shift_change_successful_approval'
        )

        log_time_punch_successful_approval = rail.WriteLogOperator(
            task_id='log_time_punch_successful_approval',
            log='{{ result("create_time_punch_log") }}',
            message='Approved Successfully',
            severity="Success",
            properties={
                    'username': '{{ dag_run.conf.username}}',
                    'timesheetperiod': '{{ dag_run.conf.timesheetperiod }}',
                    'datetime': pendulum.now(config.time_zone).strftime("%m/%d/%Y"),
                    'status': "Success",
                    'remarks': "Approved Successfully",
                    'jobid': '{{ dag_run.conf.jobid}}',
                    'childjobid': '{{ dag_run_ecid() }}'
            },
        )

        log_shift_change_successful_approval = rail.WriteLogOperator(
            task_id='log_shift_change_successful_approval',
            log='{{ result("create_shift_change_log") }}',
            message='Approved Successfully',
            severity="Success",
            properties={
                    'username': '{{ dag_run.conf.username}}',
                    'timesheetperiod': '{{ dag_run.conf.timesheetperiod }}',
                    'datetime': pendulum.now(config.time_zone).strftime("%m/%d/%Y"),
                    'status': "Success",
                    'remarks': "Approved Successfully",
                    'jobid': '{{ dag_run.conf.jobid}}',
                    'childjobid': '{{ dag_run_ecid() }}'
            },
        )

        is_submittable = rail.IfOperator(
            task_id='is_submittable',
            test=python_callable.check_submittable,
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

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >>\
        create_time_punch_log >> create_shift_change_log >> get_timesheet_approval_details \
            >> get_expected_approvers >> approval_history_list >> final_approver_check_list >> check_status_reopen
        check_status_reopen >> rail.Label(
            "Yes") >> previous_action >> last_approver_approved >> check_approval_status
        check_approval_status >> rail.Label(
            "Yes") >> status_approve >> status >> is_force_approvable_by_system
        check_approval_status >> rail.Label(
            "No") >> status >> is_force_approvable_by_system
        check_status_reopen >> rail.Label(
            "No") >> status >> is_force_approvable_by_system

        is_force_approvable_by_system >> rail.Label(
            "Yes") >> force_approve_timesheets_by_system >> check_action_type
        is_force_approvable_by_system >> rail.Label(
            "No") >> is_force_approvable
        check_action_type >> rail.Label(
            "Yes") >> log_time_punch_successful_approval_by_system >> catch_and_log_errors
        check_action_type >> rail.Label(
            "No") >> log_shift_change_successful_approval_by_system >> catch_and_log_errors

        is_force_approvable >> rail.Label(
            "Yes") >> force_approve_timesheets >> check_action_type_2
        is_force_approvable >> rail.Label("No") >> is_submittable
        check_action_type_2 >> rail.Label(
            "Yes") >> log_time_punch_successful_approval >> catch_and_log_errors
        check_action_type_2 >> rail.Label(
            "No") >> log_shift_change_successful_approval >> catch_and_log_errors

        is_submittable >> rail.Label(
            "Yes") >> submit_timesheets >> check_action_type_3
        is_submittable >> rail.Label("No") >> catch_and_log_errors
        check_action_type_3 >> rail.Label(
            "Yes") >> log_time_punch_successful_submit >> catch_and_log_errors
        check_action_type_3 >> rail.Label(
            "No") >> log_shift_change_successful_submit >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
