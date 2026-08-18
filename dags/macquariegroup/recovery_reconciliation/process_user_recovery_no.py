import rail
from macquariegroup.recovery_reconciliation.utils.request_payload import get_update_user_payload
from macquariegroup.recovery_reconciliation.utils.custom_methods import get_effective_dates_to_apply


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"macquarie_recovery_reconciliation_process_users_recovery_no_child_{config.instance}",
        description=f"Macquarie Recovery Reconciliation Master {config.instance}",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=10
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_conf")

        get_users_current_timesheet_end_date = rail.RepliconServiceOperator(
            task_id="get_users_current_timesheet_end_date",
            endpoint="services/TimesheetService1.svc/GetNextTimesheetDueDate",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}",
                "asOfDate": {
                    "year": "{{ dag_run.conf.running_date.year}}",
                    "month": "{{ dag_run.conf.running_date.month}}",
                    "day": "{{ dag_run.conf.running_date.day}}"
                }
            }
        )

        get_effective_dates = rail.PythonOperator(
            task_id="get_effective_dates",
            python_callable=get_effective_dates_to_apply
        )

        update_user_details = rail.RepliconServiceOperator(
            task_id="update_user_details",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: get_update_user_payload(dag_run, "No")
        )

        is_update_failed = rail.IfOperator(
            task_id="is_update_failed",
            test="{{ result('update_user_details').errors | is_truthy }}",
            yes_task="log_update_failed",
            no_task="log_update_success"
        )

        log_update_failed = rail.WriteLogOperator(
            task_id="log_update_failed",
            message="{{ result('update_user_details').errors.DisplayText }}",
            severity="Error",
            properties={
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_type": "{{dag_run.conf.employee_type}}",
                "department": "{{dag_run.conf.department}}",
                "cost_centre": "{{dag_run.conf.cost_centre}}",
                "action": "Update",
                "Status": "Error",
                "details": "{{ result('update_user_details').errors.DisplayText }}"
            }
        )

        log_update_success = rail.WriteLogOperator(
            task_id="log_update_success",
            severity="Success",
            message="Updated User's Recovery Enabled to 'No'",
            properties={
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_type": "{{dag_run.conf.employee_type}}",
                "department": "{{dag_run.conf.department}}",
                "cost_centre": "{{dag_run.conf.cost_center}}",
                "action": "Update",
                "Status": "Success",
                "details": "Updated user's recovery enabled to 'No'"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            severity="Error",
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_type": "{{dag_run.conf.employee_type}}",
                "department": "{{dag_run.conf.department}}",
                "cost_centre": "{{dag_run.conf.cost_center}}",
                "action": "Update",
                "Status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        get_users_current_timesheet_end_date >> get_effective_dates >> update_user_details >> is_update_failed
        is_update_failed >> rail.Label("Yes") >> log_update_failed >> rail.Label(
            "On Error") >> catch_and_log_error
        is_update_failed >> rail.Label("No") >> log_update_success >> rail.Label(
            "On Error") >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
