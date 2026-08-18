import rail
#pylint: disable = line-too-long
from macquariegroup.recovery_reconciliation.utils.request_payload import get_update_user_payload, get_timesheet_period_payload_to_apply,get_remove_user_end_date_payload
from macquariegroup.recovery_reconciliation.utils.custom_methods import get_first_timesheet_period_name_from_query, get_today_date
from macquariegroup.recovery_reconciliation.utils import data_handlers

null=None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"macquarie_recovery_reconciliation_process_users_recovery_yes_child_{config.instance}",
        description=f"Macquarie Recovery Reconciliation Update recovery Yes {config.instance}",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=10
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_conf")

        remove_user_end_date = rail.RepliconServiceOperator(
            task_id= "remove_user_end_date",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data= get_remove_user_end_date_payload
        )

        enable_user = rail.RepliconServiceOperator(
            task_id="enable_user",
            endpoint="/services/securityservice1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}"
            }
        )

        is_user_recovery_enable_no = rail.IfOperator(
            task_id="is_user_recovery_enable_no",
            test=lambda dag_run: dag_run.conf['employee_recovery_enable_status'] in (
                'No', ''),
            yes_task="get_timesheet_period_to_apply_from_feed",
            no_task="update_user_recovery_group"
        )

        get_timesheet_period_to_apply_from_feed = rail.QueryCollectionOperator(
            task_id="get_timesheet_period_to_apply_from_feed",
            query="""SELECT timesheet_period from valid_records WHERE employee_type= :EMP_TYPE
                      AND department= :DEPT
                      AND cost_centre= :CC""",
            query_params={
                "EMP_TYPE": "{{dag_run.conf.employee_type}}",
                "DEPT": "{{ dag_run.conf.department}}",
                "CC": "{{ dag_run.conf.cost_center }}"
            }
        )

        get_first_timesheet_period_details_from_list = rail.PythonOperator(
            task_id="get_first_timesheet_period_details_from_list",
            python_callable=get_first_timesheet_period_name_from_query
        )

        update_user_recovery_group = rail.RepliconServiceOperator(
            task_id="update_user_recovery_group",
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=lambda dag_run: get_update_user_payload(dag_run, "Yes")
        )

        is_update_failed = rail.IfOperator(
            task_id="is_update_failed",
            test="{{ result('update_user_recovery_group').errors | is_truthy }}",
            yes_task="log_update_user_recovery_group_failed",
            no_task="can_update_users_timesheet"
        )

        log_update_user_recovery_group_failed = rail.WriteLogOperator(
            task_id="log_update_user_recovery_group_failed",
            message="{{ result('update_user_recovery_group').errors.DisplayText }}",
            severity="Error",
            properties={
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_type": "{{dag_run.conf.employee_type}}",
                "department": "{{dag_run.conf.department}}",
                "cost_centre": "{{dag_run.conf.cost_center}}",
                "action": "Update",
                "Status": "Error",
                "details": "{{ result('update_user_recovery_group').errors.DisplayText }}"
            }
        )

        can_update_users_timesheet = rail.IfOperator(
            task_id="can_update_users_timesheet",
            test="{{ result('get_first_timesheet_period_details_from_list').timesheet_period_details | is_truthy }}",
            yes_task="update_user_timesheet_periods",
            no_task="log_timesheet_period_not_present_in_instance"
        )

        update_user_timesheet_periods = rail.RepliconServiceOperator(
            task_id="update_user_timesheet_periods",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['user_uri']
                },
                "modifications": {
                    "timesheetPeriodScheduleToApply": get_timesheet_period_payload_to_apply(dag_run=dag_run,timesheet_period= rail.result(
                                            "get_first_timesheet_period_details_from_list")['first_timesheet_period_from_feed'])
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        is_timesheet_period_update_failed = rail.IfOperator(
            task_id="is_timesheet_period_update_failed",
            test="{{ result('update_user_timesheet_periods').errors | is_truthy }}",
            yes_task="log_timesheet_period_update_failed",
            no_task="get_timesheeturis_to_delete"
        )

        get_timesheeturis_to_delete = rail.RepliconServiceOperator(
            task_id='get_timesheeturis_to_delete',
            endpoint="/services/TimesheetListService1.svc/GetData",
            data=data_handlers.get_timesheet_details_payload,
            data_handler=data_handlers.get_timesheet_uris
        )

        is_timesheet_uris_to_delete = rail.IfOperator(
            task_id='is_timesheet_uris_to_delete',
            test="{{ result('get_timesheeturis_to_delete') | length > 0 }}",
            yes_task="create_timesheet_delete_batch",
            no_task="generate_timesheet_period",
        )

        create_timesheet_delete_batch = rail.RepliconServiceOperator(
            task_id='create_timesheet_delete_batch',
            endpoint="/services/TimesheetService1.svc/CreateTimesheetDeleteBatch",
            data=lambda: {
                "timesheetUris": rail.result('get_timesheeturis_to_delete'),
                "deleteOptionUri": "urn:replicon:timesheet-delete-option:delete-overlapping-time-and-payable-time-entries"
            }
        )

        execute_timesheet_delete_batch = rail.RepliconServiceOperator(
            task_id='execute_timesheet_delete_batch',
            endpoint="/services/TimesheetService1.svc/ExecuteTimesheetDeleteBatch",
            data={
                "timesheetDeleteBatchUri": "{{ result('create_timesheet_delete_batch') }}"
            }
        )

        generate_timesheet_period = rail.RepliconServiceOperator(
            task_id="generate_timesheet_period",
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetailsForDate",
            data=lambda dag_run:{
                "userUri": dag_run.conf['user_uri'],
                "date": get_today_date(),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        log_timesheet_period_update_failed = rail.WriteLogOperator(
            task_id="log_timesheet_period_update_failed",
            message="{{ result('update_user_timesheet_periods').errors.DisplayText }}",
            severity="Error",
            properties={
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_type": "{{dag_run.conf.employee_type}}",
                "department": "{{dag_run.conf.department}}",
                "cost_centre": "{{dag_run.conf.cost_center}}",
                "action": "Update",
                "Status": "Error",
                "details": "{{ result('update_user_timesheet_periods').errors.DisplayText }}"
            }
        )

        log_timesheet_period_not_present_in_instance = rail.WriteLogOperator(
            task_id="log_timesheet_period_not_present_in_instance",
            message="User Recovery is set to Yes, however Timesheet period '' is not present in the instance. Please assigned manually",
            severity="Exception",
            properties={
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_type": "{{dag_run.conf.employee_type}}",
                "department": "{{dag_run.conf.department}}",
                "cost_centre": "{{dag_run.conf.cost_center}}",
                "action": "Update",
                "Status": "Exception",
                "details": 'User Recovery is set to Yes, however Timesheet period '' is not present in the instance. Please assigned manually'
            }
        )

        log_update_success = rail.WriteLogOperator(
            task_id="log_update_success",
            severity="Success",
            message="Updated User's Recovery Enabled to 'Yes'",
            properties={
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_type": "{{dag_run.conf.employee_type}}",
                "department": "{{dag_run.conf.department}}",
                "cost_centre": "{{dag_run.conf.cost_center}}",
                "action": "Update",
                "Status": "Success",
                "details": "Updated user's recovery enabled to 'Yes'"
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

        remove_user_end_date >> enable_user >> is_user_recovery_enable_no >> rail.Label(
            "No") >> update_user_recovery_group
        is_user_recovery_enable_no >> rail.Label("Yes") >> get_timesheet_period_to_apply_from_feed >> get_first_timesheet_period_details_from_list >>\
            update_user_recovery_group >> is_update_failed >> rail.Label("Yes") >> log_update_user_recovery_group_failed\
            >> rail.Label("On Error") >> catch_and_log_error >> log_to_sumo
        is_update_failed >> rail.Label("No") >> can_update_users_timesheet >> rail.Label(
            "Yes") >> update_user_timesheet_periods >> is_timesheet_period_update_failed
        can_update_users_timesheet >> rail.Label(
            "No") >> log_timesheet_period_not_present_in_instance >> rail.Label("On Error") >> catch_and_log_error

        is_timesheet_period_update_failed >> rail.Label(
            "Yes") >> log_timesheet_period_update_failed >> rail.Label("On Error") >> catch_and_log_error
        is_timesheet_period_update_failed >> rail.Label(
            "No") >> get_timesheeturis_to_delete >> is_timesheet_uris_to_delete

        is_timesheet_uris_to_delete >> rail.Label(
            "Yes") >> create_timesheet_delete_batch >> execute_timesheet_delete_batch >> generate_timesheet_period >> log_update_success >> rail.Label(
            "On Error") >> catch_and_log_error

        is_timesheet_uris_to_delete >> rail.Label(
            "No") >> generate_timesheet_period

    return dag


rail.for_each_instance(create_child_dag)
