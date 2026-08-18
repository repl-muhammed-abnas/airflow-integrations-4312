from datetime import timedelta
import rail
from itvdaytime.user_import.utils import custom_methods, data_handler
from itvdaytime.user_import.utils.request_payload import get_assign_timeoff_payload
from itvdaytime.user_import.tasks.supervisor_task import get_supervisor_task


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"itvdaytime_user_import_update_user_{config.instance}",
        description=f"iTV DayTime User Import Update User {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{dag_run.conf.user_uri}}"
                    }
                ],
            }
        )

        is_end_date_present = rail.IfOperator(
            task_id="is_end_date_present",
            test="{{dag_run.conf.termination_date | is_truthy}}",
            yes_task="dummy_end_date_update",
            no_task="prepare_update_payload"
        )

        def get_update_employment_date_range_payload(dag_run):
            start_date = rail.result("get_user_details")[0]['userDetails'][
                'employmentDateRange']['startDate']
            end_date = custom_methods.get_replicon_date(
                dag_run.conf['termination_date'])
            return{
                "userUri": rail.result("get_user_details")[0]["userDetails"]['uri'],
                "dateRange": {
                    "startDate": {
                        "year": start_date['year'],
                        "month": start_date['month'],
                        "day": start_date['day']
                    },
                    "endDate": end_date,
                    "relativeDateRangeUri": None,
                    "relativeDateRangeAsOfDate": None
                }
            }
        dummy_end_date_update = rail.EmptyOperator(
            task_id="dummy_end_date_update",
        )
        update_end_date = rail.RepliconServiceOperator(
            task_id="update_end_date",
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=get_update_employment_date_range_payload
        )

        disable_user_login = rail.RepliconServiceOperator(
            task_id="disable_user_login",
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            }

        )

        log_user_disabled = rail.WriteLogOperator(
            task_id="log_user_disabled",
            severity="Success",
            message="User Disabled successfully",
            properties={
                "employee_number": "{{dag_run.conf.employee_number}}",
                "loginname": "{{dag_run.conf.first_name}}" + '.' + "{{dag_run.conf.last_name}}",
                "status": "Success",
                "action": "Update",
                "details": "User Disabled successfully",
                "line_manager": "{{dag_run.conf.line_manager}}",
                "user_uri": "{{dag_run.conf.user_uri}}",
                "allowed_for_supervisor_processing": "No"
            }

        )

        def get_effective_service_center(response):
            return_value = {"service_center": None, "employee_type": None}
            if response['serviceCenters']:
                return_value['service_center'] = response['serviceCenters'][0]['serviceCenter']['serviceCenter']
            if response['employeeTypes']:
                return_value['employee_type'] = response['employeeTypes'][0]['employeeType']['employeeType']

            return return_value

        get_effective_groups = rail.RepliconServiceOperator(
            task_id="get_effective_groups",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{dag_run.conf.user_uri}}"
            },
            data_handler=get_effective_service_center
        )

        get_all_timeoffs = rail.RepliconServiceOperator(
            task_id="get_all_timeoffs",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            data_handler=data_handler.get_timeoff_types
        )

        prepare_update_payload = rail.PythonOperator(
            task_id="prepare_update_payload",
            python_callable=custom_methods.get_update_user_payload
        )

        update_user_details = rail.RepliconServiceOperator(
            task_id="update_user_details",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data="{{result('prepare_update_payload').payload | to_json}}"
        )

        has_update_failed = rail.IfOperator(
            task_id="has_update_failed",
            test="{{result('update_user_details').errors | is_truthy}}",
            yes_task="fail_update_user",
            no_task="process_supervisor_and_timeoff"
        )

        fail_update_user = rail.FailOperator(
            task_id="fail_update_user",
            message='{{get_error_message()}}'
        )

        user_uri = "{{dag_run.conf.user_uri}}"

        supervisor_start, supervisor_end = get_supervisor_task(user_uri)

        process_supervisor_and_timeoff = rail.EmptyOperator(
            task_id="process_supervisor_and_timeoff"
        )

        is_job_role_changed = rail.IfOperator(
            task_id="is_job_role_changed",
            test=lambda dag_run: True if not dag_run.conf['job_role'] else (dag_run.conf['job_role'] != custom_methods.get_custom_field_value(
                data=rail.result('get_user_details')[
                    0]['userDetails']['customFieldValues'],
                search_value="Job Title")),
            yes_task="get_new_timeoff_types_to_assign",
            no_task="can_update_balance"
        )

        get_new_timeoff_types_to_assign = rail.PythonOperator(
            task_id="get_new_timeoff_types_to_assign",
            python_callable=lambda dag_run: custom_methods.get_timeoffs_to_assign_from_mapper(
                config, dag_run)
        )

        assign_new_timeoffs = rail.RepliconServiceOperator(
            task_id="assign_new_timeoffs",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: get_assign_timeoff_payload(
                dag_run, is_create=False)
        )

        def bool_can_update_balance(dag_run):
            if dag_run.conf['annual_leave_entitlement'] and dag_run.conf['ale_effective_date']:
                return True
            if dag_run.conf['carry_forward'] and dag_run.conf['carry_forward_effective_date']:
                return True
            if dag_run.conf['relish_purchased_holiday'] and dag_run.conf['relish_start_date']:
                return True
            return False

        can_update_balance = rail.IfOperator(
            task_id="can_update_balance",
            test=bool_can_update_balance,
            yes_task="process_each_timeoff",
            no_task="log_update_complete"
        )

        def get_conf(item, dag_run):
            effective_date, balance = custom_methods.get_effective_date_balance(
                dag_run, item)
            return {
                "file_name": dag_run.conf['file_name'],
                "employee_number": dag_run.conf['employee_number'],
                "loginname": dag_run.conf['first_name']+'.'+dag_run.conf['last_name'],
                "line_manager": dag_run.conf['line_manager'],
                "timeoff_name": item,
                "timeoff_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeoffs"), 'name', item, 'uri'),
                "user_uri": dag_run.conf['user_uri'],
                "effective_date": effective_date,
                "balance": balance,
                "timeoff_type_details": rail.find_first_by_attr_and_get_attr(
                    custom_methods.get_data_from_document(dag_run.conf['time_off_details_collection']), 'name', item)
            }

        process_each_timeoff = rail.TriggerDagRunForEachItemOperator(
            task_id="process_each_timeoff",
            items=lambda dag_run: custom_methods.get_timeoffs_to_assign_from_mapper(
                config, dag_run),
            trigger_dag_id=f"itvdaytime_user_import_process_each_timeoff_{config.instance}",
            conf=get_conf,
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_for_process_each_timeoff = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_each_timeoff",
            dag_runs="{{result('process_each_timeoff')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_failed_logs = rail.GatherResultsFromDagRunsOperator(
            dagrun_task_id="catch_and_log_error",
            task_id="gather_failed_logs",
            dag_runs="{{result('process_each_timeoff')}}",
            flatten=True
        )

        can_log_success = rail.IfOperator(
            task_id="can_log_success",
            test="{{result('gather_failed_logs') | is_falsy}}",
            yes_task="log_update_complete"
        )

        log_update_complete = rail.WriteLogOperator(
            task_id="log_update_complete",
            severity="Success",
            message="User updated successfully",
            properties={
                "employee_number": "{{dag_run.conf.employee_number}}",
                "loginname": "{{dag_run.conf.first_name}}" + '.' + "{{dag_run.conf.last_name}}",
                "status": "Success",
                "action": "Update",
                "details": "User updated successfully {{result('prepare_update_payload').changed_fields}}",
                "line_manager": "{{dag_run.conf.line_manager}}",
                "user_uri": "{{dag_run.conf.user_uri}}",
                "allowed_for_supervisor_processing": "No"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                    'employee_number': '{{ dag_run.conf.employee_number }}',
                    'loginname': '{{ dag_run.conf.first_name }}' + '.{{dag_run.conf.last_name}}',
                    "action": "Update",
                    'status': "Error",
                    'details': 'User partially updated; {{get_error_message()}}',
                    "line_manager": "{{dag_run.conf.line_manager}}",
                    "user_uri": "{{dag_run.conf.user_uri}}",
                    "allowed_for_supervisor_processing": "Yes"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        get_user_details >> get_effective_groups >> get_all_timeoffs >> is_end_date_present >> rail.Label(
            "No") >> prepare_update_payload >> update_user_details >> has_update_failed

        is_end_date_present >> rail.Label("Yes") >> dummy_end_date_update >> update_end_date >> disable_user_login >> log_user_disabled >> rail.Label(
            "On error") >> catch_and_log_errors
        has_update_failed >> rail.Label("Yes") >> fail_update_user >> rail.Label(
            "On error") >> catch_and_log_errors

        has_update_failed >> rail.Label("No") >> process_supervisor_and_timeoff >> [
            supervisor_start, is_job_role_changed]

        is_job_role_changed >> rail.Label(
            "Yes") >> get_new_timeoff_types_to_assign >> assign_new_timeoffs >> can_update_balance

        is_job_role_changed >> rail.Label("No") >> can_update_balance
        can_update_balance >> rail.Label("No") >> log_update_complete
        can_update_balance >> rail.Label(
            "Yes") >> process_each_timeoff >> wait_for_process_each_timeoff >> gather_failed_logs >> can_log_success >> rail.Label("Yes") >> log_update_complete
        supervisor_end >> log_update_complete >> rail.Label(
            "On error") >> catch_and_log_errors >> log_to_sumo

        return dag


rail.for_each_instance(create_child_dag)
