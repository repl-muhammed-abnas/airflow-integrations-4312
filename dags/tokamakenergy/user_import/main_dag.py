from datetime import datetime, timedelta
import pendulum
import rail
from tokamakenergy.user_import.utils import request_payload, response_filters, custom_methods
from tokamakenergy.user_import.tasks.send_logs import get_send_logs
from airflow.models import Variable
null = None


# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'TokamakEnergy BambooHR to Polaris User Sync Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_active_runs
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_lastsync_time_and_current_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_lastsync_time_and_current_time',
            end_task='update_lastsync_time',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_lastsync_time_and_current_time = rail.PythonOperator(
            task_id='get_lastsync_time_and_current_time',
            python_callable=lambda: {
                "process_start_time": pendulum.now(config.time_zone).strftime(config.STANDARD_EMAIL_DATE_FORMAT),
                "last_synctime": Variable.get(config.last_synctime),
                "current_time": pendulum.now(config.time_zone).strftime(config.BAMBOOHR_LASTCHANGED_DATE_FORMAT)
            }
        )

        get_enabled_employee_type_groups = rail.RepliconServiceOperator(
            task_id='get_enabled_employee_type_groups',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups"
        )

        get_enabled_department_groups = rail.RepliconServiceOperator(
            task_id='get_enabled_department_groups',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.get_department_groups_data_payload,
            data_handler=response_filters.get_enabled_departments
        )

        bamboohr_get_employee_datasets_fields = rail.BambooHROperator(
            task_id='bamboohr_get_employee_datasets_fields',
            company_domain=config.bamboohr_domain,
            request_method='GET',
            endpoint="/datasets/employee/fields",
            bamboohr_conn_id=config.bamboohr_conn_id,
            data_handler=lambda response: response_filters.get_required_employee_datasets_fields(
                response, config.required_employee_fields)
        )

        bamboohr_all_employees_data = rail.BambooHROperator(
            task_id='bamboohr_all_employees_data',
            company_domain=config.bamboohr_domain,
            request_method='POST',
            endpoint="/datasets/employee",
            bamboohr_conn_id=config.bamboohr_conn_id,
            data=lambda: request_payload.get_bamboohr_employees_request("All"),
            data_handler=lambda response: response_filters.get_filtered_employees_details(
                response, "All", config.jobgrade_effective_date_field),
            target='artifact'
        )

        get_job_table_records = rail.BambooHROperator(
            task_id='get_job_table_records',
            company_domain=config.bamboohr_domain,
            request_method='GET',
            endpoint="/employees/changed/tables/jobInfo?since=" + "{{ result('get_lastsync_time_and_current_time').last_synctime }}",
            bamboohr_conn_id=config.bamboohr_conn_id
        )

        get_employment_table_records = rail.BambooHROperator(
            task_id='get_employment_table_records',
            company_domain=config.bamboohr_domain,
            request_method='GET',
            endpoint="/employees/changed/tables/employmentStatus?since=" + "{{ result('get_lastsync_time_and_current_time').last_synctime }}",
            bamboohr_conn_id=config.bamboohr_conn_id
        )

        get_jobgrade_table_records = rail.BambooHROperator(
            task_id='get_jobgrade_table_records',
            company_domain=config.bamboohr_domain,
            request_method='GET',
            endpoint="/employees/changed/tables/customJobGrade?since=" + "{{ result('get_lastsync_time_and_current_time').last_synctime }}",
            bamboohr_conn_id=config.bamboohr_conn_id
        )

        bamboohr_updated_employees_data = rail.BambooHROperator(
            task_id='bamboohr_updated_employees_data',
            company_domain=config.bamboohr_domain,
            request_method='POST',
            endpoint="/datasets/employee",
            bamboohr_conn_id=config.bamboohr_conn_id,
            data=lambda: request_payload.get_bamboohr_employees_request("Updated"),
            data_handler=lambda response: response_filters.get_filtered_employees_details(
                response, "Updated", config.jobgrade_effective_date_field),
            target='artifact'
        )

        has_bamboohr_updated_employees_data = rail.IfOperator(
            task_id='has_bamboohr_updated_employees_data',
            test=lambda: len(rail.load_all_records(rail.result('bamboohr_updated_employees_data'))) > 0,
            yes_task='create_log',
            no_task='send_no_records_email'
        )

        send_no_records_email = rail.EmailOperator(
            task_id='send_no_records_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon User Sync from BambooHR to Polaris - Completed - No Records Processed | {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/no_records_email.html"
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_permission_set_uris = rail.RepliconServiceOperator(
            task_id='get_permission_set_uris',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda response: response_filters.get_required_permission_set_uris(response, config.supervisor_permission_set)
        )

        get_all_user_oefs = rail.RepliconServiceOperator(
            task_id="get_all_user_oefs",
            endpoint="services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data=lambda: {
                    "bindingContextUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda oefs_list: {
                "budgetcode_oef": rail.find_first_by_attr_and_get_attr(oefs_list, 'name', "Budget Code", 'uri'),
                "budgetsubsystemcode_oef": rail.find_first_by_attr_and_get_attr(oefs_list, 'name', "Budget Sub-System Code", 'uri'),
                "careerfamily_oef": rail.find_first_by_attr_and_get_attr(oefs_list, 'name', "Career Family", 'uri'),
                "grade_oef": rail.find_first_by_attr_and_get_attr(oefs_list, 'name', "Grade: 1-8", 'uri'),
                "overtimestatus_oef": rail.find_first_by_attr_and_get_attr(oefs_list, 'name', "Overtime Status", 'uri'),
                "timesheetuser_oef": rail.find_first_by_attr_and_get_attr(oefs_list, 'name', "Timesheet User", 'uri')
            }
        )

        get_budgetcode_oef_values = rail.RepliconServiceOperator(
            task_id='get_budgetcode_oef_values',
            endpoint="/services/ObjectExtensionTagService1.svc/GetPageOfObjectExtensionTagsFilteredBySearch",
            data=lambda: request_payload.get_oef_values_payload(rail.result("get_all_user_oefs")["budgetcode_oef"]),
            data_handler=lambda response: response_filters.get_oef_dropdown_value_uri(response, "Budget Code",
                "budgetcode_oef", "budgetcode")
        )

        get_budgetcodesystem_oef_values = rail.RepliconServiceOperator(
            task_id='get_budgetcodesystem_oef_values',
            endpoint="/services/ObjectExtensionTagService1.svc/GetPageOfObjectExtensionTagsFilteredBySearch",
            data=lambda: request_payload.get_oef_values_payload(rail.result("get_all_user_oefs")["budgetsubsystemcode_oef"]),
            data_handler=lambda response: response_filters.get_oef_dropdown_value_uri(response, "Budget Sub-System Code",
                "budgetsubsystemcode_oef", "budgetsubsystemcode")
        )

        get_careerfamily_oef_values = rail.RepliconServiceOperator(
            task_id='get_careerfamily_oef_values',
            endpoint="/services/ObjectExtensionTagService1.svc/GetPageOfObjectExtensionTagsFilteredBySearch",
            data=lambda: request_payload.get_oef_values_payload(rail.result("get_all_user_oefs")["careerfamily_oef"]),
            data_handler=lambda response: response_filters.get_oef_dropdown_value_uri(response, "Career Family",
                "careerfamily_oef", "careerfamily")
        )

        get_grade_oef_values = rail.RepliconServiceOperator(
            task_id='get_grade_oef_values',
            endpoint="/services/ObjectExtensionTagService1.svc/GetPageOfObjectExtensionTagsFilteredBySearch",
            data=lambda: request_payload.get_oef_values_payload(rail.result("get_all_user_oefs")["grade_oef"]),
            data_handler=lambda response: response_filters.get_oef_dropdown_value_uri(response, "Grade: 1-8",
                "grade_oef", "grade")
        )

        get_overtimestatus_oef_values = rail.RepliconServiceOperator(
            task_id='get_overtimestatus_oef_values',
            endpoint="/services/ObjectExtensionTagService1.svc/GetPageOfObjectExtensionTagsFilteredBySearch",
            data=lambda: request_payload.get_oef_values_payload(rail.result("get_all_user_oefs")["overtimestatus_oef"]),
            data_handler=lambda response: response_filters.get_oef_dropdown_value_uri(response, "Overtime Status",
                "overtimestatus_oef", "overtimestatus")
        )

        get_timesheetuser_oef_values = rail.RepliconServiceOperator(
            task_id='get_timesheetuser_oef_values',
            endpoint="/services/ObjectExtensionTagService1.svc/GetPageOfObjectExtensionTagsFilteredBySearch",
            data=lambda: request_payload.get_oef_values_payload(rail.result("get_all_user_oefs")["timesheetuser_oef"]),
            data_handler=lambda response: response_filters.get_oef_dropdown_value_uri(response, "Timesheet User",
                "timesheetuser_oef", "timesheetuser")
        )

        trigger_process_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_process_user',
            items='{{ result("bamboohr_updated_employees_data") | load_all_records | to_json }}',
            trigger_dag_id=config.process_user_child_dagid,
            conf=lambda item: {
                "user_details": item,
                "oef_uris": rail.result("get_all_user_oefs"),
                "supervisor_permission_sets": rail.result("get_permission_set_uris"),
                "last_synctime": rail.result('get_lastsync_time_and_current_time')['last_synctime'],
                "oef_details": [
                    rail.result("get_budgetcode_oef_values"),
                    rail.result("get_budgetcodesystem_oef_values"),
                    rail.result("get_careerfamily_oef_values"),
                    rail.result("get_grade_oef_values"),
                    rail.result("get_overtimestatus_oef_values"),
                    rail.result("get_timesheetuser_oef_values")
                ],
                "process_start_time": datetime.strptime(rail.result('get_lastsync_time_and_current_time')['process_start_time'],
                    config.STANDARD_EMAIL_DATE_FORMAT).strftime(config.MDY_DATE_FORMAT)
            }
        )

        wait_for_process_users = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_users",
            dag_runs="{{result('trigger_process_user')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs='{{ result("trigger_process_user") }}',
            dagrun_task_id='create_user_log',
            flatten=True
        )

        format_log_records = rail.CreateCollectionOperator(
            task_id='format_log_records',
            source=custom_methods.do_format_logs,
            columns=["username", "employee_id", "action", "status", "comments", "ecid"],
            name='timeoff_bookings_records'
        )

        send_logs_enter, send_logs_end = get_send_logs(config)

        update_lastsync_time = rail.PythonOperator(
            task_id='update_lastsync_time',
            python_callable=lambda: Variable.set(key=config.last_synctime, value=rail.result("get_lastsync_time_and_current_time")["current_time"])
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_user_sync'
        )

        fail_user_sync = rail.FailOperator(
            task_id='fail_user_sync',
            message='{{ get_error_message() }}'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> update_lastsync_time
        can_run_batch_task >> rail.Label(
            'No') >> get_lastsync_time_and_current_time >> get_enabled_employee_type_groups \
                >> get_enabled_department_groups >> bamboohr_get_employee_datasets_fields >> bamboohr_all_employees_data \
                    >> get_job_table_records >> get_employment_table_records >> get_jobgrade_table_records \
                        >> bamboohr_updated_employees_data >> has_bamboohr_updated_employees_data
        has_bamboohr_updated_employees_data >> rail.Label("Yes") >> create_log >> get_permission_set_uris \
            >> get_all_user_oefs >> get_budgetcode_oef_values >> get_budgetcodesystem_oef_values \
                >> get_careerfamily_oef_values >> get_grade_oef_values >> get_overtimestatus_oef_values \
                    >> get_timesheetuser_oef_values >> trigger_process_user \
            >> wait_for_process_users >> gather_user_logs >> format_log_records >> send_logs_enter
        send_logs_end >> update_lastsync_time
        has_bamboohr_updated_employees_data >> rail.Label("No") >> send_no_records_email >> update_lastsync_time >> dagrun_log_to_sumo >> should_fail_dag
        should_fail_dag >> rail.Label("Yes") >> fail_user_sync

    return dag


rail.for_each_instance(create_main_dag)
