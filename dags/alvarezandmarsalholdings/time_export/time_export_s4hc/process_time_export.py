from datetime import timedelta
from pendulum import datetime
import rail
from alvarezandmarsalholdings.time_export.time_export_s4hc.utils import custom_methods

#pylint: disable=too-many-statements

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.time_export_to_s4hc_dag_id,
        description="Alvarez and Marsal Holdings Time Export process time export",
        start_date=datetime(2023, 12, 1, tz=config.time_zone),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        response_from_dag_var = rail.SetVariableOperator(
            task_id="response_from_dag_var",
            name='response_from_dag',
            append=False,
            value="Success"
        )

        time_export_download_script_uri = rail.RepliconServiceOperator(
            task_id='time_export_download_script_uri',
            endpoint='/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts',
            data_handler=lambda response: custom_methods.get_timeexport_fileformat(
                config, response)
        )

        create_download_batch = rail.RepliconServiceOperator(
            task_id='create_download_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch',
            data=lambda dag_run: custom_methods.form_download_parameters(
                rail.result('time_export_download_script_uri'), dag_run),
        )

        execute_download_batch, wait_for_download_batch = rail.batch_execution(
            group_id='execute_download_batch',
            creation_task_id=create_download_batch.task_id,
        )

        get_download_url = rail.RepliconServiceOperator(
            task_id='get_download_url',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults',
            data={
                "timeDataDownloadBatchUri": "{{ result('" + create_download_batch.task_id + "') }}"
            },
            data_handler=lambda response: response['downloadUrl'],

        )

        download_export = rail.HTTPDownloadFileOperator(
            task_id='download_export',
            url="{{ result('get_download_url') }}",
        )

        load_export = rail.LoadCSVFileOperator(
            task_id='load_export',
            document="{{ result('download_export') }}",
        )

        create_raw_timeexport_data_collection = rail.CreateCollectionOperator(
            task_id="create_raw_timeexport_data_collection",
            source="{{result('load_export')}}",
            name="raw_timeexport_data",
            columns={
                'TimeEntryID': 'timeentry_id',
                'EmployeeID': 'employee_id',
                'CostCenterCode': 'cost_center_code',
                'EntryDate': 'entry_date',
                'ControllingArea': 'controlling_area',
                'JobCategoryName': 'job_category_name',
                'WorkPackage_WorkItemFullPath': 'work_package_work_item_full_path',
                'BillingControlCategory': 'billing_control_category',
                'Comments': 'comments',
                'Hours': 'hours',
                'WorkLocationCode': 'work_location_code',
                'ProjectProfile':'project_profile',
                'TimeOffTypeName': 'timeoff_type_name',
                'LoginName': 'login_name',
                'EntryId': 'entry_id',
                'WorkPackage_WorkItem_Code': 'work_package_work_item_code',
                'ProjectCode': 'wbs_project_code',
                'work_package_code': 'work_package_code'
            }
        )

        has_any_timeexport_data = rail.IfOperator(
            task_id="has_any_timeexport_data",
            test="{{result('create_raw_timeexport_data_collection', 'length') > 0 }}",
            yes_task="query_blank_employee_id_records",
            no_task="set_response_from_dag_no_data"
        )

        set_response_from_dag_no_data = rail.SetVariableOperator(
            task_id="set_response_from_dag_no_data",
            name='response_from_dag',
            append=False,
            value="No Data in export"
        )

        query_blank_employee_id_records = rail.QueryCollectionOperator(
            task_id="query_blank_employee_id_records",
            query="""SELECT DISTINCT login_name , employee_id FROM raw_timeexport_data rtd WHERE NULLIF(rtd.employee_id, '') IS NULL"""
        )

        has_any_blank_emp_id = rail.IfOperator(
            task_id="has_any_blank_emp_id",
            test="{{ result('query_blank_employee_id_records', 'length') > 0}}",
            yes_task="empty_has_any_blank_emp_id_yes_task",
            no_task="trigger_post_to_api"
        )

        empty_has_any_blank_emp_id_yes_task = rail.EmptyOperator(
            task_id="empty_has_any_blank_emp_id_yes_task"
        )

        missing_employeeid_csv = rail.WriteCSVFileOperator(
            task_id='missing_employeeid_csv',
            source="{{ result('query_blank_employee_id_records') }}",
            header=['LoginName', 'EmployeeID'],
            row=lambda item: [
                item['login_name'],
                item["employee_id"]
            ]
        )

        generate_download_link_missing_employeeid_records_csv = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link_missing_employeeid_records_csv',
            artifact_name="{{result('missing_employeeid_csv')}}",
            output_file_name="Invalid_TimeExport_records_{{dag_run_ecid()}}.csv",
            expires_in_seconds=7*24*60*60
        )

        send_invalid_records_email = rail.EmailOperator(
            task_id='send_invalid_records_email',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='{{ get_company_key() }} | Replicon Time Data Export to S4HC - Invalid records found - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_invalid_records_in_export.html"
        )

        set_response_from_dag_blank_employee_id_found = rail.SetVariableOperator(
            task_id="set_response_from_dag_blank_employee_id_found",
            name='response_from_dag',
            append=False,
            value="Blank employee id entry found, thus stopping the time export"
        )

        trigger_post_to_api = rail.TriggerDagRunOperator(
            task_id = "trigger_post_to_api",
            trigger_dag_id=config.time_export_post_to_s4hc_child_dag_id,
            conf = lambda dag_run: {**dag_run.conf},
            retries=0,
            execution_timeout = timedelta(days=config.execution_timeout_days_for_posting)
        )

        wait_for_trigger_post_to_api = rail.WaitForDagRunsSensor(
            task_id = "wait_for_trigger_post_to_api",
            dag_runs="{{result('trigger_post_to_api')}}",
            retries=0,
            execution_timeout = timedelta(days=config.execution_timeout_days_for_posting)
        )

        catch_error =rail.SetVariableOperator(
            task_id="catch_error",
            trigger_rule="one_failed",
            name='response_from_dag',
            append=False,
            value="Error in child dag - Time export to S4HC"
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule="all_done",
            python_callable=lambda: rail.get_dag_run_var('response_from_dag')
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            trigger_rule="all_done",
            test="{{ get_error_message() | is_truthy }}",
            yes_task="fail_dag_due_to_error",
        )
 
        fail_dag_due_to_error = rail.FailOperator(
            task_id="fail_dag_due_to_error",
            message='Failure in processing Time Export to S4HC- {{ get_error_message() }}'
        )

        response_from_dag_var >> time_export_download_script_uri >> create_download_batch >> \
            execute_download_batch >> wait_for_download_batch >> get_download_url >> download_export >> \
                load_export >> create_raw_timeexport_data_collection >> has_any_timeexport_data

        has_any_timeexport_data >> rail.Label("No") >> set_response_from_dag_no_data >> catch_error

        has_any_timeexport_data >> rail.Label("Yes") >> query_blank_employee_id_records >> has_any_blank_emp_id

        has_any_blank_emp_id >> rail.Label("Yes") >> empty_has_any_blank_emp_id_yes_task >> missing_employeeid_csv >> \
            generate_download_link_missing_employeeid_records_csv >> send_invalid_records_email >> \
                set_response_from_dag_blank_employee_id_found >> catch_error

        has_any_blank_emp_id >> rail.Label("No") >> trigger_post_to_api >> \
            wait_for_trigger_post_to_api >> catch_error

        catch_error >> final_response_from_dag >> can_fail_dag 
        can_fail_dag >> rail.Label("Yes") >> fail_dag_due_to_error
        
    return dag


rail.for_each_instance(create_main_dag)
