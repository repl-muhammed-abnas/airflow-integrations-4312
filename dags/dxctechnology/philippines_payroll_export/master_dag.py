from datetime import timedelta, datetime as dt
import pendulum
import rail
from dxctechnology.philippines_payroll_export.utils import request_payload, custom_method


def create_child_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'DXCTechnology_philippines_Payroll_Export_Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date= pendulum.datetime(2022, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_process_run = rail.IfOperator(
            task_id="can_process_run",
            test=lambda: custom_method.can_process_run_test(config),
            yes_task="get_all_scripts"
        )

        get_all_scripts = rail.RepliconServiceOperator(
            task_id='get_all_scripts',
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts"
        )

        get_enabled_companycodes = rail.RepliconServiceOperator(
            task_id='get_enabled_companycodes',
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
            data_handler=lambda res: [rail.find_first_by_attr_and_get_attr(
                res, 'displayText', name, 'uri') for name in config.division_names]
        )

        get_enabled_employeetype_groups = rail.RepliconServiceOperator(
            task_id='get_enabled_employeetype_groups',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups"
        )

        get_child_hierarchy_data = rail.RepliconServiceOperator(
            task_id='get_child_hierarchy_data',
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetChildHierarchyData",
            data=request_payload.get_employeetype_child_hierarchy,
            response_filter=custom_method.convert_location_hierarchy
        )

        current_export_details = rail.PythonOperator(
            task_id="current_export_details",
            python_callable=lambda: custom_method.get_current_export_details(config)
        )

        create_payroll_download_batch = rail.RepliconServiceOperator(
            task_id="create_payroll_download_batch",
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data=request_payload.get_create_payroll_download_batch_payload
        )

        execute_payroll_download_batch, wait_for_payroll_download_batch = rail.batch_execution(
            'execute_payroll_download_batch', create_payroll_download_batch.task_id)

        get_payroll_run_batch_result = rail.RepliconServiceOperator(
            task_id="get_payroll_run_batch_result",
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={
                "payrollDownloadBatchUri": "{{ result('create_payroll_download_batch') }}"
            }
        )

        download_payload_file_from_url = rail.HTTPDownloadFileOperator(
            task_id="download_payload_file_from_url",
            url="{{ result('get_payroll_run_batch_result').downloadUrl }}"
        )

        load_payload_file = rail.LoadCSVFileOperator(
            task_id="load_payload_file",
            document="{{ result('download_payload_file_from_url') }}"
        )

        create_payroll_data_collection = rail.CreateCollectionOperator(
            task_id='create_payroll_data_collection',
            name='payroll_data',
            source="{{ result('load_payload_file') }}"
        )

        has_payroll_data = rail.IfOperator(
            task_id='has_payroll_data',
            test="{{ result('create_payroll_data_collection','length') > 0 }}",
            yes_task='create_payrun_batch',
            no_task='send_email_for_no_payroll_data'
        )

        send_email_for_no_payroll_data = rail.EmailOperator(
            task_id='send_email_for_no_payroll_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon Philippines Payroll Export is Skipped on - {{ current_time_in_specified_tz() }}',
            html_content="templates/email/blank_export.html"
        )

        create_payrun_batch = rail.RepliconServiceOperator(
            task_id="create_payrun_batch",
            endpoint="/services/PayRunService1.svc/CreatePayRunBatch",
            data=request_payload.get_create_payrun_batch_payload
        )

        execute_payrun_batch, wait_forpayrun_batch = rail.batch_execution(
            'execute_payrun_batch', create_payrun_batch.task_id)

        get_payrun_batch_result = rail.RepliconServiceOperator(
            task_id="get_payrun_batch_result",
            endpoint="/services/PayRunService1.svc/GetCreatePayRunBatchResults",
            data={"payRunBatchUri": "{{ result('create_payrun_batch') }}"}
        )

        update_payrun_name = rail.RepliconServiceOperator(
            task_id="update_payrun_name",
            endpoint="/services/PayRunService1.svc/UpdatePayRunName",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}",
                },
                "name":  "{{ result('current_export_details').replicon_export_name }}"
            }
        )

        update_payrun_description = rail.RepliconServiceOperator(
            task_id="update_payrun_description",
            endpoint="/services/PayRunService1.svc/UpdatePayRunDescription",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}",
                },
                "description":  "{{ result('current_export_details').replicon_export_name }}"
            }
        )

        create_payrun_download_batch = rail.RepliconServiceOperator(
            task_id="create_payrun_download_batch",
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data=request_payload.get_create_payrun_download_batch_payload
        )

        execute_payrun_download_batch, wait_for_payrun_download_batch = rail.batch_execution(
            'execute_payrun_download_batch', create_payrun_download_batch.task_id)

        get_payrun_download_batch_result = rail.RepliconServiceOperator(
            task_id="get_payrun_download_batch_result",
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={
                "payrollDownloadBatchUri": "{{ result('create_payrun_download_batch') }}"}
        )

        mark_payrun_as_complete = rail.RepliconServiceOperator(
            task_id="mark_payrun_as_complete",
            endpoint="/services/PayRunService1.svc/MarkPayRunAsComplete",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}"
                }
            }
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed'
        )

        download_final_payload_file_from_url = rail.HTTPDownloadFileOperator(
            task_id="download_final_payload_file_from_url",
            url="{{ result('get_payrun_download_batch_result').downloadUrl }}"
        )

        load_final_payload_file = rail.LoadCSVFileOperator(
            task_id="load_final_payload_file",
            document="{{ result('download_final_payload_file_from_url') }}"
        )

        create_csv_lines_for_raw_data = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_for_raw_data',
            source="{{ result('load_final_payload_file') }}",
            header=['Employee_ID',
                    'Entry_Date',
                    'Pay_Code_Code',
                    'Pay_Code_Name',
                    'Pay_Code_Hours',
                    'Worker_Type',
                    'User_Status',
                    'Cost_Center_Name',
                    'User'],
            row=lambda item: [
                item['Employee ID'],
                dt.strptime(item['Entry Date'], '%d %B %Y').strftime(
                    "%Y-%m-%d") if item['Entry Date'] else None,
                item['Pay Code Code'],
                item['Pay Code Name'],
                item['Pay Code Hours'],
                item['Worker Type'],
                item['User Status'],
                item['Cost Center Name'],
                item['User']
            ],
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        create_final_payroll_data_collection = rail.CreateCollectionOperator(
            task_id='create_final_payroll_data_collection',
            name='finalpayrolldata',
            source="{{ result('create_csv_lines_for_raw_data') }}"
        )

        query_final_payroll_data_without_empid = rail.QueryCollectionOperator(
            task_id='query_final_payroll_data_without_empid',
            query='''SELECT * From finalpayrolldata WHERE NULLIF(Employee_ID, '') IS NULL OR Employee_ID=="" '''
        )

        has_empty_empid_data = rail.IfOperator(
            task_id='has_empty_empid_data',
            test="{{ result('query_final_payroll_data_without_empid','length') > 0 }}",
            yes_task='mark_payrun_as_draft',
            no_task='get_all_pay_codes_from_mapper'
        )

        mark_payrun_as_draft = rail.RepliconServiceOperator(
            task_id="mark_payrun_as_draft",
            endpoint="/services/PayRunService1.svc/MarkPayRunAsDraft",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}"
                }
            }
        )

        cancel_payrun = rail.RepliconServiceOperator(
            task_id="cancel_payrun",
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}"
                }
            }
        )

        fail_export = rail.FailOperator(
            task_id="fail_export",
            message="Employee ID not present for some users. Users available to validate in payrun \
                '{{ result('current_export_details').replicon_export_name }}'"
        )

        get_all_pay_codes_from_mapper = rail.PythonOperator(
            task_id='get_all_pay_codes_from_mapper',
            python_callable=lambda: request_payload.get_all_required_pacodes(config.regular_paycodes_mapper) + ',' + request_payload.get_all_required_pacodes(
                config.timeoff_paycodes)
        )

        query_list_in_final_payroll_collection = rail.QueryCollectionOperator(
            task_id='query_list_in_final_payroll_collection',
            query="""SELECT * FROM finalpayrolldata WHERE Pay_Code_Code IN ({{result('get_all_pay_codes_from_mapper')}})"""
        )

        has_payroll_data_to_export = rail.IfOperator(
            task_id='has_payroll_data_to_export',
            test="{{ result('query_list_in_final_payroll_collection','length') > 0 }}",
            yes_task='process_regular_export_child'
        )

        process_regular_export_child = rail.TriggerDagRunOperator(
            task_id='process_regular_export_child',
            retries=0,
            trigger_dag_id=config.regular_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.child_dag_conf
        )

        process_timeoff_export_child = rail.TriggerDagRunOperator(
            task_id='process_timeoff_export_child',
            retries=0,
            trigger_dag_id=config.timeoff_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.child_dag_conf
        )

        can_process_run >> rail.Label(
            "Yes") >> get_all_scripts >> get_enabled_companycodes >> get_enabled_employeetype_groups >> get_child_hierarchy_data >> current_export_details

        current_export_details >> create_payroll_download_batch >> \
            execute_payroll_download_batch >> wait_for_payroll_download_batch >> get_payroll_run_batch_result >> download_payload_file_from_url >> \
            load_payload_file >> create_payroll_data_collection >> has_payroll_data

        has_payroll_data >> rail.Label(
            'Yes') >> create_payrun_batch >> execute_payrun_batch >> wait_forpayrun_batch >> get_payrun_batch_result

        has_payroll_data >> rail.Label(
            'No') >> send_email_for_no_payroll_data

        get_payrun_batch_result >> update_payrun_name >> update_payrun_description >> create_payrun_download_batch >> execute_payrun_download_batch \
            >> wait_for_payrun_download_batch >> get_payrun_download_batch_result >> mark_payrun_as_complete

        mark_payrun_as_complete >> rail.Label(
            "on_success") >> download_final_payload_file_from_url

        mark_payrun_as_complete >> rail.Label(
            "on_error") >> catch_error >> cancel_payrun >> fail_export

        download_final_payload_file_from_url >> load_final_payload_file >> create_csv_lines_for_raw_data >> create_final_payroll_data_collection >> \
            query_final_payroll_data_without_empid >> has_empty_empid_data

        has_empty_empid_data >> rail.Label(
            'Yes') >> mark_payrun_as_draft >> cancel_payrun >> fail_export

        has_empty_empid_data >> rail.Label(
            'No') >> get_all_pay_codes_from_mapper >> query_list_in_final_payroll_collection >> has_payroll_data_to_export

        has_payroll_data_to_export >> rail.Label(
            'Yes') >> process_regular_export_child >> process_timeoff_export_child

    return dag


rail.for_each_instance(create_child_dag)
