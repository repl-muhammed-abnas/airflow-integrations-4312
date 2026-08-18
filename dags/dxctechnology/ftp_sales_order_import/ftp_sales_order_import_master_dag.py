from datetime import timedelta
import rail
from dxctechnology.ftp_sales_order_import import request_payload
from dxctechnology.ftp_sales_order_import import response_filter
from dxctechnology.ftp_sales_order_import.mapper.ftp_so_cost_centers import so_cost_center_mapper
from dxctechnology.ftp_sales_order_import.send_logs import get_send_logs

# config
# https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/ftp_sales_order_import/config.py

def create_main_airflow_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id = f'dxctechnology_ftp_sales_order_import_master{dag_id_postfix}',
        description = 'DXC_FTP_SalesOrder_Automation Master',
        company_key = config.company_key,
        replicon_conn_id = config.replicon_conn_id,
        schedule_interval = timedelta(seconds=30),
        max_active_runs = 1,
        default_args = {
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id = 'new_file_sensor',
            path = config.input_filepath,
            soft_fail_timeout= timedelta(minutes=10),
        )

        is_xml = rail.IfOperator(
            task_id = 'is_xml',
            test = '{{ result("new_file_sensor") | file_ext | lower == "xml" }}',
            yes_task = 'download_file',
            no_task = 'send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id = 'send_bad_file_format_email',
            to = config.tenant_email,
            subject = '{{ get_company_key() }} | Replicon project sync for FTP Sales Order - Incorrect File Format - {{ current_time_in_specified_tz() }}',
            html_content = "email_bad_file_format.html",
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id = 'download_file',
            remote_filepath = "{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id = 'was_new_file_found',
            trigger_rule = 'all_done',
            test = '{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task = 'archive_file',
            no_task = 'delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
                task_id = 'archive_file',
                trigger_rule='all_done',
                existing_filename = '{{ result("new_file_sensor") }}',
                new_filename = config.archive_filepath + "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        parse_xml = rail.LoadXMLFileOperator(
            task_id = 'parse_xml',
            document = "{{ result('download_file') }}",
            xsd_document = './dags/dxctechnology/ftp_sales_order_import/input_schema.xsd'
        )

        has_data = rail.IfOperator(
            task_id = 'has_data',
            test = '{{ result("parse_xml") | xpath("Records") | length > 0 }}',
            yes_task = 'get_company_codes',
            no_task = 'send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id = 'send_blank_payload_email',
            to = config.tenant_email,
            subject = '{{ get_company_key() }} | Replicon project sync for FTP Sales Order - Blank Payload - {{ current_time_in_specified_tz() }}',
            html_content = "email_blank_payload.html",
        )

        get_company_codes= rail.RepliconServiceOperator(
                task_id = "get_company_codes",
                endpoint = "/services/DivisionListService1.svc/GetData",
                data=request_payload.get_division_payload,
                response_filter=response_filter.get_filtered_company_code
        )

        get_department_groups= rail.RepliconServiceOperator(
                task_id = "get_department_groups",
                endpoint = "/services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups",
        )

        get_all_permission_set = rail.RepliconServiceOperator(
                task_id = "get_all_permission_set",
                endpoint = "/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_project_oefs= rail.RepliconServiceOperator(
                task_id = "get_project_oefs",
                endpoint = "/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
                data={  "bindingContextUri": "urn:replicon:object-type:project" },
                data_handler=lambda oefs: {
                'parentwbs': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Parent WBS', 'uri'),
                'wbstype': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'WBS Type', 'uri'),
                'masterwbs': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Master WBS (SO, WO)', 'uri')
            },
        )

        get_cost_centers = rail.RepliconServiceOperator(
                task_id = "get_cost_centers",
                endpoint = "/services/CostCenterService1.svc/GetEnabledCostCenters",
        )

        get_cost_center_uri = rail.PythonOperator(
            task_id="get_cost_center_uri",
            python_callable=request_payload.get_cost_center_uri,
            op_args=['get_cost_centers',so_cost_center_mapper]
        )

        get_details_from_xml = rail.XMLAdaptorOperator(
                task_id = "get_details_from_xml",
                source = '{{ result("parse_xml") }}',
                target = 'artifact',
                adaptor = [
                    'Records',
                    {
                        'Project_Code': 'Project_Code/text()',
                        'Project_Name': 'Project_Name/text()',
                        'Start_date': 'Start_date/text()',
                        'End_date': 'End_date/text()',
                        'Project_Status': 'Project_Status/text()',
                        'ProjectManager_EmpID': 'ProjectManager_EmpID/text()',
                        'ProjectCoManager_EmpID': 'ProjectCoManager_EmpID/text()',
                        'CompanyCode': 'CompanyCode/text()',
                        'WBS_Element': 'WBS_Element/text()',
                        'PROJ_CODE': 'PROJ_CODE/text()',
                    },
                ]
            )

        create_input_program_collection = rail.CreateCollectionOperator(
            task_id="create_input_program_collection",
            source='{{ result("get_details_from_xml") }}',
            columns=[
                'Project_Code',
                'Project_Name',
                'Start_date',
                'End_date',
                'Project_Status',
                'ProjectManager_EmpID',
                'ProjectCoManager_EmpID',
                'CompanyCode',
                'WBS_Element',
                'PROJ_CODE',
                ]
        )

        query_distinct_projcode=rail.QueryCollectionOperator(
            task_id= 'query_distinct_projcode',
            query= "SELECT DISTINCT PROJ_CODE FROM create_input_program_collection where PROJ_CODE IS NOT NULL"
        )

        has_projcode = rail.IfOperator(
            task_id = "has_projcode",
            test = "{{result('query_distinct_projcode') | length > 0 }}",
            yes_task = "process_program",
            no_task = "process_remedy"
        )

        process_program = rail.TriggerDagRunForEachItemOperator(
            task_id = 'process_program',
            items = "{{ result('query_distinct_projcode') }}",
            trigger_dag_id = f'dxctechnology_ftp_sales_order_import_child_process_program{dag_id_postfix}',
            conf = lambda item:{
                'programname' : item['PROJ_CODE'],
            },
            execution_timeout = timedelta(hours=12),
            retries = 0,
        )

        wait_for_process_each_program = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_program',
            dag_runs='{{ result("process_program") }}',
            execution_timeout=timedelta(days=14),
        )

        process_remedy= rail.TriggerDagRunForEachItemOperator(
            task_id = 'process_remedy',
            items = "{{ result('get_details_from_xml') }}",
            trigger_dag_id = f'dxctechnology_ftp_sales_order_import_child_process_salesorder{dag_id_postfix}',
            conf = request_payload.get_remedy_conf,
            execution_timeout = timedelta(hours=12),
            retries = 0,
        )

        wait_for_process_each_remedy = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_remedy',
            dag_runs='{{ result("process_remedy") }}',
            execution_timeout=timedelta(days=14),
        )

        send_logs_enter, _ = get_send_logs(config)


        is_xml >> rail.Label("No") >> send_bad_file_format_email
        has_data >> rail.Label("No") >> send_blank_payload_email
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        new_file_sensor >> is_xml >> rail.Label("Yes") >> download_file >> rail.Label("Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        download_file >>  parse_xml >> has_data >> rail.Label("Yes")  >> get_company_codes
        get_company_codes >> get_department_groups >> get_all_permission_set >> get_project_oefs
        get_project_oefs >> get_cost_centers >> get_cost_center_uri >> get_details_from_xml >> create_input_program_collection
        create_input_program_collection >> query_distinct_projcode >> has_projcode
        has_projcode >>  rail.Label("Yes") >> process_program >> wait_for_process_each_program >> process_remedy
        has_projcode >>  rail.Label("No") >> process_remedy >> wait_for_process_each_remedy >> send_logs_enter

    return dag

rail.for_each_instance(create_main_airflow_dag)
