from datetime import timedelta
import rail
from dxctechnology.ftp_wbs_import.utils import request_payload
from dxctechnology.ftp_wbs_import.utils import response_filter
from dxctechnology.ftp_wbs_import.send_logs import get_send_logs


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_ftp_wbs_import_master_{config.instance}',
        description='DXC_FTP_WBS_Automation Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10),
        )

        is_xml = rail.IfOperator(
            task_id='is_xml',
            test='{{ result("new_file_sensor") | file_ext | lower == "xml" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Replicon project sync for FTP WBS - Incorrect File Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/email/bad_file_format.html",
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        parse_xml = rail.LoadXMLFileOperator(
            task_id='parse_xml',
            document="{{ result('download_file') }}",
            xsd_document='./dags/dxctechnology/ftp_wbs_import/xsdschema/input_schema.xsd'
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("Records") | length > 0 }}',
            yes_task='get_company_codes',
            no_task='send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Replicon project sync for FTP WBS - Blank Payload - {{ current_time_in_specified_tz() }}',
            html_content="templates/email/blank_payload.html",
        )

        get_company_codes = rail.RepliconServiceOperator(
            task_id="get_company_codes",
            endpoint="/services/DivisionListService1.svc/GetData",
            data=request_payload.get_division_payload,
            response_filter=response_filter.get_filtered_company_code
        )

        get_department_groups = rail.RepliconServiceOperator(
            task_id="get_department_groups",
            endpoint="/services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups",
        )

        get_all_permission_set = rail.RepliconServiceOperator(
            task_id="get_all_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_project_oefs = rail.RepliconServiceOperator(
            task_id="get_project_oefs",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={"bindingContextUri": "urn:replicon:object-type:project"},
            data_handler=lambda oefs: {
                'parentwbs': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Parent WBS', 'uri'),
                'wbstype': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'WBS Type', 'uri'),
                'businessarea': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Business Area', 'uri'),
                'billingelement': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'FTP Billing Indicator', 'uri'),
                'projecttype': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Project Type', 'uri'),
                'masterwbs': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Master WBS (SO, WO)', 'uri'),
                'profitcenter': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Profit Center', 'uri'),
                'objectclass': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'FTP Object Class', 'uri'),
                'customer': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Sold to Party', 'uri'),
                'functionalarea': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'FTP Functional Area', 'uri'),
                'product': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'FTP Product', 'uri'),
                'stage': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'FTP Engagement Stage', 'uri'),
                'salesforceopportunityid': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Salesforce Opportunity ID', 'uri'),
                'iwono': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'IWO Number', 'uri')
            },
        )

        get_oef_dropdown_values_project_type = rail.RepliconServiceOperator(
            task_id="get_oef_dropdown_values_project_type",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result("get_project_oefs")['projecttype']
            }
        )

        get_oef_dropdown_ftp_billing_indicator = rail.RepliconServiceOperator(
            task_id="get_oef_dropdown_ftp_billing_indicator",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result("get_project_oefs")['billingelement']
            }
        )

        get_details_from_xml = rail.XMLAdaptorOperator(
            task_id="get_details_from_xml",
            source='{{ result("parse_xml") }}',
            target='artifact',
            adaptor=[
                'Records',
                    {
                        'ProjectCode': 'ProjectCode/text()',
                        'ProjectName': 'ProjectName/text()',
                        'Project_StartDate': 'Project_StartDate/text()',
                        'Project_EndDate': 'Project_EndDate/text()',
                        'Status': 'Status/text()',
                        'Project': 'Project/text()',
                        'ProjectManager_EmpID': 'ProjectManager_EmpID/text()',
                        'ProjectCoManager_EmpID': 'ProjectCoManager_EmpID/text()',
                        'ProjectGroup': 'ProjectGroup/text()',
                        'ProjectUDF': 'ProjectUDF/text()',
                        'Type': 'Type/text()',
                        'Billing_Indicator': 'Billing_Indicator/text()',
                        'PRCTR': 'PRCTR/text()',
                        'SCOPE': 'SCOPE/text()',
                        'CUST_USR00': 'CUST_USR00/text()',
                        'FunctionalArea': 'FunctionalArea/text()',
                        'Product': 'Product/text()',
                        'Stage': 'Stage/text()',
                        'SalesForceID': 'SalesForceID/text()',
                        'IWONo': 'IWONo/text()',
                        'Client': 'Client/text()',
                    },
            ]
        )

        create_input_program_collection = rail.CreateCollectionOperator(
            task_id="create_input_program_collection",
            source='{{ result("get_details_from_xml") }}',
            columns=[
                'ProjectCode',
                'ProjectName',
                'Project_StartDate',
                'Project_EndDate',
                'Status',
                'Project',
                'ProjectManager_EmpID',
                'ProjectCoManager_EmpID',
                'ProjectGroup',
                'ProjectUDF',
                'Type',
                'Billing_Indicator',
                'PRCTR',
                'SCOPE',
                'CUST_USR00',
                'FunctionalArea',
                'Product',
                'Stage',
                'SalesForceID',
                'IWONo',
                'Client'
            ]
        )

        query_distinct_client = rail.QueryCollectionOperator(
            task_id='query_distinct_client',
            query="SELECT DISTINCT Client FROM create_input_program_collection where Client IS NOT NULL"
        )

        process_client = rail.TriggerDagRunForEachItemOperator(
            task_id='process_client',
            items="{{ result('query_distinct_client') }}",
            trigger_dag_id=f'dxctechnology_ftp_wbs_import_child_process_client_{config.instance}',
            conf=lambda item: {
                'client': item['Client'],
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_each_client = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_client',
            dag_runs='{{ result("process_client") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_distinct_project = rail.QueryCollectionOperator(
            task_id='query_distinct_project',
            query="SELECT DISTINCT Project FROM create_input_program_collection where Project IS NOT NULL"
        )

        has_projcode = rail.IfOperator(
            task_id="has_projcode",
            test="{{result('query_distinct_project') | length > 0 }}",
            yes_task="process_program",
            no_task="process_wbs_dummy"
        )

        process_program = rail.TriggerDagRunForEachItemOperator(
            task_id='process_program',
            items="{{ result('query_distinct_project') }}",
            trigger_dag_id=f'dxctechnology_ftp_wbs_import_child_process_program_{config.instance}',
            conf=lambda item: {
                'programname': item['Project'],
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_each_program = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_program',
            dag_runs='{{ result("process_program") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        
        process_wbs_dummy = rail.EmptyOperator(
            task_id = 'process_wbs_dummy'
        )

        process_wbs = rail.trigger_parallel_dagrun(
            task_id='process_wbs',
            items="{{ result('get_details_from_xml') }}",
            trigger_dag_id=f'dxctechnology_ftp_wbs_import_child_process_wbs_{config.instance}',
            conf=request_payload.get_wbs_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count = config.process_wbs_child_parallel_dagruns_count,
        )

        send_logs_enter, _ = get_send_logs(config)

        is_xml >> rail.Label("No") >> send_bad_file_format_email
        has_data >> rail.Label("No") >> send_blank_payload_email
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        new_file_sensor >> is_xml >> rail.Label("Yes") >> download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        download_file >> parse_xml >> has_data >> rail.Label(
            "Yes") >> get_company_codes
        get_company_codes >> get_department_groups >> get_all_permission_set >> get_project_oefs
        get_project_oefs >> get_oef_dropdown_values_project_type >> get_oef_dropdown_ftp_billing_indicator
        get_oef_dropdown_ftp_billing_indicator >> get_details_from_xml >> create_input_program_collection
        create_input_program_collection >> query_distinct_client >> process_client >> wait_for_process_each_client
        wait_for_process_each_client >> query_distinct_project >> has_projcode
        has_projcode >> rail.Label(
            "Yes") >> process_program >> wait_for_process_each_program >> process_wbs_dummy
        has_projcode >> rail.Label(
            "No") >> process_wbs_dummy
        process_wbs_dummy >> process_wbs >> send_logs_enter

    return dag


rail.for_each_instance(create_main_airflow_dag)
