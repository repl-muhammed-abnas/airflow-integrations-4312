from datetime import timedelta
import itertools
from os import path
import rail
from dxctechnology.gsap_wbs_import_v3.utils import request_payload
from dxctechnology.gsap_wbs_import_v3.utils import response_filter
from dxctechnology.gsap_wbs_import_v3.tasks.run_base_report import run_base_report
from rail.lib.ecid import get_dagrun_ecid
from rail.filters import split

# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description='DXC_GSAP_WBS_Automation Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
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
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon project sync for GSAP WBS - Incorrect File Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/bad_file_format.html",
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
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_xml = rail.LoadXMLFileOperator(
            task_id='parse_xml',
            document="{{ result('download_file') }}",
            xsd_document='./dags/dxctechnology/gsap_wbs_import_v3/xml_schema/input_schema.xsd'
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("Records") | length > 0 }}',
            yes_task='get_details_from_xml',
            no_task='send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon project sync for GSAP WBS - No records to process - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html",
        )

        get_details_from_xml = rail.XMLAdaptorOperator(
            task_id="get_details_from_xml",
            source='{{ result("parse_xml") }}',
            target='artifact',
            adaptor=[
                'Records',
                {
                    "WBS_Name": "WBS_Name/text()",
                    "WBS_Code": "WBS_Code/text()",
                    "Company_Code": "Company_Code/text()",
                    "Project_Type": "Project_Type/text()",
                    "Profit_Centre": "Profit_Centre/text()",
                    "Task_Indicator": "Task_Indicator/text()",
                    "Project_Start": "Project_Start/text()",
                    "Project_End": "Project_End/text()",
                    "Primary_Project_Manager_ID": "Primary_Project_Manager_ID/text()",
                    "Primary_Project_Manager_Name": "Primary_Project_Manager_Name/text()",
                    "WBS_Currency": "WBS_Currency/text()",
                    "Parent_Project": "Parent_Project/text()",
                    "WBS_Parent_Project": "WBS_Parent_Project/text()",
                    "Salesforce_Opportunity_ID": "Salesforce_Opportunity_ID/text()",
                    "Sold_to_Party": "Sold_to_Party/text()",
                    "Customer_Name": "Customer_Name/text()",
                    "Controlling_Area": "Controlling_Area/text()",
                    "PSA_Flag": "PSA_Flag/text()",
                    "Reference_Mandatory": "Reference_Mandatory/text()",
                    "Comments_Mandatory": "Comments_Mandatory/text()"
                },
            ],
        )

        create_project_collection = rail.CreateCollectionOperator(
            task_id='create_project_collection',
            source="{{ result('get_details_from_xml')}}",
        )

        create_skip_log = rail.CreateLogOperator(
            task_id='create_skip_log'
        )

        get_all_enabled_company_codes = rail.RepliconServiceOperator(
            task_id="get_all_enabled_company_codes",
            endpoint="/services/DivisionListService1.svc/GetData",
            data=request_payload.get_all_enabled_company_codes,
            data_handler=response_filter.get_all_enabled_company_codes
        )

        gsap_company_code_collection = rail.CreateCollectionOperator(
            task_id='gsap_company_code_collection',
            source=lambda: list(filter(lambda x: x['parent'] == 'GSAP', rail.result(
                'get_all_enabled_company_codes'))),
            name='gsapcompanycodes',
        )

        get_cost_centers = rail.RepliconServiceOperator(
            task_id="get_cost_centers",
            endpoint="/services/CostCenterListService1.svc/GetData",
            data=request_payload.get_cost_centers,
            data_handler=response_filter.get_cost_centers
        )

        cost_center_collection = rail.CreateCollectionOperator(
            task_id='cost_center_collection',
            source=lambda: list(
                map(lambda item: {'uri': item['uri']}, rail.result('get_cost_centers'))),
            name='costcenters',
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id="get_all_locations",
            endpoint="/services/LocationListService1.svc/GetData",
            data=request_payload.get_all_locations,
            data_handler=response_filter.filter_all_locations
        )

        get_enabled_department_groups = rail.RepliconServiceOperator(
            task_id="get_enabled_department_groups",
            endpoint="services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups"
        )

        get_all_filter_definitions = rail.RepliconServiceOperator(
            task_id="get_all_filter_definitions",
            endpoint="/services/ProjectListService1.svc/GetAllFilterDefinitions",
        )

        get_all_columns = rail.RepliconServiceOperator(
            task_id="get_all_columns",
            endpoint="/services/ProjectListService1.svc/GetAllColumns",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'][0]['columns'], 'displayText', 'Parent WBS', 'uri')
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id="get_all_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        get_all_employeetype_groups = rail.RepliconServiceOperator(
            task_id="get_all_employeetype_groups",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
            data_handler=response_filter.map_non_contractor_employeetype_groups,
        )

        get_custom_field_group = rail.RepliconServiceOperator(
            task_id="get_custom_field_group",
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroup",
            data={
                "objectTypeUri": "urn:replicon:object-type:task"
            }
        )

        get_task_type_udf = rail.RepliconServiceOperator(
            task_id="get_task_type_udf",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{result('get_custom_field_group').uri}}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Task Type', 'uri')
        )

        get_all_object_extension_fields = rail.RepliconServiceOperator(
            task_id="get_all_object_extension_fields",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:project"},
            data_handler=lambda oefs: {
                'projecttypeuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Project Type', 'uri'),
                'gsapprojecttypeuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'GSAP Project Type', 'uri'),
                'profitcenteruri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Profit Center', 'uri'),
                'wbscurrencyuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'WBS Currency', 'uri'),
                'parentwbsuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Parent WBS', 'uri'),
                'salesforceoppurtunityuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Salesforce Opportunity ID', 'uri'),
                'soldtopartyuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Sold to Party', 'uri'),
                'controllingareauri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Controlling Area', 'uri'),
                'psaflaguri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'PSA Flag', 'uri'),
                'referencemandatoryuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Reference Mandatory', 'uri'),
                'commentsmandatoryuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Comments Mandatory', 'uri'),
                'itemcategoryuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Item Category', 'uri'),
                'taskindicatoruri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'GSAP Task Required', 'uri'),
                'wbstypeuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'WBS Type', 'uri'),
                'iwoindicatoruri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'IWO Indicator', 'uri'),
                'iwowbselementuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'IWO WBS Element', 'uri'),
                'timetrackingattributeuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Time Tracking Required Attribute', 'uri'),
                'parentserviceorderuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Parent Service Order', 'uri'),
                'tnmindicatoruri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'COMPASS T&M Indicator', 'uri'),
                "parentcontrollingareauri": rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Parent Controlling Area', 'uri'),
                "gsapchilduri": rail.find_first_by_attr_and_get_attr(oefs, 'name', 'GSAP Child', 'uri'),
            },
        )

        get_oef_drop_down_values_gsap_child = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_gsap_child",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_all_object_extension_fields').gsapchilduri }}"},
        )

        get_oef_drop_down_values_project_type = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_project_type",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_all_object_extension_fields').projecttypeuri }}"},
        )

        get_oef_drop_down_values_gsap_project_type = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_gsap_project_type",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_all_object_extension_fields').gsapprojecttypeuri }}"},
        )

        get_oef_drop_down_values_item_category = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_item_category",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_all_object_extension_fields').itemcategoryuri }}"},
        )

        get_oef_drop_down_values_task_indicator = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_task_indicator",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_all_object_extension_fields').taskindicatoruri }}"},
        )

        get_oef_drop_down_values_reference_mandatory = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_reference_mandatory",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_all_object_extension_fields').referencemandatoryuri }}"},
        )

        get_oef_drop_down_values_wbs_type = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_wbs_type",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_all_object_extension_fields').wbstypeuri }}"},
        )

        get_oef_drop_down_values_comments_mandatory = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_comments_mandatory",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_all_object_extension_fields').commentsmandatoryuri }}"},
        )

        get_oef_drop_down_values_iwo_indicator = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_iwo_indicator",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_all_object_extension_fields').iwoindicatoruri }}"},
        )

        get_oef_drop_down_values_psa_flag = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_psa_flag",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_all_object_extension_fields').psaflaguri }}"},
        )

        query_all_non_gsap_records = rail.QueryCollectionOperator(
            task_id='query_all_non_gsap_records',
            name='nongsaprecordscollection',
            query="""SELECT * FROM create_project_collection WHERE
                    NULLIF(Company_Code, '') IS NULL or Company_Code NOT IN (SELECT name FROM gsapcompanycodes)"""
        )

        has_non_gsap_records = rail.IfOperator(
            task_id='has_non_gsap_records',
            test='{{ result("query_all_non_gsap_records", "length") > 0 }}',
            yes_task="log_non_gsap_records",
            no_task="no_non_gsap_records_present",
        )

        no_non_gsap_records_present = rail.EmptyOperator(
            task_id='no_non_gsap_records_present'
        )

        log_non_gsap_records = rail.WriteLogOperator(
            task_id='log_non_gsap_records',
            log='{{ result("create_skip_log") }}',
            items='{{result("query_all_non_gsap_records")}}',
            message='Wbs does not belong GSAP',
            severity='Skipped',
            properties=lambda item: {
                'projectname': item['WBS_Name'],
                'status': 'Skipped'
            }
        )

        query_all_gsap_records = rail.QueryCollectionOperator(
            task_id='query_all_gsap_records',
            name='gsaprecordscollection',
            query="""SELECT DISTINCT * FROM create_project_collection WHERE Company_Code IN (SELECT name FROM gsapcompanycodes)"""
        )

        has_gsap_records = rail.IfOperator(
            task_id='has_gsap_records',
            test='{{ result("query_all_gsap_records", "length") > 0 }}',
            yes_task="get_report_details",
            no_task="no_gsap_records_present",
        )

        get_report_details, report_collection = run_base_report(config)

        no_gsap_records_present = rail.EmptyOperator(
            task_id='no_gsap_records_present'
        )

        query_unique_project_types = rail.QueryCollectionOperator(
            task_id='query_unique_project_types',
            query="""SELECT DISTINCT Project_Type FROM gsaprecordscollection WHERE NULLIF(Project_Type, '') IS NOT NULL"""
        )

        has_project_types = rail.IfOperator(
            task_id='has_project_types',
            test='{{ result("query_unique_project_types", "length") > 0 }}',
            yes_task="process_project_types",
            no_task="get_updated_oef_drop_down_values_gsap_project_type",

        )

        process_project_types = rail.TriggerDagRunForEachItemOperator(
            task_id='process_project_types',
            retries=0,
            items="{{ result('query_unique_project_types') }}",
            trigger_dag_id=config.process_project_types_dagid,
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout_days),
            conf=lambda item:{
                'projecttype': item['Project_Type'],
                'gsapprojecttypeuri': rail.result("get_all_object_extension_fields")['gsapprojecttypeuri'],
                'gsapprojecttypetaguris': rail.result("get_oef_drop_down_values_gsap_project_type")['tags']
            }
        )

        wait_for_process_project_types = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_project_types',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout_days),
            dag_runs='{{ result("process_project_types") }}',
        )

        get_updated_oef_drop_down_values_gsap_project_type = rail.RepliconServiceOperator(
            task_id="get_updated_oef_drop_down_values_gsap_project_type",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_all_object_extension_fields').gsapprojecttypeuri }}"},
        )

        query_unique_clients_from_payload = rail.QueryCollectionOperator(
            task_id='query_unique_clients_from_payload',
            query="""SELECT DISTINCT Customer_Name FROM gsaprecordscollection WHERE NULLIF(Customer_Name, '') IS NOT NULL"""
        )

        has_clients = rail.IfOperator(
            task_id='has_clients',
            test='{{ result("query_unique_clients_from_payload", "length") > 0 }}',
            yes_task="dummy_process_clients",
            no_task="dummy_process_wbs",
        )

        dummy_process_clients = rail.EmptyOperator(
            task_id='dummy_process_clients'
        )

        process_clients = rail.trigger_parallel_dagrun(
            task_id='process_clients',
            items="{{ result('query_unique_clients_from_payload') }}",
            parallel_count=config.trigger_parallel_dagrun_count_client,
            trigger_dag_id=config.process_clients_dagid,
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout_days),
            conf={
                'clientname': '{{ item.Customer_Name }}',
            }
        )

        dummy_process_wbs = rail.EmptyOperator(
            task_id='dummy_process_wbs'
        )

        process_wbs_item = rail.trigger_parallel_dagrun(
            task_id='process_wbs_item',
            items=lambda: rail.result('query_all_gsap_records'),
            parallel_count=config.trigger_parallel_dagrun_count_project,
            trigger_dag_id=config.process_wbs_dagid,
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout_days),
            conf=request_payload.get_project_conf
        )

        get_process_each_wbs_task_ids =rail.PythonOperator(
            task_id= 'get_process_each_wbs_task_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_wbs_item_{x+1}'), range(config.trigger_parallel_dagrun_count_project))))),
            show_return_value_in_logs= False
        )

        gather_each_wbs_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_each_wbs_logs',
            dag_runs='{{ result("get_process_each_wbs_task_ids") }}',
            dagrun_task_id='create_wbs_log',
            execution_timeout=timedelta(
                hours=config.gather_each_wbs_logs_timeout_hours),
            flatten=True
        )

        query_distinct_parents = rail.QueryCollectionOperator(
            task_id='query_distinct_parents',
            query="""SELECT DISTINCT Parent_Project,WBS_Parent_Project
             FROM gsaprecordscollection WHERE (NULLIF(Parent_Project, '') IS NOT NULL or NULLIF(WBS_Parent_Project, '') IS NOT NULL)"""
        )

        has_parents = rail.IfOperator(
            task_id='has_parents',
            test='{{ result("query_distinct_parents", "length") > 0 }}',
            yes_task="process_iwo_element",
            no_task="dummy_process_log_generation",
        )

        process_iwo_element = rail.TriggerDagRunForEachItemOperator(
            task_id='process_iwo_element',
            retries=0,
            items=lambda: rail.result('query_distinct_parents'),
            trigger_dag_id=config.process_iwo_element_dagid,
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout_days),
            conf=request_payload.get_iwoelement_dag_confg
        )

        dummy_process_log_generation = rail.EmptyOperator(
            task_id='dummy_process_log_generation'
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.child_wait_execution_timeout_days),
            trigger_dag_id=config.process_log_generation_dagid,
            conf=lambda dag_run:{
                'wbs_logs': rail.result('gather_each_wbs_logs'),
                'skip_logs': rail.result('create_skip_log'),
                # pylint: disable=line-too-long
                'log_filename': f'log_{ get_dagrun_ecid(dag_run).replace(":", "-")}_{split(string=path.split(rail.result("new_file_sensor"))[1], separator=".")[0] }.csv'
            }
        )

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule="all_done",
            test=lambda:  rail.get_current_context()['dag_run'].get_task_instance(
                delete_this_dagrun.task_id).current_state().lower() != "success",
            yes_task="log_to_sumo",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule="all_done",
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "file_name": "{{result('new_file_sensor')}}",
                "archive_file": "{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}",
                "log_file_name": 'log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv'
            }
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        new_file_sensor >> is_xml
        is_xml >> rail.Label("No") >> send_bad_file_format_email
        is_xml >> rail.Label('Yes') >> download_file
        download_file >> parse_xml
        parse_xml >> has_data
        has_data >> rail.Label("No") >> send_blank_payload_email
        has_data >> rail.Label('Yes') >> get_details_from_xml

        download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        get_details_from_xml >> create_project_collection >> create_skip_log >> get_all_enabled_company_codes >> gsap_company_code_collection
        gsap_company_code_collection >> get_cost_centers >> cost_center_collection >> get_enabled_department_groups
        get_enabled_department_groups >> get_all_locations >> get_all_filter_definitions >> get_all_columns >> get_all_employeetype_groups
        get_all_employeetype_groups >> get_custom_field_group >> get_task_type_udf >> get_all_object_extension_fields
        get_all_object_extension_fields >> [get_oef_drop_down_values_project_type, get_oef_drop_down_values_gsap_project_type,
            get_oef_drop_down_values_item_category,get_oef_drop_down_values_gsap_child,
            get_oef_drop_down_values_task_indicator, get_oef_drop_down_values_reference_mandatory,
            get_oef_drop_down_values_comments_mandatory, get_oef_drop_down_values_wbs_type, get_oef_drop_down_values_iwo_indicator,
            get_oef_drop_down_values_psa_flag] >> get_all_permission_sets >> [query_all_gsap_records, query_all_non_gsap_records]
        query_all_non_gsap_records >> has_non_gsap_records >> rail.Label(
            'No') >> no_non_gsap_records_present >> dummy_process_log_generation
        has_non_gsap_records >> rail.Label(
            'Yes') >> log_non_gsap_records >> dummy_process_log_generation
        query_all_gsap_records >> has_gsap_records >> rail.Label(
            'Yes') >> get_report_details
        report_collection >> query_unique_project_types >> has_project_types >> rail.Label('No') >> get_updated_oef_drop_down_values_gsap_project_type
        get_updated_oef_drop_down_values_gsap_project_type >> query_unique_clients_from_payload
        has_project_types >> rail.Label('Yes') >> process_project_types >> wait_for_process_project_types >> get_updated_oef_drop_down_values_gsap_project_type
        has_gsap_records >> rail.Label(
            'No') >> no_gsap_records_present >> dummy_process_log_generation
        query_unique_clients_from_payload >> has_clients >> rail.Label(
            'Yes') >> dummy_process_clients >> process_clients
        has_clients >> rail.Label('No') >> dummy_process_wbs
        process_clients >> dummy_process_wbs >> process_wbs_item
        process_wbs_item >> get_process_each_wbs_task_ids >> gather_each_wbs_logs
        gather_each_wbs_logs >> query_distinct_parents >> has_parents >> rail.Label(
            'No') >> dummy_process_log_generation
        has_parents >> rail.Label(
            'Yes') >> process_iwo_element >> dummy_process_log_generation
        process_log_generation >> can_log_to_sumo >> rail.Label("Yes") >> log_to_sumo >> can_fail_dag >> rail.Label(
            'Yes') >> fail_dagrun

        dummy_process_log_generation >> process_log_generation

    return dag


rail.for_each_instance(create_main_dag)
