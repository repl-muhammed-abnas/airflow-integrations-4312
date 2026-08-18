from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.c1_wbs_import_v1 import python_callable_method
from dxctechnology.c1_wbs_import_v1 import response_filter
from dxctechnology.c1_wbs_import_v1 import request_payload

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_wbs_import/config.py


# pylint: disable=too-many-statements
def create_main_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_c1_wbs_import_master_v1{dag_id_postfix}',
        description=f'DXC_C1_WBS_Automation Master V1 - SFTP {config.instance}',
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
            # We do the timeout with a soft fail here to yield to potential other waiting executions of this DAG
            # Since max_active_runs is set to 1, if this sensor ran indefinitiely then someone manually wanting to
            # retry failed tasks in a past run would also be waiting indefinitely. This way it'll give them a window
            # every 10 minutes to run their tasks.
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
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon project sync for C1 WBS - Incorrect File Format - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="email_bad_file_format.html",
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
            xsd_document='./dags/dxctechnology/c1_wbs_import/xml_schema/input_schema.xsd'
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
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon project sync for C1 WBS - No records to process - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="email_blank_payload.html",
        )

        get_details_from_xml = rail.XMLAdaptorOperator(
            task_id="get_details_from_xml",
            source='{{ result("parse_xml") }}',
            target='artifact',
            # If the tag is not present in the input file it will take null as the value for the record
            # and it will exclude the any extra tag present.
            # This is added so if the XML tags are mismatched it will not fail while retriving the data.
            adaptor=[
                'Records',
                {
                    "ProjectDefinition": "ProjectDefinition/text()",
                    "ProjectDescription": "ProjectDescription/text()",
                    "DXCProjectID": "DXCProjectID/text()",
                    "WBSElementName": "WBSElementName/text()",
                    "InternalSAPObjectNumber": "InternalSAPObjectNumber/text()",
                    "PrimaryWBSOwner1": "PrimaryWBSOwner1/text()",
                    "PrimaryWBSOwnerName": "PrimaryWBSOwnerName/text()/text()",
                    "CompanyCode": "CompanyCode/text()",
                    "ProjectType": "ProjectType/text()",
                    "AccountAssignmentIndicator": "AccountAssignmentIndicator/text()",
                    "Currency": "Currency/text()",
                    "ContractLineStartDate": "ContractLineStartDate/text()",
                    "ContractLineEndDate": "ContractLineEndDate/text()",
                    "WBSSTATUS": "WBSSTATUS/text()",
                    "ContractType": "ContractType/text()",
                    "WBSOwner2": "WBSOwner2/text()",
                    "WBSOwner2Name": "WBSOwner2Name/text()",
                    "Changedby": "Changedby/text()",
                    "Changedon": "Changedon/text()",
                    "IWO": "IWO/text()",
                    "ICWBSNumber": "ICWBSNumber/text()",
                    "ServiceOrderNumber": "ServiceOrderNumber/text()",
                    "ServiceOrderType": "ServiceOrderType/text()",
                    "ServiceOrderText": "ServiceOrderText/text()",
                    "CreatedOnDate": "CreatedOnDate/text()",
                    "ChangedOnDate": "ChangedOnDate/text()",
                    "ServiceOrderCompanyCode": "ServiceOrderCompanyCode/text()",
                    "Plant": "Plant/text()",
                    "ServiceOrderSystemStatus": "ServiceOrderSystemStatus/text()",
                    "BasicStartDate": "BasicStartDate/text()",
                    "BasicFinishDate": "BasicFinishDate/text()",
                    "ServiceOrderInternalSAPobjectnumber": "ServiceOrderInternalSAPobjectnumber/text()",
                    "SOPersonResponsible": "SOPersonResponsible/text()",
                    "SOPersonResponsibleName": "SOPersonResponsibleName/text()",
                    "SOPartnerWBSOwner2": "SOPartnerWBSOwner2/text()",
                    "SOPartnerWBSOwner2Name": "SOPartnerWBSOwner2Name/text()",
                    "ResponsibleCostCenter": "ResponsibleCostCenter/text()",
                    "ServiceOffering": "ServiceOffering/text()",
                    "SalesforceOpportunityID": "SalesforceOpportunityID/text()",
                    "SalesforceOpportunityName": "SalesforceOpportunityName/text()",
                    "HigherLevelCustomerID": "HigherLevelCustomerID/text()",
                    "HigherLevelCustomerName": "HigherLevelCustomerName/text()"
                },
            ],
        )

        create_project_collection = rail.CreateCollectionOperator(
            task_id='create_project_collection',
            source="{{ result('get_details_from_xml')}}",
        )

        get_enabled_divisions_company_codes = rail.RepliconServiceOperator(
            task_id="get_enabled_divisions_company_codes",
            endpoint="/services/DivisionListService1.svc/GetData",
            data=request_payload.get_enabled_divisions_company_codes_payload,
            response_filter=response_filter.map_list_data_to_companycode_list
        )

        get_enabled_department_groups = rail.RepliconServiceOperator(
            task_id="get_enabled_department_groups",
            endpoint="services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups")

        get_all_currencies = rail.RepliconServiceOperator(
            task_id="get_all_currencies",
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies"
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id="get_all_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        get_all_employeetype_groups = rail.RepliconServiceOperator(
            task_id="get_all_employeetype_groups",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
            response_filter=response_filter.map_non_contractor_employeetype_groups,
        )

        get_all_project_custom_fields = rail.RepliconServiceOperator(
            task_id="get_all_project_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={"objectUri": "urn:replicon:object-type:project"}
        )

        get_all_object_extension_field = rail.RepliconServiceOperator(
            task_id="get_all_object_extension_field",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:project"}
        )

        get_all_object_extension_field_projects = rail.PythonOperator(
            task_id="get_all_object_extension_field_projects",
            python_callable=python_callable_method.map_project_oef_field,
            op_args=['get_all_object_extension_field']
        )

        def rusia_custom_field_param():
            data = rail.result('get_all_project_custom_fields')
            uri = rail.find_first_by_attr_and_get_attr(
                data, 'displayText', "Russia IWO WBS", 'uri', '')
            return {
                "customFieldUri": uri
            }

        get_rusia_custom_field_dropdown_options = rail.RepliconServiceOperator(
            task_id="get_rusia_custom_field_dropdown_options",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=rusia_custom_field_param)

        get_oef_drop_down_values_project_type = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_project_type",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_all_object_extension_field_projects').projecttype }}"},
        )

        get_oef_drop_down_values_item_category = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_item_category",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_all_object_extension_field_projects').itemcategory }}"},
        )

        get_oef_drop_down_values_service_order_type = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_service_order_type",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_all_object_extension_field_projects').serviceordertype }}"},
        )

        get_new_project_types_to_add = rail.PythonOperator(
            task_id='get_new_project_types_to_add',
            python_callable=python_callable_method.get_new_project_types,
            op_args=[
                'create_project_collection',
                'get_oef_drop_down_values_project_type'])

        has_new_project_types = rail.IfOperator(
            task_id='has_new_project_types',
            test='{{ result("get_new_project_types_to_add") | length > 0 }}',
            no_task="init_updated_project_type_oef_values",
            yes_task="add_new_project_type_oef_values",
        )

        def get_putobjectextensiontags_payload():
            return {
                "objectExtensionTagDefinition": {
                    "uri": rail.result('get_all_object_extension_field_projects')['projecttype'],
                },
                "objectExtensionTags": request_payload.get_combined_tags_param(
                    rail.result('get_oef_drop_down_values_project_type'),
                    rail.result('get_new_project_types_to_add'))}

        add_new_project_type_oef_values = rail.RepliconServiceOperator(
            task_id="add_new_project_type_oef_values",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data=get_putobjectextensiontags_payload,
        )

        get_project_type_oef_values = rail.RepliconServiceOperator(
            task_id="get_project_type_oef_values",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={"objectExtensionTagDefinitionUri":
                  "{{ result('get_all_object_extension_field_projects').projecttype }}"},
        )

        init_updated_project_type_oef_values = rail.PythonOperator(
            task_id="init_updated_project_type_oef_values",
            python_callable=lambda: rail.result('get_project_type_oef_values') if len(rail.result(
                "get_new_project_types_to_add")) > 0 else rail.result('get_oef_drop_down_values_project_type')
        )

        can_create_client = rail.IfOperator(
            task_id='can_create_client',
            test=lambda: Variable.get(
                config.can_create_client_var_name, default_var='true').lower() == "true",
            yes_task='query_unique_clients_from_payload',
            no_task='query_unique_programs_from_payload'
        )

        query_unique_clients_from_payload = rail.QueryCollectionOperator(
            task_id='query_unique_clients_from_payload',
            query="""SELECT HigherLevelCustomerID, HigherLevelCustomerName
                        FROM create_project_collection
                        WHERE NULLIF(HigherLevelCustomerID, '') IS NOT NULL
                        GROUP BY HigherLevelCustomerID
                        """
        )

        has_clients = rail.IfOperator(
            task_id='has_clients',
            test='{{ result("query_unique_clients_from_payload", "length") > 0 }}',
            no_task="query_unique_programs_from_payload",
            yes_task="dummy_process_clients",
        )

        dummy_process_clients = rail.EmptyOperator(
            task_id='dummy_process_clients'
        )

        process_clients = rail.trigger_parallel_dagrun(
            task_id='process_clients',
            items="{{ result('query_unique_clients_from_payload') }}",
            trigger_dag_id=config.child_dag_id_client,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'client_name': '{{ item.HigherLevelCustomerID }}',
                'client_code': '{{ item.HigherLevelCustomerName }}'
            },
            parallel_count=20
        )

        query_unique_programs_from_payload = rail.QueryCollectionOperator(
            task_id='query_unique_programs_from_payload',
            query="""SELECT DISTINCT ProjectDefinition, IFNULL(ProjectDescription, '') AS ProjectDescription
                        FROM create_project_collection
                        WHERE NULLIF(ProjectDefinition, '') IS NOT NULL
                        """
        )

        has_programs = rail.IfOperator(
            task_id='has_programs',
            test='{{ result("query_unique_programs_from_payload", "length") > 0 }}',
            no_task="get_all_cost_center",
            yes_task="dummy_process_programs",
        )

        dummy_process_programs = rail.EmptyOperator(
            task_id='dummy_process_programs'
        )

        process_programs = rail.trigger_parallel_dagrun(
            task_id='process_programs',
            items="{{ result('query_unique_programs_from_payload') }}",
            trigger_dag_id=config.child_dag_id_program,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'program_name': f"{item['ProjectDefinition']}-{item['ProjectDescription']}"
            },
            parallel_count=20
        )

        get_all_cost_center = rail.RepliconServiceOperator(
            task_id="get_all_cost_center",
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
        )

        get_unique_cost_center_to_add = rail.PythonOperator(
            task_id="get_unique_cost_center_to_add",
            python_callable=python_callable_method.get_unique_cost_center_to_add,
            op_args=['create_project_collection', 'get_all_cost_center']
        )

        has_cost_center = rail.IfOperator(
            task_id='has_cost_center',
            test='{{ result("get_unique_cost_center_to_add") | length > 0 }}',
            no_task="dummy_process_wbs_item",
            yes_task="dummy_process_cost_center",
        )

        dummy_process_cost_center = rail.EmptyOperator(
            task_id='dummy_process_cost_center'
        )

        process_cost_center = rail.TriggerDagRunForEachItemOperator(
            task_id='process_cost_center',
            retries=0,
            items=lambda: rail.result('get_unique_cost_center_to_add'),
            trigger_dag_id=config.child_dag_id_cost_center,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'cost_center': '{{ item }}'
            }
        )

        wait_for_process_cost_center = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_cost_center',
            dag_runs='{{ result("process_cost_center") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        dummy_process_wbs_item = rail.EmptyOperator(
            task_id='dummy_process_wbs_item'
        )

        process_wbs_item = rail.trigger_parallel_dagrun(
            task_id='process_wbs_item',
            items=lambda: rail.result('create_project_collection'),
            trigger_dag_id=config.child_dag_id_project,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_project_dag_confg,
            parallel_count=20,
        )

        get_unique_icwbsnumber_to_add = rail.PythonOperator(
            task_id="get_unique_icwbsnumber_to_add",
            python_callable=python_callable_method.get_unique_icwbsnumber_to_add,
            op_args=['create_project_collection']
        )

        has_icwbsnumber_data = rail.IfOperator(
            task_id='has_icwbsnumber_data',
            test='{{ result("get_unique_icwbsnumber_to_add") | is_truthy }}',
            no_task="generate_output_log",
            yes_task="dummy_process_icwbsnumber_item",
        )

        dummy_process_icwbsnumber_item = rail.EmptyOperator(
            task_id='dummy_process_icwbsnumber_item'
        )

        process_icwbsnumber_item = rail.trigger_parallel_dagrun(
            task_id='process_icwbsnumber_item',
            items=lambda: rail.result('get_unique_icwbsnumber_to_add'),
            trigger_dag_id=config.child_dag_id_icwbsnumber,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_icwbsnumber_dag_confg,
            parallel_count=20,
        )

        generate_output_log = rail.EmptyOperator(task_id='generate_output_log')

        get_errored_projects = rail.FilterLogEntriesOperator(
            task_id='get_errored_projects',
            properties={'status': 'Error'}
        )

        get_exception_projects = rail.FilterLogEntriesOperator(
            task_id='get_exception_projects',
            properties={'status': 'Exception'}
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=[
                '{{ current_time("%d/%m/%YT%H:%M:%S") }}',
                'Number of Rows: {{ result("create_project_collection", key="length") }}',
                'Function: C1 WBS Master inbound',
                '',
                ''],
            row=[
                '{{ item.properties | attr_or_default("projectname", "") }}',
                '{{ item.properties | attr_or_default("projecttype", "") }}',
                '{{ item.properties.status }}',
                '{{ item.message }}',
                '{{ item.ecid }}'],
            footer=[
                # pylint: disable=line-too-long
                'Number of Records Processed Successfully: {{- result("create_project_collection", key="length") - result("get_errored_projects", key="length") - result("get_exception_projects", key="length") }}',
                'Number of Records with Error: {{ result("get_errored_projects", key="length") }}',
                'Number of Records with Exception: {{ result("get_exception_projects", key="length") }}',
                '',
                ''],
        )

        def file_upload_failed(context):
            subject = '{{ get_company_key() }} | C1 WBS master automation - Uploading Logs to SFTP failed - {{ data_interval_end }}'
            email = rail.EmailOperator(
                task_id='send_time_data_to_sftp_failure_email',
                to=config.tenant_email,
                bcc=config.alert_email,
                subject=subject,
                html_content="email_sftp_upload_failed.html",
                params={
                    'dag_id': f'dxctechnology_c1_wbs_import_master{dag_id_postfix}'
                },
                files=[
                    ("{{ result('render_logs_csv') }}")
                ]
            )
            email.render_template_fields(context)
            email.execute(context)

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv',
            on_failure_callback=file_upload_failed
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_projects', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon project sync for C1 WBS - " }} \
                {%- if result("get_errored_projects", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_projects", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="email_import_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        new_file_sensor >> is_xml

        is_xml >> rail.Label("No") >> send_bad_file_format_email
        is_xml >> rail.Label('Yes') >> download_file
        download_file >> parse_xml
        parse_xml >> has_data
        has_data >> rail.Label("No") >> send_blank_payload_email
        has_data >> rail.Label('Yes') >> get_details_from_xml

        get_details_from_xml >> create_project_collection >> \
            [get_enabled_divisions_company_codes, get_enabled_department_groups, get_all_currencies,
                get_all_currencies, get_all_permission_sets, get_all_employeetype_groups, get_all_project_custom_fields, get_all_object_extension_field] >> \
            get_rusia_custom_field_dropdown_options >> get_all_object_extension_field_projects
        get_all_object_extension_field_projects >> get_oef_drop_down_values_project_type >> get_oef_drop_down_values_item_category
        get_oef_drop_down_values_item_category >> get_oef_drop_down_values_service_order_type >> get_new_project_types_to_add >> has_new_project_types

        has_new_project_types >> rail.Label(
            "No") >> init_updated_project_type_oef_values
        has_new_project_types >> rail.Label(
            "Yes") >> add_new_project_type_oef_values >> get_project_type_oef_values >> init_updated_project_type_oef_values

        init_updated_project_type_oef_values >> can_create_client

        can_create_client >> rail.Label(
            'No') >> query_unique_programs_from_payload
        can_create_client >> rail.Label(
            'Yes') >> query_unique_clients_from_payload >> has_clients
        has_clients >> rail.Label("No") >> query_unique_programs_from_payload
        has_clients >> rail.Label(
            "Yes") >> dummy_process_clients >> process_clients >> query_unique_programs_from_payload

        query_unique_programs_from_payload >> has_programs
        has_programs >> rail.Label("No") >> get_all_cost_center
        has_programs >> rail.Label(
            "Yes") >> dummy_process_programs >> process_programs >> get_all_cost_center

        get_all_cost_center >> get_unique_cost_center_to_add >> has_cost_center

        has_cost_center >> rail.Label(
            "No") >> dummy_process_wbs_item >> process_wbs_item
        has_cost_center >> rail.Label(
            "Yes") >> dummy_process_cost_center >> process_cost_center >> wait_for_process_cost_center >> dummy_process_wbs_item >> process_wbs_item

        process_wbs_item >> get_unique_icwbsnumber_to_add >> has_icwbsnumber_data >> rail.Label(
            "Yes") >> dummy_process_icwbsnumber_item >> process_icwbsnumber_item >> generate_output_log
        has_icwbsnumber_data >> rail.Label(
            "No") >> generate_output_log >> [
            get_errored_projects,
            get_exception_projects] >> render_logs_csv >> upload_log_to_sftp >> send_import_complete_email

        download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
