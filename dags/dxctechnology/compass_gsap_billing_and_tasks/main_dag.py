from datetime import timedelta
import rail
from dxctechnology.compass_gsap_billing_and_tasks.send_logs import get_send_logs

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/compass_gsap_billing_and_tasks/config.py

def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_gsap_billing_and_tasks_import_{config.sub_erp_name}_master',
        description=f'DXC COMPASS GSAP Billing and Tasks - {config.sub_erp_name}',
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
            soft_fail_timeout=timedelta(minutes=15),
            # We do the timeout with a soft fail here to yield to make sure this dag cycles once in a while so that transient network
            # failures have less of a chance of causing the dag to fail, and people to get notified. If this dag ran indefinitely
            # then 3 network failures several days apart would cause alerts to
            # be sent out, which really is not necessary.
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
            subject='{{ get_company_key() }} | Replicon task sync for Compass GSAP Billing and Tasks - Incorrect File Format - {{ current_time() }}',
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

        if config.debug:
            archive_file = rail.EmptyOperator(
                task_id='archive_file',
            )
        else:
            archive_file = rail.SFTPMoveFileOperator(
                task_id='archive_file',
                existing_filename='{{ result("new_file_sensor") }}',
                new_filename=config.archive_filepath +
                "/{{ ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
            )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_xml = rail.LoadXMLFileOperator(
            task_id='parse_xml',
            document="{{ result('download_file') }}"
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("Records") | length > 0 }}',
            yes_task='process_data',
            no_task='send_blank_payload_email',
        )

        process_data = rail.EmptyOperator(
            task_id='process_data',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon task sync for Compass GSAP Billing and Tasks - Blank Payload - {{ current_time() }}',
            html_content="email_blank_payload.html",
        )

        get_project_oefs = rail.RepliconServiceOperator(
            task_id="get_project_oefs",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={"bindingContextUri": "urn:replicon:object-type:project"},
            data_handler=lambda oefs: {
                'gsaptaskrequired': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'GSAP Task Required', 'uri')
            },
        )

        get_gsaptaskrequired_values = rail.RepliconServiceOperator(
            task_id="get_gsaptaskrequired_values",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_project_oefs').gsaptaskrequired }}"},
            data_handler=lambda resp: resp['tags'],
        )

        get_task_type_custom_field = rail.RepliconServiceOperator(
            task_id="get_task_type_custom_field",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={"objectUri": "urn:replicon:object-type:task"},
            data_handler=lambda data: rail.find_first_by_attr_and_get_attr(
                data, 'displayText', 'Task Type', 'uri'),
        )

        get_task_type_options = rail.RepliconServiceOperator(
            task_id="get_task_type_options",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_task_type_custom_field') }}"},
            data_handler=lambda data: {
                'gsaptask': rail.find_first_by_attr_and_get_attr(data, 'displayText', 'GSAP Task', 'uri'),
                'gsapbillingkey': rail.find_first_by_attr_and_get_attr(data, 'displayText', 'GSAP Billing Key', 'uri'),
            },
        )

        get_billing_keys_from_xml = rail.XMLAdaptorOperator(
            task_id="get_billing_keys_from_xml",
            source='{{ result("parse_xml") }}',
            target='artifact',
            adaptor=[
                'Records/BillingKeys[../Header/WBS/text() and BillingKey/text()]',
                {
                    'WBS': '../Header/WBS/text()',
                    'TaskRequired': '../Header/TaskRequired/text()',
                    'Name': 'BillingKey/text()',
                    'StartDate': 'StartDate/text()',
                    'EndDate': 'EndDate/text()',
                    'Description': 'Description/text()',
                },
            ],
        )

        get_tasks_from_xml = rail.XMLAdaptorOperator(
            task_id="get_tasks_from_xml",
            source='{{ result("parse_xml") }}',
            target='artifact',
            adaptor=[
                'Records/Tasks[../Header/WBS/text() and Task/text()]',
                {
                    'WBS': '../Header/WBS/text()',
                    'TaskRequired': '../Header/TaskRequired/text()',
                    'TaskName': 'Task/text()',
                    'StartDate': 'StartDate/text()',
                    'EndDate': 'EndDate/text()',
                    'Description': 'Description/text()',
                },
            ],
        )

        get_records_missing_wbs_from_xml = rail.XMLAdaptorOperator(
            task_id="get_records_missing_wbs_from_xml",
            source='{{ result("parse_xml") }}',
            target='artifact',
            adaptor=[
                'Records/Tasks[Task/text() and not(../Header/WBS/text())] | Records/BillingKeys[BillingKey/text() and not(../Header/WBS/text())]',
                {
                    'WBS': '../Header/WBS/text()',
                    'TaskRequired': '../Header/TaskRequired/text()',
                    'BillingKey': 'BillingKey/text()',
                    'TaskName': 'Task/text()',
                    'StartDate': 'StartDate/text()',
                    'EndDate': 'EndDate/text()',
                    'Description': 'Description/text()',
                },
            ],
        )

        log_records_missing_wbs = rail.WriteLogOperator(
            task_id="log_records_missing_wbs",
            items='{{ result("get_records_missing_wbs_from_xml") }}',
            message='WBS is not present',
            severity='Exception',
            properties={
                'WBS': '{{ item.WBS }}',
                'Task': '{{ item.TaskName }}',
                'BillingKey': '{{ item.BillingKey }}',
            }
        )

        create_billing_keys_collection = rail.CreateCollectionOperator(
            task_id="create_billing_keys_collection",
            source='{{ result("get_billing_keys_from_xml") }}',
            columns=[
                'WBS',
                'TaskRequired',
                'Name',
                'StartDate',
                'EndDate',
                'Description'],
        )

        create_task_collection = rail.CreateCollectionOperator(
            task_id="create_task_collection",
            source='{{ result("get_tasks_from_xml") }}',
            columns=[
                'WBS',
                'TaskRequired',
                'TaskName',
                'StartDate',
                'EndDate',
                'Description'],
        )

        query_merged_projects = rail.QueryCollectionOperator(
            task_id="query_merged_projects",
            query='''SELECT DISTINCT WBS, TaskRequired
                     FROM (SELECT WBS, TaskRequired FROM create_billing_keys_collection UNION SELECT WBS, TaskRequired FROM create_task_collection)''',
        )

        process_each_wbs = rail.TriggerDagRunForEachItemOperator(
            task_id="process_each_wbs",
            trigger_dag_id=f"dxctechnology_compass_gsap_billing_and_tasks_import_{config.sub_erp_name}_child_wbs",
            items="{{ result('query_merged_projects') }}",
            execution_timeout=timedelta(days=14),
            conf={
                "WBS": "{{ item.WBS }}",
                "DefinitionUri": "{{ result('get_project_oefs').gsaptaskrequired }}",
                "TagUri": "{{ result('get_gsaptaskrequired_values') | find_first_by_attr_and_get_attr('name', item.TaskRequired, 'uri', '') }}",
                "TaskTypeOptionUri": "{{ result('get_task_type_custom_field') }}",
                "TaskTypeOptionValueUri": "{{ result('get_task_type_options').gsaptask }}",
                "BillingKeyOptionValueUri": "{{ result('get_task_type_options').gsapbillingkey }}",
            },
            retries=0,
        )

        wait_for_process_each_wbs = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_wbs',
            dag_runs='{{ result("process_each_wbs") }}',
            execution_timeout=timedelta(days=14),
        )

        send_logs_enter, _ = get_send_logs(config)

        new_file_sensor >> is_xml >> rail.Label(
            'Yes') >> download_file >> parse_xml >> has_data >> rail.Label('Yes') >> process_data
        process_data >> get_project_oefs >> get_gsaptaskrequired_values >> process_each_wbs >> wait_for_process_each_wbs
        process_data >> get_task_type_custom_field >> get_task_type_options >> process_each_wbs >> wait_for_process_each_wbs
        process_data >> get_billing_keys_from_xml >> create_billing_keys_collection >> query_merged_projects >> process_each_wbs >> \
            wait_for_process_each_wbs >> send_logs_enter
        process_data >> get_tasks_from_xml >> create_task_collection >> query_merged_projects >> process_each_wbs >> wait_for_process_each_wbs
        process_data >> get_records_missing_wbs_from_xml >> log_records_missing_wbs >> send_logs_enter
        is_xml >> rail.Label('No') >> send_bad_file_format_email
        has_data >> rail.Label('No') >> send_blank_payload_email
        download_file >> rail.Label(
            'Always') >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun

    return dag

rail.for_each_instance(create_main_airflow_dag)
