from datetime import timedelta
import rail
from dxctechnology.iwo_perner_mapping.generate_report_batch import report_batch
from dxctechnology.iwo_perner_mapping.send_logs import get_send_logs
from dxctechnology.iwo_perner_mapping import request_payload

def create_main_airflow_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id = f'dxctechnology_iwo_perner_mapping_{config.sub_erp_name}_master{dag_id_postfix}',
        description = f'DXC_IWO Perner Mapping Automation Master V1.0 - SFTP - {config.sub_erp_name}',
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
            bcc = config.internal_logs_email,
            subject = '{{ get_company_key() }} | Replicon user attribute sync for Perner mapping - Incorrect File Format - {{ current_time() }}',
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
            xsd_document= './dags/dxctechnology/iwo_perner_mapping/input_schema.xsd'
        )

        has_data = rail.IfOperator(
            task_id = 'has_data',
            test = '{{ result("parse_xml") | xpath("Records") | length > 0 }}',
            yes_task = 'get_all_custom_field',
            no_task = 'send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id = 'send_blank_payload_email',
            to = config.tenant_email,
            bcc = config.internal_logs_email,
            subject = '{{ get_company_key() }} | Replicon user attribute sync for Perner mapping - No records to process - {{ current_time() }}',
            html_content = "email_blank_payload.html",
        )

        get_all_custom_field = rail.RepliconServiceOperator(
                task_id = "get_all_custom_field",
                endpoint = "/services/CustomFieldService1.svc/GetAllCustomFields",
                data = {"objectUri": "urn:replicon:object-type:user" },
        )

        load_report,create_report_collection=report_batch(config)

        query_active_user=rail.QueryCollectionOperator(
            task_id= 'query_active_user',
            query= "SELECT * FROM create_report_collection"
        )

        get_active_users = rail.PythonOperator(
            task_id="get_active_users",
            python_callable=request_payload.get_active_users,
            op_args=['query_active_user']
        )

        get_details_from_xml = rail.XMLAdaptorOperator(
                task_id = "get_details_from_xml",
                source = '{{ result("parse_xml") }}',
                target = 'artifact',
                adaptor = [
                    'Records',
                    {
                        'COMPASSPersonnelNumber': 'COMPASSPersonnelNumber/text()',
                        'C1GSAPPersonnelNumber': 'C1GSAPPersonnelNumber/text()',
                    },
                ],
            )

        process_perner_mapping=rail.TriggerDagRunForEachItemOperator(
            task_id = 'process_perner_mapping',
            retries = 0,
            items = "{{ result('get_details_from_xml') }}",
            trigger_dag_id = f'dxctechnology_iwo_perner_mapping_{config.sub_erp_name}_child{dag_id_postfix}',
            conf = lambda item: {
                'COMPASSPersonnelNumber' : item['COMPASSPersonnelNumber'],
                'C1GSAPPersonnelNumber' : item['C1GSAPPersonnelNumber'],
                'Udfuri' : rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field'), 'displayText', 'PERNER', 'uri'),
                'C1useruri': rail.find_first_by_attr_and_get_attr(rail.result('get_active_users'), 'employeeid', item['C1GSAPPersonnelNumber'] , 'useruri')
                            if rail.find_first_by_attr_and_get_attr(rail.result('get_active_users'), 'employeeid', item['C1GSAPPersonnelNumber']) else
                            rail.find_first_by_attr_and_get_attr(rail.result('get_active_users'), 'iapernerid', item['C1GSAPPersonnelNumber'] , 'useruri')
                            if rail.find_first_by_attr_and_get_attr(rail.result('get_active_users'), 'iapernerid', item['C1GSAPPersonnelNumber']) else
                            rail.find_first_by_attr_and_get_attr(rail.result('get_active_users'), 'cwfalternateid', item['C1GSAPPersonnelNumber'] , 'useruri')
            },
            execution_timeout = timedelta(hours=12),
        )

        wait_for_process_perner_mapping = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_perner_mapping',
            dag_runs='{{ result("process_perner_mapping") }}',
            execution_timeout=timedelta(days=14),
        )

        send_logs_enter, _ = get_send_logs(config)
        new_file_sensor >> is_xml >> rail.Label("Yes") >> download_file >> rail.Label("Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        download_file >>  parse_xml >> has_data >> rail.Label("Yes")  >> get_all_custom_field >> load_report
        create_report_collection >> query_active_user >> get_active_users >> get_details_from_xml >> process_perner_mapping >> wait_for_process_perner_mapping
        wait_for_process_perner_mapping >> send_logs_enter
        is_xml >> rail.Label("No") >> send_bad_file_format_email
        has_data >> rail.Label("No") >> send_blank_payload_email
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

    return dag

rail.for_each_instance(create_main_airflow_dag)
