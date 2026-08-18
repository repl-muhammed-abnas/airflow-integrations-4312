from datetime import timedelta
import rail
from dxctechnology.gsap_cost_center.utils import request_payload
from dxctechnology.gsap_cost_center.utils import response_filter
from dxctechnology.gsap_cost_center.utils import python_callable_method
from dxctechnology.gsap_cost_center.tasks.send_logs import get_send_logs


# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_cost_center_master_{config.instance}',
        description='DXC_GSAP_COST_CENTER Master',
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
            subject='{{ get_company_key() }} | GSAP Cost Center Import - Incorrect File Format - {{ current_time_in_specified_tz() }}',
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
            xsd_document='./dags/dxctechnology/gsap_cost_center/xml_schema/input_schema.xsd'
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
            subject='{{ get_company_key() }} | GSAP Cost Center Import - No records to process - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html",
        )

        get_details_from_xml = rail.XMLAdaptorOperator(
            task_id="get_details_from_xml",
            source='{{ result("parse_xml") }}',
            target='artifact',
            adaptor=[
                'Records',
                {
                    "costcentername": "Cost_Center_Name/text()",
                    "costcenterparent": "Cost_Center_Name_parent/text()",
                },
            ],
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('get_details_from_xml')}}",
        )
        query_invalid_records = rail.QueryCollectionOperator(
            task_id='query_invalid_records',
            name='invalidrecords',
            query="""SELECT * FROM create_input_data_collection WHERE NULLIF(costcentername, '') IS NULL"""
        )

        has_invalid_records = rail.IfOperator(
            task_id='has_invalid_records',
            test='{{ result("query_invalid_records", "length") > 0 }}',
            yes_task="log_invalid_records",
            no_task="no_invalid_records",
        )

        no_invalid_records = rail.EmptyOperator(
            task_id='no_invalid_records'
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            items='{{result("query_invalid_records")}}',
            message='Cost Center Name is Blank in feed file',
            severity='Skipped',
            properties=lambda item: {
                'costcentername': item['costcentername'],
                'status': 'Skipped'
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            name='validrecords',
            query="""SELECT * FROM create_input_data_collection WHERE NULLIF(costcentername, '') IS NOT NULL"""
        )

        process_valid_records = rail.EmptyOperator(
            task_id='process_valid_records'
        )

        query_non_psa_costcenters = rail.QueryCollectionOperator(
            task_id='query_non_psa_costcenters',
            name='nonpsacostcenters',
            query="""SELECT DISTINCT * FROM validrecords WHERE NULLIF(costcenterparent, '') IS NULL or  costcenterparent NOT IN ('X','x')"""
        )

        has_non_psa_cost_centers = rail.IfOperator(
            task_id='has_non_psa_cost_centers',
            test='{{ result("query_non_psa_costcenters", "length") > 0 }}',
            yes_task="log_non_psa_cost_centers",
            no_task="no_non_psa_cost_centers",
        )

        no_non_psa_cost_centers = rail.EmptyOperator(
            task_id='no_non_psa_cost_centers'
        )

        log_non_psa_cost_centers = rail.WriteLogOperator(
            task_id='log_non_psa_cost_centers',
            items='{{result("query_non_psa_costcenters")}}',
            message='Non O2C/PSA Cost center',
            severity='Skipped',
            properties=lambda item: {
                'costcentername': item['costcentername'],
                'status': 'Skipped'
            }
        )

        query_psa_costcenters = rail.QueryCollectionOperator(
            task_id='query_psa_costcenters',
            name='psacostcenters',
            query="""SELECT DISTINCT * FROM validrecords WHERE NULLIF(costcenterparent, '') IS NOT NULL and costcenterparent IN ('X','x')"""
        )

        has_psa_cost_centers = rail.IfOperator(
            task_id='has_psa_cost_centers',
            test='{{ result("query_psa_costcenters", "length") > 0 }}',
            yes_task="get_cost_centers",
            no_task="no_psa_cost_centers",
        )

        no_psa_cost_centers = rail.EmptyOperator(
            task_id='no_psa_cost_centers'
        )

        get_cost_centers = rail.RepliconServiceOperator(
            task_id="get_cost_centers",
            endpoint="/services/CostCenterListService1.svc/GetData",
            data=request_payload.get_cost_centers,
            data_handler=response_filter.get_cost_centers
        )

        cost_center_collection = rail.CreateCollectionOperator(
            task_id='cost_center_collection',
            name='allcostcenters',
            source=lambda: rail.result('get_cost_centers'),
        )

        psa_parent_cost_center_uri = rail.PythonOperator(
            task_id='psa_parent_cost_center_uri',
            python_callable=python_callable_method.psa_parent_cost_center_uri
        )

        disabled_psa_cost_centers = rail.QueryCollectionOperator(
            task_id='disabled_psa_cost_centers',
            name='disabledpsacostcenters',
            query="""SELECT * FROM psacostcenters WHERE costcentername IN
                (Select name FROM allcostcenters WHERE status IS FALSE)"""
        )

        has_disabled_psa_costcenters = rail.IfOperator(
            task_id='has_disabled_psa_costcenters',
            test='{{ result("disabled_psa_cost_centers", "length") > 0 }}',
            yes_task="log_disabled_psa_cost_centers",
            no_task="no_disabled_psa_cost_centers",
        )

        no_disabled_psa_cost_centers = rail.EmptyOperator(
            task_id='no_disabled_psa_cost_centers'
        )

        log_disabled_psa_cost_centers = rail.WriteLogOperator(
            task_id='log_disabled_psa_cost_centers',
            items='{{result("disabled_psa_cost_centers")}}',
            message='Cost center is present in Disabled State',
            severity='Exception',
            properties=lambda item: {
                'costcentername': item['costcentername'],
                'status': 'Exception'
            }
        )

        valid_psa_cost_centers = rail.QueryCollectionOperator(
            task_id='valid_psa_cost_centers',
            name='validpsacostcenters',
            query="""SELECT DISTINCT costcentername FROM psacostcenters WHERE costcentername  NOT IN
                (Select name FROM allcostcenters WHERE status IS FALSE)"""
        )

        has_valid_psa_costcenters = rail.IfOperator(
            task_id='has_valid_psa_costcenters',
            test='{{ result("valid_psa_cost_centers", "length") > 0 }}',
            yes_task="process_psa_cost_centers",
            no_task="no_valid_psa_cost_centers",
        )

        no_valid_psa_cost_centers = rail.EmptyOperator(
            task_id='no_valid_psa_cost_centers'
        )

        process_psa_cost_centers = rail.TriggerDagRunForEachItemOperator(
            task_id='process_psa_cost_centers',
            retries=0,
            items="{{ result('valid_psa_cost_centers') }}",
            trigger_dag_id=f'dxctechnology_gsap_cost_center_process_cost_centers_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=request_payload.process_psa_cost_centers_conf
        )

        wait_for_process_psa_cost_centers = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_psa_cost_centers',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            dag_runs='{{ result("process_psa_cost_centers") }}',
        )

        send_logs_enter, send_logs_end = get_send_logs(config)

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule="all_done",
            test=lambda:  rail.get_current_context()['dag_run'].get_task_instance(
                delete_this_dagrun.task_id).current_state().lower() != "success" and
                rail.get_current_context()['dag_run'].get_task_instance(
                download_file.task_id).current_state().lower() == "success",
            yes_task="log_to_sumo",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
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

        get_details_from_xml >> create_input_data_collection >> [
            query_valid_records, query_invalid_records]
        query_invalid_records >> has_invalid_records >> rail.Label(
            'No') >> no_invalid_records >> send_logs_enter
        has_invalid_records >> rail.Label(
            'Yes') >> log_invalid_records >> send_logs_enter
        query_valid_records >> process_valid_records >> [
            query_psa_costcenters, query_non_psa_costcenters]
        query_non_psa_costcenters >> has_non_psa_cost_centers >> rail.Label(
            'No') >> no_non_psa_cost_centers >> send_logs_enter
        has_non_psa_cost_centers >> rail.Label(
            'Yes') >> log_non_psa_cost_centers >> send_logs_enter
        query_psa_costcenters >> has_psa_cost_centers >> rail.Label(
            'No') >> no_psa_cost_centers >> send_logs_enter
        has_psa_cost_centers >> rail.Label(
            'Yes') >> get_cost_centers >> cost_center_collection >> psa_parent_cost_center_uri
        psa_parent_cost_center_uri >> [
            disabled_psa_cost_centers, valid_psa_cost_centers]
        disabled_psa_cost_centers >> has_disabled_psa_costcenters >> rail.Label(
            'Yes') >> log_disabled_psa_cost_centers >> send_logs_enter
        has_disabled_psa_costcenters >> rail.Label(
            'No') >> no_disabled_psa_cost_centers >> send_logs_enter
        valid_psa_cost_centers >> has_valid_psa_costcenters
        has_valid_psa_costcenters >> rail.Label(
            'No') >> no_valid_psa_cost_centers >> send_logs_enter
        has_valid_psa_costcenters >> rail.Label(
            'Yes') >> process_psa_cost_centers >> wait_for_process_psa_cost_centers
        wait_for_process_psa_cost_centers >> send_logs_enter
        send_logs_end >> can_log_to_sumo >> log_to_sumo >> can_fail_dag >> rail.Label(
            'Yes') >> fail_dagrun

    return dag


rail.for_each_instance(create_main_dag)
