from datetime import timedelta
import itertools
from os import path
import rail
from rail.lib.ecid import get_dagrun_ecid
from rail.filters import split

from dxctechnology.gsap_billing_key_master.utils import request_payload
from dxctechnology.gsap_billing_key_master.utils import python_callable_method


null = None

# pylint: disable=too-many-statements


def create_attribute_1_master_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_billing_key_master_{config.dag_id_postfix}',
        description=f'DXC GSAP Billing Key Master V1.0 {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath_attr1,
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
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon GSAP Billing Key Master - Incorrect file format {{ current_time() }}',
            html_content='templates/email/bad_file_format.html',
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
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_xml = rail.LoadXMLFileOperator(
            task_id='parse_xml',
            document='{{ result("download_file") }}',
            xsd_document='./dags/dxctechnology/gsap_billing_key_master/xsdschema/input_schema.xsd'
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("Records") | length > 0 }}',
            yes_task='get_all_customfields',
            no_task='send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon GSAP Billing Key Master - Blank Payload  {{ current_time() }}',
            html_content="templates/email/blank_payload.html",
        )

        get_all_customfields = rail.RepliconServiceOperator(
            task_id='get_all_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={"objectUri": "urn:replicon:object-type:task"}
        )

        get_all_customfield_drop_down_options = rail.RepliconServiceOperator(
            task_id='get_all_customfield_drop_down_options',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                "customFieldUri": '{{ result("get_all_customfields") | find_first_by_attr_and_get_attr("displayText","Task Type","uri") }}'
            }
        )

        get_wbs_records_from_xml = rail.XMLAdaptorOperator(
            task_id='get_wbs_records_from_xml',
            source='{{  result("parse_xml") }}',
            target='result',
            adaptor=[
                'Records',
                {
                    'wbs': 'WBS_Name/text()',
                    'taskName': 'Task_Name/text()',
                    'taskCode': 'Task_Code/text()'
                }
            ],
        )

        filter_valid_wbs_records = rail.PythonOperator(
            task_id='filter_valid_wbs_records',
            python_callable=python_callable_method.get_valid_wbs_records
        )

        filter_blank_wbs_records = rail.PythonOperator(
            task_id='filter_blank_wbs_records',
            python_callable=python_callable_method.get_blank_wbs_records
        )

        create_skip_log = rail.CreateLogOperator(
            task_id='create_skip_log'
        )

        check_all_valid_wbs = rail.IfOperator(
            task_id='check_all_valid_wbs',
            test=lambda: len(rail.result('filter_valid_wbs_records')) > 0,
            yes_task='valid_wbs_collection',
            no_task='log_no_record_to_process'
        )

        log_no_record_to_process = rail.WriteLogOperator(
            task_id='log_no_record_to_process',
            log='{{ result("create_skip_log") }}',
            message='All records are invalid in file to process',
            items='{{ result("filter_blank_wbs_records") | to_json }}',
            properties={
                'wbs': 'na',
                'taskname': '',
                'taskcode': '',
                'action': null,
                'status': 'skipped',
                'details': 'All records are invalid in file to process',
            }
        )

        valid_wbs_collection = rail.CreateCollectionOperator(
            task_id = "valid_wbs_collection",
            source= "{{ result('filter_valid_wbs_records') | to_json}}",
            name="valid_wbs"
        )

        get_unique_wbs = rail.QueryCollectionOperator(
            task_id = "get_unique_wbs",
            query= """SELECT distinct wbs from valid_wbs"""
        )
        dummy_process_wbs_attribute = rail.EmptyOperator(
            task_id='dummy_process_wbs_attribute'
        )

        process_each_wbs_attribute = rail.trigger_parallel_dagrun(
            task_id='process_each_wbs_attribute',
            items='{{ result("get_unique_wbs") }}',
            parallel_count=config.trigger_parallel_dagrun_count_project,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_gsap_billing_key_process_wbs_file_processing_{config.dag_id_postfix}',
            conf=request_payload.get_process_unique_wbs_conf
        )

        get_process_each_wbs_attribute_dag_ids =rail.PythonOperator(
            task_id= 'get_process_each_wbs_attribute_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_each_wbs_attribute_{x+1}'), range(config.trigger_parallel_dagrun_count_project))))),
            show_return_value_in_logs= False
        )

        gather_each_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_each_logs',
            dag_runs='{{ result("get_process_each_wbs_attribute_dag_ids") }}',
            dagrun_task_id='create_billing_key_log',
            flatten=True
        )

        gather_each_logs_for_missing_wbs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_each_logs_for_missing_wbs',
            dag_runs='{{ result("get_process_each_wbs_attribute_dag_ids") }}',
            dagrun_task_id='log_wbs_record_for_reprocessing',
            flatten=True
        )

        get_reprocess_log = rail.CreateLogOperator(
            task_id = "get_reprocess_log",
            tenant_wide_name=config.reprocess_wbs_log_name,
            existing_log_mode="append"
        )

        log_wbs_records_for_reprocessing = rail.WriteLogOperator(
            task_id = "log_wbs_records_for_reprocessing",
            log="{{result('get_reprocess_log')}}",
            items=lambda: rail.result(gather_each_logs_for_missing_wbs.task_id),
            message=lambda item : f"Logging WBS {item['properties']['wbs']} for reprocessing",
            severity="Reprocess",
            properties=lambda item: {
                **item['properties']
            }
        )

        dummy_process_log_generation = rail.EmptyOperator(
            task_id='dummy_process_log_generation'
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_gsap_billing_key_process_log_generation_{config.instance}',
            conf=lambda dag_run:{
                'billing_key_logs': (rail.result('gather_each_logs') if len(rail.result('filter_valid_wbs_records')) > 0 else None),
                'skip_logs': rail.result('create_skip_log'),
                # pylint: disable=line-too-long
                'log_filename': f'log_{ get_dagrun_ecid(dag_run).replace(":", "-")}_{split(string=path.split(rail.result("new_file_sensor"))[1], separator=".")[0] }.csv',
                'record_count': len(rail.result("get_wbs_records_from_xml"))
            }
        )

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

        new_file_sensor >> is_xml

        is_xml >> rail.Label(
            'Yes') >> download_file >> parse_xml >> has_data
        is_xml >> rail.Label('No') >> send_bad_file_format_email

        has_data >> rail.Label(
            'Yes') >> get_all_customfields >> get_all_customfield_drop_down_options >> \
            get_wbs_records_from_xml >> filter_valid_wbs_records >> filter_blank_wbs_records >> create_skip_log >> check_all_valid_wbs

        check_all_valid_wbs >> rail.Label(
            'Yes') >> valid_wbs_collection >> get_unique_wbs >> dummy_process_wbs_attribute >> process_each_wbs_attribute

        process_each_wbs_attribute >> get_process_each_wbs_attribute_dag_ids >> gather_each_logs >> gather_each_logs_for_missing_wbs >> dummy_process_log_generation >> process_log_generation
        check_all_valid_wbs >> rail.Label(
            'No') >> log_no_record_to_process >> dummy_process_log_generation >> process_log_generation

        process_log_generation >> get_reprocess_log >> log_wbs_records_for_reprocessing >> can_log_to_sumo >> rail.Label("Yes") >> log_to_sumo

        has_data >> rail.Label('No') >> send_blank_payload_email
        # was_new_file_found has trigger_rule = 'all_done', so it will execute whenever download_file is done, regardless of whether it
        # succeeded, failed, or was skipped
        download_file >> rail.Label(
            'Always') >> was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_attribute_1_master_dag)
