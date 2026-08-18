import itertools
from datetime import timedelta, datetime
import rail
from pwcglobal.custom_import_for_teammanager_permission.utils import request_payload
from pwcglobal.custom_import_for_teammanager_permission.utils.request_payload import get_invalid_record



def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'PwC Custom Import for Team Manager Permission {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
        schedule_interval=timedelta(seconds=config.master_schedule_interval),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_csv_content',
            no_task='send_incorrect_file_format_email',
        )

        send_incorrect_file_format_email = rail.EmailOperator(
            task_id='send_incorrect_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Custom Import to Team Manager Permission with Supervisory Org Restriction - skipped {{ current_time_in_specified_tz() }} ''',
            html_content="templates/emails/incorrect_fileformat_mail.html"
        )

        download_csv_content = rail.SFTPDownloadFileOperator(
            task_id='download_csv_content',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        get_current_time_tz = rail.PythonOperator(
            task_id='get_current_time_tz',
            python_callable=lambda: rail.render_template('{{current_time_in_specified_tz()}}')
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_csv_content = rail.LoadCSVFileOperator(
            task_id="load_csv_content",
            document="{{ result('download_csv_content') }}",
            encoding='utf-8-sig',
        )

        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('load_csv_content') }}",
            name="raw_input_data",
            columns={
                'GUID': 'guid',
                'Permission Name': 'permission_name',
                'Supervisory Org': 'supervisory_org'
            }
        )

        if_csv_has_data = rail.IfOperator(
            task_id='if_csv_has_data',
            test='''{{ result('load_csv_content') | load_all_records | length > 0 }}''',
            yes_task="create_custom_import_sup_org_logs",
            no_task="send_mail_skipped_import",
        )

        send_mail_skipped_import = rail.EmailOperator(
            task_id='send_mail_skipped_import',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Custom Import to Team Manager Permission with Supervisory Org Restriction - skipped {{ result('get_current_time_tz') }} ''',
            html_content="templates/emails/no_records_in_file_mail.html"
        )

        create_custom_import_sup_org_logs = rail.CreateLogOperator(
            task_id = 'create_custom_import_sup_org_logs'
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            name='invalidrecords',
            query=f"""SELECT * FROM raw_input_data WHERE NULLIF(guid, '') IS NULL
              OR NULLIF(permission_name, '') IS NULL OR NULLIF(supervisory_org, '') IS NULL"""
        )

        has_invalid_records = rail.IfOperator(
            task_id="has_invalid_records",
            test="{{result('query_invalid_records', 'length') > 0}}",
            yes_task="log_invalid_records",
            no_task="query_valid_records"
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{ result('create_custom_import_sup_org_logs') }}",
            items='{{result("query_invalid_records")}}',
            message=request_payload.get_mandatory_fields_exception_message,
            severity='Exception',
            properties=get_invalid_record
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='validrecords',
            query=f"""SELECT * FROM raw_input_data WHERE NULLIF(guid, '') IS NOT NULL
              and NULLIF(permission_name, '') IS NOT NULL and NULLIF(supervisory_org, '') IS NOT NULL"""
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task='get_all_permission_set',
            no_task="process_log_generation"
        )

        get_all_permission_set = rail.RepliconServiceOperator(
            task_id="get_all_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        process_user_for_permission_for_supervisory_org = rail.trigger_parallel_dagrun(
            task_id="process_user_for_permission_for_supervisory_org",
            items="{{ result('query_valid_records') }}",
            trigger_dag_id=config.process_supervisory_org_permission_assignment_child,
            parallel_count=config.parallel_supervisory_org_permission_assignment_child_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item,
                **{
                    'permission_to_be_assigned_uri':rail.find_first_by_attr_and_get_attr(
                        rail.result('get_all_permission_set'), 'displayText', item['permission_name'], 'uri', '')
                }
            }
        )

        get_process_user_for_permission_dag_ids =rail.PythonOperator(
            task_id= 'get_process_user_for_permission_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_user_for_permission_for_supervisory_org_{x+1}'), range(config.parallel_supervisory_org_permission_assignment_child_count))))),
            show_return_value_in_logs= False
        )

        gather_process_user_for_supervisory_orgs_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_process_user_for_supervisory_orgs_logs',
            dag_runs='{{ result("get_process_user_for_permission_dag_ids") }}',
            dagrun_task_id='create_process_supervisory_org_child_logs',
            execution_timeout=timedelta(
                hours=config.gather_process_user_for_supervisory_orgs_logs_timeout_hours),
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation,
            conf={
                'supervisory_org_logs': "{{result('gather_process_user_for_supervisory_orgs_logs')}}",
                'otherlogs': "{{result('create_custom_import_sup_org_logs')}}",
                'log_filename': '{{ result("new_file_sensor") | file_name | replace(".csv", "") }}' + '_' + str(
                    datetime.now().strftime("%Y%m%d%H%M%S")) + '_' + config.country_code + '_logs.csv'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )
        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{get_error_message()|is_truthy}}',
            yes_task="fail_dagrun"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{get_error_message()}}'
        )

        new_file_sensor >> is_csv
        is_csv >> rail.Label('Yes') >> download_csv_content >> get_current_time_tz >> was_new_file_found
        get_current_time_tz >> load_csv_content >> create_collection_from_csv

        create_collection_from_csv >> if_csv_has_data
        if_csv_has_data >> rail.Label('Yes') >> create_custom_import_sup_org_logs >> query_invalid_records >> has_invalid_records
        if_csv_has_data >> rail.Label('No') >> send_mail_skipped_import

        has_invalid_records >> rail.Label('Yes') >> log_invalid_records >> query_valid_records
        has_invalid_records >> rail.Label('No') >> query_valid_records

        query_valid_records >> has_valid_records

        has_valid_records >> rail.Label('Yes') >> get_all_permission_set >> process_user_for_permission_for_supervisory_org >> \
            get_process_user_for_permission_dag_ids >> gather_process_user_for_supervisory_orgs_logs >> process_log_generation
        has_valid_records >> rail.Label('No') >> process_log_generation

        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        is_csv >> rail.Label('No') >> send_incorrect_file_format_email

        process_log_generation >> log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag

rail.for_each_instance(create_dag)
