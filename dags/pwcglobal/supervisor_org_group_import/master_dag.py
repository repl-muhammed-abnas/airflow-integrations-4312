import itertools
from datetime import timedelta
import rail
from pwcglobal.supervisor_org_group_import.utils import python_callable, request_payload, response_filter


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'PwC Supervisory Org Custom Import Master {config.instance}',
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
            subject='''{{get_company_key()}} | Supervisory Org Custom Import - skipped {{ current_time_in_specified_tz() }} ''',
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
            # yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        # archive_file = rail.SFTPMoveFileOperator(
        #     task_id='archive_file',
        #     existing_filename='{{ result("new_file_sensor") }}',
        #     new_filename=config.archive_filepath +
        #     "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        # )

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
                'Supervisory Org': 'cost_center',
                'Action': 'action'
            }
        )

        if_csv_has_data = rail.IfOperator(
            task_id='if_csv_has_data',
            test='''{{ result('load_csv_content') | load_all_records | length > 0 }}''',
            yes_task="create_sup_org_logs",
            no_task="send_mail_skipped_import",
        )

        send_mail_skipped_import = rail.EmailOperator(
            task_id='send_mail_skipped_import',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Supervisory Org Custom Import - skipped {{ result('get_current_time_tz') }} ''',
            html_content="templates/emails/no_records_in_file_mail.html"
        )

        create_sup_org_logs = rail.CreateLogOperator(
            task_id = 'create_sup_org_logs'
        )

        get_cost_center_hierarchy_data = rail.RepliconServicePageOperator(
            task_id='get_cost_center_hierarchy_data',
            endpoint='/services/CostCenterListService1.svc/GetHierarchyData',
            data=request_payload.get_costcenter_hierarchy_payload,
            page_handler=request_payload.page_handler,
            all_result_data_handler=response_filter.get_costcenter_hierarchy_list
        )

        get_add_update_cost_center = rail.PythonOperator(
            task_id='get_add_update_cost_center',
            python_callable=python_callable.get_add_update_costcenters
        )

        if_hash_invalid_data = rail.IfOperator(
            task_id='if_hash_invalid_data',
            test=lambda: bool(rail.result('get_add_update_cost_center')['invalid_data']),
            yes_task="log_invalid_data",
            no_task="process_add_costcenter_dummy",
        )

        log_invalid_data = rail.WriteLogOperator(
            task_id='log_invalid_data',
            log="{{ result('create_sup_org_logs') }}",
            items="{{ result('get_add_update_cost_center').invalid_data | to_json }}",
            message="Invalid Data",
            severity="Exception",
            properties={
                "Supervisory Org": "{{ item['Supervisory Org'] }}",
                "Action": "{{ item['Action'] }}",
                "Status": "Exception",
                "Details": "{{ item['Details'] }}"
            }
        )

        process_add_costcenter_dummy = rail.EmptyOperator(
            task_id='process_add_costcenter_dummy'
        )

        process_add_costcenter = rail.trigger_parallel_dagrun(
            task_id="process_add_costcenter",
            items="{{ result('get_add_update_cost_center').levels_to_add | to_json }}",
            trigger_dag_id=config.add_dagid,
            parallel_count=config.parallel_add_child_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item
            }
        )

        disable_path_not_found = rail.IfOperator(
            task_id='disable_path_not_found',
            test=lambda: bool(rail.result('get_add_update_cost_center')['levels_to_disable']['path_unavailable']),
            yes_task="log_path_not_found",
            no_task="process_disable_costcenter_dummy",
        )

        log_path_not_found = rail.WriteLogOperator(
            task_id='log_path_not_found',
            log="{{ result('create_sup_org_logs') }}",
            items="{{ result('get_add_update_cost_center').levels_to_disable.path_unavailable | to_json }}",
            message="Path Not Found",
            severity="Exception",
            properties={
                "Supervisory Org": "{{ item.costcenter_path }}",
                "Action": "Disable",
                "Status": "Exception",
                "Details": "Received Supervisory Org path is not present or already disabled in Repilcon."
            }
        )

        process_disable_costcenter_dummy = rail.EmptyOperator(
            task_id='process_disable_costcenter_dummy'
        )

        process_disable_costcenter = rail.trigger_parallel_dagrun(
            task_id="process_disable_costcenter",
            items="{{ result('get_add_update_cost_center').levels_to_disable.path_available | to_json }}",
            trigger_dag_id=config.disable_dagid,
            parallel_count=config.parallel_disable_child_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item,
                "lookuptable": rail.result("create_sup_org_logs"),
                
            }
        )

        get_process_add_costcenter_dag_ids =rail.PythonOperator(
            task_id= 'get_process_add_costcenter_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_add_costcenter_{x+1}'), range(config.parallel_add_child_count))))),
            show_return_value_in_logs= False
        )

        gather_add_costcenter_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_add_costcenter_logs',
            dag_runs='{{ result("get_process_add_costcenter_dag_ids") }}',
            dagrun_task_id='create_add_child_logs',
            execution_timeout=timedelta(
                hours=config.gather_add_costcenter_logs_timeout_hours),
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation,
            conf={
                'supervisory_org_logs': "{{result('gather_add_costcenter_logs')}}",
                'otherlogs': "{{result('create_sup_org_logs')}}",
                'log_filename': '{{ result("new_file_sensor") | file_name | replace(".csv", "") }}_logs.csv'
            }
        )

        finish =  rail.EmptyOperator(
            task_id='finish'
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
        # was_new_file_found >> rail.Label('Yes') >> archive_file
        create_collection_from_csv >> if_csv_has_data
        if_csv_has_data >> rail.Label('Yes') >> create_sup_org_logs >> get_cost_center_hierarchy_data >> \
        get_add_update_cost_center >> if_hash_invalid_data
        if_hash_invalid_data >> rail.Label("Yes") >> log_invalid_data >> process_add_costcenter_dummy
        if_hash_invalid_data >> rail.Label("No") >> process_add_costcenter_dummy >> process_add_costcenter >> disable_path_not_found
        disable_path_not_found >> rail.Label("Yes") >> log_path_not_found >> process_disable_costcenter_dummy
        disable_path_not_found >> rail.Label("No") >> process_disable_costcenter_dummy >> process_disable_costcenter >> \
        get_process_add_costcenter_dag_ids >> gather_add_costcenter_logs >> process_log_generation >> finish
        if_csv_has_data >> rail.Label('No') >> send_mail_skipped_import

        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        is_csv >> rail.Label('No') >> send_incorrect_file_format_email
        finish >> log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag

rail.for_each_instance(create_dag)
