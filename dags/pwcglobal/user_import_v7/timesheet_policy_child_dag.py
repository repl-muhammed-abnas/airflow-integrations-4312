from datetime import timedelta, datetime
import rail

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/user_import_v7/config.py


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.timesheet_punch_entry_policy_update_dag_id,
        description=f'PwCGlobal_User_Import Punch Entry Policy update Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        schedule_interval=timedelta(hours=1),
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_policy_update_log',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_policy_update_log = rail.CreateLogOperator(
            task_id="get_policy_update_log",
            tenant_wide_name=config.punch_entrypolicy_log_name,
            existing_log_mode="append",
        )

        def do_filter_log(log):
            return log['properties']['effectivedate'] and datetime(**log['properties']['effectivedate']) <= datetime.utcnow()

        filter_log = rail.FilterLogEntriesOperator(
            task_id='filter_log',
            log="{{ result('get_policy_update_log')}}",
            filter_callable=do_filter_log,
            remove_filtered_entries=True,
        )

        has_any_data = rail.IfOperator(
            task_id="has_any_data",
            test=lambda: bool(rail.load_all_records(
                rail.result('filter_log'))),
            yes_task='write_csv_backup',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun")

        write_csv_backup = rail.WriteCSVFileOperator(
            task_id="write_csv_backup",
            source="{{ result('filter_log') }}",
            header=[
                'execution-correlation-id',
                'user-uri',
                'policyuri',
                'action',
                'message',
                'effectivedate',
            ],
            row=['{{ item.ecid }}', '{{ item.properties.user_uri }}', '{{ item.properties.policyuri }}', '{{ item.properties.action }}',
                 '{{ item.message }}', '{{ item.properties.effectivedate }}'],
        )

        archive_input_webhooks = rail.SFTPUploadFileOperator(
            task_id='archive_input_webhooks',
            content="{{ result('write_csv_backup') }}",
            sftp_conn_id=config.secondary_sftp_conn_id,
            remote_filepath=config.archive_filepath +
            '/{{ ecid() | replace(":", "-") }}_policy_update_data.csv',
        )

        load_policy_assignment_records = rail.PythonOperator(
            task_id='load_policy_assignment_records',
            python_callable=lambda: rail.load_all_records(
                rail.result('filter_log')),
        )

        has_add_record = rail.IfOperator(
            task_id="has_add_record",
            test=lambda: len(list(filter(lambda x: x['properties']['action'].lower(
            ) == 'add', rail.result('load_policy_assignment_records')))) > 0,
            yes_task='assign_policy',
            no_task='has_remove_record',
        )

        assign_policy = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_policy',
            endpoint='/services/PolicySetService1.svc/AssignPolicySetToUser',
            execution_timeout=timedelta(days=14),
            items=lambda: list(filter(lambda x: x['properties']['action'].lower(
            ) == 'add', rail.result('load_policy_assignment_records'))),
            data={
                "userUri": "{{ item.properties.user_uri }}",
                "policySetUri": "{{ item.properties.policyuri }}",
            }
        )

        has_remove_record = rail.IfOperator(
            task_id="has_remove_record",
            test=lambda: len(list(filter(lambda x: x['properties']['action'].lower(
            ) == 'remove', rail.result('load_policy_assignment_records')))) > 0,
            yes_task='remove_policy',
            no_task='finish'
        )

        remove_policy = rail.RepliconServiceCallForEachItemOperator(
            task_id='remove_policy',
            execution_timeout=timedelta(days=14),
            endpoint='/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser',
            items=lambda: list(filter(lambda x: x['properties']['action'].lower(
            ) == 'remove', rail.result('load_policy_assignment_records'))),
            data={
                "userUri": "{{ item.properties.user_uri }}",
                "policySetUris": []
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        batch_task >> get_policy_update_log
        batch_task >> finish

        get_policy_update_log >> filter_log >> has_any_data

        has_any_data >> rail.Label('Yes') >> write_csv_backup >> archive_input_webhooks >>\
            load_policy_assignment_records >> has_add_record
        has_any_data >> rail.Label('No') >> delete_this_dagrun >> finish

        has_add_record >> rail.Label(
            'Yes') >> assign_policy >> has_remove_record
        has_add_record >> rail.Label('No') >> has_remove_record >> finish
        has_remove_record >> rail.Label('Yes') >> remove_policy >> finish
        has_remove_record >> rail.Label('No') >> finish

    return dag


rail.for_each_instance(create_dag)
