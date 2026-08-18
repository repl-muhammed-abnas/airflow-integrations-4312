from datetime import timedelta
from pendulum import now
import rail
import pytz
from airflow.models import Variable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'pwcfr_time_off_recal_force_approve_time_off_batch_child_{config.instance}',
        description=f'Pwc_time_off_recal_force_approve_time_off_batch_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_current_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_current_time',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_current_time = rail.PythonOperator(
            task_id='log_current_time',
            python_callable=lambda: now(pytz.timezone(
                "America/New_York")).strftime("%Y-%m-%eT%H:%M%S.%f")
        )

        search_entries_force_approve_lookup_table = rail.FilterLogEntriesOperator(
            task_id='search_entries_force_approve_lookup_table',
            log="{{dag_run.conf.lookup_table}}",
            properties={
                'jobid': "{{dag_run.conf.parent_jobid}}",
            }
        )

        if_log_entry_not_present = rail.IfOperator(
            task_id='if_log_entry_not_present',
            test="{{result('search_entries_force_approve_lookup_table', 'length') == 0}}",
            yes_task='finish_job',
            no_task='if_log_entry_present'
        )

        if_log_entry_present = rail.IfOperator(
            task_id='if_log_entry_present',
            test="{{result('search_entries_force_approve_lookup_table', 'length') > 0}}",
            yes_task='declare_batch_variable',
            no_task='if_error_present'
        )

        declare_batch_variable = rail.SetVariableOperator(
            task_id='declare_batch_variable',
            append=False,
            name='Batch Size',
            value=lambda: (
                (int(rail.result('search_entries_force_approve_lookup_table', 'length'))) // int(200))
        )

        if_declare_variable_has_no_value = rail.IfOperator(
            task_id='if_declare_variable_has_no_value',
            test="{{ result('declare_batch_variable').value == 0 }}",
            yes_task="update_batch_variable",
            no_task="log_remainder_of_batch",
        )

        update_batch_variable = rail.SetVariableOperator(
            task_id='update_batch_variable',
            append=False,
            name='{{ result("declare_batch_variable").name }}',
            value=1
        )

        log_remainder_of_batch = rail.PythonOperator(
            task_id='log_remainder_of_batch',
            python_callable=lambda: (
                (int(rail.result('search_entries_force_approve_lookup_table', 'length'))) % int(config.batchsize))
        )

        if_remainder_has_value = rail.IfOperator(
            task_id='if_remainder_has_value',
            test=lambda: rail.result('log_remainder_of_batch') > 0 and rail.result(
                'search_entries_force_approve_lookup_table', 'length') > 200,
            yes_task="update_variable",
            no_task="create_csv",
        )

        update_variable = rail.SetVariableOperator(
            task_id='update_variable',
            append=False,
            name='{{ result("declare_batch_variable").name }}',
            value=lambda: rail.result('declare_batch_variable')['value'] + 1
        )

        create_csv = rail.WriteCSVFileOperator(
            task_id='create_csv',
            source=lambda: rail.result(
                'search_entries_force_approve_lookup_table'),
            header=['user',
                    'timeoffuri',
                    'workid',
                    'comments'],
            delimiter=",",
            row=lambda item: [
                item['properties']['username'],
                item['properties']['timeoffuri'],
                item['properties']['workid'],
                item['properties']['comments'],
            ]
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id="load_csv",
            document="{{result('create_csv')}}"
        )

        create_forceapprovedata = rail.CreateCollectionOperator(
            task_id='create_forceapprovedata',
            source="{{ result('load_csv') }}",
            name="forceapprovedata",
            columns={
                'user': 'user',
                'timeoffuri': 'timeoffuri',
                'workid': 'workid',
                'comments': 'comments'
            }
        )

        def get_repeat_helper_list():
            records = []
            for item in range(0, int(rail.get_dag_run_var('Batch Size'))):
                records.append(item)
            return records

        create_variable = rail.PythonOperator(
            task_id='create_variable',
            python_callable=get_repeat_helper_list

        )

        process_no_batch_users_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_no_batch_users_child',
            retries=0,
            items=lambda: rail.result('create_variable'),
            trigger_dag_id=f'pwcfr_timeoff_recal_no_batch_users_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "item_list": item,
                "parent_jobid": dag_run.conf['parent_jobid']
            }
        )

        wait_for_process_no_batch_users_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_no_batch_users_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_no_batch_users_child") }}'
        )

        gather_list_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_list_data',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('process_no_batch_users_child') }}",
            dagrun_task_id='accumulate_list_items',
            flatten=True
        )

        if_error_present = rail.IfOperator(
            task_id='if_error_present',
            test="{{result('gather_list_data') | is_truthy}}",
            yes_task="stop_job_with_an_error_message",
            no_task="batch_connect"
        )

        stop_job_with_an_error_message = rail.FailOperator(
            task_id='stop_job_with_an_error_message',
            message='Approval failed for few uris'
        )

        finish_job = rail.EmptyOperator(
            task_id='finish_job'
        )

        batch_connect = rail.EmptyOperator(
            task_id='batch_connect'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> log_current_time
        log_current_time >> search_entries_force_approve_lookup_table
        search_entries_force_approve_lookup_table >> if_log_entry_not_present >> rail.Label(
            'Yes') >> finish_job >> batch_connect
        if_log_entry_present >> rail.Label(
            'Yes') >> declare_batch_variable >> if_declare_variable_has_no_value
        if_declare_variable_has_no_value >> rail.Label(
            'Yes') >> update_batch_variable >> log_remainder_of_batch
        if_declare_variable_has_no_value >> rail.Label(
            'No') >> log_remainder_of_batch >> if_remainder_has_value
        if_remainder_has_value >> rail.Label(
            'Yes') >> update_variable >> create_csv
        if_remainder_has_value >> rail.Label(
            'No') >> create_csv >> load_csv >> create_forceapprovedata >> create_variable
        create_variable >> process_no_batch_users_child >> wait_for_process_no_batch_users_child
        wait_for_process_no_batch_users_child >> gather_list_data >> if_error_present
        if_log_entry_present >> rail.Label('No') >> if_error_present >> rail.Label(
            'Yes') >> stop_job_with_an_error_message >> batch_connect >> log_to_sumo
        if_error_present >> rail.Label(
            'No') >> batch_connect
        if_log_entry_not_present >> rail.Label('No') >> if_log_entry_present

    return dag


rail.for_each_instance(create_dag)
