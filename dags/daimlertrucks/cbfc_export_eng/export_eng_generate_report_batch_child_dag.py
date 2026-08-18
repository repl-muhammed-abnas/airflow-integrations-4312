# pylint: disable=too-many-statements, line-too-long
import rail
from daimlertrucks.cbfc_export_eng.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'{config.company_key}_dtna_cbfc_export_eng_generatereportbatch_child_{config.instance}',
        description=f'DTNA_CbFC_Export_ENG_generate report batch_Child_V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=config.schedule_interval,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        execute_batch_in_background = rail.RepliconServiceOperator(
            task_id='execute_batch_in_background',
            endpoint="/services/reportservice1.svc/ExecuteBatchInBackground",
            data={
                "batchUri": "{{ dag_run.conf.batchUri }}"
            }
        )

        create_run_list = rail.PythonOperator(
            task_id='create_run_list',
            python_callable=lambda: list({i: i} for i in range(30))
        )

        declare_variable = rail.SetVariableOperator(
            task_id='declare_variable',
            append=False,
            name='report_processing',
            value='pending'
        )

        foreach_create_list = rail.ForEachOperator(
            task_id='foreach_create_list',
            items=lambda: rail.result('create_run_list'),
            start_task='get_variable_data',
            end_task='foreach_create_list_end'
        )
        get_variable_data = rail.GetVariableOperator(
            task_id='get_variable_data',
            name='{{ result("declare_variable").name }}'
        )
        if_declare_variable_value_equals_to_pending = rail.IfOperator(
            task_id='if_declare_variable_value_equals_to_pending',
            test="{{ result('get_variable_data').value == 'pending'}}",
            yes_task="wait_for_interval",
            no_task="foreach_create_list_end",
        )

        wait_for_interval = rail.PythonOperator(
            task_id='wait_for_interval',
            python_callable=python_callable.wait_for_batch
        )

        get_batch_status = rail.RepliconServiceOperator(
            task_id='get_batch_status',
            endpoint="/services/reportservice1.svc/GetBatchStatus",
            data={
                "batchUri": "{{ dag_run.conf.batchUri }}"
            }
        )

        if_executionstate_downcase_contains_failed = rail.IfOperator(
            task_id='if_executionstate_downcase_contains_failed',
            test="{{ result('get_batch_status').executionState.lower() | ends_with('failed') }}",
            yes_task="batch_execution_status_failed",
            no_task="if_executionstate_downcase_contains_succeeded",
        )

        batch_execution_status_failed = rail.FailOperator(
            task_id='batch_execution_status_failed',
            message='''Batch Execution Failed'''
        )

        if_executionstate_downcase_contains_succeeded = rail.IfOperator(
            task_id='if_executionstate_downcase_contains_succeeded',
            test="{{ result('get_batch_status').executionState.lower() | ends_with('succeeded') }}",
            yes_task="update_variable",
            no_task="foreach_create_list_end",
        )

        update_variable = rail.SetVariableOperator(
            task_id='update_variable',
            append=False,
            name='report_processing',
            value="")

        foreach_create_list_end = rail.EmptyOperator(
            task_id='foreach_create_list_end',
        )

        if_executionstate_downcase_contains_notstarted = rail.IfOperator(
            task_id='if_executionstate_downcase_contains_notstarted',
            test="{{ result('get_batch_status').executionState.lower() | ends_with('not-started') }}",
            yes_task="batch_execution_not_started",
            no_task="finish",
        )

        batch_execution_not_started = rail.FailOperator(
            task_id='batch_execution_not_started',
            message='''Batch Execution Not Started'''
        )

        # log_to_sumo = rail.DagRunLogToSumoOperator(
        #     task_id='log_to_sumo',
        #     sumo_conn_id='sumologic-dagrunlogger',
        #     trigger_rule='all_done',
        # )
        finish = rail.EmptyOperator(
            task_id='finish',
        )
        execute_batch_in_background >> create_run_list >> declare_variable >> foreach_create_list >> get_variable_data >> if_declare_variable_value_equals_to_pending
        if_declare_variable_value_equals_to_pending >> rail.Label(
            'Yes') >> wait_for_interval >> get_batch_status >> if_executionstate_downcase_contains_failed
        if_executionstate_downcase_contains_failed >> rail.Label(
            'Yes') >> batch_execution_status_failed
        if_executionstate_downcase_contains_failed >> rail.Label(
            'No') >> if_executionstate_downcase_contains_succeeded
        if_executionstate_downcase_contains_succeeded >> rail.Label(
            'Yes') >> update_variable >> foreach_create_list_end
        if_executionstate_downcase_contains_succeeded >> rail.Label(
            'No') >> foreach_create_list_end >> if_executionstate_downcase_contains_notstarted
        if_declare_variable_value_equals_to_pending >> rail.Label(
            'No') >> foreach_create_list_end
        foreach_create_list >> foreach_create_list_end
        if_executionstate_downcase_contains_notstarted >> rail.Label(
            'Yes') >> batch_execution_not_started >> finish
        if_executionstate_downcase_contains_notstarted >> rail.Label(
            'No') >> finish

    return dag


rail.for_each_instance(create_dag)
