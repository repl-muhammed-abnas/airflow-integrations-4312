from datetime import timedelta
import rail
from airflow.models import Variable


null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'pwcfr_timeoff_recal_no_batch_users_child_{config.instance}',
        description=f'Pwcfr_timeoff_recal_no_users_batch_child {config.instance}',
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
            no_task='log_offset'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_offset',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_offset = rail.PythonOperator(
            task_id='log_offset',
            python_callable=lambda dag_run:  int(
                200) * dag_run.conf['item_list']
        )

        declare_timeoffuri_list = rail.SetVariableOperator(
            task_id='declare_timeoffuri_list',
            append=False,
            name='timeoffuris',
            value=[]
        )

        query_list_forceapprovedata = rail.QueryCollectionOperator(
            task_id='query_list_forceapprovedata',
            query="""SELECT * FROM  forceapprovedata LIMIT 200 OFFSET {{ result('log_offset') }}""",
        )

        def get_timeoffuri():
            records = rail.load_all_records(
                rail.result('query_list_forceapprovedata'))
            timeoffuri = [item['timeoffuri'] for item in records]
            return timeoffuri

        get_timeoff_uri = rail.PythonOperator(
            task_id='get_timeoff_uri',
            python_callable=get_timeoffuri
        )

        process_child_for_batch = rail.TriggerDagRunOperator(
            task_id='process_child_for_batch',
            retries=0,
            trigger_dag_id=f'pwcfr_timeoff_recal_batch_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parent_jobid'],
                "timeoffuris": rail.result("get_timeoff_uri"),
                "comments": rail.load_all_records(rail.result("query_list_forceapprovedata"))[0]['comments']
            }
        )

        wait_for_process_child_for_batch = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_child_for_batch',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_child_for_batch") }}'
        )

        gather_userreference_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_userreference_data',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('process_child_for_batch') }}",
            dagrun_task_id='create_forceapprove_batch',
            flatten=True
        )

        accumulate_list = rail.SetVariableOperator(
            task_id='accumulate_list',
            name='Successbatch',
            append=True,
            value={
                "batchuri": "{{result('gather_userreference_data')}}"
            }
        )

        catch = rail.EmptyOperator(
            task_id='catch',
            trigger_rule='one_failed',
        )

        accumulate_list_items = rail.SetVariableOperator(
            task_id='accumulate_list_items',
            name='failedbatch',
            append=True,
            value={
                "error": "{{get_error_message()}}",
                "batchuri": "{{result('gather_userreference_data')}}"
            }
        )

        end_task = rail.EmptyOperator(
            task_id='end_task'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> log_offset

        log_offset >> declare_timeoffuri_list >> query_list_forceapprovedata
        query_list_forceapprovedata >> get_timeoff_uri >> process_child_for_batch
        process_child_for_batch >> wait_for_process_child_for_batch >> gather_userreference_data
        gather_userreference_data >> accumulate_list >> catch >> accumulate_list_items >> end_task >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
