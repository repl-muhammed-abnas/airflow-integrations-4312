from datetime import timedelta
import rail
from dxctechnology.compass_attributes_1_and_2.utils import python_callable_method
from dxctechnology.compass_attributes_1_and_2.utils import request_payload
from dxctechnology.compass_attributes_1_and_2.utils import custom_methods

null = None


def create_attribute_1_process_copy_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_attribute_1_process_copy_data_child_{config.dag_id_postfix}',
        description=f'DXC_Compass_Attribute 1 Child - Create Task {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_task_copy_batch = rail.RepliconServiceOperator(
            task_id='create_task_copy_batch',
            endpoint='/services/TaskService1.svc/CreateTaskCopyBatch',
            data=request_payload.get_create_task_copy_batch
        )

        create_task_copy_batch_group_entry, create_task_copy_batch_group_exit = rail.batch_execution(
            'execute_create_task_copy_batch', create_task_copy_batch.task_id)

        get_task_copy_batch_results = rail.RepliconServiceOperator(
            task_id='get_task_copy_batch_results',
            endpoint='/services/TaskService1.svc/GetTaskCopyBatchResults',
            data={
                "taskCopyBatchUri": "{{ result('create_task_copy_batch') }}"
            }
        )

        get_created_task_list = rail.PythonOperator(
            task_id='get_created_task_list',
            python_callable=python_callable_method.retrieve_created_task_list
        )

        created_tasks_collection = rail.CreateCollectionOperator(
            task_id='created_tasks_collection',
            source='{{ result("get_created_task_list") | to_json }}',
            name='created_tasks'
        )

        is_uri_present_in_copy_batch = rail.IfOperator(
            task_id='is_uri_present_in_copy_batch',
            test=lambda: python_callable_method.check_key_values_present_in_list(
                'created_tasks_collection', 'uri'),
            yes_task='process_task_hierachy_add',
        )

        process_task_hierachy_add = rail.TriggerDagRunForEachItemOperator(
            task_id='process_task_hierachy_add',
            retries=0,
            items=lambda: [custom_methods.get_conf()],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_compass_attribute_1_task_hierachy_add_child_{config.dag_id_postfix}',
            conf=request_payload.get_add_data_conf
        )

        wait_for_process_task_hierachy_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_task_hierachy_add',
            dag_runs='{{ result("process_task_hierachy_add") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            items='{{ dag_run.conf.data | to_json }}',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '{{ item.name }}',
                'attributenumber': '{{ item.attributenumber }}',
                'action': 'add',
                'status': 'Error',
                # pylint: disable=line-too-long
                'details': '{{ get_error_message() }}',
                'recordcount': ''
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'wbs ': '{{ dag_run.conf.wbs }}',
                'attribute': '1',
                'attributecount': '{{ dag_run.conf.data | length }}',
                'filename': '{{ dag_run.conf.filename }}'
            }
        )

        create_task_copy_batch >> create_task_copy_batch_group_entry >> \
            create_task_copy_batch_group_exit >> get_task_copy_batch_results \
            >> get_created_task_list >> created_tasks_collection >> is_uri_present_in_copy_batch

        is_uri_present_in_copy_batch >> rail.Label(
            'Yes') >> process_task_hierachy_add >> wait_for_process_task_hierachy_add >> finish
        is_uri_present_in_copy_batch >> rail.Label(
            'No') >> finish

        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_attribute_1_process_copy_child_dag)
