from datetime import datetime, timedelta
from airflow.utils.edgemodifier import Label

import rail
from dxctechnology.ppmc_project_and_tasks_import import request_payload

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/ppmc_project_and_tasks_import/config.py


# pylint: disable=too-many-statements
def create_child_task_create_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_ppmc_project_task_import_child_task_create{dag_id_postfix}',
        description=f'DXC PPMC Project and Tasks - Child_createtask V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        start_date=datetime(2022, 1, 1)
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        has_wbs_parent_task = rail.IfOperator(
            task_id="has_wbs_parent_task",
            test="{{ True if dag_run.conf.wbsparenttaskuri and dag_run.conf.attrlist | length == 0 else False }}",
            yes_task="put_parent_task",
            no_task="has_attr_parent_task",
        )

        put_parent_task = rail.RepliconServiceOperator(
            task_id="put_parent_task",
            endpoint="/services/ProjectService1.svc/PutTask",
            data=request_payload.get_put_parent_task_param
        )

        has_attr_parent_task = rail.IfOperator(
            task_id="has_attr_parent_task",
            test="{{  dag_run.conf.attrlist | length > 0 }}",
            yes_task="put_task_with_parent",
            no_task="put_task_without_parent",
        )

        put_task_with_parent = rail.RepliconServiceCallForEachItemOperator(
            task_id="put_task_with_parent",
            endpoint="/services/ProjectService1.svc/PutTask",
            data='{{item}}',
            execution_timeout=timedelta(days=14),
            items=request_payload.get_put_task_with_parent_param,
        )

        put_task_without_parent = rail.RepliconServiceOperator(
            task_id="put_task_without_parent",
            endpoint="/services/ProjectService1.svc/PutTask",
            data=request_payload.get_put_task_without_parent_param
        )

        log_completion_task = rail.WriteLogOperator(
            task_id='log_completion_task',
            message='Task added successfully',
            properties={
                'wbs': '{{ dag_run.conf.wbsname }}',
                'task': '{{ dag_run.conf.name }}',
                'status': 'Success',
            })

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            message='{{ get_error_message()}}',
            properties={
                'wbs': '{{ dag_run.conf.wbsname }}',
                'task': '{{ dag_run.conf.name }}',
                'status': 'Error',
            })

        has_wbs_parent_task >> Label(
            "Yes") >> put_parent_task >> log_completion_task
        has_wbs_parent_task >> Label("No") >> has_attr_parent_task

        has_attr_parent_task >> Label(
            "Yes") >> put_task_with_parent >> log_completion_task
        has_attr_parent_task >> Label(
            "No") >> put_task_without_parent >> log_completion_task

        log_completion_task >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_task_create_dag)
