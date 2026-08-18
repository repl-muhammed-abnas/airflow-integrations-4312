import rail
from dxctechnology.adhoc.gsap_task_adhoc_put_task.utils import request_payload

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_task_adhoc_put_task_child_{config.instance}_batch2',
        description='DXC_GSAP_Task ADHOC - CHILD',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_ignore_record = rail.IfOperator(
            task_id = "can_ignore_record",
            test= lambda dag_run: len(dag_run.conf['code']) > 50,
            yes_task="dummy_ignore_record",
            no_task="put_gsap_task"
        )

        dummy_ignore_record = rail.EmptyOperator(
            task_id = "dummy_ignore_record"
        )

        put_gsap_task = rail.RepliconServiceOperator(
            task_id="put_gsap_task",
            endpoint="/services/ProjectService1.svc/PutTask",
            data=request_payload.get_put_task_payload,
        )


        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                 'wbs': '{{dag_run.conf.wbs}}',
                'task1': '{{dag_run.conf.task1}}',
                'task2': '{{dag_run.conf.task2}}',
                'status': 'Error'
            }
        )

        can_ignore_record >> rail.Label("No") >> put_gsap_task >> catch_and_log_errors
        can_ignore_record >> rail.Label("Yes") >> dummy_ignore_record

    return dag


rail.for_each_instance(create_child_dag_wbs)
