
from datetime import timedelta
import rail
from adtalem.deletion_of_rejected_timeoff.utils import request_payload

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_rejected_timeoff_deletion_master_{config.instance}',
        description=f'Adtalem Rejected Timeoff deletion Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
    ) as dag:

        get_row_counts=rail.RepliconServiceOperator(
            task_id='get_row_counts',
            endpoint="/services/TimeOffListService1.svc/GetRowCounts",
            data=request_payload.get_row_counts_payload
        )

        is_row_count_greater_than_0=rail.IfOperator(
            task_id='is_row_count_greater_than_0',
            test=lambda: rail.result('get_row_counts')[0] > 0,
            yes_task="delete_rejected_timeoff"
        )

        delete_rejected_timeoff=rail.TriggerDagRunForEachItemOperator(
            task_id='delete_rejected_timeoff',
            retries=0,
            batch_size=50,
            items=lambda: list(map(lambda num: num, range(0, rail.result("get_row_counts")[0]))),
            trigger_dag_id=f'adtalem_delete_rejected_timeoff_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "page": item[0]+1,
                "size": 50
            }
        )

        wait_for_delete_rejected_timeoff = rail.WaitForDagRunsSensor(
            task_id='wait_for_delete_rejected_timeoff',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("delete_rejected_timeoff") }}'
        )

        get_row_counts >> is_row_count_greater_than_0
        is_row_count_greater_than_0 >> rail.Label('Yes') >> delete_rejected_timeoff >> wait_for_delete_rejected_timeoff

    return dag

rail.for_each_instance(create_dag)
