from datetime import timedelta
from adtalem.deletion_of_rejected_timeoff.utils import request_payload
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_delete_rejected_timeoff_child_{config.instance}',
        description=f'Adtalem Delete Rejected Timeoff child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_all_rejected_timeoffs=rail.RepliconServiceOperator(
            task_id='get_all_rejected_timeoffs',
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=request_payload.get_all_rejected_timeoffs_payload,
            response_filter=lambda response: [data[0]["uri"] for data in [row_data["cells"] for row_data in response.json()['d']["rows"]]]
        )

        is_rejected_timeoff_exists = rail.IfOperator(
            task_id='is_rejected_timeoff_exists',
            test=lambda: len(rail.result("get_all_rejected_timeoffs")) > 0,
            yes_task='create_time_off_delete_batch'
        )

        create_time_off_delete_batch=rail.RepliconServiceOperator(
            task_id='create_time_off_delete_batch',
            endpoint="/services/TimeOffService1.svc/CreateTimeOffDeleteBatch",
            data=lambda: {
                "timeOffUris": rail.result('get_all_rejected_timeoffs')
            }
        )

        execute_time_off_delete_batch=rail.RepliconServiceOperator(
            task_id='execute_time_off_delete_batch',
            endpoint="/services/TimeOffService1.svc/ExecuteTimeOffDeleteBatch",
            data={
                "timeOffDeleteBatchUri": "{{ result('create_time_off_delete_batch') }}"
            }
        )

        wait_for_batch = rail.RepliconBatchExecutionSensor(
            task_id='wait_for_batch',
            batch_uri='{{ result("create_time_off_delete_batch") }}',
            execution_timeout=timedelta(seconds=config.wait_timeout),
        )

        get_all_rejected_timeoffs >> is_rejected_timeoff_exists >> rail.Label("Yes") >> create_time_off_delete_batch\
            >> execute_time_off_delete_batch >> wait_for_batch

    return dag

rail.for_each_instance(create_dag)
