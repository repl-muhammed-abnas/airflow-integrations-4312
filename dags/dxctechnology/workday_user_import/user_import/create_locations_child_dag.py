from pendulum import datetime
import rail
from airflow.models import Variable

from dxctechnology.workday_user_import.user_import.common_utils import request_payload
from datetime import timedelta

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_process_location_creation_dag,
        description="dxctechnology workday user sync Master",
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.max_active_run_master
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="get_parent_location_details"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_parent_location_details",
            end_task="create_new_locations",
            execution_timeout=timedelta(days=14)
        )

        get_parent_location_details = rail.RepliconServiceOperator(
            task_id="get_parent_location_details",
            endpoint="/services/LocationListService1.svc/GetData",
            data=request_payload.get_parent_location_payload,
            data_handler=lambda response, dag_run: list(map(lambda row: {
                "name" : row['cells'][0]['textValue'],
                "uri": row['cells'][0]['uri'],
                "full_path": rail.smartjoin_by_delim([_item['textValue'] for _item in row['cells'][1]['cellCollection']],
                                                     separator=request_payload.LOCATION_DELIMITER)
                } ,(filter(lambda item: item['cells'][0]['textValue'] == dag_run.conf['parent_location_name'],
                                                               response['rows']))))
        )

        create_new_locations = rail.RepliconServiceOperator(
            task_id="create_new_locations",
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data=request_payload.get_create_locations_payload
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> create_new_locations
        can_run_batch_task >> rail.Label("No") >> get_parent_location_details

        get_parent_location_details >> create_new_locations

    return dag

rail.for_each_instance(create_dag)
