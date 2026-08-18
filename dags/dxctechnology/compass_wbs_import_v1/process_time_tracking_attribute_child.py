import rail
from dxctechnology.compass_wbs_import_v1.request_payload import get_update_time_tracking_attribute

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_time_tracking_attribute_dagid,
        description='DXC_COMPASS_WBS_Automation Child V2.0 - Process Time Tracking Attribute',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_time_tracking_attribute,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        update_childwbs_timetrackingattribute = rail.RepliconServiceOperator(
            task_id = 'update_childwbs_timetrackingattribute',
            endpoint = '/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data = get_update_time_tracking_attribute
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        update_childwbs_timetrackingattribute >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
