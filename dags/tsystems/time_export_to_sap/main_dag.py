from pendulum import datetime
from datetime import timedelta
from airflow.models import Variable
import rail
import json
from tsystems.time_export_to_sap.utils.python_callable import check_export_date_matches
from tsystems.time_export_to_sap.utils import request_payload
from tsystems.time_export_to_sap.utils import python_callable


def create_unified_dag(config):
    """Create single unified DAG that checks schedule and exports data to SAP"""
    
    with rail.create_airflow_dag(
        dag_id=config.tsystems_dag,
        description=f'T-Systems Time Export {config.instance} - Checks schedule and transfers time data to SAP',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2025, 1, 1),
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_tasks=config.dag_max_active_tasks,
        max_active_runs=config.master_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config
        )

        process_start_time = rail.PythonOperator(
            task_id='process_start_time',
            python_callable=request_payload.get_berlin_timenow_in_fmt
        )

        check_mapper_schedule = rail.PythonOperator(
            task_id='check_mapper_schedule',
            python_callable=lambda: check_export_date_matches(config.pacific_timezone, config.EXPORT_SCHEDULE_MAPPER) if Variable.get(
                config.can_use_variable_mapper, default_var='false').lower() == 'false' else check_export_date_matches(config.pacific_timezone, json.loads(Variable.get(
                config.tsystem_mapper_variable))),
        )

        determine_export = rail.IfOperator(
            task_id='determine_export',
            test="{{ result('check_mapper_schedule') | is_truthy }}",
            yes_task='get_fileformat_script',
            no_task='no_export_today'
        )

        get_fileformat_script = rail.RepliconServiceOperator(
            task_id='get_fileformat_script',
            endpoint='/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText', config.default_file_format, 'uri')
        )

        get_user_oef_uri = rail.RepliconServiceOperator(
            task_id='get_user_oef_uri',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data = {
                "bindingContextUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'name', "Legal Unit", 'uri')
        )

        trigger_time_export_to_sap_process_export_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_time_export_to_sap_process_export_child',
            retries=0,
            items=lambda: rail.result('check_mapper_schedule'),
            trigger_dag_id=config.timeexport_to_sap_child_dag,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=python_callable.timeexport_process_conf,
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.dagrun_log_sumo_conn_id,
            trigger_rule='all_done'
        )

        no_export_today = rail.EmptyOperator(
            task_id='no_export_today'
        )

        # DAG Flow
        process_start_time >> check_mapper_schedule >> determine_export
        determine_export >> rail.Label("Export Scheduled") >> get_fileformat_script >> get_user_oef_uri >> trigger_time_export_to_sap_process_export_child

        determine_export >> rail.Label("No Export") >> no_export_today
        trigger_time_export_to_sap_process_export_child >> dagrun_log_to_sumo

    return dag

rail.for_each_instance(create_unified_dag)