import rail
from dxctechnology.compass_wbs_import_v3.utils import request_payload

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_child_project_dagid,
        description='DXC_COMPASS_WBS_Automation Child - Process Child Projects',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child_project,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        update_childwbs_timetrackingattribute = rail.RepliconServiceOperator(
            task_id = 'update_childwbs_timetrackingattribute',
            endpoint = '/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data = request_payload.get_update_time_tracking_attribute
        )

        update_wbsofferinggroup = rail.RepliconServiceOperator(
            task_id='update_wbsofferinggroup',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data= request_payload.get_wbsofferinggrp_oef_param
        )

        update_tmwbsindicator = rail.RepliconServiceOperator(
            task_id='update_tmwbsindicator',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data= request_payload.get_tmwbsindicator_oef_param
        )

        can_update_psa_flag = rail.IfOperator(
            task_id="can_update_psa_flag",
            test="{{ dag_run.conf.wbsofferinggroupvalue == 'Velocity Only' }}",
            yes_task="update_psa_flag_x",
            no_task="update_psa_flag_blank",
        )

        update_psa_flag_x = rail.RepliconServiceOperator(
            task_id='update_psa_flag_x',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data= lambda dag_run:request_payload.get_psa_flag_oef_param('X', dag_run)
        )

        update_psa_flag_blank = rail.RepliconServiceOperator(
            task_id='update_psa_flag_blank',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data= lambda dag_run: request_payload.get_psa_flag_oef_param('blank', dag_run)
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        update_childwbs_timetrackingattribute >> update_wbsofferinggroup >> update_tmwbsindicator>>\
        can_update_psa_flag >> rail.Label('No') >> update_psa_flag_blank >> log_to_sumo
        can_update_psa_flag >> rail.Label('Yes') >> update_psa_flag_x >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
