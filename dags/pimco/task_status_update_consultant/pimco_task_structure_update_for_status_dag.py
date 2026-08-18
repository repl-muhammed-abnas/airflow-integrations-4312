
from datetime import timedelta
from pendulum import datetime as dt
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pimco_consultant_task_structure_update_for_status_master_{config.instance}',
        description=f'PIMCO consultant Task Structure Update for status {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2023, 1, 1, tz=config.pst_timezone),
        schedule_interval=config.schedule_interval_structure_update,
        max_active_runs=config.max_active_runs_structure_update,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_structure_update, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_project_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_project_details',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data= {
                "projects": [
                    {
                    "uri": null,
                    "name": config.project_name,
                    "code": null,
                    "parameterCorrelationId": null
                    }
                ]
            }
        )

        get_all_project_tasks=rail.RepliconServiceOperator(
            task_id='get_all_project_tasks',
            endpoint="/services/ProjectService1.svc/BulkGetTaskDetails2",
            data=lambda: {
                "pageIndex": "1",
                "pageSize": "100000",
                "projectUris": [
                    rail.result('get_project_details')[0]['projectDetails']['uri']
                ],
                "taskDataInclusionOptionUris": []
            }
        )

        truncate_all_entries = rail.CreateLogOperator(
            task_id="truncate_all_entries",
            tenant_wide_name="pimco_task_table_for_consultant_model_project",
            existing_log_mode="truncate",
        )

        add_entries_pimco_task_table_for_model_project=rail.WriteLogOperator(
            task_id='add_entries_pimco_task_table_for_model_project',
            items="{{result('get_all_project_tasks') | to_json }}",
            message='na',
            log='{{ result("truncate_all_entries")}}',
            properties=lambda item:{
                "project": config.project_name,
                "taskdisplayname": item["displayText"],
                "name": item["name"],
                "code": item["code"],
                "uri": item["uri"],
                #pylint: disable= line-too-long
                "fullpath": item["parent"]["task"]["displayText"] if item["parent"] and item["parent"]["task"] and item["parent"]["task"]["displayText"] else '',
                "status": item["isClosed"]
            }
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_project_details >> get_all_project_tasks
        get_all_project_tasks >> truncate_all_entries >> add_entries_pimco_task_table_for_model_project >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
