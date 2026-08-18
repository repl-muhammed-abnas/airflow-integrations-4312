from datetime import datetime
from airflow.utils.edgemodifier import Label

import rail
from dxctechnology.ppmc_project_and_tasks_import import request_payload

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/ppmc_project_and_tasks_import/config.py


# pylint: disable=too-many-statements
def create_child_task_update_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_ppmc_project_task_import_child_task_update{dag_id_postfix}',
        description=f'DXC PPMC Project and Tasks - Child_updatetask V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        start_date=datetime(2022, 1, 1)
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        has_task_uri = rail.IfOperator(
            task_id="has_task_uri",
            test="{{ True if dag_run.conf.taskuri else False }}",
            yes_task="get_task_details",
            no_task="log_resource_exception",
        )

        log_resource_exception = rail.WriteLogOperator(
            task_id='log_resource_exception',
            message='task not available in Input or Replicon',
            properties={
                'wbs': '{{ dag_run.conf.wbsname }}',
                'task': '{{ dag_run.conf.name }}',
                'status': 'Exception',
            }
        )

        get_task_details = rail.RepliconServiceOperator(
            task_id="get_task_details",
            endpoint="/services/TaskService1.svc/GetTaskDetails",
            data={'taskUri': '{{ dag_run.conf.taskuri }}'}
        )

        can_update_code = rail.IfOperator(
            task_id="can_update_code",
            test="{{ True if dag_run.conf.description != result('get_task_details').code else False }}",
            yes_task="update_code",
            no_task="can_update_date",
        )

        update_code = rail.RepliconServiceOperator(
            task_id="update_code",
            endpoint="/services/TaskService1.svc/UpdateCode",
            data={
                "taskUri": '{{ dag_run.conf.taskuri }}',
                "code": '{{ dag_run.conf.description }}',
            }
        )

        update_description = rail.RepliconServiceOperator(
            task_id="update_description",
            endpoint="/services/TaskService1.svc/UpdateDescription",
            data={
                "taskUri": '{{ dag_run.conf.taskuri }}',
                "description": '{{ dag_run.conf.description }}',
            }
        )

        log_completion_update_code = rail.WriteLogOperator(
            task_id='log_completion_update_code',
            message='The PPMC project\'s Description & Code  is updated to Description',
            properties={
                'wbs': '{{ dag_run.conf.wbsname }}',
                'task': '{{ dag_run.conf.name }}',
                'status': 'Success',
            })

        can_update_date = rail.IfOperator(
            task_id="can_update_date",
            test=lambda: request_payload.get_replicon_date(request_payload.get_dag_run_conf()['startdate']) !=
            rail.result('get_task_details')['timeEntryDateRange']['startDate'] or
            request_payload.get_replicon_date(request_payload.get_dag_run_conf()['enddate']) !=
            rail.result('get_task_details')['timeEntryDateRange']['endDate'],
            yes_task="update_date",
            no_task="can_update_resource",
        )

        update_date = rail.RepliconServiceOperator(
            task_id="update_date",
            endpoint="/services/TaskService1.svc/UpdateTimeEntryDateRange",
            data=lambda: {
                "taskUri": request_payload.get_dag_run_conf()['taskuri'],
                "dateRange": {
                    "startDate": request_payload.get_replicon_date(request_payload.get_dag_run_conf()['startdate']),
                    "endDate": request_payload.get_replicon_date(request_payload.get_dag_run_conf()['enddate'])
                }
            }
        )

        log_completion_update_date = rail.WriteLogOperator(
            task_id='log_completion_update_date',
            message='The  time entry date range is  updated for PPMC project',
            properties={
                'wbs': '{{ dag_run.conf.wbsname }}',
                'task': '{{ dag_run.conf.name }}',
                'status': 'Success',
            })

        can_update_resource = rail.IfOperator(
            task_id="can_update_resource",
            test="{{ dag_run.conf.resourceuris | length > 0 }}",
            yes_task="update_resource",
            no_task="can_update_estimated_hours",
        )

        update_resource = rail.RepliconServiceOperator(
            task_id="update_resource",
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda : {
                "taskUri": request_payload.get_dag_run_conf()['taskuri'],
                "resourceUris": request_payload.get_dag_run_conf()['resourceuris'],
                "isAssigned": "true"
            }
        )

        log_completion_update_resource = rail.WriteLogOperator(
            task_id='log_completion_update_resource',
            message='The time entry date range is updated for PPMC project',
            properties={
                'wbs': '{{ dag_run.conf.wbsname }}',
                'task': '{{ dag_run.conf.name }}',
                'status': 'Success',
            })

        can_update_estimated_hours = rail.IfOperator(
            task_id="can_update_estimated_hours",
            test=lambda: request_payload.get_dag_run_conf()['task2estimatedhours'] and
            float(request_payload.get_dag_run_conf()
                  ['task2estimatedhours']) > 0,
            yes_task="update_hours",
            no_task="finish",
        )

        update_hours = rail.RepliconServiceOperator(
            task_id="update_hours",
            endpoint="/services/TaskService1.svc/UpdateEstimatedHours",
            data=lambda: {
                "taskUri": request_payload.get_dag_run_conf()['taskuri'],
                "estimatedHours": request_payload.get_replicon_hours(request_payload.get_dag_run_conf()['task2estimatedhours'])
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        has_task_uri >> Label("Yes") >> get_task_details
        has_task_uri >> Label("No") >> log_resource_exception >> finish

        get_task_details >> can_update_code
        can_update_code >> Label(
            "Yes") >> update_code >> update_description >> log_completion_update_code >> can_update_date
        can_update_code >> Label("No") >> can_update_date

        can_update_date >> Label(
            "Yes") >> update_date >> log_completion_update_date >> can_update_resource
        can_update_date >> Label("No") >> can_update_resource

        can_update_resource >> Label(
            "Yes") >> update_resource >> log_completion_update_resource >> can_update_estimated_hours
        can_update_resource >> Label("No") >> can_update_estimated_hours

        can_update_estimated_hours >> Label("Yes") >> update_hours >> finish
        can_update_estimated_hours >> Label("No") >> finish

    return dag


rail.for_each_instance(create_child_task_update_dag)
