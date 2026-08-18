from datetime import timedelta
import rail
from pimco.project_import.utils import request_payload
from airflow.models import Variable

# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pimco_project_import_process_each_record_{config.instance}",
        description=f"PIMCO Entity Consultant Project Import master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_project_max_active_run,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="load_project"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task="load_project",
            end_task="catch_and_log_error"
        )

        def get_project_name(dag_run):
            return dag_run.conf['Projectcode']

        load_project = rail.RepliconServiceOperator(
            task_id= 'load_project',
            endpoint= '/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=lambda dag_run: {
                "projects": [
                    {
                    "code": get_project_name(dag_run)
                    }
                ]
            },
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails'],
        )

        does_project_exist = rail.IfOperator(
            task_id="does_project_exist",
            test="{{ result('load_project') is not none  }}",
            yes_task="process_update_project",
            no_task= 'process_create_project'
        )

        process_update_project = rail.TriggerDagRunOperator(
            task_id="process_update_project",
            trigger_dag_id=f"pimco_update_project_child_{config.instance}",
            conf=lambda dag_run: request_payload.get_process_update_project_conf(dag_run, dag_run.conf['caller']),
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_for_update_project = rail.WaitForDagRunsSensor(
            task_id = "wait_for_update_project",
            dag_runs="{{ result('process_update_project')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        process_create_project = rail.TriggerDagRunOperator(
            task_id="process_create_project",
            trigger_dag_id=f"pimco_create_project_child_{config.instance}",
            conf=lambda dag_run: request_payload.get_process_create_project_conf(dag_run, dag_run.conf['caller']),
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_for_process_create_project = rail.WaitForDagRunsSensor(
            task_id = "wait_for_process_create_project",
            dag_runs="{{ result('process_create_project')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'Projectcode': '{{ dag_run.conf.Projectcode }}',
                'Projectname': "{{ dag_run.conf.Projectname }}",
                'Status': "failed",
                'JobId': '{{ dag_run_ecid() }}',
                'details': '{{ get_error_message() }}',
                'flag': "{{ dag_run.conf.flag }}",
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> load_project >> does_project_exist >> rail.Label("Yes") >> process_update_project >> wait_for_update_project
        does_project_exist >> rail.Label("No") >> process_create_project >> wait_for_process_create_project

        [wait_for_process_create_project, wait_for_update_project] >> rail.Label("on_error") >> catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_main_dag)
