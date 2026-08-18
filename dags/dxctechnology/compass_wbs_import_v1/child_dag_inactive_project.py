import uuid
from datetime import timedelta
from airflow.models import Variable
import rail

def create_child_inactive_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.inactive_project_dagid,
        description='DXC_COMPASS_WBS_Automation Child V2.0 - Inactive Projects Processing',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_inactive_projects,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_project'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_project',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_project = rail.RepliconServiceOperator(
            task_id='get_project',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={"projects": [{"name": "{{ dag_run.conf.item.WBS }}"}]},
            data_handler=lambda x: x[0]['projectDetails'] if len(x) > 0 else None,
        )

        project_exists_in_wts = rail.IfOperator(
            task_id='project_exists_in_wts',
            test="{{ result('get_project') is not none }}",
            yes_task="is_in_progress_in_wts",
            no_task="log_new_inactive_project",
        )

        log_new_inactive_project = rail.WriteLogOperator(
            task_id='log_new_inactive_project',
            message='New project sent with Inactive status',
            properties={
                'projectname': '{{ dag_run.conf.item.WBS }}',
                'projectcode': '{{ dag_run.conf.item.WBSDescription }}',
                'status': 'Skipped',
            }
        )

        is_in_progress_in_wts = rail.IfOperator(
            task_id='is_in_progress_in_wts',
            test="{{ result('get_project').status.name != 'Completed' and result('get_project').uri is not none }}",
            yes_task="set_project_complete_in_wts",
            no_task="log_project_already_completed",
        )

        set_project_complete_in_wts = rail.RepliconServiceOperator(
            task_id='set_project_complete_in_wts',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data={
                'target': {'uri': '{{ result("get_project").uri }}'},
                'modifications': {
                    'statusToApply': {'name': 'Completed'},
                },
                'unitOfWorkId': str(uuid.uuid4()),
            }
        )

        log_project_updated_as_completed = rail.WriteLogOperator(
            task_id='log_project_updated_as_completed',
            message='Project updated as completed',
            properties={
                'projectname': '{{ dag_run.conf.item.WBS }}',
                'projectcode': '{{ dag_run.conf.item.WBSDescription }}',
                'status': 'Success',
            }
        )

        log_project_already_completed = rail.WriteLogOperator(
            task_id='log_project_already_completed',
            message='Project is already marked as completed',
            properties={
                'projectname': '{{ dag_run.conf.item.WBS }}',
                'projectcode': '{{ dag_run.conf.item.WBSDescription }}',
                'status': 'Skipped',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'projectname': '{{ dag_run.conf.item.WBS }}',
                'projectcode': '{{ dag_run.conf.item.WBSDescription }}',
                'status': 'Error',
            },
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_project

        get_project >> project_exists_in_wts >> rail.Label('Yes') >> is_in_progress_in_wts >> rail.Label(
            'Yes') >> set_project_complete_in_wts >> log_project_updated_as_completed >> catch_and_log_errors
        project_exists_in_wts >> rail.Label(
            'No') >> log_new_inactive_project >> catch_and_log_errors
        is_in_progress_in_wts >> rail.Label(
            'No') >> log_project_already_completed >> catch_and_log_errors


    return dag

rail.for_each_instance(create_child_inactive_airflow_dag)
