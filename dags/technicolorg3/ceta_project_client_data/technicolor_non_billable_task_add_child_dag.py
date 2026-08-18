from datetime import timedelta
from airflow.models import Variable
import rail
from technicolorg3.ceta_project_client_data.utils import request_payload

null = None

# pylint: disable=too-many-statements


def create_dropdown_option_addition_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_project_client_details_non_billable_task_add_{config.instance}',
        description=f'Technicolor CETA Non-billable Task add {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='').lower() == 'true',
            yes_task='batch_task',
            no_task='client_project_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='client_project_logs',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            end_task='catch_and_log_errors'
        )

        client_project_logs = rail.CreateLogOperator(
            task_id='client_project_logs',
            tenant_wide_name=f'{config.client_project_logs}',
            existing_log_mode='append',
        )

        check_taskname_present = rail.IfOperator(
            task_id='check_taskname_present',
            test=lambda dag_run: bool(dag_run.conf['taskname']),
            yes_task='put_task',
            no_task='task_name_not_present'
        )

        put_task = rail.RepliconServiceOperator(
            task_id='put_task',
            endpoint='/services/ProjectService1.svc/PutTask',
            data=request_payload.get_put_task_payload
        )

        task_name_not_present = rail.PythonOperator(
            task_id='task_name_not_present',
            python_callable=lambda: 'Task not created as there is no task present'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("client_project_logs") }}',
            trigger_rule='one_failed',
            severity='Error',
            message=config.error_template,
            properties={
                'db': '',
                'client': '',
                'project': '',
                'status': 'Exception',
                'action': 'Add Dropdown Option',
                'details': {config.error_template},
                'reference': '',
                'exported': 'No'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'taskname': '{{ dag_run.conf.taskname }}',
                'projectname': '{{ dag_run.conf.projectname }}'
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> client_project_logs

        client_project_logs >> check_taskname_present >> rail.Label(
            'Yes') >> put_task >> finish
        check_taskname_present >> rail.Label(
            'No') >> task_name_not_present >> finish

        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dropdown_option_addition_child_dag)
