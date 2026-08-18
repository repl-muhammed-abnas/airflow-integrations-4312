from datetime import timedelta
from airflow.models import Variable
import rail

# pylint:disable = too-many-statements
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.create_discipline_roles_dag_id,
        description=f'merrick_create_discipline_roles_{config.instance}',
        company_key=config.company_key,
        max_active_runs=config.child_dag_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
        default_args={}
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_role'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_role',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # conf: {name: role_name (discipline description), code: discipline_suffix}
        create_role = rail.RepliconServiceOperator(
            task_id='create_role',
            endpoint='/services/ProjectRoleService1.svc/PutProjectRole',
            data=lambda dag_run: {
                "projectRoleUri": {
                    "target": {
                        "uri": null,
                        "name": dag_run.conf['name']
                    },
                    "name": dag_run.conf['name'],
                    "description": dag_run.conf['code'],
                    "isArchived": "false",
                    "isBillable": "true",
                    "rateSchedule": null
                }
            }
        )

        log_role_created = rail.WriteLogOperator(
            task_id='log_role_created',
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "action": "Discipline Sync",
                "status": "Succeeded",
                "reason": f"Polaris role '{dag_run.conf['name']}' "
                          f"(code '{dag_run.conf['code']}') created successfully",
                "employeeid": ""
            }
        )

        catch_role_creation_error = rail.WriteLogOperator(
            task_id='catch_role_creation_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties=lambda dag_run: {
                "action": "Discipline Sync",
                "status": "Error",
                "reason": f"Failed to create Polaris role '{dag_run.conf['name']}' "
                          f"(code '{dag_run.conf['code']}'): " + "{{ get_error_message() }}",
                "employeeid": ""
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> create_role >> log_role_created >> \
            catch_role_creation_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
