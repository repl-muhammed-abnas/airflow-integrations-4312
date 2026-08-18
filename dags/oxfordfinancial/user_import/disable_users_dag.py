from datetime import timedelta
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/oxfordfinancial/user_import/config.py


def create_disableuser_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'oxfordfinancial_user_import_disable_users_{config.instance}',
        description=f'Disable Users {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_administration_permission'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_administration_permission',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_administration_permission = rail.RepliconServiceOperator(
            task_id='get_administration_permission',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ dag_run.conf.uri }}"
            },
            data_handler=lambda response: bool(rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:administration', 'description', ''))
        )

        is_not_admin_user = rail.IfOperator(
            task_id='is_not_admin_user',
            test="{{ result('get_administration_permission') | is_falsy }}",
            yes_task="disable_user",
            no_task="write_disableuser_log"
        )

        disable_user = rail.RepliconServiceOperator(
            task_id='disable_user',
            endpoint="/services/Securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.uri }}"
            }
        )

        write_disableuser_log = rail.WriteLogOperator(
            task_id='write_disableuser_log',
            log="{{ dag_run.conf.log }}",
            message="Disabled",
            severity="Success",
            properties={
                "loginname": "{{ dag_run_ecid() }}",
                "sf18digitid": "{{ dag_run.conf.Salesforce_ID }}",
                "status": "Success",
                "reason": "Disabled"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log }}",
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties={
                "loginname": "{{ dag_run_ecid() }}",
                "sf18digitid": "{{ dag_run.conf.Salesforce_ID }}",
                "status": "Error",
                "reason": '{{ get_error_message() }}'
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> get_administration_permission
        get_administration_permission >> is_not_admin_user
        is_not_admin_user >> rail.Label(
            'Yes') >> disable_user >> write_disableuser_log
        is_not_admin_user >> rail.Label(
            'No') >> write_disableuser_log >> catch_and_log_errors

        catch_and_log_errors >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_disableuser_dag)
