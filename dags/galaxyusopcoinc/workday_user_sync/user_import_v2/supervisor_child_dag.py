import rail
from galaxyusopcoinc.workday_user_sync.user_import_v2.tasks.update_supervisor import get_update_supervisor
from airflow.models import Variable

def create_supervisor_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_supervisor_dag_id,
        description=f'VialtoPartners_User Import_Child_ update supervisor {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.user_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_required_fields_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="is_required_fields_present",
            end_task="catch_and_log_errors"
        )

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        (supervisor_start, _) = get_update_supervisor(caller="supervisor")

        is_required_fields_present = rail.IfOperator(
            task_id="is_required_fields_present",
            test="{{dag_run.conf.useruri | is_truthy and dag_run.conf.managerid | is_truthy}}",
            yes_task="process_supervisor"
        )

        process_supervisor = rail.EmptyOperator(
            task_id="process_supervisor"
        )

        supervisor_found = rail.IfOperator(
            task_id="supervisor_found",
            test=lambda: bool(rail.result("search_supervisor_by_employeeid")),
            no_task="log_supervisor_not_found"
        )

        log_supervisor_not_found = rail.WriteLogOperator(
            task_id="log_supervisor_not_found",
            message='Supervisor with ID : {{ dag_run.conf.managerid }} was not found',
            log="{{dag_run.conf.create_user_log}}",
            severity='Exception',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.username}}',
                'loginname': '{{dag_run.conf.loginname}}',
                'status': 'Exception',
                'action': '{{dag_run.conf.action}}',
                'message': 'Supervisor with ID : {{ dag_run.conf.managerid }} was not found',
                "allowed_for_supervisor_dag": "False",
                "user_uri": "",
                "managerid": "{{dag_run.conf.managerid}}",
                "is_add_and_errored": "False"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            log="{{dag_run.conf.create_user_log}}",
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'username':  '{{dag_run.conf.username}}',
                'loginname': '{{dag_run.conf.loginname}}',
                'status': 'Error',
                'action': '{{dag_run.conf.action}}',
                'message': '{{ get_error_message() }}',
                "allowed_for_supervisor_dag": "False",
                "user_uri": "{{dag_run.conf.useruri}}",
                "managerid": "{{dag_run.conf.managerid}}",
                "is_add_and_errored": "{{True if dag_run.conf.action == 'Add' else False}}"
            },
        )
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> is_required_fields_present
        is_required_fields_present >> rail.Label("Yes") >> process_supervisor >> supervisor_start >> supervisor_found >> rail.Label(
            "No") >> log_supervisor_not_found >> rail.Label("On error") >> catch_and_log_errors
    return dag


rail.for_each_instance(create_supervisor_dag)
