from datetime import timedelta
from dataaxle.user_import.utils import custom_methods
import rail


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_process_users_dag_id,
        description=f"Dataaxle User Import - Process Users Child DAG {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_process_users_child,
    ) as dag:

        view_dagrun_config = rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        user_check = rail.PythonOperator(
            task_id="user_check",
            python_callable=lambda dag_run: dag_run.conf.get("user_uri")
        )

        if_user_exists_in_replicon = rail.IfOperator(
            task_id="if_user_exists_in_replicon",
            test='{{ result("user_check") | is_truthy }}',
            yes_task="trigger_update_user_child",
            no_task="trigger_create_user_child",
        )

        trigger_update_user_child = rail.TriggerDagRunOperator(
            task_id="trigger_update_user_child",
            trigger_dag_id=config.child_update_user_dag_id,
            conf=lambda dag_run: {
                **dag_run.conf,
                "user_uri": rail.result("user_check"),
            },
        )

        wait_for_update_user_child = rail.WaitForDagRunsSensor(
            task_id="wait_for_update_user_child",
            dag_runs="{{ result('trigger_update_user_child') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        trigger_create_user_child = rail.TriggerDagRunOperator(
            task_id="trigger_create_user_child",
            trigger_dag_id=config.child_create_user_dag_id,
            conf=lambda dag_run: dag_run.conf,
        )

        wait_for_create_user_child = rail.WaitForDagRunsSensor(
            task_id="wait_for_create_user_child",
            dag_runs="{{ result('trigger_create_user_child') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log="{{ dag_run.conf.user_import_log }}",
            trigger_rule="one_failed",
            severity="Error",
            message="{{ get_error_message() }}",
            properties=lambda dag_run: custom_methods.build_user_import_log(
                dag_run,
                action="process_users",
                status="failed",
                details="{{ get_error_message() }}",
                parent_job_id=dag_run.conf.get("parent_job_id"),
                child_job_id=rail.render_template("{{ dag_run_ecid() }}")
            )
        )

        view_dagrun_config >> user_check >> if_user_exists_in_replicon
        if_user_exists_in_replicon >> rail.Label("Yes") >> trigger_update_user_child >> wait_for_update_user_child >> catch_and_log_error
        if_user_exists_in_replicon >> rail.Label("No") >> trigger_create_user_child >> wait_for_create_user_child >> catch_and_log_error

        return dag


rail.for_each_instance(create_child_dag)
