from airflow.models import Variable
import rail


def create_location_group_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_location_group_child_dag_id,
        description="WCG User Import - Manage Location Groups (Workato Steps 102-108 pattern)",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="check_if_location_exists"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="check_if_location_exists",
            end_task="catch_and_log_errors"
        )

        check_if_location_exists = rail.IfOperator(
            task_id="check_if_location_exists",
            test='{{ dag_run.conf.location_uri | is_truthy }}',
            yes_task="finish",
            no_task="create_new_draft_location"
        )

        create_new_draft_location = rail.RepliconServiceOperator(
            task_id="create_new_draft_location",
            endpoint="/services/LocationService1.svc/CreateNewDraft",
            data={
                "parentLocationUri": None
            }
        )

        update_location_name = rail.RepliconServiceOperator(
            task_id="update_location_name",
            endpoint="/services/LocationService1.svc/UpdateName",
            data=lambda dag_run: {
                "locationUri": rail.result("create_new_draft_location", {}),
                "name": dag_run.conf.get("location_name")
            }
        )

        publish_draft_location = rail.RepliconServiceOperator(
            task_id="publish_draft_location",
            endpoint="/services/LocationService1.svc/PublishDraft",
            data=lambda: {
                "draftUri": rail.result("create_new_draft_location", {})
            }
        )

        finish = rail.EmptyOperator(task_id="finish")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.log_artifact }}',
            message='{{ get_error_message() }}',
            severity="Error",
            trigger_rule="one_failed",
            properties={
                "location_name": '{{ dag_run.conf.location_name }}',
                "action": "CreateLocation",
                "status": "Error",
                "details": '{{ get_error_message() }}',
                "runid": '{{ dag_run.conf.runid }}'
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> check_if_location_exists

        check_if_location_exists >> rail.Label("Yes") >> finish
        check_if_location_exists >> rail.Label("No") >> create_new_draft_location

        create_new_draft_location >> update_location_name >> publish_draft_location >> catch_and_log_errors


    return dag


rail.for_each_instance(create_location_group_child_dag)
