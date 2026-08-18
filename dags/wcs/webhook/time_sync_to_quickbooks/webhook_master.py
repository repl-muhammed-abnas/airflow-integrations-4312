from datetime import timedelta
from pendulum import datetime, now
import rail
from airflow.models import Variable, DagRun
from airflow.sensors.time_delta import TimeDeltaSensorAsync
from airflow.utils.state import DagRunState
from airflow.utils.session import NEW_SESSION, provide_session
from wcs.webhook.time_sync_to_quickbooks.utils import custom_methods


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description="WCS time export to Quickbooks - Webhook Receiver",
        start_date=datetime(2026, 4, 1, tz=config.time_zone),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        webhook_conf=[
            rail.WebhookConf(hmac_secret_var=config.wcs_time_sync_to_quickbooks_bearer_token_var)
        ]
    ) as dag:
        
        @provide_session
        def get_dagruns_to_process(session=NEW_SESSION):
            one_hour_ago = now(config.time_zone) - timedelta(hours=1)
            dag_runs_to_filter = (
                session.query(DagRun.id, DagRun.dag_id, DagRun.state)
                .select_from(DagRun)
                .filter(
                    DagRun.dag_id == config.process_timesheet_data_child_id,
                    DagRun.state == DagRunState.SUCCESS,
                    DagRun.start_date >= one_hour_ago)
                .group_by(DagRun.id, DagRun.dag_id, DagRun.state)
                .all()
            )
            return [item[0] for item in dag_runs_to_filter] if dag_runs_to_filter else []

        # Log the incoming webhook payload for debugging
        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        # Skip processing if the timesheet is not in approved status
        is_timesheet_approved = rail.IfOperator(
            task_id="is_timesheet_approved",
            test=lambda dag_run: dag_run.conf["webhook"]["data"].get("timesheetStatusUri") == "urn:replicon:timesheet-status:approved",
            yes_task="get_tenant_wide_log_artifact",
            no_task="stop_execution",
        )

        # Retrieve the tenant-wide log artifact name stored in Airflow; returns None if not yet created
        get_tenant_wide_log_artifact = rail.PythonOperator(
            task_id="get_tenant_wide_log_artifact",
            python_callable=lambda: Variable.get(config.tenant_wide_log_var, default_var=None)
        )

        # Determine whether the tenant-wide log artifact already exists
        if_tenant_wide_log_variable_exists_in_airflow = rail.IfOperator(
            task_id="if_tenant_wide_log_variable_exists_in_airflow",
            test=lambda: bool(rail.result("get_tenant_wide_log_artifact")),
            yes_task="search_for_timesheet_uri_from_the_log",
            no_task="create_tenant_wide_log",
        )

        # First-time run: create the tenant-wide log artifact
        create_tenant_wide_log = rail.CreateLogOperator(
            task_id="create_tenant_wide_log",
            tenant_wide_name=config.company_key,
            existing_log_mode="append",
        )

        # Persist the new artifact name in Airflow so future runs can locate the log
        set_tenant_wide_log_artifact_in_airflow = rail.PythonOperator(
            task_id="set_tenant_wide_log_artifact_in_airflow",
            python_callable=lambda: Variable.set(
                config.tenant_wide_log_var,
                rail.result('create_tenant_wide_log')
            )
        )

        # Filter the tenant-wide log for entries matching the incoming timesheet URI
        search_for_timesheet_uri_from_the_log = rail.FilterLogEntriesOperator(
            task_id='search_for_timesheet_uri_from_the_log',
            log=f"{{{{ var.value['{config.tenant_wide_log_var}'] }}}}",
            properties={
                "timesheet_uri": "{{ dag_run.conf.webhook.data.timesheet.uri }}"
            }
        )

        # Load all matching log entries into memory for the duplicate check
        load_all_records_from_filtered_log = rail.PythonOperator(
            task_id='load_all_records_from_filtered_log',
            python_callable=lambda: rail.load_all_records(
                rail.result("search_for_timesheet_uri_from_the_log")),
        )

        # Skip if the timesheet URI was already processed; proceed otherwise
        is_timesheet_uri_present_in_log = rail.IfOperator(
            task_id="is_timesheet_uri_present_in_log",
            test=lambda: len(rail.result("load_all_records_from_filtered_log")) > 0,
            yes_task="stop_execution_duplicate_timesheet",
            no_task="get_timesheet_details"
        )

        stop_execution_duplicate_timesheet = rail.EmptyOperator(
            task_id="stop_execution_duplicate_timesheet"
        )

        # Fetch full timesheet details from Replicon
        get_timesheet_details = rail.RepliconServiceOperator(
            task_id="get_timesheet_details",
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data=lambda dag_run: {
                "timesheetUri": dag_run.conf["webhook"]["data"]["timesheet"]["uri"]
            }
        )

        # Count completed child DAG runs in the last 60 minutes to mirror Workato throttle logic
        get_dag_run_completion_details = rail.PythonOperator(
            task_id="get_dag_run_completion_details",
            python_callable=get_dagruns_to_process
        )

        # Gate on completed run count in the last hour: hold if at or above 40
        if_dag_run_completion_greater_than_39 = rail.IfOperator(
            task_id="if_dag_run_completion_greater_than_39",
            test=lambda: len(rail.result("get_dag_run_completion_details")) > 39,
            yes_task="wait_for_an_hour",
            no_task="trigger_process_timesheet_data_child"
        )

        # Pause for an hour until the completed run count in the last hour drops below the cap
        wait_for_an_hour = TimeDeltaSensorAsync(
            task_id="wait_for_an_hour",
            delta=timedelta(hours=1),
        )

        # Notify the tenant that sync has been queued due to high volume
        send_timesheet_initiated_email = rail.EmailOperator(
            task_id="send_timesheet_initiated_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="WCS | Time sync to Quickbooks initiated - {{ current_time_in_specified_tz(tz=params.time_zone, fmt='%Y-%m-%dT%H:%M:%S%z') }}",
            html_content="templates/emails/timesheet_sync_initiated.html",
            params={"time_zone": config.time_zone},
        )

        # Terminal no-op for all early-exit paths
        stop_execution = rail.EmptyOperator(task_id="stop_execution")

        # Hand off timesheet data to the child DAG for Quickbooks processing
        # Not passing timesheet status since it is being used in the child in Workato
        trigger_process_timesheet_data_child = rail.TriggerDagRunOperator(
            task_id='trigger_process_timesheet_data_child',
            trigger_dag_id=config.process_timesheet_data_child_id,
            conf=custom_methods.build_child_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        # Always log the DAG run outcome to Sumo Logic regardless of path taken
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done",
            extra_info=lambda dag_run: {
                "timesheet_uri": dag_run.conf["webhook"]["data"]["timesheet"]["uri"],
                "timesheet_status_uri": dag_run.conf["webhook"]["data"]["timesheetStatusUri"],
            }
        )

        # Reject non-approved timesheets immediately
        is_timesheet_approved >> rail.Label("No") >> stop_execution >> log_to_sumo

        # Approved: check if a tenant-wide log exists in Airflow
        is_timesheet_approved >> rail.Label("Yes") >> get_tenant_wide_log_artifact >> if_tenant_wide_log_variable_exists_in_airflow

        # Log exists: search for duplicate timesheet URI
        if_tenant_wide_log_variable_exists_in_airflow >> rail.Label("Yes") >> search_for_timesheet_uri_from_the_log >> load_all_records_from_filtered_log >> is_timesheet_uri_present_in_log

        # Log does not exist: create it, persist it, then proceed to fetch timesheet
        if_tenant_wide_log_variable_exists_in_airflow >> rail.Label("No") >> create_tenant_wide_log >> set_tenant_wide_log_artifact_in_airflow >> get_timesheet_details

        # Duplicate found: stop
        is_timesheet_uri_present_in_log >> rail.Label("Yes") >> stop_execution_duplicate_timesheet >> log_to_sumo

        # No duplicate: fetch timesheet details
        is_timesheet_uri_present_in_log >> rail.Label("No") >> get_timesheet_details

        # Check completed dag run count in the last hour before triggering child
        get_timesheet_details >> get_dag_run_completion_details >> if_dag_run_completion_greater_than_39

        # Too many active runs: wait an hour, notify, then trigger
        if_dag_run_completion_greater_than_39 >> rail.Label("Yes") >> wait_for_an_hour >> send_timesheet_initiated_email >> trigger_process_timesheet_data_child >> log_to_sumo

        # Within limit: trigger immediately
        if_dag_run_completion_greater_than_39 >> rail.Label("No") >> trigger_process_timesheet_data_child >> log_to_sumo

    return dag

rail.for_each_instance(create_main_dag)