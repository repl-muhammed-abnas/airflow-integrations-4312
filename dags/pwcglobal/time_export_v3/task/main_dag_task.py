from datetime import datetime, timedelta, timezone
import rail
from pwcglobal.time_export_v3 import python_callable_method, request_payload


def main_dag_task_group(config, uat_key_value):

    location_dag_ids = list(map(
        lambda code: f'pwc_time_export_child_location_{code}_{config.instance}_v3', config.location_codes))

    with rail.TaskGroup(group_id='main_dag_task', prefix_group_id=False) as main_dag_task:

        get_last_child_location_dagrun = rail.PythonOperator(
            task_id="get_last_child_location_dagrun",
            python_callable=python_callable_method.get_last_child_location_dagrun,
            op_args=[location_dag_ids, 'running']
        )

        is_location_dag_runs_pending = rail.IfOperator(
            task_id="is_location_dag_runs_pending",
            test=lambda: bool(rail.result(
                'get_last_child_location_dagrun')),
            yes_task="should_send_long_running_email" if config.send_long_running_job_email else "finish",
            no_task=["get_all_scripts", "get_enabled_locations", "get_all_employee_type_groups",
                     "search_export_entries_from_time_extract_mapper"]
        )

        if config.send_long_running_job_email:

            should_send_long_running_email = rail.IfOperator(
                task_id="should_send_long_running_email",
                test=lambda: int((datetime.now(timezone.utc) - datetime.fromisoformat(rail.result(
                    'get_last_child_location_dagrun')['execution_date'])).total_seconds() / 3600) > 2,
                yes_task="send_long_running_jobs_email",
                no_task="finish"
            )

            send_long_running_jobs_email = rail.EmailOperator(
                task_id="send_long_running_jobs_email",
                to=config.alert_email,
                subject="{{ get_company_key() }} | Long job running for Time data export automation {{ current_time('%Y-%m-%dT%H:%M:%S') }}",
                html_content="email_long_running_email.html"
            )

        get_all_scripts = rail.RepliconServiceOperator(
            task_id='get_all_scripts',
            endpoint='/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts'
        )

        get_enabled_locations = rail.RepliconServiceOperator(
            task_id='get_enabled_locations',
            endpoint='/services/LocationService1.svc/GetEnabledLocations'
        )

        get_all_employee_type_groups = rail.RepliconServiceOperator(
            task_id='get_all_employee_type_groups',
            endpoint='/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups'
        )

        search_export_entries_from_time_extract_mapper = rail.PythonOperator(
            task_id='search_export_entries_from_time_extract_mapper',
            python_callable=python_callable_method.get_export_entries,
            op_args=[config.time_extract_mapper, uat_key_value]
        )

        get_export_period = rail.PythonOperator(
            task_id="get_export_period",
            python_callable=python_callable_method.get_export_period,
            op_args=[location_dag_ids, 'success', search_export_entries_from_time_extract_mapper.task_id, config.instance]
        )

        process_timeexport_location = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timeexport_location',
            retries=0,
            items="{{ result('search_export_entries_from_time_extract_mapper') | to_json }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            # Any new versions the dag_id also need to be updated in get_export_period
            trigger_dag_id=lambda item: f"pwc_time_export_child_location_{item['code'].lower()}_{config.instance}_v3",
            conf=request_payload.get_process_timeexport_location
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        get_last_child_location_dagrun >> is_location_dag_runs_pending

        if config.send_long_running_job_email:
            is_location_dag_runs_pending >> rail.Label(
                "Yes") >> should_send_long_running_email

            should_send_long_running_email >> rail.Label(
                "Yes") >> send_long_running_jobs_email >> finish

            should_send_long_running_email >> rail.Label(
                "No") >> finish
        else:
            is_location_dag_runs_pending >> rail.Label(
                "Yes") >> finish

        is_location_dag_runs_pending >> search_export_entries_from_time_extract_mapper >> get_export_period >> process_timeexport_location
        is_location_dag_runs_pending >> rail.Label(
            "No") >> [get_all_scripts, get_enabled_locations, get_all_employee_type_groups] >> \
            process_timeexport_location

    return main_dag_task
