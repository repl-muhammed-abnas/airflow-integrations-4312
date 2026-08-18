from datetime import timedelta
import rail
from pwcglobal.time_export_v3 import python_callable_method, request_payload, response_filter
from pwcglobal.time_export_v3.task.current_timesheet_period import current_timesheet_period_task

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/time_extract_v3/config.py


# pylint:disable = too-many-statements
def create_child_location_dag(config):
    location_dags = []

    for location in config.location_codes:
        with rail.create_airflow_dag(
            dag_id=f'pwc_time_export_child_location_{location}_{config.instance}_v3',
            description=f'Time Export for {location} {config.instance}',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.custom_max_active_run_child_each_location[location]
                if location in config.location_configured_for_custom_max_active_runs else config.default_max_active_run_other_locations,
            max_active_tasks=config.dag_max_active_tasks
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            process_start_time = rail.PythonOperator(
                task_id='process_start_time',
                python_callable=request_payload.get_paris_timenow_in_fmt
            )

            current_timesheet_period_before_usersearch = current_timesheet_period_task(
                'before_usersearch')

            is_current_timesheet_period_not_exists = rail.IfOperator(
                task_id='is_current_timesheet_period_not_exists',
                test='{{ result("current_timesheet_period_replicon_before_usersearch").rows | length < 1 }}',
                yes_task='search_user_by_location',
                no_task='map_current_timesheet_period'
            )

            search_user_by_location = rail.RepliconServiceOperator(
                task_id='search_user_by_location',
                endpoint='/services/UserListService1.svc/GetData',
                data=request_payload.get_search_user_by_location_payload,
                data_handler=response_filter.map_user_by_location
            )

            get_user_with_timesheet_template = rail.RepliconServiceOperator(
                task_id='get_user_with_timesheet_template',
                endpoint='/services/ImportService1.svc/BulkGetUsers3',
                data=lambda: {
                    "users": [{
                        "uri": x['user_uri']
                    } for x in rail.result('search_user_by_location')],
                    "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
                },
                data_handler=response_filter.get_first_user_with_timesheet_template
            )

            is_timesheet_template_exists_for_user = rail.IfOperator(
                task_id='is_timesheet_template_exists_for_user',
                test="{{ result('get_user_with_timesheet_template') | is_truthy }}",
                yes_task='get_current_timesheet',
                no_task='map_current_timesheet_period'
            )

            get_current_timesheet = rail.RepliconServiceOperator(
                task_id='get_current_timesheet',
                endpoint='/services/TimesheetService1.svc/GetTimesheetForDate2',
                data=lambda: {
                    "userUri": rail.result('get_user_with_timesheet_template'),
                    "date": request_payload.get_today_date_in_paris_timezone(),
                    "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
                }
            )

            current_timesheet_period_after_usersearch = current_timesheet_period_task(
                'after_usersearch')

            map_current_timesheet_period = rail.PythonOperator(
                task_id='map_current_timesheet_period',
                python_callable=python_callable_method.map_current_timesheet_period
            )

            is_timesheet_period_exists = rail.IfOperator(
                task_id='is_timesheet_period_exists',
                test=lambda: len(rail.result(
                    'map_current_timesheet_period')) > 0,
                yes_task='map_twb_enddate_startdate',
                no_task='dagrun_log_to_sumo'
            )

            map_twb_enddate_startdate = rail.PythonOperator(
                task_id='map_twb_enddate_startdate',
                python_callable=python_callable_method.map_twb_enddate_startdate
            )

            create_timedata_batch = rail.RepliconServiceOperator(
                task_id='create_timedata_batch',
                endpoint='/services/TimeDataExportService1.svc/CreateTimeDataItemDataBatch',
                data=request_payload.get_create_timedata_item_batch
            )

            (process_timedata_batch, wait_for_timedata_batch) = rail.batch_execution(
                group_id='process_timedata_batch',
                creation_task_id=create_timedata_batch.task_id
            )

            get_timedata_batch = rail.RepliconServiceOperator(
                task_id='get_timedata_batch',
                endpoint='/services/TimeDataExportService1.svc/GetTimeDataItemDataBatchResults',
                data={
                    "timeDataItemDataBatchUri": "{{ result('create_timedata_batch') }}"
                },
                data_handler=response_filter.map_timedata_batch
            )

            is_batch_present = rail.IfOperator(
                task_id='is_batch_present',
                test=lambda: bool(rail.result('get_timedata_batch')['error'] or rail.result(
                    'get_timedata_batch')['user_list']),
                yes_task='check_batch_error',
                no_task='dagrun_log_to_sumo'
            )

            check_batch_error = rail.EmptyOperator(
                task_id='check_batch_error'
            )

            fail_dag_if_error = rail.IfOperator(
                task_id='fail_dag_if_error',
                test=lambda: bool(rail.result('get_timedata_batch')['error']),
                yes_task='fail_with_error_log',
                no_task='get_user_list_collection'
            )

            fail_with_error_log = rail.FailOperator(
                task_id='fail_with_error_log',
                message="{{ result('get_timedata_batch').error }}"
            )

            get_user_list_collection = rail.CreateCollectionOperator(
                task_id='get_user_list_collection',
                source="{{ result('get_timedata_batch').user_list | to_json }}"
            )

            is_user_present = rail.IfOperator(
                task_id='is_user_present',
                test="{{ result('get_user_list_collection', 'length') > 0 }}",
                yes_task='trigger_process_user_batch',
                no_task='dagrun_log_to_sumo'
            )

            trigger_process_user_batch = rail.TriggerDagRunForEachItemOperator(
                task_id='trigger_process_user_batch',
                retries=0,
                items=lambda: rail.result('get_user_list_collection'),
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                batch_size=config.post_batch_size,
                trigger_dag_id=f'pwc_time_export_user_batch_child_{location}_{config.instance}_v3',
                conf=request_payload.get_process_user_batch_conf
            )

            dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id='dagrun_log_to_sumo',
                sumo_conn_id=config.dagrun_log_sumo_conn_id,
                trigger_rule='all_done',
                extra_info={
                    'location': '{{ dag_run.conf.location }}',
                    'exportperiod': '{{ dag_run.conf.export_period }}',
                    'timedatapresent': "{{ 'Yes' if get_task_state('is_batch_present') == 'success' and \
                        result('is_batch_present') == 'check_batch_error' else 'No' }}",
                    'usercount': "{{ result('get_user_list_collection', 'length') if \
                        get_task_state('get_user_list_collection') == 'success' else '0' }}"
                }
            )

            process_start_time >> current_timesheet_period_before_usersearch >> \
                is_current_timesheet_period_not_exists

            is_current_timesheet_period_not_exists >> rail.Label(
                "Yes") >> search_user_by_location >> get_user_with_timesheet_template >> \
                is_timesheet_template_exists_for_user

            is_timesheet_template_exists_for_user >> rail.Label(
                "Yes") >> get_current_timesheet >> current_timesheet_period_after_usersearch >> map_current_timesheet_period

            is_timesheet_template_exists_for_user >> rail.Label(
                "No") >> map_current_timesheet_period

            is_current_timesheet_period_not_exists >> rail.Label(
                "No") >> map_current_timesheet_period

            map_current_timesheet_period >> is_timesheet_period_exists

            is_timesheet_period_exists >> rail.Label(
                "Yes") >> map_twb_enddate_startdate >> create_timedata_batch >> process_timedata_batch

            wait_for_timedata_batch >> get_timedata_batch >> is_batch_present

            is_timesheet_period_exists >> rail.Label(
                "No") >> dagrun_log_to_sumo

            is_batch_present >> rail.Label(
                "Yes") >> check_batch_error >> fail_dag_if_error

            is_batch_present >> rail.Label(
                "No") >> dagrun_log_to_sumo

            fail_dag_if_error >> rail.Label(
                "Yes") >> fail_with_error_log >> dagrun_log_to_sumo

            fail_dag_if_error >> rail.Label(
                "No") >> get_user_list_collection >> is_user_present

            is_user_present >> rail.Label(
                "Yes") >> trigger_process_user_batch >> dagrun_log_to_sumo

            is_user_present >> rail.Label(
                "No") >> dagrun_log_to_sumo

        location_dags.append(dag)

    return location_dags


rail.for_each_instance(create_child_location_dag)
