from datetime import timedelta, datetime as dt
from pendulum import datetime
from dateutil.relativedelta import relativedelta
import pendulum
from capgemini.time_export_global_v8.utils import custom_methods
from capgemini.time_export_global_v8.utils import request_payload
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Capgemini Time Export Global Past and Current Period Master {config.instance} V8',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 6, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
            'retries': 0
        },
    ) as dag:

        today = pendulum.now(config.time_zone)

        process_start_time = rail.PythonOperator(
            task_id='process_start_time',
            python_callable=lambda: today.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        )

        get_specific_location_uri = rail.RepliconServiceOperator(
            task_id='get_specific_location_uri',
            endpoint="/services/LocationService1.svc/GetAllLocations",
            data_handler=lambda response: custom_methods.get_specific_location_uri(response, config.timesheet_period_base_user_location)
        )

        get_specific_location_users = rail.RepliconServiceOperator(
            task_id='get_specific_location_users',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_specific_location_users_payload,
            data_handler=lambda response: list(map(lambda user_data: user_data["cells"][0]["uri"], response["rows"]))
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_bulk_user_data,
            data_handler=custom_methods.users_data_with_timesheet_template
        )

        is_user_with_timesheet_template_present = rail.IfOperator(
            task_id='is_user_with_timesheet_template_present',
            test='{{ result("get_user_details") | is_truthy }}',
            yes_task='start_date_var'
        )

        start_date_var = rail.SetVariableOperator(
            task_id='start_date_var',
            name='start_date',
            value=(today - relativedelta(day=1)).strftime("%Y-%m-%d")
        )

        timesheet_periods_var = rail.SetVariableOperator(
            task_id='timesheet_periods_var',
            name='timesheet_periods',
            value=[]
        )

        end_of_month = (today + relativedelta(day=31) + timedelta(days=1)).date()

        for_each_start = rail.ForEachOperator(
            task_id='for_each_start',
            items=[1,2,3,4,5,6],
            start_task='get_start_date_var',
            end_task='for_each_end'
        )

        get_start_date_var = rail.GetVariableOperator(
            task_id='get_start_date_var',
            name='start_date',
        )

        is_ts_end_date_less_than_end_of_month = rail.IfOperator(
            task_id='is_ts_end_date_less_than_end_of_month',
            test=lambda: dt.strptime(rail.result("get_start_date_var")["value"], "%Y-%m-%d").date() < end_of_month,
            yes_task='get_timesheet_details',
            no_task='for_each_end'
        )

        get_timesheet_details = rail.RepliconServiceOperator(
            task_id='get_timesheet_details',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetailsForDate",
            data=request_payload.get_timesheet_details
        )

        update_start_date_var = rail.SetVariableOperator(
            task_id='update_start_date_var',
            name='start_date',
            value=custom_methods.get_updated_start_date
        )

        append_timesheet_periods_var = rail.SetVariableOperator(
            task_id='append_timesheet_periods_var',
            name='timesheet_periods',
            append=True,
            value=custom_methods.get_timesheet_period
        )

        for_each_end = rail.EmptyOperator(
            task_id='for_each_end'
        )

        get_timesheet_periods_var = rail.GetVariableOperator(
            task_id='get_timesheet_periods_var',
            name='timesheet_periods'
        )

        get_time_export_date_range_json = rail.PythonOperator(
            task_id='get_time_export_date_range_json',
            python_callable=custom_methods.get_past_and_current_time_export_date_range_json,
            op_args=[config.time_zone, config.export_file_prefix, today]
        )

        create_export_for_each_timesheet_period = rail.TriggerDagRunForEachItemOperator(
            task_id='create_export_for_each_timesheet_period',
            items='{{ result("get_time_export_date_range_json") | to_json }}',
            trigger_dag_id=config.time_export_child_dag_id,
            conf=lambda item: {
                "logging_details": item,
                "process_start_time": rail.result("process_start_time")
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        process_start_time >> get_specific_location_uri >> get_specific_location_users >> get_user_details >> is_user_with_timesheet_template_present
        is_user_with_timesheet_template_present >> rail.Label("Yes") >> start_date_var \
            >> timesheet_periods_var >> for_each_start >> get_start_date_var >> is_ts_end_date_less_than_end_of_month
        is_ts_end_date_less_than_end_of_month >> rail.Label("Yes") >> get_timesheet_details \
            >> update_start_date_var >> append_timesheet_periods_var >> for_each_end
        is_ts_end_date_less_than_end_of_month >> rail.Label("No") >> for_each_end

        for_each_start >> for_each_end >> get_timesheet_periods_var >> get_time_export_date_range_json \
            >> create_export_for_each_timesheet_period

    return dag

rail.for_each_instance(create_dag)
