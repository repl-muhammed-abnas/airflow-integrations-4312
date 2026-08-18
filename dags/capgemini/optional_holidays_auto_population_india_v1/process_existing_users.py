from datetime import timedelta
from capgemini.optional_holidays_auto_population_india_v1.utils import response_filter
from capgemini.optional_holidays_auto_population_india_v1.utils import request_payload
from capgemini.optional_holidays_auto_population_india_v1.tasks.send_logs import get_send_logs
from airflow.models import Variable
import rail

null = None

# pylint:disable = too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_esisting_users_dagid,
        description=f'Capgemini Auto Population of Optional Holidays India Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_process_holidays_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
            'retries': 0
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_log',
            end_task='dagrun_log_to_sumo',
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        is_schedule_e1_or_e2 = rail.IfOperator(
            task_id='is_schedule_e1_or_e2',
            test='{{ dag_run.conf.properties.schedule == "E1" or dag_run.conf.properties.schedule == "E2" }}',
            yes_task='is_state_uri_present',
            no_task='dagrun_log_to_sumo'
        )

        is_state_uri_present = rail.IfOperator(
            task_id='is_state_uri_present',
            test='{{ dag_run.conf.properties.state_uri | is_truthy }}',
            yes_task='is_holiday_calendar_uri_present',
            no_task='dagrun_log_to_sumo'
        )

        is_holiday_calendar_uri_present = rail.IfOperator(
            task_id='is_holiday_calendar_uri_present',
            test='{{ dag_run.conf.properties.optional_holiday_calendar_uri | is_truthy }}',
            yes_task='get_locations_under_state',
            no_task='dagrun_log_to_sumo'
        )

        get_locations_under_state = rail.RepliconServiceOperator(
            task_id='get_locations_under_state',
            endpoint='/services/LocationListService1.svc/GetHierarchyData',
            data=lambda dag_run: request_payload.get_location_hierarchy_payload(
                dag_run.conf["properties"]["state_name"]),
            data_handler=response_filter.get_locations_list
        )

        get_optional_holiday_balance_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_optional_holiday_balance_report_details',
            report_name=config.optional_holiday_balance_report
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params=request_payload.optional_holiday_balance_report_payload
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task='fail_report_generation',
            no_task='report_has_data'
        )

        fail_report_generation = rail.FailOperator(
            task_id='fail_report_generation',
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{result('run_report.get_report_result','has_data')}}",
            yes_task='is_report_has_expected_columns',
            no_task='dagrun_log_to_sumo'
        )

        is_report_has_expected_columns = rail.IfOperator(
            task_id='is_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{result('run_report.get_report_result').reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_balance_report_columns,
            yes_task='load_csv',
            no_task='fail_no_expected_columns',
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id='fail_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id='load_csv',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}"
        )

        optional_holiday_balance_collection = rail.CreateCollectionOperator(
            task_id='optional_holiday_balance_collection',
            source='{{ result("load_csv") }}',
            columns={
                "User Name": "username",
                "Time Off Type": "timeoff_type",
                "Time Off Balance": "timeoff_balance",
                "useruri": "user_uri",
                "Employee ID": "employee_id"
            },
            name='optional_holiday_balance_data'
        )

        # pylint: disable=comparison-of-constants
        query_balance = rail.QueryCollectionOperator(
            task_id='query_balance',
            query="SELECT * FROM optional_holiday_balance_data WHERE CAST(timeoff_balance AS DECIMAL) > CAST(:check_num AS INTEGER)",
            query_params={
                "check_num": "{{ 1 if dag_run.conf.properties.schedule == 'E1' else 0 }}"
            }
        )

        is_balance_record_exists = rail.IfOperator(
            task_id='is_balance_record_exists',
            test='{{ result("query_balance", "length") > 0 }}',
            yes_task='get_bookable_holidays_in_date_range',
            no_task='send_no_required_balance_records_email'
        )

        send_no_required_balance_records_email = rail.EmailOperator(
            task_id='send_no_required_balance_records_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | The Auto population of Optional holiday booking is skipped for state \
                {{ dag_run.conf.properties.state_name }} - {{ dag_run.conf.properties.process_start_time }}',
            html_content='/templates/emails/no_required_balances.html'
        )

        get_bookable_holidays_in_date_range = rail.RepliconServiceOperator(
            task_id='get_bookable_holidays_in_date_range',
            endpoint='/services/HolidayCalendarService2.svc/GetHolidaysInDateRange',
            data=lambda dag_run: request_payload.holiday_bookings_in_daterange_payload(
                dag_run, config),
            data_handler=response_filter.get_holidays_list
        )

        check_for_bookable_holidays = rail.IfOperator(
            task_id='check_for_bookable_holidays',
            test=lambda: len(rail.result(
                "get_bookable_holidays_in_date_range")) > 0,
            yes_task='check_for_multiple_bookable_holidays',
            no_task='send_no_bookable_holidays_email'
        )

        check_for_multiple_bookable_holidays = rail.IfOperator(
            task_id='check_for_multiple_bookable_holidays',
            test=lambda: len(rail.result(
                "get_bookable_holidays_in_date_range")) > 1,
            yes_task='send_multiple_bookable_holidays_email',
            no_task='book_optional_holiday'
        )

        book_optional_holiday = rail.TriggerDagRunForEachItemOperator(
            task_id='book_optional_holiday',
            items='{{ result("query_balance") }}',
            batch_size=config.trigger_booking_childs_batch_size,
            trigger_dag_id=config.trigger_booking_batch_childs_dagid,
            conf=lambda item, dag_run: {
                "items": item,
                "master_index": dag_run.conf['master_index'],
                "properties": dag_run.conf["properties"],
                "optional_holiday_booking_date_json": request_payload.get_optional_holiday_booking_date(),
                "optional_holiday_booking_date": rail.result("get_bookable_holidays_in_date_range")[0]["holiday_date"],
                "log_artifact": rail.result('create_log')
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_booking = rail.WaitForDagRunsSensor(
            task_id="wait_for_booking",
            dag_runs="{{result('book_optional_holiday')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        send_no_bookable_holidays_email = rail.EmailOperator(
            task_id='send_no_bookable_holidays_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | The Auto population of Optional holiday booking is skipped for state \
                {{ dag_run.conf.properties.state_name }} - {{ dag_run.conf.properties.process_start_time }}',
            html_content='/templates/emails/no_bookable_dates.html',
            params={
                "value": "no"
            }
        )

        send_multiple_bookable_holidays_email = rail.EmailOperator(
            task_id='send_multiple_bookable_holidays_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | The Auto population of Optional holiday booking is skipped for state \
                {{ dag_run.conf.properties.state_name }} - {{ dag_run.conf.properties.process_start_time }}',
            html_content='/templates/emails/no_bookable_dates.html',
            params={
                "value": "multiple"
            }
        )

        process_logs = rail.EmptyOperator(
            task_id='process_logs'
        )

        send_logs_enter, send_logs_exit = get_send_logs(config)

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label("No") >> create_log

        create_log >> is_schedule_e1_or_e2 >> rail.Label(
            "Yes") >> is_state_uri_present
        is_state_uri_present >> rail.Label(
            "Yes") >> is_holiday_calendar_uri_present
        is_state_uri_present >> rail.Label("No") >> dagrun_log_to_sumo
        is_schedule_e1_or_e2 >> rail.Label("No") >> dagrun_log_to_sumo

        is_holiday_calendar_uri_present >> rail.Label("Yes") >> get_locations_under_state \
            >> get_optional_holiday_balance_report_details >> run_report_group_entry
        is_holiday_calendar_uri_present >> rail.Label(
            "No") >> dagrun_log_to_sumo

        run_report_group_exit >> is_report_failed

        is_report_failed >> rail.Label(
            "Yes") >> fail_report_generation >> dagrun_log_to_sumo
        is_report_failed >> rail.Label("No") >> report_has_data

        report_has_data >> rail.Label("No") >> dagrun_log_to_sumo
        report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns

        is_report_has_expected_columns >> rail.Label(
            "No") >> fail_no_expected_columns >> dagrun_log_to_sumo
        is_report_has_expected_columns >> rail.Label("Yes") >> load_csv >> optional_holiday_balance_collection \
            >> query_balance >> is_balance_record_exists

        is_balance_record_exists >> rail.Label(
            "Yes") >> get_bookable_holidays_in_date_range >> check_for_bookable_holidays
        is_balance_record_exists >> rail.Label(
            "No") >> send_no_required_balance_records_email >> dagrun_log_to_sumo

        check_for_bookable_holidays >> rail.Label(
            "Yes") >> check_for_multiple_bookable_holidays
        check_for_multiple_bookable_holidays >> rail.Label(
            "No") >> book_optional_holiday >> wait_for_booking >> process_logs >> send_logs_enter
        check_for_multiple_bookable_holidays >> rail.Label(
            "Yes") >> send_multiple_bookable_holidays_email >> dagrun_log_to_sumo
        send_logs_exit >> dagrun_log_to_sumo
        check_for_bookable_holidays >> rail.Label(
            "No") >> send_no_bookable_holidays_email >> dagrun_log_to_sumo

        dagrun_log_to_sumo >> can_fail_dag >> rail.Label("Yes") >> fail_dagrun

    return dag


rail.for_each_instance(create_child_dag)
