
from datetime import timedelta, datetime, date
from pendulum import datetime as dt
import pytz
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'pwcfr_time_off_recal_master_{config.instance}',
        description=f'Pwcfr_time_off_recal_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=dt(2023, 5, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs_master
    ) as dag:

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data=None
        )

        def get_current_date():
            return datetime.now(pytz.timezone("Europe/Paris")).strftime("%Y-%m-%eT%H:%M%S.%f")

        log_current_date = rail.PythonOperator(
            task_id='log_current_date',
            python_callable=get_current_date
        )

        def firstworkingday():
            date_time = datetime.now(pytz.timezone("Europe/Paris"))
            work_day = (date_time.replace(day=1) + timedelta(days=1)).strftime("%Y-%m-%d") if date_time.replace(day=1).strftime("%A") in ("Sunday") else ((date_time.replace(
                day=1) + timedelta(days=2)).strftime("%Y-%m-%d") if date_time.replace(day=1).strftime("%A") in ("Saturday") else (date_time.replace(day=1) + timedelta(days=0)).strftime("%Y-%m-%d"))
            return work_day

        def get_end_date():
            enddate = (datetime.now(pytz.timezone("Europe/Paris"))).replace(day=1)
            result = enddate.replace(month = (datetime.now(pytz.timezone("Europe/Paris"))).month + 1 ) if ((datetime.now(pytz.timezone("Europe/Paris")).month) < 12) else (enddate.replace(month = 1).replace(year=(datetime.now(pytz.timezone("Europe/Paris"))).year + 1))
            return str((result - timedelta(days=1)).strftime("%m/%d/%Y"))

        invoke_custom_ruby_code = rail.PythonOperator(
            task_id='invoke_custom_ruby_code',
            python_callable=lambda: {
                "firstworkingday": firstworkingday(),
                "beginningofcurrentmonth": datetime.now(pytz.timezone("Europe/Paris")).replace(day=1).strftime("%Y-%m-%dT%H:%M%S.%f"),
                "beginningofcurrentmonthday": int(datetime.now(pytz.timezone("Europe/Paris")).replace(day=1).strftime("%d")),
                "beginningofcurrentmonthmonth": int(datetime.now(pytz.timezone("Europe/Paris")).replace(day=1).strftime("%m")),
                "beginningofcurrentmonthyear": int(datetime.now(pytz.timezone("Europe/Paris")).replace(day=1).strftime("%Y")),
                "holidaycalendaruri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_holiday_calendars'), 'displayText', "France", ('uri'), ""),
                "beginningofcurrentmonthweekday": datetime.now(pytz.timezone("Europe/Paris")).replace(day=1).strftime("%A"),
                "upperlimit": get_end_date(),
                "todayday": int(datetime.now(pytz.timezone("Europe/Paris")).strftime("%d")),
                "todaymonth": int(datetime.now(pytz.timezone("Europe/Paris")).strftime("%m")),
                "todayyear": int(datetime.now(pytz.timezone("Europe/Paris")).strftime("%Y")),
                "jobdate": datetime.now(pytz.timezone("Europe/Paris")).strftime("%Y-%m-%d")
            }
        )

        if_holidaycalendaruri_blank = rail.IfOperator(
            task_id='if_holidaycalendaruri_blank',
            test="{{ result('invoke_custom_ruby_code').holidaycalendaruri | is_falsy }}",
            yes_task="stop_job_with_error",
            no_task="declare_variable",
        )

        stop_job_with_error = rail.FailOperator(
            task_id='stop_job_with_error',
            message="France - Holiday Calendar not found"
        )

        declare_variable = rail.SetVariableOperator(
            task_id='declare_variable',
            append=False,
            name='lower_limit_date',
            value=(datetime.now(pytz.timezone("Europe/Paris")) - timedelta(days=datetime.now(
                pytz.timezone("Europe/Paris")).day)).replace(day=1).strftime("%m/%d/%Y")
        )

        get_holidays_in_date_range = rail.RepliconServiceOperator(
            task_id='get_holidays_in_date_range',
            endpoint="/services/HolidayCalendarService2.svc/GetHolidaysInDateRange",
            data=lambda: {
                "holidayCalendarUri": rail.result('invoke_custom_ruby_code')['holidaycalendaruri'],
                "dateRange": {
                    "startDate": {
                        "year": rail.result('invoke_custom_ruby_code')['beginningofcurrentmonthyear'],
                        "month": rail.result('invoke_custom_ruby_code')['beginningofcurrentmonthmonth'],
                        "day": rail.result('invoke_custom_ruby_code')['beginningofcurrentmonthday']
                    },
                    "endDate": {
                        "year": rail.result('invoke_custom_ruby_code')['todayyear'],
                        "month": rail.result('invoke_custom_ruby_code')['todaymonth'],
                        "day": rail.result('invoke_custom_ruby_code')['todayday']
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        declare_list = rail.SetVariableOperator(
            task_id='declare_list',
            append=False,
            name='holiday_list',
            value=[]
        )

        if_get_holidays_in_date_range_greater_than = rail.IfOperator(
            task_id='if_get_holidays_in_date_range_greater_than',
            test=lambda: rail.result('get_holidays_in_date_range'),
            yes_task="insert_to_list",
            no_task="get_dag_run_variable_data",
        )

        insert_to_list = rail.SetVariableOperator(
            task_id='insert_to_list',
            append=True,
            name='{{ result("declare_list").name }}',
            value=lambda: {"finallist": [{"date": str(item['date']['day']) + '/' + str(item['date']['month']) + '/' + str(
                item['date']['year']), "name": item['name']}for item in rail.result('get_holidays_in_date_range')]}
        )

        get_dag_run_variable_data = rail.GetVariableOperator(
            task_id='get_dag_run_variable_data',
            name='{{result("declare_list").name}}'
        )

        invoke_custom_ruby_code_for_holidaylist = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_for_holidaylist',
            python_callable=lambda: {
                "input": rail.result('get_dag_run_variable_data')['value'][0]['finallist'] if rail.result('get_dag_run_variable_data')['value'] else []
            }
        )

        if_last_date_not_equal_to_today = rail.IfOperator(
            task_id='if_last_date_not_equal_to_today',
            test=lambda: (rail.result('invoke_custom_ruby_code_for_holidaylist')['input'][-1]['date'] if (rail.result('invoke_custom_ruby_code_for_holidaylist')['input'] and rail.result('invoke_custom_ruby_code_for_holidaylist')[
                'input'][0]['date']) else date.today()) != rail.result('invoke_custom_ruby_code')['firstworkingday'],
            yes_task="log_previous_date",
            no_task="log_today_date",
        )

        log_previous_date = rail.PythonOperator(
            task_id='log_previous_date',
            python_callable=lambda: ((datetime.strptime(rail.result('invoke_custom_ruby_code')[
                                     'jobdate'], '%Y-%m-%d')) - timedelta(days=1)).strftime('%Y-%m-%d')
        )

        check_previous_date_present = rail.PythonOperator(
            task_id='check_previous_date_present',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'invoke_custom_ruby_code_for_holidaylist')['input'], 'date', rail.result('log_previous_date'), 'name', null) if rail.result('invoke_custom_ruby_code_for_holidaylist')[
                'input'] else null
        )

        log_today_date = rail.PythonOperator(
            task_id='log_today_date',
            python_callable=lambda: rail.smartjoin_by_delim(rail.find_first_by_attr_and_get_attr(rail.result('invoke_custom_ruby_code_for_holidaylist')[
                'input'], 'date', rail.result('invoke_custom_ruby_code')['jobdate'], 'name', null), "") if rail.result('invoke_custom_ruby_code_for_holidaylist')[
                'input'] else null
        )

        if_today_date_not_present = rail.IfOperator(
            task_id='if_today_date_not_present',
            test="{{ result('log_today_date') | is_falsy }}",
            yes_task="update_variable_for_lower_limit_date",
            no_task="get_report_details",
        )

        update_variable_for_lower_limit_date = rail.SetVariableOperator(
            task_id='update_variable_for_lower_limit_date',
            append=False,
            name='lower limit date ',
            value=[(datetime.now(pytz.timezone("Europe/Paris")) -
                    timedelta(days=60)).replace(day=1).strftime("%m/%d/%Y")]
        )

        if_previous_date_present = rail.IfOperator(
            task_id='if_previous_date_present',
            test="{{ result('check_previous_date_present') | is_truthy }}",
            yes_task='check_for_today_date',
            no_task='get_report_details'
        )

        check_for_today_date = rail.PythonOperator(
            task_id='check_for_today_date',
            python_callable=lambda: rail.smartjoin_by_delim(rail.find_first_by_attr_and_get_attr(rail.result('invoke_custom_ruby_code_for_holidaylist')[
                'input'], 'date', rail.result('invoke_custom_ruby_code')['jobdate'], 'name', null), "")
        )

        if_previous_date_not_present = rail.IfOperator(
            task_id='if_check_previous_date_not_present',
            test="{{ result('check_previous_date_present') | is_falsy }}",
            yes_task="update_variable_for_lower_limit",
            no_task="get_report_details",
        )

        update_variable_for_lower_limit = rail.SetVariableOperator(
            task_id='update_variable_for_lower_limit',
            append=False,
            name='lower limit date ',
            value=[(datetime.now(pytz.timezone("Europe/Paris")) -
                    timedelta(days=60)).replace(day=1).strftime("%m/%d/%Y")]
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        log_approval_status_filter = rail.PythonOperator(
            task_id='log_approval_status_filter',
            python_callable=lambda: rail.smartjoin_by_delim(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'ApprovalStatusFilter', 'uri', null), "")
        )

        log_daterange_filter = rail.PythonOperator(
            task_id='log_daterange_filter',
            python_callable=lambda: rail.smartjoin_by_delim(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'DateRangeFilter', 'uri', null), "")
        )

        generate_report_group = rail.run_report2(
            group_id='generate_report_group',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details')['uri'],
                        "filterValues": [
                            {
                                "reportFilterUri": rail.result('log_approval_status_filter'),
                                "value": "2"
                            },
                            {
                                "reportFilterUri": rail.result('log_daterange_filter'),
                                "value": null
                            },
                            {
                                "reportFilterUri": rail.result('log_daterange_filter'),
                                "value": rail.result('declare_variable')['value']
                            },
                            {
                                "reportFilterUri": rail.result('log_daterange_filter'),
                                "value": rail.result('invoke_custom_ruby_code')['upperlimit']
                            }
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
        )

        if_payload_has_data = rail.IfOperator(
            task_id='if_payload_has_data',
            test='{{result("generate_report_group.get_report_result", "has_data") | is_truthy}}',
            yes_task="create_user_list",
            no_task="stop_job"
        )

        stop_job = rail.EmptyOperator(
            task_id='stop_job'
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('generate_report_group.get_report_result').reportGenerationResults[0].payload }}",
            headers=['User Name', 'Absence(s) approved in Workday Type', 'Booking Start Date', 'Booking End Date', 'Approval Status', 'Absence(s) approved in Workday Days',
                     'Absence(s) approved in Workday Comments', 'Scheduled Hrs', 'Absence(s) approved in Workday Hrs', 'requiredtimeoffhrs', 'test', 'useruri', 'timeoffuri', 'bookinguri', 'Time-off Tracking'],
        )

        create_user_list = rail.CreateCollectionOperator(
            task_id='create_user_list',
            source='{{ result("parse_csv")}}',
            name="userlist",
            columns={
                "User Name": "username",
                "Absence(s) approved in Workday Type": "timeofftype",
                "Booking Start Date": "bookingstartdate",
                "Booking End Date": "bookingenddate",
                "Approval Status": "approvalstatus",
                "Absence(s) approved in Workday Days": "workdays",
                "Absence(s) approved in Workday Comments": "timeoffcmts",
                "Scheduled Hrs": "schedulehrs",
                "Absence(s) approved in Workday Hrs": "timeoffhrs",
                "requiredtimeoffhrs": "requiredtimeoffhrs",
                "test": "test",
                "useruri": "useruri",
                "timeoffuri": "timeoffuri",
                "bookinguri": "bookinguri",
                "Time-off Tracking": "customfieldtext"
            }
        )

        query_list_for_timeoff_bookings = rail.QueryCollectionOperator(
            task_id='query_list_for_timeoff_bookings',
            query="""SELECT * FROM userlist WHERE userlist.test="Yes" AND (userlist.timeoffcmts="D" OR userlist.timeoffcmts="H") AND userlist.schedulehrs > 0""",
        )

        query_list_for_timeoff_bookings_schedule_hours = rail.QueryCollectionOperator(
            task_id='query_list_for_timeoff_bookings_schedule_hours',
            query="""SELECT * FROM userlist WHERE userlist.test="Yes" AND (userlist.timeoffcmts="D" OR userlist.timeoffcmts="H") AND userlist.schedulehrs == 0""",
        )

        pwc_timeoff_recal_lookuptable = rail.CreateLogOperator(
            task_id='pwc_timeoff_recal_lookuptable'
        )

        force_timeoff_batch_lookuptable = rail.CreateLogOperator(
            task_id='force_timeoff_batch_lookuptable'
        )

        if_query_list_for_timeoff_bookings_schedule_hours_has_data = rail.IfOperator(
            task_id='if_query_list_for_timeoff_bookings_schedule_hours_has_data',
            test="{{result('query_list_for_timeoff_bookings_schedule_hours', 'length') > 0 }}",
            yes_task='add_entry_for_skipped_logs',
            no_task='if_query_list_for_timeoff_bookings_has_data'
        )

        add_entry_for_skipped_logs = rail.WriteLogOperator(
            task_id='add_entry_for_skipped_logs',
            log="{{result('pwc_timeoff_recal_lookuptable')}}",
            items="{{result('query_list_for_timeoff_bookings_schedule_hours')}}",
            message="na",
            severity="Ignored",
            properties=lambda item: {
                "username": item['username'],
                "timeofftype": item['timeofftype'],
                "start_date": item['bookingstartdate'],
                "booking_hours": item['timeoffhrs'],
                "schedule_hours": item['schedulehrs'],
                "jobid": "{{dag_run_ecid()}}",
                "status": "Skipped",
                "reason": "Scheduled hours is 0",
            }
        )

        if_query_list_for_timeoff_bookings_has_data = rail.IfOperator(
            task_id='if_query_list_for_timeoff_bookings_has_data',
            test="{{result('query_list_for_timeoff_bookings', 'length') > 0}}",
            yes_task='process_timeoff_recal_child',
            no_task='search_entries_in_lookup_table'
        )

        process_timeoff_recal_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timeoff_recal_child',
            retries=0,
            items='{{ result("query_list_for_timeoff_bookings") }}',
            trigger_dag_id=f'pwcfr_timeoff_recal_no_batch_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "timeoff_batch_items": item,
                "lookup_table": rail.result('force_timeoff_batch_lookuptable'),
                "pwc_lookuptable": rail.result('pwc_timeoff_recal_lookuptable'),
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "parentjobid": rail.render_template("{{dag_run_ecid()}}")
            }
        )

        wait_for_process_timeoff_recal_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timeoff_recal_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_timeoff_recal_child") }}'
        )

        process_force_approve_timeoff_batch_child = rail.TriggerDagRunOperator(
            task_id='process_force_approve_timeoff_batch_child',
            retries=0,
            trigger_dag_id=f'pwcfr_time_off_recal_force_approve_time_off_batch_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "parent_jobid": rail.render_template("{{dag_run_ecid()}}"),
                "lookup_table": rail.result('force_timeoff_batch_lookuptable'),
            }
        )

        wait_for_process_force_approve_timeoff_batch_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_force_approve_timeoff_batch_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_force_approve_timeoff_batch_child") }}'
        )

        search_entries_in_lookup_table = rail.FilterLogEntriesOperator(
            task_id='search_entries_in_lookup_table',
            log="{{result('pwc_timeoff_recal_lookuptable')}}",
            properties={
                'jobid': "{{ dag_run_ecid() }}",
            }
        )

        if_entry_present = rail.IfOperator(
            task_id='if_entry_present',
            test="{{result('search_entries_in_lookup_table', 'length') > 0}}",
            yes_task='compose_csv',
            no_task='process_timesheet_reapprove_master'
        )

        compose_csv = rail.WriteCSVFileOperator(
            task_id='compose_csv',
            source=lambda: rail.result(
                'search_entries_in_lookup_table'),
            header=['User Name',
                    'Time Off type',
                    'Start Date',
                    'booking Hours',
                    'scheduled hours',
                    'Tracking ID',
                    'Job ID',
                    'Status',
                    'Reason'],
            delimiter=",",
            row=lambda item: [
                item['properties']['user_name'],
                item['properties']['time_off_type'],
                item['properties']['start_date'],
                item['properties']['booking_hours'],
                item['properties']['schedule_hours'],
                item['properties']['tracking_id'],
                item['properties']['jobid'],
                item['properties']['status'],
                item['properties']['reason'],

            ]
        )

        upload_reference_s3_file = rail.S3UploadFileOperator(
            task_id='upload_reference_s3_file',
            aws_conn_id=config.aws_conn_id,
            source="{{ result('compose_csv') }}",
            bucket_name=lambda: Variable.get(config.bucket_name),
            key_name=lambda: config.log_file_path + "_" +
            rail.result("log_current_date") + '.csv',
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_csv')}}",
            output_file_name='timeoffrecal_logs_ecid.csv',
            expires_in_seconds=7*24*60*60,
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            log="{{result('pwc_timeoff_recal_lookuptable')}}",
            severity='Error'
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.internal_logs_email,
            bcc="{%- if result('get_logged_errors', 'length') > 0  -%}\
                    "+config.alert_email+"\
                {%- else -%}\
                    "+config.internal_logs_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() }} | Time Off recalculation job - {{" "}} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    Completed with error   \
                {%- else -%} \
                    Completed Successfully  \
                {%- endif -%} \
                    {{result("log_current_date")}}',
            html_content="/templates/emails/update_completion_mail.html",
        )

        def get_end_date1():
            enddate = (datetime.now(pytz.timezone("Europe/Paris"))).replace(day=1)
            result = enddate.replace(month = (datetime.now(pytz.timezone("Europe/Paris"))).month + 1 ) if ((datetime.now(pytz.timezone("Europe/Paris")).month) < 12) else (enddate.replace(month = 1).replace(year=(datetime.now(pytz.timezone("Europe/Paris"))).year + 1))
            return str((result - timedelta(days=1)).strftime("%m/%d/%Y"))

        process_timesheet_reapprove_master = rail.TriggerDagRunOperator(
            task_id='process_timesheet_reapprove_master',
            retries=0,
            trigger_dag_id=f'pwcfr_timesheet_reapprove_master_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda:{
                "start": rail.result('declare_variable')['value'],
                "end": get_end_date1(),
            }
        )

        wait_for_process_timesheet_reapprove_master = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timesheet_reapprove_master',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_timesheet_reapprove_master") }}'
        )

        get_all_holiday_calendars >> log_current_date >> invoke_custom_ruby_code >> if_holidaycalendaruri_blank
        if_holidaycalendaruri_blank >> rail.Label(
            'Yes') >> stop_job_with_error >> declare_variable
        if_holidaycalendaruri_blank >> rail.Label(
            'No') >> declare_variable >> get_holidays_in_date_range >> declare_list
        declare_list >> if_get_holidays_in_date_range_greater_than >> rail.Label(
            'Yes') >> insert_to_list >> get_dag_run_variable_data
        if_get_holidays_in_date_range_greater_than >> rail.Label(
            'No') >> get_dag_run_variable_data >> invoke_custom_ruby_code_for_holidaylist
        invoke_custom_ruby_code_for_holidaylist >> if_last_date_not_equal_to_today >> rail.Label(
            'Yes') >> log_previous_date >> check_previous_date_present
        check_previous_date_present >> if_previous_date_present >> rail.Label(
            'Yes') >> check_for_today_date >> if_previous_date_not_present >> rail.Label(
            'Yes') >> update_variable_for_lower_limit >> get_report_details
        if_previous_date_not_present >> rail.Label(
            'No') >> get_report_details
        if_previous_date_present >> rail.Label(
            'No') >> get_report_details
        if_last_date_not_equal_to_today >> rail.Label(
            'No') >> log_today_date >> if_today_date_not_present >> rail.Label(
            'Yes') >> update_variable_for_lower_limit_date >> get_report_details
        if_today_date_not_present >> rail.Label(
            'No') >> get_report_details >> log_approval_status_filter >> log_daterange_filter
        log_daterange_filter >> generate_report_group >> parse_csv
        parse_csv >> if_payload_has_data
        if_payload_has_data >> rail.Label(
            'Yes') >> create_user_list >> query_list_for_timeoff_bookings
        query_list_for_timeoff_bookings >> query_list_for_timeoff_bookings_schedule_hours
        query_list_for_timeoff_bookings_schedule_hours >> pwc_timeoff_recal_lookuptable
        pwc_timeoff_recal_lookuptable >> force_timeoff_batch_lookuptable
        force_timeoff_batch_lookuptable >> if_query_list_for_timeoff_bookings_schedule_hours_has_data >> rail.Label(
            'Yes') >> add_entry_for_skipped_logs
        if_query_list_for_timeoff_bookings_schedule_hours_has_data >> rail.Label(
            'No') >> if_query_list_for_timeoff_bookings_has_data
        add_entry_for_skipped_logs >> if_query_list_for_timeoff_bookings_has_data >> rail.Label(
            'Yes') >> process_timeoff_recal_child >> wait_for_process_timeoff_recal_child
        wait_for_process_timeoff_recal_child >> process_force_approve_timeoff_batch_child
        process_force_approve_timeoff_batch_child >> wait_for_process_force_approve_timeoff_batch_child
        wait_for_process_force_approve_timeoff_batch_child >> search_entries_in_lookup_table
        if_query_list_for_timeoff_bookings_has_data >> rail.Label(
            'No') >> search_entries_in_lookup_table >> if_entry_present >> rail.Label(
            'Yes') >> compose_csv >> upload_reference_s3_file >> generate_download_link
        generate_download_link >> get_logged_errors >> send_import_complete_email >> process_timesheet_reapprove_master
        if_entry_present >> rail.Label(
            'No') >> process_timesheet_reapprove_master >> wait_for_process_timesheet_reapprove_master >> stop_job
        if_payload_has_data >> rail.Label(
            'No') >> stop_job

        return dag


rail.for_each_instance(create_dag)
