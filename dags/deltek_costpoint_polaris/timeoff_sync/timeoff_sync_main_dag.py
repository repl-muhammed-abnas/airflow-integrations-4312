from datetime import timedelta, timezone
import pendulum
from datetime import datetime
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import rail
import itertools


null = None

# pylint: disable=too-many-statements


def create_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_timeoff_sync_main_{config.instance}',
        description=f'deltek_costpoint_timeoff_sync_main_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        default_args={
        },
        max_active_runs=1,
    ) as dag:

        def do_get_last_run_date():
            current_time = pendulum.now(
                config.cp_timezone) - timedelta(seconds=2)
            lookup_timestamp_value = Variable.get(
                config.last_run_date_var_name, default_var=None)
            last_run_date = datetime.strptime(
                lookup_timestamp_value, "%Y-%m-%d %H:%M:%S") if lookup_timestamp_value else current_time
            formated_last_run = last_run_date.strftime("%Y-%m-%d %H:%M:%S")
            rail.set_result(current_time.strftime(
                "%Y-%m-%d %H:%M:%S"), 'current_time')
            return formated_last_run

        get_last_run_date = rail.PythonOperator(
            task_id='get_last_run_date',
            python_callable=do_get_last_run_date
        )

        choose_data_source = rail.IfOperator(
            task_id='choose_data_source',
            test=lambda: config.isFromSql,
            yes_task='build_sql_query',
            no_task='get_cp_timeoff_bookings',
        )

        build_sql_query = rail.PythonOperator(
            task_id='build_sql_query',
            python_callable=lambda: config.sql_query.replace(
                '> ?', f"> '{rail.result('get_last_run_date')}'")
        )

        get_sql_timeoff_bookings_info = rail.MsSqlEncryptedOperator(
            task_id='get_sql_timeoff_bookings_info',
            mssql_conn_id=config.deltek_cospoint_sql_conn_id,
            sql="{{ result('build_sql_query') }}",
        )

        def get_timeoff_bookings_info():
            source_task = 'get_sql_timeoff_bookings_info' if config.isFromSql else 'get_cp_timeoff_bookings'
            costpoint_timeoffbookings = rail.result(source_task)
            timeoffbookings = []
            for timeoff_info in costpoint_timeoffbookings:
                if config.isFromSql:
                    # SQLExecuteQueryOperator returns rows as lists:
                    # [0]=USER_ID, [1]=TIME_OFF_TYPE, [2]=LEAVE_TYPE_CD,
                    # [3]=TIME_OFF_STARTDATE, [4]=TIME_OFF_ENDDATE, [5]=TIME_OFF_HOURS,
                    # [6]=LAST_MODIFIED, [7]=ROWVERSION, [8]=TIME_OFF_STATUS
                    def _fmt(dt):
                        return dt.strftime('%Y-%m-%d %H:%M:%S') if hasattr(dt, 'strftime') else str(dt)

                    def _fmt_hours(h):
                        return float(h) if h is not None else 0.0
                    timeoffbookings.append({
                        "emp_id": timeoff_info[0],
                        "timeoff_type": timeoff_info[1],
                        "timeoff_start_date": _fmt(timeoff_info[3]),
                        "timeoff_end_date": _fmt(timeoff_info[4]),
                        "timeoff_hours": _fmt_hours(timeoff_info[5]),
                        "timeoff_modification_status": timeoff_info[7],
                        "timeoff_status": timeoff_info[8]
                    })
                else:
                    timeoffbookings.append({
                        "emp_id": timeoff_info['USER_ID'],
                        "timeoff_type": timeoff_info['TIME_OFF_TYPE'],
                        "timeoff_start_date": timeoff_info['TIME_OFF_STARTDATE'],
                        "timeoff_end_date": timeoff_info['TIME_OFF_ENDDATE'],
                        "timeoff_hours": timeoff_info['TIME_OFF_HOURS'],
                        "timeoff_modification_status": timeoff_info['ROWVERSION'],
                        "timeoff_status": timeoff_info['TIME_OFF_STATUS']
                    })
            return timeoffbookings

        get_cp_timeoff_bookings = rail.DeltekCostPointODBCOperator(
            task_id='get_cp_timeoff_bookings',
            deltek_costpoint_odbc_conn_id=config.odbc_conn_id,
            query=config.odbc_query,
            query_params=["{{result('get_last_run_date')}}",
                          "{{result('get_last_run_date')}}", "{{result('get_last_run_date')}}"]
        )

        get_timeoff_bookings = rail.PythonOperator(
            task_id="get_timeoff_bookings",
            python_callable=get_timeoff_bookings_info,
        )

        update_last_run_date = rail.PythonOperator(
            task_id='update_last_run_date',
            python_callable=lambda: Variable.set(config.last_run_date_var_name,
                                                 rail.result('get_last_run_date', 'current_time'))
        )

        is_booking_timeoff_available = rail.IfOperator(
            task_id='is_booking_timeoff_available',
            test='''{{ result('get_timeoff_bookings') | length > 0 }}''',
            yes_task="get_specfic_time_off_types",
            no_task="delete_this_dagrun",
        )

        def get_specfic_time_off_type(response):
            data = response.json()['d']
            return list(filter(lambda x: x['displayText'] and x['displayText'].lower() == config.cp_timeoff_name.lower(), data))

        get_specfic_time_off_types = rail.RepliconServiceOperator(
            task_id='get_specfic_time_off_types',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            response_filter=get_specfic_time_off_type
        )

        is_timeoff_type_available = rail.IfOperator(
            task_id='is_timeoff_type_available',
            test='''{{ result('get_specfic_time_off_types') | length > 0 }}''',
            yes_task="get_user_data",
            no_task="send_configuration_error",
        )

        def get_timeoff_users():
            user_request = []
            planned_leaves_holidays = rail.result(
                'get_timeoff_bookings')
            for leaves_holidays in planned_leaves_holidays:
                if rail.find_first_by_attr_and_get_attr(user_request, 'employeeId', leaves_holidays['emp_id'], 'employeeId', None) is None:
                    user_request.append(
                        {
                            "uri": null,
                            "loginName": null,
                            "employeeId": leaves_holidays['emp_id'],
                            "parameterCorrelationId": null
                        }
                    )

            return {
                "users": user_request
            }

        def get_user_uris(response):
            user_uris = []
            for user_info in response:
                if user_info:
                    user_uris.append({
                        "user_uri": user_info['uri'],
                        "login_name": user_info['loginName']
                    })
            return user_uris

        get_user_data = rail.RepliconServiceOperator(
            task_id="get_user_data",
            endpoint="/services/UserService1.svc/BulkGetUsers2",
            data=get_timeoff_users,
            data_handler=lambda response: get_user_uris(response)
        )

        def get_timeoff_to_process():
            process_timeoff_bookings = []
            timeoff_bookings = rail.result('get_timeoff_bookings')
            replicon_users = rail.result('get_user_data')
            for to_info in timeoff_bookings:
                user_uri = rail.find_first_by_attr_and_get_attr(
                    replicon_users, 'login_name', to_info['emp_id'], 'user_uri')
                if user_uri:
                    process_timeoff_bookings.append({
                        "emp_id": to_info['emp_id'],
                        "timeoffhours": to_info['timeoff_hours'],
                        "useruri": user_uri,
                        "timeoffdate": to_info['timeoff_start_date'],
                        "existingtimeoff": True,
                        "timeoffuri": rail.result('get_specfic_time_off_types')[0]['uri'],
                        "timeoff_type": to_info['timeoff_type']
                    })

            grouped_user_timeoff = [{"user_timeoffs": list(to_bookings)} for user_id, to_bookings in itertools.groupby(
                process_timeoff_bookings, key=lambda to: to['emp_id'])]
            return grouped_user_timeoff

        process_each_timeoff = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_timeoff',
            retries=0,
            items=get_timeoff_to_process,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_costpoint_process_each_user_timeoff_records_child_{config.instance}',
            conf=lambda item: item
        )

        wait_for_completion_trigger_process_each_timeoff = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_process_each_timeoff',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_timeoff") }}'
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_timeoff") }}',
            dagrun_task_id='gather_child_logs',
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=lambda: list(list(itertools.chain(
                *list(map(rail.load_all_records, rail.result('gather_logs'))))))
        )

        get_logged_errors = rail.PythonOperator(
            task_id='get_logged_errors',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('format_logs'), 'properties.status', 'Error')
        )

        has_error_logs = rail.IfOperator(
            task_id='has_error_logs',
            test=lambda: bool(rail.result('get_logged_errors')),
            yes_task='create_csv_lines',
            no_task='process_timeoff_deletion'
        )

        create_csv_lines = rail.WriteCSVFileOperator(
            task_id='create_csv_lines',
            source="{{ result('format_logs') | to_json }}",
            header=['User',
                    'Time Off',
                    'Time Off Date',
                    'Action',
                    'Status',
                    'Details',
                    'Job ID'],
            row=[
                "{{ item.properties.user }}",
                "{{ item.properties.timeoff }}",
                "{{ item.properties.timeoffdate }}",
                "{{ item.properties.action }}",
                "{{ item.properties.status }}",
                "{{ item.properties.get('details','') }}",
                "{{ item.ecid }}",
            ]
        )

        log_filename = rail.PythonOperator(
            task_id='log_filename',
            python_callable=lambda:  rail.render_template(
                "Log_{{ dag_run_ecid() }}_timeoff_sync.csv")
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines')}}",
            output_file_name='{{ result("log_filename") }}',
            expires_in_seconds=7*24*60*60,
        )

        send_mail_error = rail.EmailOperator(
            task_id='send_mail_error',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Deltek Costpoint Timeoff sync Completed with Errors - {{ current_time() }}''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Deltek Costpoint Timeoff sync is completed with failures based on the file - '{{ result('log_filename') }}'. Please find the  link below to download the logs.
            <br /> <br /> <a href="{{ result('generate_download_link') }}">Download log file</a><br /> <br /><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
            <br />
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Replicon Inc.</p> ''',
            params=None,
        )

        send_configuration_error = rail.EmailOperator(
            task_id='send_configuration_error',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Deltek Costpoint Timeoff sync Completed with Errors - {{ current_time() }}''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Deltek Costpoint Timeoff sync is completed with error as timeoff is not present in polaris<br />
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Replicon Inc.</p> ''',
            params=None,
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        process_timeoff_deletion = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timeoff_deletion',
            retries=0,
            items=[-1],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'process_delete_timeoff_records_main_{config.instance}',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        get_last_run_date >> choose_data_source
        choose_data_source >> rail.Label(
            'Yes') >> build_sql_query >> get_sql_timeoff_bookings_info >> get_timeoff_bookings
        choose_data_source >> rail.Label(
            'No') >> get_cp_timeoff_bookings >> get_timeoff_bookings
        get_timeoff_bookings >> update_last_run_date >> is_booking_timeoff_available
        is_booking_timeoff_available >> rail.Label(
            'No') >> delete_this_dagrun >> finish
        is_booking_timeoff_available >> rail.Label(
            'Yes') >> get_specfic_time_off_types >> is_timeoff_type_available
        is_timeoff_type_available >> rail.Label(
            'No') >> send_configuration_error >> finish
        is_timeoff_type_available >> rail.Label('Yes') >> get_user_data >> process_each_timeoff >> \
            wait_for_completion_trigger_process_each_timeoff >> gather_logs >> format_logs >> \
            get_logged_errors >> has_error_logs
        has_error_logs >> rail.Label(
            'Yes') >> create_csv_lines >> log_filename >> generate_download_link >> \
            send_mail_error >> process_timeoff_deletion >> finish
        has_error_logs >> rail.Label(
            'no') >> process_timeoff_deletion >> finish

    return dag


rail.for_each_instance(create_dag)
