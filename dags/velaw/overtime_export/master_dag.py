import rail
from pendulum import datetime
from velaw.overtime_export.utils import python_callable
from velaw.overtime_export.utils.response_filter import get_locationlistinput

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'velaw_overtime_export_velaw_overtimeexport_master_{config.instance}',
        description=f'Velaw_overtime_export {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 5, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.export_base_report_name,
        )

        run_my_report_entry, run_my_report_exit = rail.run_report(
            group_id='run_report_user_data',
            report_params={
                "reportParameters": [
                    {
                     "reportUri": "{{ result('get_report_details').uri }}",
                     "filterValues": [],
                     "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id="load_report_data",
            document="{{result('run_report_user_data.get_report_result').reportGenerationResults[0].payload }}",
        )

        if_payload_has_nodata = rail.IfOperator(
            task_id='if_payload_has_nodata',
            test='{{result("run_report_user_data.get_report_result", "has_data")}}',
            yes_task="if_payload_has_no_columns",
            no_task="send_no_report_data_mail"
        )

        send_no_report_data_mail = rail.EmailOperator(
            task_id='send_no_report_data_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} - Overtime Export completed - {{current_time_in_specified_tz()}}''',
            html_content='templates/emails/no_data_mail.html'
        )

        stop_job = rail.EmptyOperator(
            task_id='stop_job',

        )

        if_payload_has_no_columns = rail.IfOperator(
            task_id='if_payload_has_no_columns',
            # pylint: disable=line-too-long
            test="{{result('run_report_user_data.get_report_result').reportGenerationResults[0].payload |starts_with('Client Code,Matter Code,Employee ID,OVR,Country ISO Code,Entry Date,Hours Worked,N,User Name,Activity Code,Location,Timesheet End Date,Comments')| is_falsy}}",
            yes_task="stop_job_with_failure_message",
            no_task="create_overtime_hours_list",
        )

        stop_job_with_failure_message = rail.FailOperator(
            task_id='stop_job_with_failure_message',
            message='Report column order changed'
        )

        create_overtime_hours_list = rail.CreateCollectionOperator(
            task_id='create_overtime_hours_list',
            source="{{ result('load_report_data') }}",
            name="overtimehours",
            columns={
                'Client Code': 'ClientCode',
                'Matter Code': 'MatterCode',
                'Employee ID': 'EmployeeID',
                'OVR': 'OVR',
                'Country ISO Code': 'CountryISOCode',
                'Entry Date': 'EntryDate',
                'Hours Worked': 'HoursWorked',
                'N': 'N',
                'User Name': 'username',
                'Activity Code': 'ActivityCode',
                'Location': 'Location',
                'Timesheet End Date': 'timesheetenddate',
                'Comments': 'comments'
            }
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:location-list-column:name",
                    "urn:replicon:location-list-column:description"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler = get_locationlistinput
        )

        query_list_for_us = rail.QueryCollectionOperator(
            task_id='query_list_for_us',
            query="""SELECT * FROM  overtimehours WHERE  overtimehours.CountryISOCode = 'US' AND overtimehours.ClientCode <>'VIN999'""",
        )

        query_list_for_gb = rail.QueryCollectionOperator(
            task_id='query_list_for_gb',
            query="""SELECT * FROM  overtimehours WHERE  overtimehours.CountryISOCode='GB' AND  overtimehours.ClientCode
            <>'VIN999'""",
        )

        log_previousweeks_enddate = rail.PythonOperator(
            task_id='log_previousweeks_enddate',
            python_callable=python_callable.get_formatted_date
        )

        if_query_list_for_us_greater_than = rail.IfOperator(
            task_id='if_query_list_for_us_greater_than',
            test="{{ result('query_list_for_us') | length > 0 }}",
            yes_task="create_csv_for_us",
            no_task="if_query_list_for_gb_greater_than",
        )

        create_csv_for_us = rail.WriteCSVFileOperator(
            task_id='create_csv_for_us',
            source="{{ result('query_list_for_us') }}",
            delimiter=' ',
            header=None,
            row=lambda item: [
                item['ClientCode'].ljust(6, " ")[:6],

                ' '.ljust(2, " "),

                item['MatterCode'].ljust(5, " ")[:5],

                ' '.ljust(9, " "),

                item['EmployeeID'].rjust(6, "0")[:6],

                ' '.ljust(11, " "),

                item['OVR'],

                ' '.ljust(17, " "),

                python_callable.get_modified_list_data(item['EntryDate']),

                ' '.ljust(16, " "),

                python_callable.get_calculated_working_hours(
                    item['HoursWorked'], item['ActivityCode'], item['Location']),

                ' '.ljust(303, " "),

                python_callable.get_comments(
                    item['comments'], item['username'])

            ]
        )

        read_csv_artifact = rail.PythonOperator(
            task_id='read_csv_artifact',
            python_callable=python_callable.remove_quotes
        )

        write_csv_file = rail.PythonOperator(
            task_id='write_csv_file',
            python_callable=lambda: rail.write_artifact(
                rail.result('read_csv_artifact'))
        )

        upload_us_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_us_file_to_sftp',
            content="{{result('write_csv_file')}}",
            remote_filepath=config.log_filepath +
            "{{result('log_previousweeks_enddate')}}.txt"
        )

        if_query_list_for_gb_greater_than = rail.IfOperator(
            task_id='if_query_list_for_gb_greater_than',
            test="{{ result('query_list_for_gb') | length > 0 }}",
            yes_task="merge_csv_data",
            no_task="if_create_csv_lines_present"
        )

        merge_csv_data = rail.PythonOperator(
            task_id='merge_csv_data',
            python_callable=python_callable.do_merge_csv_data
        )

        create_csv_for_gb = rail.WriteCSVFileOperator(
            task_id='create_csv_for_gb',
            source="{{result('merge_csv_data') | to_json}}",
            delimiter=' ',
            header=None,
            row=lambda item: [
                item['ClientCode'].ljust(
                    6, " ") if item['CountryISOCode'] == 'GB' else item['ClientCode'].ljust(6, " ")[:6],

                ' '.ljust(2, " "),

                item['MatterCode'].ljust(
                    5, " ") if item['CountryISOCode'] == 'GB' else item['MatterCode'].ljust(5, " ")[:5],

                ' '.ljust(9, " "),

                item['EmployeeID'].rjust(
                    6, "0") if item['CountryISOCode'] == 'GB' else item['EmployeeID'].rjust(6, "0")[:6],

                ' '.ljust(11, " "),

                item['OVR'] if item['CountryISOCode'] == 'GB' else item['OVR'],

                ' '.ljust(17, " "),

                python_callable.get_modified_data(
                    item['EntryDate']) if item['CountryISOCode'] == 'GB' else python_callable.get_modified_list_data(item['EntryDate']),

                ' '.ljust(16, " "),

                python_callable.get_calculated_hours(
                    item['HoursWorked'], item['ActivityCode'], item['Location'])if item['CountryISOCode'] == 'GB'
                else python_callable.get_calculated_working_hours(item['HoursWorked'], item['ActivityCode'], item['Location']),

                ' '.ljust(303, " "),

                python_callable.get_comments_data(
                    item['comments'], item['username']) if item['CountryISOCode'] == 'GB' else python_callable.get_comments(item['comments'], item['username'])

            ]
        )

        read_gb_csv_artifact = rail.PythonOperator(
            task_id='read_gb_csv_artifact',
            python_callable=python_callable.remove_quote
        )

        write_gb_csv_file = rail.PythonOperator(
            task_id='write_gb_csv_file',
            python_callable=lambda: rail.write_artifact(
                rail.result('read_gb_csv_artifact'))
        )

        upload_gb_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_gb_file_to_sftp',
            content="{{result('write_gb_csv_file')}}",
            remote_filepath=config.log_filepath +
            "{{result('log_previousweeks_enddate')}}.txt"
        )

        if_create_csv_lines_present = rail.IfOperator(
            task_id='if_create_csv_lines_present',
            test="{{ result('create_csv_for_us') | is_truthy  or result('create_csv_for_gb') | is_truthy }}",
            yes_task="send_success_mail",
            no_task="send_no_data_export_mail",
        )

        send_success_mail = rail.EmailOperator(
            task_id='send_success_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} - Overtime Export completed - {{current_time_in_specified_tz()}}''',
            html_content='templates/emails/export_successful_mail.html'
        )

        send_no_data_export_mail = rail.EmailOperator(
            task_id='send_no_data_export_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} - Overtime Export completed - {{current_time_in_specified_tz()}}''',
            html_content='templates/emails/no_data_export_mail.html'
        )



        finish = rail.EmptyOperator(
            task_id='finish',
        )

        get_report_details >> run_my_report_entry
        run_my_report_exit >> load_report_data >> if_payload_has_nodata
        if_payload_has_nodata >> rail.Label(
            'Yes') >> send_no_report_data_mail >> stop_job >> if_payload_has_no_columns
        if_payload_has_nodata >> rail.Label(
            'No') >> if_payload_has_no_columns
        if_payload_has_no_columns >> rail.Label(
            'Yes') >> stop_job_with_failure_message >> create_overtime_hours_list
        if_payload_has_no_columns >> rail.Label(
            'No') >> create_overtime_hours_list >> get_all_locations
        get_all_locations >> query_list_for_us >> query_list_for_gb >> log_previousweeks_enddate
        log_previousweeks_enddate >> if_query_list_for_us_greater_than >> rail.Label(
            'Yes') >> create_csv_for_us >> read_csv_artifact >> write_csv_file >> upload_us_file_to_sftp
        upload_us_file_to_sftp >> if_query_list_for_gb_greater_than
        if_query_list_for_us_greater_than >> rail.Label(
            'No') >> if_query_list_for_gb_greater_than
        if_query_list_for_gb_greater_than >> rail.Label(
            'Yes') >> merge_csv_data >> create_csv_for_gb >> read_gb_csv_artifact >> write_gb_csv_file
        write_gb_csv_file >> upload_gb_file_to_sftp >> if_create_csv_lines_present
        if_query_list_for_gb_greater_than >> rail.Label(
            'No') >> if_create_csv_lines_present
        if_create_csv_lines_present >> rail.Label(
            'Yes') >> send_success_mail >> finish
        if_create_csv_lines_present >> rail.Label(
            'No') >> send_no_data_export_mail >> finish

        return dag


rail.for_each_instance(create_dag)
