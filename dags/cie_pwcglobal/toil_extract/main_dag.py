# pylint: disable=line-too-long too-many-statements
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pendulum
import pytz
import rail
from cie_pwcglobal.toil_extract.utils import request_payload, data_formatting


def create_dag(config):
    dag_id_prefix = f'{config.team_id}_'
    dag_id_postfix = f'_{config.dag_id_post_fix}'

    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_{config.customisation_name}{dag_id_postfix}'.lower(),
        description=f'{dag_id_prefix}{config.company_key}_{config.customisation_name}{dag_id_postfix} - {config.version}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(
            2022, 10, 10,  tz=config.schedule_timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
        },
    ) as dag:

        # start = rail.EmptyOperator(task_id='start')

        create_log = rail.CreateLogOperator(
            task_id='create_log',
        )

        start = rail.PythonOperator(
            task_id='start',
            python_callable=data_formatting.get_process_times,
            op_args=[config]
        )

        curr_date = datetime.now(pytz.timezone(config.time_zone))

        sd = (curr_date - relativedelta(months=config.prev_period_in_months, day = 1)).strftime(config.report_filter_date_format)
        ed = curr_date.strftime(config.report_filter_date_format)

        write_log_start_run = rail.WriteLogOperator(
            task_id='write_log_start_run',
            log='{{ result("create_log") }}',
            severity='INFO',
            message=f"The { config.company_key } Toil Extract Run is started for country - { config.location } with the filters: start date { sd } to end date { ed } for toil time off types { config.toil_to_types }",
            properties={
                'location': config.location,
                'st_date': sd,
                'ed_date': ed,
                'toil_to_types': config.toil_to_types,
                },
        )

        get_enabled_locations = rail.RepliconServiceOperator(
            task_id="get_enabled_locations",
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'displayText', config.location, 'uri', None)
        )

        is_location_present = rail.IfOperator(
            task_id="is_location_present",
            test="{{ result('get_enabled_locations') | is_truthy }}",
            yes_task="get_location_code",
            no_task="write_log_location_not_found"
        )

        get_location_code = rail.RepliconServiceOperator(
            task_id="get_location_code",
            endpoint="/services/LocationService1.svc/GetLocationDetails",
            data={
                "locationUri": "{{ result('get_enabled_locations') }}"
            },
            response_filter=lambda response: response.json()['d']['code'] or ""
        )

        write_log_location_found = rail.WriteLogOperator(
            task_id='write_log_location_found',
            log='{{ result("create_log") }}',
            severity='INFO',
            message=f'Country - { config.location } details are found in { config.company_key }.',
        )

        write_log_location_not_found = rail.WriteLogOperator(
            task_id='write_log_location_not_found',
            log='{{ result("create_log") }}',
            severity='Warning',
            message=f'The country with name { config.location } is not found in { config.company_key }',
        )

        get_toil_totypes = rail.RepliconServiceOperator(
            task_id="get_toil_totypes",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            response_filter=lambda response: data_formatting.get_filter_timeoff_values(response, config)
        )

        is_to_types_present = rail.IfOperator(
            task_id="is_to_types_present",
            test="{{ result('get_toil_totypes') | length > 0 }}",
            yes_task="write_log_to_types_found",
            no_task="write_log_to_types_not_found"
        )

        write_log_to_types_found = rail.WriteLogOperator(
            task_id='write_log_to_types_found',
            log='{{ result("create_log") }}',
            severity='INFO',
            message=lambda : f'The Toil time off types are found in the { config.company_key } are: ' + str(data_formatting.get_toil_to_types()),
        )

        write_log_to_types_not_found = rail.WriteLogOperator(
            task_id='write_log_to_types_not_found',
            log='{{ result("create_log") }}',
            severity='Warning',
            message=f'The Toil Extract Run is skipped as Toil Time off types are missing in the {config.company_key}. Please verify the configured Toil Types: { config.toil_to_types }',
        )

        get_ts_day_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_ts_day_report_details',
            report_name=config.report_config["timesheet_day_template_report_name"],
        )

        run_ts_day_report_details = rail.run_report2(
            group_id='run_ts_day_report_details',
            report_params=lambda : request_payload.get_ts_day_params(sd, ed),
            replicon_conn_id=config.replicon_conn_id,
            target='artifact',
        )

        is_ts_day_report_failed = rail.IfOperator(
            task_id="is_ts_day_report_failed",
            test='{{ (result("run_ts_day_report_details.get_report_result") | load_json_artifact ).reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_ts_day_report_generation",
            no_task="ts_day_report_has_data"
        )

        fail_ts_day_report_generation = rail.FailOperator(
            task_id="fail_ts_day_report_generation",
            message="{{ (result('run_ts_day_report_details.get_report_result') | load_json_artifact).reportGenerationResults[0].error}}"
        )

        ts_day_report_has_data = rail.IfOperator(
            task_id="ts_day_report_has_data",
            test="{{ result('run_ts_day_report_details.get_report_result', 'has_data') }}",
            yes_task='is_ts_day_report_has_expected_columns',
            no_task='write_log_ts_report_no_data',
        )

        write_log_ts_report_no_data = rail.WriteLogOperator(
            task_id='write_log_ts_report_no_data',
            log='{{ result("create_log") }}',
            severity='Warning',
            message=f'The { config.report_config["timesheet_day_template_report_name"] } report has no data for { config.location }. Therefore the run is skipped.',
        )

        ts_day_report_columns = 'UserUri,TimesheetUri,Date'

        is_ts_day_report_has_expected_columns = rail.IfOperator(
            task_id="is_ts_day_report_has_expected_columns",
            test="{{ (result('run_ts_day_report_details.get_report_result') | load_json_artifact ).reportGenerationResults[0].payload | starts_with('%s') }}" % ts_day_report_columns,
            yes_task='write_log_ts_day_has_data', #'get_to_transaction_report_details',
            no_task='fail_ts_day_report_columns',
        )

        fail_ts_day_report_columns = rail.FailOperator(
            task_id="fail_ts_day_report_columns",
            message=f"{ config.report_config['timesheet_day_template_report_name'] } report doesnt have the expected columns({ts_day_report_columns})"
        )

        write_log_ts_day_has_data = rail.WriteLogOperator(
            task_id='write_log_ts_day_has_data',
            log='{{ result("create_log") }}',
            severity='INFO',
            message=f'Fetching of { config.report_config["timesheet_day_template_report_name"] } report data for { config.location } has completed. Started fetching data for { config.report_config["timeoff_transaction_report_name"] }.',
        )

        get_to_transaction_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_to_transaction_report_details',
            report_name=config.report_config["timeoff_transaction_report_name"],
        )

        run_to_transaction_report_details = rail.run_report2(
            group_id='run_to_transaction_report_details',
            report_params=lambda : request_payload.get_to_params(sd, ed),
            replicon_conn_id=config.replicon_conn_id,
            target='artifact',
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{ (result("run_to_transaction_report_details.get_report_result") | load_json_artifact ).reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="to_report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{ (result('run_to_transaction_report_details.get_report_result') | load_json_artifact).reportGenerationResults[0].error}}"
        )

        to_report_has_data = rail.IfOperator(
            task_id="to_report_has_data",
            test="{{ result('run_to_transaction_report_details.get_report_result', 'has_data') }}",
            yes_task='get_to_transaction_report_has_expected_columns',
            no_task='write_log_to_has_data', #'get_user_report_details',
        )

        to_transaction_report_columns = 'Time Off Type,Date,Event Type,Amount,UserUri'

        get_to_transaction_report_has_expected_columns = rail.IfOperator(
            task_id="get_to_transaction_report_has_expected_columns",
            test="{{ (result('run_to_transaction_report_details.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | starts_with('%s') }}" % to_transaction_report_columns,
            no_task='fail_invalid_to_transaction_report_columns',
            yes_task='write_log_to_has_data', #'get_user_report_details',
        )

        fail_invalid_to_transaction_report_columns = rail.FailOperator(
            task_id="fail_invalid_to_transaction_report_columns",
            message=f"{ config.report_config['timeoff_transaction_report_name'] } report doesnt have the expected columns({ to_transaction_report_columns })"
        )

        write_log_to_has_data = rail.WriteLogOperator(
            task_id='write_log_to_has_data',
            log='{{ result("create_log") }}',
            severity='INFO',
            message=f'Fetching of { config.report_config["timeoff_transaction_report_name"] } report data for { config.location } has completed. Started fetching data for { config.report_config["user_template_report_name"] }.',
        )

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name=config.report_config["user_template_report_name"],
        )

        # pylint: disable=unnecessary-lambda
        run_user_report_details = rail.run_report2(
            group_id='run_user_report_details',
            report_params=lambda : request_payload.get_user_params(),
            replicon_conn_id=config.replicon_conn_id,
            target='artifact',
        )

        is_user_report_failed = rail.IfOperator(
            task_id="is_user_report_failed",
            test='{{ (result("run_user_report_details.get_report_result") | load_json_artifact).reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_user_report_generation",
            no_task="user_report_has_data"
        )

        fail_user_report_generation = rail.FailOperator(
            task_id="fail_user_report_generation",
            message="{{ (result('run_user_report_details.get_report_result') | load_json_artifact).reportGenerationResults[0].error}}"
        )

        user_report_has_data = rail.IfOperator(
            task_id="user_report_has_data",
            test="{{ result('run_user_report_details.get_report_result', 'has_data') }}",
            yes_task='is_user_report_has_expected_columns',
            no_task='write_log_user_report_no_data',
        )

        write_log_user_report_no_data = rail.WriteLogOperator(
            task_id='write_log_user_report_no_data',
            log='{{ result("create_log") }}',
            severity='Warning',
            message=f'The { config.report_config["user_template_report_name"] } report has no data for { config.location }. Therefore the run is skipped.',
        )

        user_report_columns = 'User Name,Pay Rule Name,Pay Rule Effective Date,Employee ID,Workday ID,UserUri,Legal Entity Code,Legal Entity Effective Date,User Start Date'

        is_user_report_has_expected_columns = rail.IfOperator(
            task_id="is_user_report_has_expected_columns",
            test="{{ (result('run_user_report_details.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | starts_with('%s') }}" % user_report_columns,
            yes_task='write_log_user_has_data', #'get_final_toil_data_in_instance',
            no_task='fail_user_report_columns',
        )

        fail_user_report_columns = rail.FailOperator(
            task_id="fail_user_report_columns",
            message=f"{ config.report_config['user_template_report_name'] } report doesnt have the expected columns({ user_report_columns })"
        )

        write_log_user_has_data = rail.WriteLogOperator(
            task_id='write_log_user_has_data',
            log='{{ result("create_log") }}',
            severity='INFO',
            message=f'Fetching of { config.report_config["user_template_report_name"] } report data for { config.location } has completed.',
        )

        get_final_toil_data_in_instance = rail.PythonOperator(
            task_id='get_final_toil_data_in_instance',
            python_callable=data_formatting.format_final_toil_data_in_instance,
            op_args=[config],
        )

        write_log_final_toil_has_data = rail.WriteLogOperator(
            task_id='write_log_final_toil_has_data',
            log='{{ result("create_log") }}',
            severity='INFO',
            message='\
                {%- if result("get_final_toil_data_in_instance") | length > 0 -%} \
                    All the Toil data records availbale in the instance has been fetched and formatted. \
                {%- endif -%}\
                {%- if result("get_final_toil_data_in_instance") | length <= 0 -%} \
                    No Toil Data found in the instance. \
                {%- endif -%}',
        )

        has_final_toil_data_in_instance = rail.IfOperator(
            task_id="has_final_toil_data_in_instance",
            test="{{ result('get_final_toil_data_in_instance') | length > 0 }}",
            yes_task='final_toil_data_in_instance_to_csv',
            no_task='no_toil_data_email_logs_to_csv',
        )

        final_toil_data_in_instance_to_csv = rail.WriteCSVFileOperator(
            task_id="final_toil_data_in_instance_to_csv",
            source="{{ result('get_final_toil_data_in_instance') | to_json }}",
            header=["Party_ID", "EMP_ID", "Legal_Entity", "Date", "Time_Off_Type", "Pay_Rule_Name", "Amount", "Units", "Timesheet_URI", "md5"],
            row=[
                '{{item["Party_ID"]}}',
                '{{item["EMP_ID"]}}',
                '{{item["Legal_Entity"]}}',
                '{{item["Date"]}}',
                '{{item["Time_Off_Type"]}}',
                '{{item["Pay_Rule_Name"]}}',
                '{{item["Amount"]}}',
                '{{item["Units"]}}',
                '{{item["Timesheet_URI"]}}',
                '{{item["md5"]}}'
                ]
        )

        final_instance_toil_data_collection = rail.CreateCollectionOperator(
            task_id="final_instance_toil_data_collection",
            source="{{result('final_toil_data_in_instance_to_csv')}}",
            name="final_instance_toil_data_collection"
        )

        download_reference_file = rail.S3DownloadFileOperator(
            task_id='download_reference_file',
            bucket_name=config.s3_bucket_name,
            key_name=config.s3_reference_filepath + config.reference_file_name + config.file_extension,
            aws_conn_id=config.aws_conn_id
        )

        write_log_download_ref = rail.WriteLogOperator(
            task_id='write_log_download_ref',
            log='{{ result("create_log") }}',
            severity='INFO',
            message='Reference file has been downloaded from S3',
        )

        parse_reference_file = rail.LoadCSVFileOperator(
            task_id="parse_reference_file",
            document="{{result('download_reference_file')}}",
            headers=["Party_ID", "EMP_ID", "Legal_Entity", "Date", "Time_Off_Type", "Pay_Rule_Name", "Amount", "Units", "Timesheet_URI", "md5"]
        )

        reference_file_data_collection = rail.CreateCollectionOperator(
            task_id="reference_file_data_collection",
            source="{{result('parse_reference_file')}}",
            name="reference_file_data_collection"
        )

        query_final_toil_extract = rail.QueryCollectionOperator(
            task_id = "query_final_toil_extract",
            query="""SELECT * FROM final_instance_toil_data_collection where md5 not in (SELECT DISTINCT md5 from reference_file_data_collection)""",
            name= "final_toil_extract"
        )

        write_log_final_extract = rail.WriteLogOperator(
            task_id='write_log_final_extract',
            log='{{ result("create_log") }}',
            severity='INFO',
            message='Toil Extract data file is generated for new and modified data',
        )

        final_toil_extract_to_csv = rail.WriteCSVFileOperator(
            task_id="final_toil_extract_to_csv",
            source="{{ result('query_final_toil_extract') }}",
            header=["Party_ID", "EMP_ID", "Legal_Entity", "Date", "Time_Off_Type", "Pay_Rule_Name", "Amount", "Units", "Timesheet_URI"],
            row=[
                '{{item["Party_ID"]}}',
                '{{item["EMP_ID"]}}',
                '{{item["Legal_Entity"]}}',
                '{{item["Date"]}}',
                '{{item["Time_Off_Type"]}}',
                '{{item["Pay_Rule_Name"]}}',
                '{{item["Amount"]}}',
                '{{item["Units"]}}',
                '{{item["Timesheet_URI"]}}'
                ]
        )

        get_file_name = rail.PythonOperator(
            task_id="get_file_name",
            python_callable=data_formatting.get_extract_file_name,
            op_args=[config],
        )

        upload_toil_extract_file = rail.S3UploadFileOperator(
            task_id='upload_toil_extract_file',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name=config.s3_output_filepath +"{{ result('get_file_name') }}",
            source="{{ result('final_toil_extract_to_csv')}}"
        )

        upload_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_extract_to_sftp",
            content="{{result('final_toil_extract_to_csv')}}",
            remote_filepath=config.sftp_output_filepath+"{{ result('get_file_name') }}",
            sftp_conn_id=config.sftp_conn_id
        )

        write_log_upload_extract = rail.WriteLogOperator(
            task_id='write_log_upload_extract',
            log='{{ result("create_log") }}',
            severity='INFO',
            message=lambda : f'Toil Extract data file is uploaded to SFTP. Output file path: { config.sftp_output_filepath }, extract file name: {config.sftp_output_filepath }{data_formatting.get_extract_file_name(config)}',
        )

        archive_old_reference_file = rail.S3MoveFileOperator(
            task_id='archive_old_reference_file',
            source_bucket_name=config.s3_bucket_name,
            existing_key_name=config.s3_reference_filepath + config.reference_file_name + config.file_extension,
            new_key_name=config.s3_reference_archive_filepath + config.reference_file_name + "_{{ result('start')['pst'] }}_{{ result('get_location_code') }}" + config.file_extension,
            aws_conn_id=config.aws_conn_id,
        )

        upload_new_reference_file = rail.S3UploadFileOperator(
            task_id='upload_new_reference_file',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name=config.s3_reference_filepath + config.reference_file_name + config.file_extension,
            source="{{ result('final_toil_data_in_instance_to_csv') }}"
        )

        write_log_upload_ref = rail.WriteLogOperator(
            task_id='write_log_upload_ref',
            log='{{ result("create_log") }}',
            severity='INFO',
            message=f'Reference file is uploaded to S3. File path: { config.s3_reference_filepath + config.reference_file_name + config.file_extension }, archive old ref file: {config.s3_reference_archive_filepath}{config.reference_file_name}' + "_{{ result('start')['pst'] }}_{{ result('get_location_code') }}" + config.file_extension,
        )
        
        write_process_completion_log = rail.WriteLogOperator(
            task_id='write_process_completion_log',
            log='{{ result("create_log") }}',
            severity='INFO',
            message="Process is completed successfully",
        )
        
        process_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="process_logs_to_csv",
            source="{{ result('create_log') }}",
            # header=None,
            # row=[
            #     '{{item}}'
            # ]
        )

        process_complete_email = rail.EmailOperator(
            task_id='process_complete_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} - ' + config.location + ' | '+ config.customisation_name + " Run is Successfully Completed - {{ result('start')['pst'] }}",
            html_content='templates/process_complete_email.html',
            params={
                'location': config.location,
                'st_date': sd,
                'ed_date': ed,
                'toil_to_types': config.toil_to_types,
                'sftp_output_file_path': config.sftp_output_filepath,
            },
            files=[
                ("Toil_Extract_log_{{ result('start')['pst'] }}_{{ result('get_location_code') }}.csv", "{{ result('process_logs_to_csv') }}")]
        )

        country_doesnt_exists_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="country_doesnt_exists_logs_to_csv",
            source="{{ result('create_log') }}",
        )

        country_doesnt_exists = rail.EmailOperator(
            task_id='country_doesnt_exists',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} - ' + config.location + ' | '+ config.customisation_name + ' | Country-' + config.location + " Not found - {{ result('start')['pst'] }}",
            html_content='templates/country_doesnt_exists.html',
            params={
                'location': config.location,
            },
            files=[
                ("Toil_Extract_log_{{ result('start')['pst'] }}.csv", "{{ result('country_doesnt_exists_logs_to_csv') }}")]
        )

        to_types_doesnt_exists_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="to_types_doesnt_exists_logs_to_csv",
            source="{{ result('create_log') }}",
            # header=None,
            # row=[
            #     '{{item}}'
            # ]
        )

        to_types_doesnt_exists = rail.EmailOperator(
            task_id='to_types_doesnt_exists',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} - ' + config.location + ' | '+ config.customisation_name + " | Toil Time off types Not found - {{ result('start')['pst'] }}",
            html_content='templates/to_types_doesnt_exists.html',
            params={
                'location': config.location,
                'toil_to_types': config.toil_to_types,
            },
            files=[
                ("Toil_Extract_log_{{ result('start')['pst'] }}_{{ result('get_location_code') }}.csv", "{{ result('to_types_doesnt_exists_logs_to_csv') }}")]
        )

        no_ts_data_email_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="no_ts_data_email_logs_to_csv",
            source="{{ result('create_log') }}",
            # header=None,
            # row=[
            #     '{{item}}'
            # ]
        )

        no_ts_data_email = rail.EmailOperator(
            task_id='no_ts_data_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} - ' + config.location + ' | '+ config.customisation_name + " | No Timesheets data found from Timesheet Base Report - {{ result('start')['pst'] }}",
            html_content='templates/no_ts_data_email.html',
            params={
                'location': config.location,
                'report_name': config.report_config["timesheet_day_template_report_name"],
                'st_date': sd,
                'ed_date': ed,
            },
            files=[
                ("Toil_Extract_log_{{ result('start')['pst'] }}_{{ result('get_location_code') }}.csv", "{{ result('no_ts_data_email_logs_to_csv') }}")]
        )

        no_user_data_email_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="no_user_data_email_logs_to_csv",
            source="{{ result('create_log') }}",
            # header=None,
            # row=[
            #     '{{item}}'
            # ]
        )

        no_user_data_email = rail.EmailOperator(
            task_id='no_user_data_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} - ' + config.location + ' | '+ config.customisation_name + " | No user data found from User Base Report - {{ result('start')['pst'] }}",
            html_content='templates/no_user_data_email.html',
            params={
                'location': config.location,
                'report_name': config.report_config["user_template_report_name"],
                'st_date': sd,
                'ed_date': ed,
            },
            files=[
                ("Toil_Extract_log_{{ result('start')['pst'] }}_{{ result('get_location_code') }}.csv", "{{ result('no_user_data_email_logs_to_csv') }}")]
        )

        no_toil_data_email_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="no_toil_data_email_logs_to_csv",
            source="{{ result('create_log') }}",
            # header=None,
            # row=[
            #     '{{item}}'
            # ]
        )

        no_toil_data_email = rail.EmailOperator(
            task_id='no_toil_data_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} - ' + config.location + ' | '+ config.customisation_name + " - No Toil Data Found - {{ result('start')['pst'] }}",
            html_content='templates/no_toil_data_email.html',
            params={
                'location': config.location,
                'st_date': sd,
                'ed_date': ed,
                'toil_to_types': config.toil_to_types,
            },
            files=[
                ("Toil_Extract_log_{{ result('start')['pst'] }}_{{ result('get_location_code') }}.csv", "{{ result('no_toil_data_email_logs_to_csv') }}")]
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )
        process_completion_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="process_completion_logs_to_csv",
            source="{{ result('create_log') }}",
            header=["timestamp", "event"],
            row=[
                '{{item.timestamp}}',
                '{{item.message}}'
            ]
        )
        
        upload_completion_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_completion_logs_to_sftp",
            content="{{result('process_completion_logs_to_csv')}}",
            remote_filepath=config.sftp_output_filepath+"Logs_"+config.toil_extract_file_name.format("{{result('start').pst}}") + config.file_extension,
            sftp_conn_id=config.sftp_conn_id
        )
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_log") }}',
            trigger_rule='one_failed',
            severity='Error',
            message='Error Occured: {{ get_error_message() }}',
        )

        write_fail_logs = rail.WriteLogOperator(
            task_id='write_fail_logs',
            log='{{ result("create_log") }}',
            severity='High',
            message="Process failed to complete successfully",
        )
        process_write_fail_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="process_write_fail_logs_to_csv",
            source="{{ result('create_log') }}",
            header=["timestamp", "event"],
            row=[
                '{{item.timestamp}}',
                '{{item.message}}'
            ]
        )
        upload_fail_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_logs_to_sftp",
            content="{{result('process_write_fail_logs_to_csv')}}",
            remote_filepath=config.sftp_output_filepath+"Logs_"+config.toil_extract_file_name.format("{{result('start').pst}}") + config.file_extension,
            sftp_conn_id=config.sftp_conn_id
        )

        fail_email = rail.EmailOperator(
            task_id='fail_email',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} - TOIL Extract has Failed {%- if result('start') and 'pst' in result('start') -%}{{ result('start')['pst'] }}{%- endif -%}",
            html_content='templates/fail_email.html',
            params={
                'location': config.location,
                'st_date': sd,
                'ed_date': ed,
                'toil_to_types': config.toil_to_types,
            },
            files=[
                ("Toil_Extract_log_{%- if result('start') and 'pst' in result('start') -%}{{ result('start')['pst'] }}{%- endif -%}{%- if result('get_location_code') -%} {{ result('get_location_code') }}{%- endif -%}_.csv", "{{ result('write_fail_logs') }}")]
        )

        def final_status(config, msg, **kwargs):
            # if config.send_fail_msg_to_hangouts:
            #     data_formatting.send_msg(config, msg)
            for task_instance in kwargs['dag_run'].get_task_instances():
                if task_instance.current_state() == "failed" and task_instance.task_id != kwargs['task_instance'].task_id:
                    raise Exception(f"Task {task_instance.task_id} failed. Failing this DAG run")

        final_status = rail.PythonOperator(
            task_id='final_status',
            python_callable=final_status,
            op_args=[config, "Airflow Dag: " + dag_id_prefix+config.company_key+'_'+config.customisation_name+dag_id_postfix + " has failed with the Error - {{ get_error_message() }}" ]
        )

        create_log >> start >> write_log_start_run >> get_enabled_locations >> is_location_present

        is_location_present >> rail.Label("Yes") >> get_location_code >> write_log_location_found >> get_toil_totypes >> is_to_types_present
        is_location_present >> rail.Label("No") >> write_log_location_not_found >> country_doesnt_exists_logs_to_csv >> country_doesnt_exists >> finish

        is_to_types_present >> rail.Label("No") >> write_log_to_types_not_found >> to_types_doesnt_exists_logs_to_csv >> to_types_doesnt_exists >> finish
        is_to_types_present >> rail.Label("Yes") >> write_log_to_types_found >> get_ts_day_report_details >> run_ts_day_report_details >> is_ts_day_report_failed

        is_ts_day_report_failed >> rail.Label("Yes") >> fail_ts_day_report_generation >> finish
        is_ts_day_report_failed >> rail.Label("No") >> ts_day_report_has_data

        ts_day_report_has_data >> rail.Label("No") >> write_log_ts_report_no_data >> no_ts_data_email_logs_to_csv >> no_ts_data_email >> finish
        ts_day_report_has_data >> rail.Label("Yes") >> is_ts_day_report_has_expected_columns

        is_ts_day_report_has_expected_columns >> rail.Label("No") >> fail_ts_day_report_columns >> finish
        is_ts_day_report_has_expected_columns >> rail.Label("Yes") >> write_log_ts_day_has_data >> get_to_transaction_report_details >> run_to_transaction_report_details >> is_report_failed

        is_report_failed >> rail.Label("Yes") >> fail_report_generation >> finish
        is_report_failed >> rail.Label("No") >> to_report_has_data

        to_report_has_data >> rail.Label("No") >> write_log_to_has_data >> get_user_report_details
        to_report_has_data >> rail.Label("Yes") >> get_to_transaction_report_has_expected_columns

        get_to_transaction_report_has_expected_columns >> rail.Label("No") >> fail_invalid_to_transaction_report_columns >> finish
        get_to_transaction_report_has_expected_columns >> rail.Label("Yes") >> write_log_to_has_data >> get_user_report_details >> run_user_report_details >> is_user_report_failed

        is_user_report_failed >> rail.Label("Yes") >> fail_user_report_generation >> finish
        is_user_report_failed >> rail.Label("No") >> user_report_has_data

        user_report_has_data >> rail.Label("No") >> write_log_user_report_no_data >> no_user_data_email_logs_to_csv >> no_user_data_email >> finish
        user_report_has_data >> rail.Label("Yes") >> is_user_report_has_expected_columns

        is_user_report_has_expected_columns >> rail.Label("No") >> fail_user_report_columns >> finish
        is_user_report_has_expected_columns >> rail.Label("Yes") >> write_log_user_has_data >> get_final_toil_data_in_instance >> write_log_final_toil_has_data >> has_final_toil_data_in_instance

        has_final_toil_data_in_instance >> rail.Label("Yes") >> final_toil_data_in_instance_to_csv >> final_instance_toil_data_collection\
            >> download_reference_file >> write_log_download_ref >> parse_reference_file >> reference_file_data_collection >> query_final_toil_extract\
                 >> write_log_final_extract >> final_toil_extract_to_csv >> get_file_name >> upload_toil_extract_file >> upload_extract_to_sftp\
                    >> write_log_upload_extract >> archive_old_reference_file >> upload_new_reference_file >> write_log_upload_ref>>write_process_completion_log >> process_logs_to_csv >> process_complete_email >> finish

        has_final_toil_data_in_instance >> rail.Label("No") >> no_toil_data_email_logs_to_csv >> no_toil_data_email >> finish

        finish >>process_completion_logs_to_csv>>upload_completion_logs_to_sftp>> catch_and_log_errors >> write_fail_logs >>process_write_fail_logs_to_csv>>upload_fail_logs_to_sftp>> fail_email >> final_status

        return dag

rail.for_each_instance(create_dag)
