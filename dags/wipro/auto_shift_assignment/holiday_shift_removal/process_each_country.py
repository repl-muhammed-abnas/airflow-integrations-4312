from datetime import timedelta
import rail
from wipro.auto_shift_assignment.holiday_shift_removal.utils import request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_country_dag_id,
        description=f"Wipro Auto Shift Assignment Monthly Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child_1
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        get_default_shift_replicon = rail.RepliconServiceOperator(
            task_id="get_default_shift_replicon",
            endpoint="/services/ShiftListService1.svc/GetData",
            data=request_payload.get_default_shift,
            data_handler=lambda response:list(map(lambda i: {"shift":i["cells"][0]["textValue"],"shift_uri":i["cells"][2]["uri"]}, response["rows"]))
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id="get_all_locations",
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters",
        )

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name=config.report_name,
        )

        genarate_user_report = rail.run_report2(
            group_id='load_user_report',
            report_params=request_payload.get_user_report_payload
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("load_user_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('load_user_report.get_report_result').reportGenerationResults[0].error}}"
        )

        has_data = rail.IfOperator(
            task_id="has_data",
            test='{{ result("load_user_report.get_report_result", "has_data") }}',
            yes_task='users_report_payload_to_csv',
            no_task='end_holiday_shift_deletion'
        )

        end_holiday_shift_deletion = rail.EmptyOperator(
            task_id='end_holiday_shift_deletion'
        )

        users_report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="users_report_payload_to_csv",
            document='{{result("load_user_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        user_report_expected_report_columns = config.column_name
        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            # pylint: disable=consider-using-f-string
            test="{{ result('load_user_report.get_report_result').reportGenerationResults[0].payload | \
                starts_with('%s') }}" % user_report_expected_report_columns,
            no_task='fail_invalid_user_report_colums',
            yes_task='users_report_data_collection',
        )

        fail_invalid_user_report_colums = rail.FailOperator(
            task_id="fail_invalid_user_report_colums",
            message="Base report column does not match"
        )

        users_report_data_collection = rail.CreateCollectionOperator(
            task_id="users_report_data_collection",
            name='getalluserdata',
            source='{{result("users_report_payload_to_csv")}}',
            columns={
                'User Name': 'user_name',
                'Employee ID': 'employee_id',
                'UserUri': 'user_uri',
                'Holiday Calendar': 'holiday_calendar',
                'Country': 'country',
                'Holiday Calendar Uri': 'holiday_calendar_uri',
                "Schedule": "schedule",
                "Legal Entity Code":"legal_entity_code",
                "Acquired Company": "acquired_company",
                "User Start Date": "user_start_date",                    # NEW
                "Onsite Direct Recruit": "onsite_direct_recruit",        # NEW
                "Onsite Start Date": "onsite_start_date"                 # NEW
            }
        )

        query_enabled_users_data = rail.QueryCollectionOperator(
            task_id="query_enabled_users_data",
            query="""SELECT * FROM getalluserdata
                    WHERE NULLIF(holiday_calendar_uri, '') IS NOT NULL
                    AND schedule = 'Shift Schedule'
                    AND (country != 'Spain' OR (country = 'Spain' AND legal_entity_code = 'W001'))"""
        )

        has_any_users_data = rail.IfOperator(
            task_id='has_any_users_data',
            test='{{ result("query_enabled_users_data", "length") > 0 }}',
            yes_task="query_distinct_holiday_calendars",
            no_task="send_no_user_mail"
        )

        query_distinct_holiday_calendars = rail.QueryCollectionOperator(
            task_id="query_distinct_holiday_calendars",
            query="""SELECT DISTINCT holiday_calendar_uri FROM query_enabled_users_data"""
        )

        def get_default_shift_uri(dag_run):
            shift_uri = []
            if isinstance(dag_run.conf['default_shift'], list):
                for i in dag_run.conf['default_shift']:
                    shift_uri.append(rail.find_first_by_attr_and_get_attr(
                        rail.result("get_default_shift_replicon"),"shift",i, "shift_uri"))
            else:
                shift_uri.append(rail.find_first_by_attr_and_get_attr(
                    rail.result("get_default_shift_replicon"),"shift",dag_run.conf['default_shift'], "shift_uri"))
            return shift_uri

        process_each_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_records',
            items="{{result('query_distinct_holiday_calendars')}}",
            trigger_dag_id=config.delete_holiday_shift_assignment_dag_id,
            batch_size=config.batch_size,
            conf=lambda item, dag_run: {
                "holiday_calendar_uri": item[0]['holiday_calendar_uri'],
                "default_shift": dag_run.conf['default_shift'],
                "default_shift_uri": get_default_shift_uri(dag_run),
                "country": dag_run.conf['country']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_process_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_records",
            dag_runs="{{result('process_each_records')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_shift_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_shift_logs',
            dag_runs="{{ result('process_each_records') }}",
            dagrun_task_id='create_shift_log',
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=request_payload.do_format_logs,
            show_return_value_in_logs=False
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=['UserName', 'EmployeeID','Country', 'Schedule','Status','Ecid'],
            row=['{{ item.username }}', '{{ item.employeeid}}','{{item.country}}',
                 '{{item.schedule}}', '{{ item.status }}','{{ item.jobid }}']
        )

        generate_presigned_url = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_presigned_url",
            output_file_name='{{get_company_key()}}' + "Holiday_Shift_Deletion_Logs" + '{{current_time_in_specified_tz("America/New_York")}}.csv',
            expires_in_seconds=7*24*60*60,
            artifact_name='{{result("render_logs_csv")}}'
        )

        send_completion_mail = rail.EmailOperator(
            task_id='send_completion_mail',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') > 0 -%}\
                "+config.alert_email+"\
            {%- else -%}\
                "+config.internal_logs_email+"\
            {%- endif -%}",
            subject='{{ get_company_key() }} | Deleting User Shift Assignments on Holidays is completed  \
                {%- if result("format_logs", "error_record_count") > 0 -%} \
                     {{" "}} with errors  \
                {%- else -%} \
                    {{" "}} Successfully - \
                {%- endif -%}\
                {{ " " + current_time_in_specified_tz("America/New_York") }}',
            html_content="templates/email/import_complete.html",
            params={
                'location': '{{dag_run.conf.country}}'
            }
        )

        send_no_user_mail = rail.EmailOperator(
            task_id='send_no_user_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Deleting User Shift Assignments on Holidays - No data to be Processed for {{dag_run.conf.country}} at {{ current_time_in_specified_tz("America/New_York") }}',
            html_content="templates/email/no_data.html",
            params={
                'location': "{{dag_run.conf.country}}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
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

    get_default_shift_replicon >> get_all_locations >>get_user_report_details >>\
    genarate_user_report >> is_report_failed >> rail.Label("Yes") >> fail_report_generation
    is_report_failed >> rail.Label("No") >> has_data >> rail.Label("Yes") >>\
    users_report_payload_to_csv >> report_has_expected_columns >> rail.Label("Yes")\
    >> users_report_data_collection >> query_enabled_users_data >> has_any_users_data >> rail.Label("Yes") >>\
    query_distinct_holiday_calendars>>\
    process_each_records >> wait_process_records >> gather_shift_logs >> format_logs >> render_logs_csv\
    >>generate_presigned_url >> send_completion_mail >> log_to_sumo
    report_has_expected_columns >> rail.Label("No") >> fail_invalid_user_report_colums
    has_any_users_data >> rail.Label("No") >> send_no_user_mail >> log_to_sumo
    has_data >> rail.Label("No") >> end_holiday_shift_deletion
    log_to_sumo >> can_fail_dag >> fail_dagrun
    return dag


rail.for_each_instance(create_child_dag)
