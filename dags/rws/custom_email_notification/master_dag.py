from datetime import timedelta, datetime, timezone
import json
from pendulum import datetime as dt
from rws.custom_email_notification.utils import python_callable_method, request_payload
from airflow.models import Variable
from rail.lib.artifact import new_artifact
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'rws_send_email_notification_for_timesheets_waiting_for_approval_master_{config.instance}',
        description=f'RWS_Send_Email Notification For_Timesheets_Waiting for approval_master_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2023, 1, 1, tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_master, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_today_saturday_or_sunday'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_today_saturday_or_sunday',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        is_today_saturday_or_sunday=rail.IfOperator(
            task_id='is_today_saturday_or_sunday',
            test=lambda: bool(datetime.now(timezone.utc).strftime("%A") == 'Saturday' or datetime.now(timezone.utc).strftime("%A") == 'Sunday'),
            no_task="get_date_in_format"
        )

        get_date_in_format=rail.PythonOperator(
            task_id='get_date_in_format',
            python_callable=python_callable_method.get_date_in_format
        )

        create_timezone_variable=rail.SetVariableOperator(
            task_id='create_timezone_variable',
            append=False,
            name='Required Time in the timezone',
            value= datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        )

        get_timesheet_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_timesheet_report_details',
            report_name=config.timesheet_report
        )

        load_timesheet_report = rail.run_report2(
            group_id='load_timesheet_report',
            report_params=request_payload.get_payload_timesheet_report_generation
        )

        is_error_present=rail.IfOperator(
            task_id='is_error_present',
            test="{{result('load_timesheet_report.get_report_result').reportGenerationResults[0].error | is_truthy}}",
            no_task="get_userlist_report_details",
        )

        get_userlist_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_userlist_report_details',
            report_name=config.user_list_report
        )

        load_user_list_report = rail.run_report2(
            group_id='load_user_list_report',
            report_params=request_payload.get_payload_userlist_report_generation
        )

        if_error_present=rail.IfOperator(
            task_id='if_error_present',
            test="{{result('load_user_list_report.get_report_result').reportGenerationResults[0].error | is_truthy}}",
            no_task="parse_csv_timesheet",
        )

        parse_csv_timesheet = rail.LoadCSVFileOperator(
            task_id='parse_csv_timesheet',
            document="{{result('load_timesheet_report.get_report_result').reportGenerationResults[0].payload}}",
            delimiter = ','
        )

        create_inputrawdata_collection = rail.CreateCollectionOperator(
            task_id="create_inputrawdata_collection",
            name="input_raw_data",
            source="{{result('parse_csv_timesheet')}}",
            columns={
                'User Name': 'username',
                'user uri': 'useruri',
                'Timesheet Period': 'timesheetperiod',
                'timesheet uri': 'timesheeturi',
                'Approval Status': 'approvalstatus',
                'waitingonapprover': 'approver',
                'Timesheet End Date': 'timesheetenddate'
            }
        )

        query_timesheet_data_for_end_date=rail.QueryCollectionOperator(
            task_id='query_timesheet_data_for_end_date',
            query="""SELECT * FROM  input_raw_data WHERE  input_raw_data.timesheetenddate = '{{result('get_date_in_format')}}'""",
        )

        is_username_not_present=rail.IfOperator(
            task_id='is_username_not_present',
            test=python_callable_method.is_username_not_present,
            no_task="parse_csv_userlist",
        )

        parse_csv_userlist=rail.LoadCSVFileOperator(
            task_id='parse_csv_userlist',
            document="{{result('load_user_list_report.get_report_result').reportGenerationResults[0].payload}}",
            delimiter = ',',
            headers=['User Name','Time Zone','useruri']
        )

        create_userlist_collection = rail.CreateCollectionOperator(
            task_id="create_userlist_collection",
            name="userlistcollection",
            source="{{result('parse_csv_userlist')}}",
            columns={
                'User Name': 'username',
                'Time Zone': 'timezone',
                'useruri': 'useruri',
            }
        )

        query_timesheets_with_required_approvers = rail.QueryCollectionOperator(
            task_id = 'query_timesheets_with_required_approvers',
            query="SELECT * FROM input_raw_data WHERE input_raw_data.approver <> 'HR, Payroll'"
        )

        def get_formatted_timesheetdata():
            records = rail.load_all_records(rail.result('query_timesheets_with_required_approvers'))
            user_list = rail.load_all_records(rail.result('create_userlist_collection'))
            timesheetlist = []
            for record in records:
                if record['approver'] != config.approver:
                    for approver in record['approver'].split(';'):
                        timesheetlist.append({
                            "timesheetperiod": record['timesheetperiod'],
                            "user": record['username'],
                            "timesheeturi": record['timesheeturi'],
                            "status": record['approvalstatus'],
                            "approver": approver.strip(),
                            "useruri": record['useruri'],
                            "approveruri": rail.find_first_by_attr_and_get_attr(user_list, 'username', approver.strip(), 'useruri'),
                            "iananame": rail.find_first_by_attr_and_get_attr(user_list, 'username', approver.strip(), 'timezone')
                        })
            with new_artifact(mode='w') as timesheetdata:
                timesheetdata.file.write(json.dumps(timesheetlist))
                timesheetdata.set_attribute(name="type", value="json")
            return timesheetdata.name

        create_formatted_timesheetdata = rail.PythonOperator(
            task_id = 'create_formatted_timesheetdata',
            python_callable=get_formatted_timesheetdata,
            show_return_value_in_logs=False
        )

        list_previous_jobs=rail.PythonOperator(
            task_id='list_previous_jobs',
            python_callable=python_callable_method.get_dag_runs,
            op_args=[config.child_dag_id]
        )

        create_formattedtimesheetdata_collection = rail.CreateCollectionOperator(
            task_id="create_formattedtimesheetdata_collection",
            name="formattedtimesheetdata",
            source= "{{ result('create_formatted_timesheetdata')}}"
        )

        query_unique_approvers_approveruri=rail.QueryCollectionOperator(
            task_id='query_unique_approvers_approveruri',
            query="""SELECT DISTINCT  formattedtimesheetdata.approver, formattedtimesheetdata.approveruri FROM  formattedtimesheetdata""",
        )

        create_moravia_email_logs_lookup_table = rail.CreateLogOperator(
            task_id="create_moravia_email_logs_lookup_table",
            tenant_wide_name="moravia_email_logs_lookup_table",
            existing_log_mode="append",
        )

        if_query_has_data=rail.IfOperator(
            task_id='if_query_has_data',
            test="{{ result('query_unique_approvers_approveruri') | length > 0 }}",
            yes_task="trigger_child_to_check_time_and_send_notification"
        )

        trigger_child_to_check_time_and_send_notification = rail.TriggerDagRunForEachItemOperator(
            task_id = 'trigger_child_to_check_time_and_send_notification',
            retries=0,
            items = "{{result('query_unique_approvers_approveruri')}}",
            trigger_dag_id=f'rws_check_time_for_approver_and_send_notification_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'approver': "{{item.approver}}",
                'approveruri': "{{item.approveruri}}",
                'timezonevariablename': "{{result('create_timezone_variable').name}}",
                'lookuptable': "{{ result('create_moravia_email_logs_lookup_table') }}",
                'previousjobs': "{{result('list_previous_jobs')}}"
            }
        )

        finish = rail.EmptyOperator(
            task_id = 'finish'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> is_today_saturday_or_sunday
        is_today_saturday_or_sunday >> rail.Label('No') >> get_date_in_format >> create_timezone_variable >> get_timesheet_report_details
        get_timesheet_report_details >> load_timesheet_report >> is_error_present
        is_error_present >> rail.Label('No') >> get_userlist_report_details >> load_user_list_report >> if_error_present
        if_error_present >> rail.Label(
            'No') >> parse_csv_timesheet >> create_inputrawdata_collection >> query_timesheet_data_for_end_date >> is_username_not_present
        is_username_not_present >> rail.Label(
            'No') >> parse_csv_userlist >> create_userlist_collection >> query_timesheets_with_required_approvers >> create_formatted_timesheetdata
        create_formatted_timesheetdata >> list_previous_jobs >> create_formattedtimesheetdata_collection >> query_unique_approvers_approveruri
        query_unique_approvers_approveruri >> create_moravia_email_logs_lookup_table >> if_query_has_data
        if_query_has_data >> rail.Label('Yes') >> trigger_child_to_check_time_and_send_notification >> finish
    return dag

rail.for_each_instance(create_dag)
