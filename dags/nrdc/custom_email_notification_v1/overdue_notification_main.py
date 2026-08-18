
from datetime import timedelta, datetime
from pendulum import datetime as dt
import rail
from nrdc.custom_email_notification_v1.utils import custom_methods, request_payload

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.overdue_notification_master_dagid,
        description=f'NRDC Custom Email Notification Overdue Notification Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval_overdue_notification,
        start_date=dt(2024, 1, 1, tz=config.et_timezone),
        max_active_runs=config.max_active_run
    ) as dag:

        get_required_due_date = rail.PythonOperator(
            task_id= 'get_required_due_date',
            python_callable=lambda dag_run: custom_methods.get_date_to_consider(dag_run, config)
        )

        get_data_for_not_submitted_timesheets = rail.RepliconServiceOperator(
            task_id='get_data_for_not_submitted_timesheets',
            endpoint="/services/timesheetlistservice1.svc/GetData",
            data=request_payload.get_data_notsubmitted_timesheets_payload,
            data_handler=custom_methods.get_not_submitted_timesheets
        )
            
        create_notsubmittedtimesheets_collection = rail.CreateCollectionOperator(
            task_id = 'create_notsubmittedtimesheets_collection',
            source=lambda: rail.result('get_data_for_not_submitted_timesheets'),
            columns=[
                "user",
                "timesheetperiod",
                "timesheeturi",
                "status",
                "useruri",
                "duedate"
            ],
            name='notsubmittedtimesheets'
        )

        if_notsubmittedtimesheets_has_data = rail.IfOperator(
            task_id = 'if_notsubmittedtimesheets_has_data',
            test="{{ result('create_notsubmittedtimesheets_collection', 'length') > 0}}",
            yes_task='get_userlist_report_details',
            no_task='finish'
        )


        get_userlist_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_userlist_report_details',
            report_name=config.user_list_for_notification_report
        )

        generate_userlist_report = rail.run_report2(
            group_id='generate_userlist_report',
            target='artifact',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_userlist_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{(result('generate_userlist_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload}}",
            delimiter=','
        )

        create_collection_user_list_replicon = rail.CreateCollectionOperator(
            task_id='create_collection_user_list_replicon',
            source="{{ result('parse_csv') }}",
            name="userlistfromreplicon",
            columns={
                'User Name': 'username',
                'Login Name': 'loginname',
                'Email Notification': 'emailnotification',
                'ueruri': 'useruri',
                'User Status': 'userstatus',
                'Type': 'type'
            }
        )

        query_get_unique_list_of_emails_c4 = rail.QueryCollectionOperator(
            task_id='query_get_unique_list_of_emails_c4',
            query="SELECT userlistfromreplicon.*, notsubmittedtimesheets.* FROM userlistfromreplicon \
                JOIN notsubmittedtimesheets ON userlistfromreplicon.useruri = notsubmittedtimesheets.useruri \
                    WHERE userlistfromreplicon.userstatus = 'Enabled' AND userlistfromreplicon.type='C4' \
                AND NULLIF(userlistfromreplicon.emailnotification, '') IS NOT NULL"
        )

        trigger_send_overdue_notification_c4 = rail.trigger_parallel_dagrun(
            task_id = 'trigger_send_overdue_notification_c4',
            trigger_dag_id=config.overdue_send_mail_c4_dagid,
            items="{{ result('query_get_unique_list_of_emails_c4') }}",
            parallel_count=config.child_dag_max_active_runs,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'username': item['username'],
                'loginname': item['loginname'],
                'emailnotification': item['emailnotification'],
                'useruri': item['useruri'],
                'userstatus': item['userstatus'],
                'type': item['type'],
                'timesheetperiod': item['timesheetperiod'],
                'timesheeturi': item['timesheeturi']
            }
            
        )

        query_get_unique_list_of_emails_c3 = rail.QueryCollectionOperator(
            task_id='query_get_unique_list_of_emails_c3',
            query="SELECT userlistfromreplicon.*, notsubmittedtimesheets.* FROM userlistfromreplicon \
                JOIN notsubmittedtimesheets ON userlistfromreplicon.useruri = notsubmittedtimesheets.useruri \
                    WHERE userlistfromreplicon.userstatus = 'Enabled' AND userlistfromreplicon.type='Lobbying Timesheet' \
                AND NULLIF(userlistfromreplicon.emailnotification, '') IS NOT NULL"
        )

        trigger_send_overdue_notification_c3 = rail.trigger_parallel_dagrun(
            task_id = 'trigger_send_overdue_notification_c3',
            trigger_dag_id=config.overdue_send_mail_c3_dagid,
            items="{{ result('query_get_unique_list_of_emails_c3') }}",
            parallel_count=config.child_dag_max_active_runs,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'username': item['username'],
                'loginname': item['loginname'],
                'emailnotification': item['emailnotification'],
                'useruri': item['useruri'],
                'userstatus': item['userstatus'],
                'type': item['type'],
                'timesheetperiod': item['timesheetperiod'],
                'timesheeturi': item['timesheeturi']
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_required_due_date >> get_data_for_not_submitted_timesheets >> create_notsubmittedtimesheets_collection >> if_notsubmittedtimesheets_has_data
        if_notsubmittedtimesheets_has_data >> rail.Label('Yes') >> get_userlist_report_details >> generate_userlist_report >> parse_csv >>\
        create_collection_user_list_replicon >> query_get_unique_list_of_emails_c4 >> trigger_send_overdue_notification_c4 >>\
        query_get_unique_list_of_emails_c3 >> trigger_send_overdue_notification_c3 >> finish

        if_notsubmittedtimesheets_has_data >> rail.Label('No') >> finish
    return dag


rail.for_each_instance(create_dag)
