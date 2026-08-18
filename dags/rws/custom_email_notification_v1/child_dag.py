from datetime import timedelta
import rail
from rws.custom_email_notification_v1.utils import request_payload
from airflow.models import Variable
null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description=f'RWS send individual custom email notification for timesheets waiting for approval child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_details',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint='/services/UserService1.svc/GetUserDetails',
            data=lambda dag_run: {
                "userUri": dag_run.conf['approveruri']
            }
        )

        create_table_details_list=rail.LoadCSVFileOperator(
            task_id="create_table_details_list",
            document='{{ dag_run.conf.inputdata}}',
        )

        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source = "{{ result('create_table_details_list') }}",
            name = "tabledetails",
            columns = {
                'timesheetperiod':'timesheetperiod', 
                'user':'user', 
                'timesheeturi':'timesheeturi', 
                'status':'status', 
                'approver':'approver', 
                'useruri':'useruri',
                'approveruri':'approveruri'
            }
        )

        query_user_and_timesheetperiod=rail.QueryCollectionOperator(
            task_id='query_user_and_timesheetperiod',
            query="""SELECT  tabledetails.timesheetperiod, tabledetails.user FROM  tabledetails""",
        )

        load_query_data= rail.PythonOperator(
            task_id='load_query_data',
            python_callable= lambda: rail.load_all_records(rail.result('query_user_and_timesheetperiod'))
        )

        get_email_body = rail.RenderTemplateOperator(
            task_id='get_email_body',
            template_file='templates/email.html',
            target='result',
        )

        get_user_notification_preferences = rail.RepliconServiceOperator(
            task_id='get_user_notification_preferences',
            endpoint="/services/NotificationScriptAdministrationService1.svc/GetUserNotificationPreferences",
            data=lambda dag_run: {
                "userUri": dag_run.conf['approveruri']
            },
            data_handler=request_payload.get_user_notification_preference
        )

        is_timesheet_notification_enabled=rail.IfOperator(
            task_id='is_timesheet_notification_enabled',
            test=lambda: rail.result('get_user_notification_preferences').split(':')[-1].lower() != 'never-deliver',
            yes_task="send_email",
            no_task="log_to_sumo",
        )

        send_email=rail.RepliconServiceOperator(
            task_id='send_email',
            endpoint="/services/NotificationService1.svc/SendEmail2",
            data=request_payload.get_payload_sendemail
        )

        send_push_notification=rail.RepliconServiceOperator(
            task_id='send_push_notification',
            endpoint="/services/NotificationService1.svc/SendPushNotification",
            data=request_payload.get_send_notification_data
        )

        is_error_in_notification_present=rail.IfOperator(
            task_id='is_error_in_notification_present',
            test=lambda: bool(  rail.result('send_email')['invalidRecipients'] and
                                rail.result('send_email')['invalidRecipients'][0] and
                                rail.result('send_email')['invalidRecipients'][0]['failureReasons'][0]['displayText'] ),
            no_task="moravia_email_delete_entry_to_update",
        )

        moravia_email_delete_entry_to_update= rail.FilterLogEntriesOperator(
            task_id='moravia_email_delete_entry_to_update',
            log="{{ dag_run.conf.email_lookuptable }}",
            properties={
                'approvername': '{{dag_run.conf.approver}}',
                'approverid': '{{dag_run.conf.approveruri.split(":")[-1]}}',
                'status': 'Pending',
                'date': '{{dag_run.conf.date}}'
            },
            remove_filtered_entries=True
        )

        moravia_email_logs_update_entry=rail.WriteLogOperator(
            task_id='moravia_email_logs_update_entry',
            log="{{ dag_run.conf.email_lookuptable }}",
            message="Updating Entry",
            properties={
                'approvername': '{{dag_run.conf.approver}}',
                'approverid': '{{dag_run.conf.approveruri.split(":")[-1]}}',
                'status': 'Email Sent',
                'date': '{{dag_run.conf.date}}'
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> get_user_details
        get_user_details >> create_table_details_list
        create_table_details_list >> create_collection_from_csv >> query_user_and_timesheetperiod >> load_query_data
        load_query_data >> get_email_body >> get_user_notification_preferences >> is_timesheet_notification_enabled
        is_timesheet_notification_enabled >> rail.Label('Yes') >> send_email
        is_timesheet_notification_enabled >> rail.Label('No') >> log_to_sumo
        send_email >> send_push_notification >> is_error_in_notification_present
        is_error_in_notification_present >> rail.Label('No') >> moravia_email_delete_entry_to_update >> moravia_email_logs_update_entry >> log_to_sumo

    return dag
rail.for_each_instance(create_dag)
