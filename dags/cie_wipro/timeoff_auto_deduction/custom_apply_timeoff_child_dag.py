# pylint: disable=line-too-long, too-many-statements trailing-whitespace
from datetime import datetime
import rail
from cie_wipro.timeoff_auto_deduction.utils import python_callable, request_payload
from cie_wipro.timeoff_auto_deduction.tasks.create_timoff_booking import get_create_timeoff


def create_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_apply_timeoff_{config.country}{dag_id_postfix}_child_v1'.lower(),
        description=f'{dag_id_prefix}_apply_timeoff_child_{config.country}{dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_child_run,
    ) as dag:

        html_body = "templates/emails/email_for_booking_success.html"

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_all_entries = rail.PythonOperator(
            task_id='get_all_entries',
            python_callable=lambda dag_run: dag_run.conf
        )

        get_booking_date = rail.PythonOperator(
            task_id='get_booking_date',
            python_callable=lambda: python_callable.get_replicon_date(
                rail.get_dag_run_conf(), config),
        )

        get_time_off_type_assignments_for_user = rail.RepliconServiceOperator(
            task_id='get_time_off_type_assignments_for_user',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}"
            },
            response_filter=lambda response: python_callable.checkTOAssignedTOUser(
                response)
        )
        get_assigned_time_off_type_uris = rail.PythonOperator(
            task_id='get_assigned_time_off_type_uris',
            python_callable=python_callable.get_assigned_Uris
        )

        check_first_to = rail.IfOperator(
            task_id='check_first_to',
            test='''{{ result('get_assigned_time_off_type_uris').get('first_timeoff_uri') | is_truthy }}''',
            yes_task="get_user_timeofftype1_balance_summary",
            no_task="get_user_timeofftype2_balance_summary",
        )
        get_user_timeofftype1_balance_summary = rail.RepliconServiceOperator(
            task_id='get_user_timeofftype1_balance_summary',
            endpoint="/services/TimeOffService1.svc/GetUserTimeOffTypeBalanceSummary",
            data=lambda dag_run: {
                "userUri":  rail.result('get_all_entries')['user_uri'],
                "timeOffTypeUri": rail.result('get_assigned_time_off_type_uris')['first_timeoff_uri'],
                "asOfDate": None
            }
        )

        get_user_timeofftype2_balance_summary = rail.RepliconServiceOperator(
            task_id='get_user_timeofftype2_balance_summary',
            endpoint="/services/TimeOffService1.svc/GetUserTimeOffTypeBalanceSummary",
            data=lambda dag_run: {
                "userUri":  rail.result('get_all_entries')['user_uri'],
                "timeOffTypeUri": rail.result('get_assigned_time_off_type_uris')['second_timeoff_uri'],
                "asOfDate": None
            }
        )
        get_booking_details = rail.PythonOperator(
            task_id='get_booking_details',
            python_callable=python_callable.get_timeoff_type_tobe_booked,
        )
        check_for_booking = rail.IfOperator(
            task_id='check_for_booking',
            test='''{{ result('get_booking_details')['book'] == True }}''',
            yes_task="eligible_for_booking",
            no_task="log_skip_entries",
        )
        eligible_for_booking = rail.EmptyOperator(
            task_id='eligible_for_booking'
        )
        create_timeoff_booking, approve_timeoff_booking = get_create_timeoff()

        check_for_useremail = rail.IfOperator(
            task_id='check_for_useremail',
            test='''{{ dag_run.conf['User Email'] | is_truthy and '@' in dag_run.conf['User Email'] }}''',
            yes_task="get_email_subject",
            no_task="log_success_entries",
        )

        get_email_subject = rail.PythonOperator(
            task_id='get_email_subject',
            python_callable=lambda dag_run: f"Time Off Booked: Replicon has booked timeoff on behaf of user for {dag_run.conf['Entry Date']}"
        )

        get_email_body = rail.RenderTemplateOperator(
            task_id='get_email_body',
            template_file=html_body,
            target='result',
        )
        send_reminder_email = rail.RepliconServiceOperator(
            task_id='send_reminder_email',
            endpoint="/services/NotificationService1.svc/SendEmail2",
            data=request_payload.get_user_payload_sendemail
        )
        log_skip_entries = rail.WriteLogOperator(
            task_id='log_skip_entries',
            log="{{ result('create_log') }}",
            message="Time Off Booking Skipped",
            severity="Skipped",
            properties={
                "JobID": "{{ dag_run_ecid() }}",
                "Date": datetime.now().strftime("%d/%m/%Y"),
                "message": "Time off booking skipped because time of balance not available",
                "Entry Date": "{{ dag_run.conf['Entry Date'] }}",
                "User Name": "{{ dag_run.conf['User Name'] }}",
                "Scheduled Hrs": "{{ dag_run.conf['Scheduled Hrs'] }}",
                "user_uri": "{{ dag_run.conf['user_uri'] }}",
                "supervisor_email": "{{ dag_run.conf['supervisor_email'] }}",
                "supervisor_uri": "{{ dag_run.conf['supervisor_uri'] }}",
                "Status": "{{ dag_run_ecid() }} - Time off Booking Skipped",
                "error": "Not Applicable"
            }
        )
        log_success_entries = rail.WriteLogOperator(
            task_id='log_success_entries',
            log="{{ result('create_log') }}",
            message="Time Off Booking Completed",
            severity="success",
            properties={
                "JobID": "{{ dag_run_ecid() }}",
                "Date": datetime.now().strftime("%d/%m/%Y"),
                "message": "Time off booked for {{ dag_run.conf['Entry Date'] }}",
                "Entry Date": "{{ dag_run.conf['Entry Date'] }}",
                "User Name": "{{ dag_run.conf['User Name'] }}",
                "Scheduled Hrs": "{{ dag_run.conf['Scheduled Hrs'] }}",
                "user_uri": "{{ dag_run.conf['user_uri'] }}",
                "supervisor_email": "{{ dag_run.conf['supervisor_email'] }}",
                "supervisor_uri": "{{ dag_run.conf['supervisor_uri'] }}",
                "Status": "{{ dag_run_ecid() }} - Time off Booked Successfully",
                "error": "Not Applicable"
            }
        )
        log_failure_entries = rail.WriteLogOperator(
            task_id='log_failure_entries',
            log="{{ result('create_log') }}",
            trigger_rule='one_failed',
            message="Time Off Booking Failed",
            severity="failed",
            properties={
                "JobID": "{{ dag_run_ecid() }}",
                "Date": datetime.now().strftime("%d/%m/%Y"),
                "message": "Time off booked for {{ dag_run.conf['Entry Date'] }}",
                "Entry Date": "{{ dag_run.conf['Entry Date'] }}",
                "User Name": "{{ dag_run.conf['User Name'] }}",
                "Scheduled Hrs": "{{ dag_run.conf['Scheduled Hrs'] }}",
                "user_uri": "{{ dag_run.conf['user_uri'] }}",
                "supervisor_email": "{{ dag_run.conf['supervisor_email'] }}",
                "supervisor_uri": "{{ dag_run.conf['supervisor_uri'] }}",
                "Status": "{{ dag_run_ecid() }} - Time off Booking Failed",
                "error": "{{ get_error_message() }}"
            }
        )

        create_log >> get_all_entries >> get_booking_date >> get_time_off_type_assignments_for_user >> get_assigned_time_off_type_uris >> check_first_to
        check_first_to >> rail.Label(
            'Yes') >> get_user_timeofftype1_balance_summary >> get_user_timeofftype2_balance_summary >> get_booking_details >> check_for_booking
        check_first_to >> rail.Label(
            'No') >> get_user_timeofftype2_balance_summary >> get_booking_details >> check_for_booking
        check_for_booking >> rail.Label(
            'Yes') >> eligible_for_booking >> create_timeoff_booking
        check_for_booking >> rail.Label(
            'No') >> log_skip_entries
        approve_timeoff_booking >> check_for_useremail
        get_email_subject >> get_email_body >> send_reminder_email
        send_reminder_email >> log_success_entries >> log_failure_entries
        check_for_useremail >> rail.Label(
            'Yes') >> get_email_subject
        check_for_useremail >> rail.Label(
            'No') >> log_success_entries
        log_skip_entries >> log_failure_entries
    return dag


rail.for_each_instance(create_dag)
