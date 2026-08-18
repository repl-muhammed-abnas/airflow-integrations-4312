import json
import rail
from eisner_amper.user_import.utils import response_filter, request_payload
from datetime import datetime, timedelta
from airflow.models import Variable

# pylint: disable=too-many-statements


def create_child_dag(config):
    update_dags = []

    for idx in range(0, config.BATCH_COUNT):

        with rail.create_airflow_dag(
            dag_id=f"{config.add_user_dag_id}_batch_{idx+1}",
            description=f"Eisner Amper add user Child {config.instance}",
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_child
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='login_status_present'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='login_status_present',
                end_task='catch_and_log_errors',
            )

            login_status_present = rail.IfOperator(
                task_id='login_status_present',
                test=lambda dag_run: bool(
                    dag_run.conf['workagreementstatus'] == "1"),
                yes_task='employeetype_is_present',
                no_task='update_to_log'
            )

            update_to_log = rail.WriteLogOperator(
                task_id='update_to_log',
                message="User status for new user is false (0)",
                log='{{dag_run.conf.log}}',
                severity='Skipped',
                properties={
                    'employeeid': "{{dag_run.conf.personexternalid}}",
                    'loginname': "{{dag_run.conf.name}}",
                    'action': "Add",
                    'status': "Skipped",
                    'details': "User status for new user is false (0)",
                    'jobid': "{{dag_run_ecid()}}",
                    'childjobid': '',
                }
            )

            employeetype_is_present = rail.IfOperator(
                task_id='employeetype_is_present',
                test=lambda dag_run: bool(
                    dag_run.conf['employeetype']),
                yes_task='create_custom_field_list',
                no_task='update_to_log_employee_type'
            )

            update_to_log_employee_type = rail.WriteLogOperator(
                task_id='update_to_log_employee_type',
                message="Employee Type is not found",
                log='{{dag_run.conf.log}}',
                severity='Skipped',
                properties={
                    'employeeid': "{{dag_run.conf.personexternalid}}",
                    'loginname': "{{dag_run.conf.name}}",
                    'action': "Add",
                    'status': "Skipped",
                    'details': "Employee Type is not found",
                    'jobid': "{{dag_run_ecid()}}",
                    'childjobid': '',
                }
            )

            create_custom_field_list = rail.SetVariableOperator(
                task_id='create_custom_field_list',
                append=False,
                name='custom_field',
                value=[]
            )

            add_custom_field_vaue_notification_sap = rail.SetVariableOperator(
                task_id='add_custom_field_vaue_notification_sap',
                append=True,
                name='customFieldValues',
                value=lambda dag_run: {
                    "customField": {
                        "uri": dag_run.conf['sapudfuri'],
                        "name": None,
                        "groupUri": None
                    },
                    "text": dag_run.conf['personworkagreement'],
                    "date": None,
                    "dropDownOption": {}
                }
            )

            add_custom_field_vaue = rail.SetVariableOperator(
                task_id='add_custom_field_vaue',
                append=True,
                name='customFieldValues',
                value=lambda dag_run: {
                    "customField": {
                        "uri": dag_run.conf['Weeklyworkinghoursudfuri'],
                        "name": None,
                        "groupUri": None
                    },
                    "text": dag_run.conf['weeklyworkinghours'],
                    "date": None,
                    "dropDownOption": {}
                }
            )

            if_weekly_working_hours_less_than_40 = rail.IfOperator(
                task_id='if_weekly_working_hours_less_than_40',
                test=lambda dag_run: (
                    int(round(float(dag_run.conf['weeklyworkinghours']))) < 40),
                yes_task='add_custom_field_vaue_notification_no',
                no_task='add_custom_field_vaue_notification_yes'
            )

            add_custom_field_vaue_notification_no = rail.SetVariableOperator(
                task_id='add_custom_field_vaue_notification_no',
                append=True,
                name='customFieldValues',
                value=lambda dag_run: {
                    "customField": {
                        "uri": dag_run.conf['notificationudfuri'],
                        "name": None,
                        "groupUri": None
                    },
                    "text": None,
                    "date": None,
                    "dropDownOption": {
                        "uri": None,
                        "name": "No"
                    }
                }
            )

            add_custom_field_vaue_notification_yes = rail.SetVariableOperator(
                task_id='add_custom_field_vaue_notification_yes',
                append=True,
                name='customFieldValues',
                value=lambda dag_run: {
                    "customField": {
                        "uri": dag_run.conf['notificationudfuri'],
                        "name": None,
                        "groupUri": None
                    },
                    "text": None,
                    "date": None,
                    "dropDownOption": {
                        "uri": None,
                        "name": "Yes"
                    }
                }
            )

            create_user = rail.RepliconServiceOperator(
                task_id="create_user",
                endpoint="/services/ImportService1.svc/PutUser3",
                data=request_payload.create_user_payload,
                data_handler=lambda response: response["uri"] if response else None
            )

            if_worklocationuri_present = rail.IfOperator(
                task_id='if_worklocationuri_present',
                test=lambda dag_run: (
                    True if (dag_run.conf['worklocationuri']) else False),
                yes_task='update_location',
                no_task='update_time_entry_approval_path_for_new_user'
            )

            update_location = rail.RepliconServiceOperator(
                task_id="update_location",
                endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
                data=request_payload.update_location_payload2
            )

            update_time_entry_approval_path_for_new_user = rail.RepliconServiceOperator(
                task_id="update_time_entry_approval_path_for_new_user",
                endpoint="/services/ImportService1.svc/ApplyUserModifications2",
                data=request_payload.update_time_entry_approval_path_for_new_user_payload
            )

            if_weekly_working_hours_less_than_40_put = rail.IfOperator(
                task_id='if_weekly_working_hours_less_than_40_put',
                test=lambda dag_run: (
                    int(round(float(dag_run.conf['weeklyworkinghours']))) < 40),
                yes_task='put_user_notification',
                no_task='update_to_log_success'
            )

            put_user_notification = rail.RepliconServiceOperator(
                task_id="put_user_notification",
                endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
                data=lambda: request_payload.put_user_notification_preferences_payload(
                    rail.result('create_user'))
            )

            update_to_log_success = rail.WriteLogOperator(
                task_id='update_to_log_success',
                message="User created successfully",
                log='{{dag_run.conf.log}}',
                severity='Success',
                properties={
                    'employeeid': "{{dag_run.conf.personexternalid}}",
                    'loginname': "{{dag_run.conf.name}}",
                    'action': "Add",
                    'status': "Success",
                    'details': "User created successfully",
                    'jobid': "{{dag_run_ecid()}}",
                    'childjobid': '',
                }
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                trigger_rule='one_failed',
                log='{{dag_run.conf.log}}',
                severity='Error',
                message='{{ get_error_message() }}',
                properties={
                    'employeeid': "{{dag_run.conf.personexternalid}}",
                    'loginname': "{{dag_run.conf.name}}",
                    'action': "Add",
                    'status': "Error",
                    'details': '{{ get_error_message() }}',
                    'jobid': "{{dag_run_ecid()}}",
                    'childjobid': '',
                },
            )

            log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id='log_to_sumo',
                sumo_conn_id='sumologic-dagrunlogger',
                trigger_rule='all_done'
            )

            can_run_batch_task >> rail.Label(
                'Yes') >> batch_task >> catch_and_log_errors

            can_run_batch_task >> rail.Label(
                'No') >> login_status_present

            login_status_present >> rail.Label("Yes") >> employeetype_is_present >> rail.Label(
                "No") >> update_to_log_employee_type >> catch_and_log_errors

            login_status_present >> rail.Label(
                "No") >> update_to_log >> catch_and_log_errors

            employeetype_is_present >> rail.Label("Yes") >> create_custom_field_list >> add_custom_field_vaue_notification_sap >> add_custom_field_vaue \
                >> if_weekly_working_hours_less_than_40 >> rail.Label("Yes") >> add_custom_field_vaue_notification_no >> create_user

            if_weekly_working_hours_less_than_40 >> rail.Label("No") >> add_custom_field_vaue_notification_yes >> create_user >> if_worklocationuri_present >> rail.Label("Yes") \
                >> update_location >> update_time_entry_approval_path_for_new_user

            if_worklocationuri_present >> rail.Label("No") >> update_time_entry_approval_path_for_new_user >> if_weekly_working_hours_less_than_40_put >> rail.Label("Yes") \
                >> put_user_notification >> update_to_log_success

            if_weekly_working_hours_less_than_40_put >> rail.Label(
                "No") >> update_to_log_success >> catch_and_log_errors >> log_to_sumo

    update_dags.append(dag)

    return dag


rail.for_each_instance(create_child_dag)
