import json
import rail
from eisner_amper.user_import_v1.utils import response_filter, request_payload
from datetime import datetime, timedelta

# pylint: disable=too-many-statements


def create_child_dag(config):
    update_dags = []

    for idx in range(0, config.BATCH_COUNT):

        with rail.create_airflow_dag(
            dag_id=f"{config.update_user_dag_id}_batch_{idx+1}",
            description=f"Eisner Amper update user Child {config.instance}",
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_child
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

            bulk_get_user = rail.RepliconServiceOperator(
                task_id='bulk_get_user',
                endpoint='/services/ImportService1.svc/BulkGetUsers3',
                data=request_payload.bulk_get_user_payload
            )

            is_user_rehire = rail.IfOperator(
                task_id='is_user_rehire',
                test=lambda dag_run: (rail.result('bulk_get_user')[
                                    0]['userDetails']['isEnabled'] == False and True if dag_run.conf['workagreementstatus'] == "1" else False),
                yes_task='enable_login',
                no_task='get_effective_user_group_membership'
            )

            enable_login = rail.RepliconServiceOperator(
                task_id='enable_login',
                endpoint='/services/SecurityService1.svc/EnableLogin',
                data=request_payload.enable_login_payload
            )

            update_employment_date_range = rail.RepliconServiceOperator(
                task_id='update_employment_date_range',
                endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
                data=request_payload.update_employment_date_range_payload
            )

            get_effective_user_group_membership = rail.RepliconServiceOperator(
                task_id='get_effective_user_group_membership',
                endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
                data=request_payload.get_effective_user_group_membership_payload,
                data_handler=response_filter.get_effectivegroup_membership_filter
            )

            is_user_first_name_same = rail.IfOperator(
                task_id='is_user_first_name_same',
                test=lambda dag_run: (bool(dag_run.conf['firstname']) and (rail.result(
                    'bulk_get_user')[0]['userDetails']['firstName']).lower() != (dag_run.conf['firstname']).lower()),
                yes_task='update_first_name_user',
                no_task='is_user_last_name_same'
            )

            update_first_name_user = rail.RepliconServiceOperator(
                task_id='update_first_name_user',
                endpoint='/services/userService1.svc/UpdateFirstName',
                data=request_payload.update_first_name_user_payload
            )

            is_user_last_name_same = rail.IfOperator(
                task_id='is_user_last_name_same',
                test=lambda dag_run: (bool(dag_run.conf['lastname']) and (rail.result(
                    'bulk_get_user')[0]['userDetails']['lastName']).lower() != (dag_run.conf['lastname']).lower()),
                yes_task='update_last_name_user',
                no_task='is_email_same'
            )

            update_last_name_user = rail.RepliconServiceOperator(
                task_id='update_last_name_user',
                endpoint='/services/userService1.svc/UpdateLastName',
                data=request_payload.update_last_name_user_payload
            )

            is_email_same = rail.IfOperator(
                task_id='is_email_same',
                test=lambda dag_run: ((rail.result('bulk_get_user')[
                                    0]['userDetails']['emailAddress']) != (dag_run.conf['defaultemailaddress'])),
                yes_task='update_email_address_user',
                no_task='create_custom_field_list'
            )

            update_email_address_user = rail.RepliconServiceOperator(
                task_id='update_email_address_user',
                endpoint='/services/userService1.svc/UpdateEmail',
                data=request_payload.update_email_address_user_payload
            )

            create_custom_field_list = rail.SetVariableOperator(
                task_id='create_custom_field_list',
                append=False,
                name='custom_field',
                value=[]
            )

            weekly_working_hours_present = rail.IfOperator(
                task_id='weekly_working_hours_present',
                test=lambda dag_run: (bool(rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_user'), 'customField.displayText', "Weekly Working Hours", 'text')) or (
                    float(dag_run.conf['weeklyworkinghours']) != float(rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_user'), 'customField.displayText', "Weekly Working Hours", 'text') if rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_user'), 'customField.displayText', "Weekly Working Hours", 'text') else 0))),
                yes_task='add_custom_field_vaue',
                no_task='if_weekly_working_hours_less_than_40'
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
                yes_task='is_notification_equals_to_yes',
                no_task='is_notification_equals_to_no'
            )

            is_notification_equals_to_yes = rail.IfOperator(
                task_id='is_notification_equals_to_yes',
                test=lambda: (rail.find_first_by_attr_and_get_attr(rail.result(
                    'bulk_get_user'), 'customField.displayText', "Notification", 'text') == "Yes"),
                yes_task='add_custom_field_vaue_notification_yes',
                no_task='sapid_not_present'
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
                        "name": "No"
                    }
                }
            )

            put_user_notification_preferences = rail.RepliconServiceOperator(
                task_id='put_user_notification_preferences',
                endpoint='/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences',
                data=lambda dag_run: request_payload.put_user_notification_preferences_payload(dag_run.conf['uri'])
            )

            is_notification_equals_to_no = rail.IfOperator(
                task_id='is_notification_equals_to_no',
                test=lambda: (rail.find_first_by_attr_and_get_attr(rail.result(
                    'bulk_get_user'), 'customField.displayText', "Notification", 'text') == "No"),
                yes_task='add_custom_field_vaue_notification_no',
                no_task='sapid_not_present'
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
                        "name": "Yes"
                    }
                }
            )

            sapid_not_present = rail.IfOperator(
                task_id='sapid_not_present',
                test=lambda dag_run: (bool(rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_user'), 'customField.displayText', "SAP Employee ID", 'text')) or (
                    rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_user'), 'customField.displayText', "SAP Employee ID", 'text') != dag_run.conf['personworkagreement'])),
                yes_task='add_custom_field_vaue_notification_sap',
                no_task='if_sapid_equal_personworkagreement'
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

            if_costcenter_uri_present = rail.IfOperator(
                task_id='if_costcenter_uri_present',
                test=lambda dag_run: ((dag_run.conf['costcenteruri']) != rail.result(
                    'get_effective_user_group_membership').get('cost_center', {}).get('uri', '')),
                yes_task='update_cost_center_group',
                no_task='if_companycodeuri_uri_not_present'
            )

            update_cost_center_group = rail.RepliconServiceOperator(
                task_id='update_cost_center_group',
                endpoint='/services/ImportService1.svc/ApplyUserModifications2',
                data=request_payload.update_costcenter_group_payload
            )

            if_companycodeuri_uri_not_present = rail.IfOperator(
                task_id='if_companycodeuri_uri_not_present',
                test=lambda dag_run: ((dag_run.conf['companycodeuri']) != rail.result(
                    'get_effective_user_group_membership').get('legal_entities', {}).get('uri', '')),
                yes_task='update_department_group',
                no_task='if_sapid_equal_personworkagreement'
            )

            update_department_group = rail.RepliconServiceOperator(
                task_id='update_department_group',
                endpoint='/services/ImportService1.svc/ApplyUserModifications2',
                data=request_payload.update_department_group_payload
            )

            if_sapid_equal_personworkagreement = rail.IfOperator(
                task_id='if_sapid_equal_personworkagreement',
                test=lambda dag_run: ((rail.find_first_by_attr_and_get_attr(rail.result(
                    'bulk_get_user'), 'customField.displayText', "SAP Employee ID", 'text') == dag_run.conf['personworkagreement'])),
                yes_task='if_costcenteruri_present',
                no_task='if_employee_type_uri_present'
            )

            if_costcenteruri_present = rail.IfOperator(
                task_id='if_costcenteruri_present',
                test=lambda dag_run: ((dag_run.conf['costcenteruri']) != rail.result(
                    'get_effective_user_group_membership').get('cost_center', {}).get('uri', '')),
                yes_task='update_cost_center_group_current_date',
                no_task='if_companycodeuri_uri_present'
            )

            update_cost_center_group_current_date = rail.RepliconServiceOperator(
                task_id='update_cost_center_group_current_date',
                endpoint='/services/ImportService1.svc/ApplyUserModifications2',
                data=request_payload.update_cost_center_group_current_date_payload
            )

            if_companycodeuri_uri_present = rail.IfOperator(
                task_id='if_companycodeuri_uri_present',
                test=lambda dag_run: ((dag_run.conf['companycodeuri']) != rail.result(
                    'get_effective_user_group_membership').get('legal_entities', {}).get('uri', '')),
                yes_task='update_department_group_current_date',
                no_task='if_employee_type_uri_present'
            )

            update_department_group_current_date = rail.RepliconServiceOperator(
                task_id='update_department_group_current_date',
                endpoint='/services/ImportService1.svc/ApplyUserModifications2',
                data=request_payload.update_department_group_current_date_payload
            )

            if_employee_type_uri_present = rail.IfOperator(
                task_id='if_employee_type_uri_present',
                test=lambda dag_run: (dag_run.conf['employeetypeuri'] != rail.result(
                    'get_effective_user_group_membership').get('employee_type', {}).get('uri', '')),
                yes_task='update_employee_type_group',
                no_task='if_worklocation_uri_present'
            )

            update_employee_type_group = rail.RepliconServiceOperator(
                task_id='update_employee_type_group',
                endpoint='/services/ImportService1.svc/ApplyUserModifications2',
                data=request_payload.update_employee_type_group_payload
            )

            if_worklocation_uri_present = rail.IfOperator(
                task_id='if_worklocation_uri_present',
                test=lambda dag_run: ((bool(dag_run.conf['worklocationuri'])) and ((dag_run.conf['worklocationuri']) != rail.result(
                    'get_effective_user_group_membership').get('location', {}).get('uri', ''))),
                yes_task='update_location',
                no_task='if_role_uri_present'
            )

            update_location = rail.RepliconServiceOperator(
                task_id='update_location',
                endpoint='/services/ImportService1.svc/ApplyUserModifications2',
                data=request_payload.update_location_payload
            )

            if_role_uri_present = rail.IfOperator(
                task_id='if_role_uri_present',
                test=lambda dag_run: (((dag_run.conf['roleuri']) != rail.result(
                    'get_effective_user_group_membership').get('divisions', {}).get('uri', ''))),
                yes_task='update_division',
                no_task='if_custom_field_value_present'
            )

            update_division = rail.RepliconServiceOperator(
                task_id='update_division',
                endpoint='/services/ImportService1.svc/ApplyUserModifications2',
                data=request_payload.update_division_payload
            )

            if_custom_field_value_present = rail.IfOperator(
                task_id='if_custom_field_value_present',
                test=lambda: json.loads(json.dumps(
                    rail.get_dag_run_var('customFieldValues'), ensure_ascii=False).replace('"date":{}', '"date":null').replace(
                    '{"year":null,"month":null,"day":null}', '{}')) if json.loads(json.dumps(
                        rail.get_dag_run_var('customFieldValues'), ensure_ascii=False).replace('"date":{}', '"date":null').replace(
                        '{"year":null,"month":null,"day":null}', '{}')) else [],
                yes_task='update_custom_value',
                no_task='schedule_policies_is_present'
            )

            update_custom_value = rail.RepliconServiceOperator(
                task_id='update_custom_value',
                endpoint='/services/ImportService1.svc/ApplyUserModifications2',
                data=request_payload.update_custom_value_payload
            )

            schedule_policies_is_present = rail.IfOperator(
                task_id='schedule_policies_is_present',
                test=lambda: request_payload.is_schedule_present,
                yes_task='current_office_schedule',
                no_task='schedule_not_equal_displaytext'
            )

            current_office_schedule = rail.PythonOperator(
                task_id='current_office_schedule',
                python_callable=request_payload.get_current_office_schedule
            )

            schedule_not_equal_displaytext = rail.IfOperator(
                task_id='schedule_not_equal_displaytext',
                test=lambda dag_run: (((bool(rail.result('current_office_schedule')[0]['displaytext'])) == False) or (
                    dag_run.conf['schedule'] != rail.result('current_office_schedule')[0]['displaytext'])),
                yes_task='update_office_schedule',
                no_task='log_add_user_log'
            )

            update_office_schedule = rail.RepliconServiceOperator(
                task_id='update_office_schedule',
                endpoint='/services/ImportService1.svc/ApplyUserModifications2',
                data=request_payload.update_office_schedule_payload
            )

            log_add_user_log = rail.WriteLogOperator(
                task_id='log_add_user_log',
                message="User updated successfully",
                log='{{dag_run.conf.log}}',
                severity='Success',
                properties={
                    'employeeid': "{{dag_run.conf.personexternalid}}",
                    'loginname': "{{dag_run.conf.name}}",
                    'action': "Update",
                    'status': "Success",
                    'details': "User updated successfully",
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
                    'action': "Update",
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

            bulk_get_user >> is_user_rehire >> rail.Label(
                "Yes") >> enable_login >> update_employment_date_range >> get_effective_user_group_membership

            is_user_rehire >> rail.Label("No") >> get_effective_user_group_membership >> is_user_first_name_same >> rail.Label(
                "Yes") >> update_first_name_user >> is_user_last_name_same

            is_user_first_name_same >> rail.Label("No") >> is_user_last_name_same >> rail.Label(
                "Yes") >> update_last_name_user >> is_email_same

            is_user_last_name_same >> rail.Label("No") >> is_email_same >> rail.Label(
                "Yes") >> update_email_address_user >> create_custom_field_list

            is_email_same >> rail.Label(
                "No") >> create_custom_field_list >> weekly_working_hours_present >> rail.Label("Yes") >> add_custom_field_vaue >> if_weekly_working_hours_less_than_40

            weekly_working_hours_present >> rail.Label("No") >> if_weekly_working_hours_less_than_40 >> rail.Label("Yes") >> is_notification_equals_to_yes >> rail.Label("Yes") >> add_custom_field_vaue_notification_yes \
                >> put_user_notification_preferences >> sapid_not_present

            is_notification_equals_to_yes >> rail.Label("No") >> sapid_not_present

            if_weekly_working_hours_less_than_40 >> rail.Label("No") >> is_notification_equals_to_no >> rail.Label(
                "Yes") >> add_custom_field_vaue_notification_no >> sapid_not_present

            is_notification_equals_to_no >> rail.Label("No") >> sapid_not_present

            sapid_not_present >> rail.Label("Yes") >> add_custom_field_vaue_notification_sap >> if_costcenter_uri_present >> rail.Label(
                "Yes") >> update_cost_center_group >> if_companycodeuri_uri_not_present

            sapid_not_present >> rail.Label("No") >> if_sapid_equal_personworkagreement >> rail.Label(
                "No") >> if_employee_type_uri_present

            if_costcenter_uri_present >> rail.Label("No") >> if_companycodeuri_uri_not_present >> rail.Label(
                "Yes") >> update_department_group >> if_sapid_equal_personworkagreement

            if_companycodeuri_uri_not_present >> rail.Label("no") >> if_sapid_equal_personworkagreement >> rail.Label("Yes") >> if_costcenteruri_present >> rail.Label("Yes") >> update_cost_center_group_current_date \
                >> if_companycodeuri_uri_present

            if_costcenteruri_present >> rail.Label("No") >> if_companycodeuri_uri_present >> rail.Label(
                "Yes") >> update_department_group_current_date >> if_employee_type_uri_present

            if_companycodeuri_uri_present >> rail.Label("No") >> if_employee_type_uri_present >> rail.Label(
                "Yes") >> update_employee_type_group >> if_worklocation_uri_present

            if_employee_type_uri_present >> rail.Label("No") >> if_worklocation_uri_present >> rail.Label(
                "Yes") >> update_location >> if_role_uri_present

            if_worklocation_uri_present >> rail.Label("No") >> if_role_uri_present

            if_role_uri_present >> rail.Label(
                "No") >> if_custom_field_value_present

            if_role_uri_present >> rail.Label("Yes") >> update_division >> if_custom_field_value_present >> rail.Label("Yes") >> update_custom_value >> schedule_policies_is_present >> rail.Label(
                "Yes") >> current_office_schedule >> schedule_not_equal_displaytext

            if_custom_field_value_present >> rail.Label("No") >> schedule_policies_is_present >> rail.Label(
                "No") >> schedule_not_equal_displaytext >> rail.Label("Yes") >> update_office_schedule >> log_add_user_log >> catch_and_log_errors >> log_to_sumo

            schedule_not_equal_displaytext >> rail.Label("No") >> log_add_user_log

    update_dags.append(dag)

    return dag


rail.for_each_instance(create_child_dag)
