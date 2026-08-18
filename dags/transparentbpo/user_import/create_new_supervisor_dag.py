"""
TransparentBPO Create Supervisor Child DAG
Creates a new supervisor user in Replicon
"""
from datetime import timedelta
from airflow.models import Variable
import rail
from uuid import uuid4
from transparentbpo.user_import.utils import request_payload, custom_methods

null = None


def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.process_add_new_supervisor_dag_id,
        description=f'TransparentBPO Add New Supervisor Child Dag',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_add_update_user,
    ) as dag:

        # View DAG configuration
        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='check_custom_training_billing_type'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='check_custom_training_billing_type',
            end_task='finish',
        )

        check_custom_training_billing_type = rail.IfOperator(
            task_id='check_custom_training_billing_type',
            test=lambda dag_run: dag_run.conf.get(
                'customTrainingBillingType') != 'production' and dag_run.conf.get(
                'customTrainingBillingType') != 'training',
            yes_task='log_invalid_training_type',
            no_task='create_supervisor_user_9'
        )

        log_invalid_training_type = rail.WriteLogOperator(
            task_id='log_invalid_training_type',
            log="{{dag_run.conf.user_log}}",
            severity='skipped',
            message='na',
            properties=lambda dag_run: {
                "employeenumber": dag_run.conf.get('employeeNumber', ''),
                "user_name": dag_run.conf.get('merged_first_middle_name', ''),
                'timelog': dag_run.conf['log_timestamp'],
                "integrationaction": "supervisor creation",
                'status': 'ignored',
                'details': f"Invalid training billing type for the user(Supervisor) '{dag_run.conf.get('customTrainingBillingType')}'"
            }
        )

        create_supervisor_user_9 = rail.RepliconServiceOperator(
            task_id='create_supervisor_user_9',
            endpoint="/services/ImportService1.svc/PutUser3",
            data=lambda dag_run: request_payload.create_supervisor_payload(
                dag_run, config.DATE_FORMAT)
        )

        get_enabled_activities_10 = rail.RepliconServiceOperator(
            task_id='get_enabled_activities_10',
            endpoint="/services/ActivityService1.svc/GetEnabledActivities",
            data_handler=lambda res: [activity['uri']
                                      for activity in res] if res else []
        )

        assign_activities_for_user = rail.RepliconServiceOperator(
            task_id='assign_activities_for_user',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_supervisor_user_9')['uri'],
                "activityUris": rail.result('get_enabled_activities_10')
            }
        )

        put_user_notification_preferences_12 = rail.RepliconServiceOperator(
            task_id='put_user_notification_preferences_12',
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data=lambda: request_payload.get_notification_preferences_payload(
                rail.result('create_supervisor_user_9')['uri'])
        )

        if_location_present_14 = rail.IfOperator(
            task_id='if_location_present_14',
            test=lambda dag_run: dag_run.conf.get('location'),
            yes_task="payrule_to_assign",
            no_task="get_modification_payload_for_fields_present_31_36",
        )

        payrule_to_assign = rail.PythonOperator(
            task_id='payrule_to_assign',
            python_callable=lambda dag_run: config.SUPERVISOR_PAYRULE_MAPPER.get(
                dag_run.conf['location'])
        )

        get_required_holiday_calendar_timezone_to_assign_22 = rail.PythonOperator(
            task_id='get_required_holiday_calendar_timezone_to_assign_22',
            python_callable=lambda dag_run:  custom_methods.get_holiday_calendar_timezone_to_assign(
                dag_run, config)
        )

        get_all_time_off_types_26 = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types_26',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        get_list_of_eligible_timeoff_types_from_mapper_27 = rail.PythonOperator(
            task_id='get_list_of_eligible_timeoff_types_from_mapper_27',
            python_callable=lambda dag_run: list(filter(
                lambda x: x["location"] == dag_run.conf['location'], config.TIME_OFF_MAPPER))
        )

        get_final_list_of_timeoff_types_to_assign_28 = rail.PythonOperator(
            task_id='get_final_list_of_timeoff_types_to_assign_28',
            python_callable=lambda: request_payload.get_timeoff_types_to_assign(
                rail.result('get_all_time_off_types_26'), rail.result('get_list_of_eligible_timeoff_types_from_mapper_27'))
        )

        apply_user_modifications_20_30 = rail.RepliconServiceOperator(
            task_id='assign_payrule_to_user_20',
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=request_payload.get_supervisor_modifications_payload
        )

        get_modification_payload_for_fields_present_31_36 = rail.PythonOperator(
            task_id='get_modification_payload_for_fields_present_31_36',
            python_callable=lambda dag_run: custom_methods.modification_payload_for_fields_present_31_36(
                dag_run, config)
        )

        if_modification_payload_for_fields_present = rail.IfOperator(
            task_id='if_modification_payload_for_fields_present',
            test=lambda: bool(rail.result(
                'get_modification_payload_for_fields_present_31_36')),
            yes_task='apply_modification_payload_for_fields_present_31_36',
            no_task='get_all_drop_down_options_for_telephony_system_39'
        )

        apply_modification_payload_for_fields_present_31_36 = rail.RepliconServiceOperator(
            task_id='apply_modification_payload_for_fields_present_31_36',
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda: {
                "target": {
                    "uri": rail.result('create_supervisor_user_9')['uri']
                },
                "modifications": rail.result('get_modification_payload_for_fields_present_31_36'),
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "unitOfWorkId": str(uuid4())
            }
        )

        get_all_drop_down_options_for_telephony_system_39 = rail.RepliconServiceOperator(
            task_id='get_all_drop_down_options_for_telephony_system_39',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": dag_run.conf['custom_field_uris']['telephony_system_cf_uri']
            }
        )

        get_all_drop_down_options_for_overtime_43 = rail.RepliconServiceOperator(
            task_id='get_all_drop_down_options_for_overtime_43',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": dag_run.conf['custom_field_uris']['overtime_cf_uri']
            }
        )

        get_all_text_and_dd_custom_fields_to_update_payload_40_59 = rail.PythonOperator(
            task_id='get_all_text_and_dd_custom_fields_to_update_payload_40_59',
            python_callable=lambda dag_run: custom_methods.get_text_dd_custom_fields_to_update_payload(dag_run, rail.result(
                'get_all_drop_down_options_for_telephony_system_39'), rail.result(
                    'get_all_drop_down_options_for_overtime_43'), "supervisor_creation")
        )

        if_custom_field_payload_present = rail.IfOperator(
            task_id='if_custom_field_payload_present',
            test=lambda: bool(rail.result(
                'get_all_text_and_dd_custom_fields_to_update_payload_40_59')),
            yes_task='put_custom_field_payload_40_59',
            no_task='if_supervisor_id_present_60'
        )

        put_custom_field_payload_40_59 = rail.RepliconServiceOperator(
            task_id='put_custom_field_payload_40_59',
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda: {
                "target": {
                    "uri": rail.result('create_supervisor_user_9')['uri']
                },
                "modifications": {
                    "customFields": rail.result('get_all_text_and_dd_custom_fields_to_update_payload_40_59')
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "unitOfWorkId": str(uuid4())
            }
        )

        if_supervisor_id_present_60 = rail.IfOperator(
            task_id='if_supervisor_id_present_60',
            test=lambda dag_run: dag_run.conf.get('supervisorId'),
            yes_task='search_supervisor_in_replicon_61',
            no_task='finish'
        )

        search_supervisor_in_replicon_61 = rail.RepliconServiceOperator(
            task_id="search_supervisor_in_replicon_61",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": dag_run.conf.get('supervisorId'),
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda res: {
                'uri': res[0]['userDetails']['uri'],
                'employee_id': res[0]['userDetails']['employeeId'],
                'status': res[0]['userDetails']['isEnabled'],
                'supervisor_permission': rail.find_first_by_attr_and_get_attr(res[0]['permissionSets'], 'name', 'Supervisor', 'uri', ''),
                'end_date': (res[0]['userDetails']['employmentDateRange']['endDate']) if res[0]['userDetails']['employmentDateRange']['endDate'] else null
            } if res else []
        )

        if_supervisor_present = rail.IfOperator(
            task_id='if_supervisor_present',
            test=lambda: rail.result('search_supervisor_in_replicon_61'),
            yes_task='if_supervisor_permission_not_present_63',
            no_task='finish'
        )

        if_supervisor_permission_not_present_63 = rail.IfOperator(
            task_id='if_supervisor_permission_not_present_63',
            test=lambda: not rail.result('search_supervisor_in_replicon_61')[
                'supervisor_permission'],
            yes_task='assign_permission_set_to_user_68',
            no_task='put_supervisor_assignment_schedule'
        )

        assign_permission_set_to_user_68 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_68',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_supervisor_user_9')['uri'],
                "permissionSetUri": dag_run.conf['supervisor_permission_set_uri']
            }
        )

        put_supervisor_assignment_schedule = rail.RepliconServiceOperator(
            task_id='put_supervisor_assignment_schedule',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data=lambda: {
                "userUri": rail.result('create_supervisor_user_9')['uri'],
                "initialSupervisorUri": rail.result('search_supervisor_in_replicon_61')['uri'],
                "scheduleEntries": []
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        is_invalid_training_type = rail.IfOperator(
            task_id='is_invalid_training_type',
            test=lambda: rail.result('log_invalid_training_type'),
            yes_task='stop_job_invalid_training_type',
            no_task='trigger_project_task_sync_to_replicon_78'
        )

        stop_job_invalid_training_type = rail.EmptyOperator(
            task_id='stop_job_invalid_training_type'
        )

        trigger_project_task_sync_to_replicon_78 = rail.TriggerDagRunOperator(
            task_id='trigger_project_task_sync_to_replicon_78',
            trigger_rule='all_done',
            retries=0,
            trigger_dag_id=config.process_project_task_creation_dag_id,
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "customLaborLevel": dag_run.conf.get('customLaborLevel', ''),
                "id": dag_run.conf.get('id', ''),
                "middleName": dag_run.conf.get('middleName', ''),
                "workEmail": dag_run.conf.get('workEmail', ''),
                "employeeNumber": dag_run.conf.get('employeeNumber', ''),
                "status": dag_run.conf.get('status', ''),
                "firstName": dag_run.conf.get('firstName', ''),
                "lastName": dag_run.conf.get('lastName', ''),
                "jobTitle": dag_run.conf.get('jobTitle', ''),
                "department": dag_run.conf.get('department', ''),
                "useruri": rail.result('create_supervisor_user_9')['uri'] if rail.result('create_supervisor_user_9') else '',
                "clientName": dag_run.conf.get('subordinate_details', {}).get('customClientName', ''),
                "projectName": dag_run.conf.get('subordinate_details', {}).get('customProjectName', ''),
                "customDirectIndirect": dag_run.conf.get('subordinate_details', {}).get('customDirectIndirect', ''),
                "project_log": dag_run.conf.get('project_log', ''),
                "job_run_date": dag_run.conf.get('job_run_date'),
                "timelog": dag_run.conf.get('log_timestamp'),
            }
        )

        get_created_user_uri = rail.PythonOperator(
            task_id='get_created_user_uri',
            python_callable=lambda: rail.result(
                'create_supervisor_user_9')['uri']
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> check_custom_training_billing_type

        check_custom_training_billing_type >> rail.Label(
            'No') >> create_supervisor_user_9
        check_custom_training_billing_type >> rail.Label(
            'Yes') >> log_invalid_training_type >> finish

        create_supervisor_user_9 >> get_enabled_activities_10 >> assign_activities_for_user >> put_user_notification_preferences_12 >> if_location_present_14

        if_location_present_14 >> rail.Label(
            'No') >> get_modification_payload_for_fields_present_31_36
        if_location_present_14 >> rail.Label(
            'Yes') >> payrule_to_assign

        payrule_to_assign >> get_required_holiday_calendar_timezone_to_assign_22 >> get_all_time_off_types_26 \
            >> get_list_of_eligible_timeoff_types_from_mapper_27 >> get_final_list_of_timeoff_types_to_assign_28 >> apply_user_modifications_20_30 \
            >> get_modification_payload_for_fields_present_31_36

        get_modification_payload_for_fields_present_31_36 >> if_modification_payload_for_fields_present

        if_modification_payload_for_fields_present >> rail.Label(
            'No') >> get_all_drop_down_options_for_telephony_system_39
        if_modification_payload_for_fields_present >> rail.Label(
            'Yes') >> apply_modification_payload_for_fields_present_31_36 >> get_all_drop_down_options_for_telephony_system_39

        get_all_drop_down_options_for_telephony_system_39 >> get_all_drop_down_options_for_overtime_43 \
            >> get_all_text_and_dd_custom_fields_to_update_payload_40_59 >> if_custom_field_payload_present

        if_custom_field_payload_present >> rail.Label(
            'No') >> if_supervisor_id_present_60
        if_custom_field_payload_present >> rail.Label(
            'Yes') >> put_custom_field_payload_40_59 >> if_supervisor_id_present_60

        if_supervisor_id_present_60 >> rail.Label(
            'No') >> finish
        if_supervisor_id_present_60 >> rail.Label(
            'Yes') >> search_supervisor_in_replicon_61 >> if_supervisor_present

        if_supervisor_present >> rail.Label(
            'No') >> finish
        if_supervisor_present >> rail.Label(
            'Yes') >> if_supervisor_permission_not_present_63

        if_supervisor_permission_not_present_63 >> rail.Label(
            'No') >> put_supervisor_assignment_schedule
        if_supervisor_permission_not_present_63 >> rail.Label(
            'Yes') >> assign_permission_set_to_user_68 >> put_supervisor_assignment_schedule >> finish

        finish >> is_invalid_training_type

        is_invalid_training_type >> rail.Label(
            'Yes') >> stop_job_invalid_training_type

        is_invalid_training_type >> rail.Label(
            'No') >> trigger_project_task_sync_to_replicon_78

        trigger_project_task_sync_to_replicon_78 >> get_created_user_uri

    return dag


rail.for_each_instance(create_child_dag)
