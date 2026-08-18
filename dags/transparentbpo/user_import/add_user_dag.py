"""
TransparentBPO Create User Child DAG
Creates a new user in Replicon
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
        dag_id=config.process_add_user_dag_id,
        description=f'TransparentBPO Add New User Child Dag',
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
            end_task='catch_and_log_error',
        )

        check_custom_training_billing_type = rail.IfOperator(
            task_id='check_custom_training_billing_type',
            test=lambda dag_run: dag_run.conf.get(
                'customTrainingBillingType') != 'production' and dag_run.conf.get(
                'customTrainingBillingType') != 'training',
            yes_task='log_invalid_training_type',
            no_task='find_user_with_same_loginname'
        )

        log_invalid_training_type = rail.WriteLogOperator(
            task_id='log_invalid_training_type',
            log="{{dag_run.conf.user_log}}",
            severity='skipped',
            message='na',
            properties=lambda dag_run: {
                "employeenumber": dag_run.conf.get('employeeNumber', ''),
                "user_name": dag_run.conf.get('user_name', ''),
                'timelog': dag_run.conf['log_timestamp'],
                "integrationaction": "add",
                'status': 'ignored',
                'details': f"Training billing type for the user '{dag_run.conf.get('customTrainingBillingType', '')}' is not allowed"
            }
        )

        find_user_with_same_loginname = rail.RepliconServiceOperator(
            task_id="find_user_with_same_loginname",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": null,
                        "loginName": dag_run.conf.get('workEmail'),
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else []
        )

        if_matching_user_present = rail.IfOperator(
            task_id='if_matching_user_present',
            test=lambda: rail.result('find_user_with_same_loginname'),
            yes_task='log_loginname_already_exists',
            no_task='create_user_9'
        )

        log_loginname_already_exists = rail.WriteLogOperator(
            task_id='log_loginname_already_exists',
            log="{{dag_run.conf.user_log}}",
            severity='skipped',
            message='na',
            properties=lambda dag_run: {
                "employeenumber": dag_run.conf.get('employeeNumber', ''),
                "user_name": dag_run.conf.get('user_name', ''),
                'timelog': dag_run.conf['log_timestamp'],
                "integrationaction": "add",
                'status': 'ignored',
                'details': f"A user already exist with same login name '{dag_run.conf.get('workEmail')}'"
            }
        )

        create_user_9 = rail.RepliconServiceOperator(
            task_id='create_user_9',
            endpoint="/services/ImportService1.svc/PutUser3",
            data=request_payload.create_user_payload
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
                "userUri": rail.result('create_user_9')['uri'],
                "activityUris": rail.result('get_enabled_activities_10')
            }
        )

        put_user_notification_preferences_12 = rail.RepliconServiceOperator(
            task_id='put_user_notification_preferences_12',
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data=lambda: request_payload.get_notification_preferences_payload(
                rail.result('create_user_9')['uri'])
        )

        if_location_present_13 = rail.IfOperator(
            task_id='if_location_present_13',
            test='''{{ dag_run.conf.location | is_truthy }}''',
            yes_task="get_required_payrule_to_assign_14_19",
            no_task="get_modification_payload_for_fields_present_31_36",
        )

        get_required_payrule_to_assign_14_19 = rail.PythonOperator(
            task_id='get_required_payrule_to_assign_14_19',
            python_callable=lambda dag_run: custom_methods.get_payrule_to_assign(
                dag_run, config)
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
            data=request_payload.get_user_modifications_payload
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
                    "uri": rail.result('create_user_9')['uri']
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
                    'get_all_drop_down_options_for_overtime_43'))
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
                    "uri": rail.result('create_user_9')['uri']
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
            no_task='log_completion_entry'
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
            no_task='dummy_trigger_create_supervisor_dag'
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
                "userUri": rail.result('create_user_9')['uri'],
                "permissionSetUri": dag_run.conf['supervisor_permission_set_uri']
            }
        )

        put_supervisor_assignment_schedule = rail.RepliconServiceOperator(
            task_id='put_supervisor_assignment_schedule',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data=lambda: {
                "userUri": rail.result('create_user_9')['uri'],
                "initialSupervisorUri": rail.result('search_supervisor_in_replicon_61')['uri'],
                "scheduleEntries": []
            }
        )

        dummy_trigger_create_supervisor_dag = rail.EmptyOperator(
            task_id='dummy_trigger_create_supervisor_dag',
        )

        get_supervisor_employee_details = rail.BambooHROperator(
            task_id='get_supervisor_employee_details',
            bamboohr_conn_id=config.bamboohr_conn_id,
            company_domain='',
            request_method='GET',
            endpoint="/employees/{{ dag_run.conf.supervisorEId }}?fields=" + ",".join(
                config.BAMBOO_STANDARD_FIELDS + config.BAMBOO_CUSTOM_FIELDS),
            data_handler=lambda response: {
                k: (v if v else "") for k, v in response.items()}
        )

        trigger_create_new_supervisor_in_replicon_71 = rail.TriggerDagRunOperator(
            task_id='trigger_create_new_supervisor_in_replicon_71',
            retries=0,
            trigger_dag_id=config.process_add_new_supervisor_dag_id,
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: request_payload.get_create_new_supervisor_payload(
                rail.result('get_supervisor_employee_details'), dag_run)
        )

        wait_for_completion_trigger_create_new_supervisor_in_replicon = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_create_new_supervisor_in_replicon',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_create_new_supervisor_in_replicon_71") }}'
        )

        gather_result_from_supervisor_creation = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_result_from_supervisor_creation',
            dag_runs="{{result('trigger_create_new_supervisor_in_replicon_71')}}",
            dagrun_task_id="get_created_user_uri",
            target='result'
        )

        put_supervisor_assignment_schedule_after_creation = rail.RepliconServiceOperator(
            task_id='put_supervisor_assignment_schedule_after_creation',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data=lambda: {
                "userUri": rail.result('create_user_9')['uri'],
                "initialSupervisorUri": rail.result('gather_result_from_supervisor_creation')[0],
                "scheduleEntries": []
            }
        )

        log_completion_entry = rail.WriteLogOperator(
            task_id='log_completion_entry',
            log="{{dag_run.conf.user_log}}",
            severity='success',
            message='na',
            properties=lambda dag_run: {
                "employeenumber": dag_run.conf.get('employeeNumber', ''),
                "user_name": dag_run.conf.get('user_name', ''),
                'timelog': dag_run.conf['log_timestamp'],
                "integrationaction": "add",
                'status': 'success',
                'details': "User created successfully" if rail.result('apply_modification_payload_for_fields_present_31_36') else "User created with exception"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{dag_run.conf.user_log}}",
            severity='error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "employeenumber": dag_run.conf.get('employeeNumber', ''),
                "user_name": dag_run.conf.get('user_name', ''),
                'timelog': dag_run.conf.get('log_timestamp'),
                "integrationaction": "add",
                'status': 'failed',
                'details': "User created successfully" if rail.result(
                    'apply_modification_payload_for_fields_present_31_36') else "User created with exception: Training billing type was not present"
            }
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
                "useruri": rail.result('create_user_9')['uri'] if rail.result('create_user_9') else '',
                "clientName": dag_run.conf.get('customClientName', ''),
                "projectName": dag_run.conf.get('customProjectName', ''),
                "customDirectIndirect": dag_run.conf.get('customDirectIndirect', ''),
                "project_log": dag_run.conf.get('project_log', ''),
                "job_run_date": dag_run.conf.get('job_run_date'),
                "timelog": dag_run.conf.get('log_timestamp'),
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> check_custom_training_billing_type

        check_custom_training_billing_type >> rail.Label(
            'No') >> find_user_with_same_loginname
        check_custom_training_billing_type >> rail.Label(
            'Yes') >> log_invalid_training_type >> catch_and_log_error

        find_user_with_same_loginname >> if_matching_user_present

        if_matching_user_present >> rail.Label(
            'No') >> create_user_9
        if_matching_user_present >> rail.Label(
            'Yes') >> log_loginname_already_exists >> catch_and_log_error

        create_user_9 >> get_enabled_activities_10 >> assign_activities_for_user >> put_user_notification_preferences_12 >> if_location_present_13

        if_location_present_13 >> rail.Label(
            'No') >> get_modification_payload_for_fields_present_31_36
        if_location_present_13 >> rail.Label(
            'Yes') >> get_required_payrule_to_assign_14_19

        get_required_payrule_to_assign_14_19 >> get_required_holiday_calendar_timezone_to_assign_22 >> get_all_time_off_types_26 \
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
            'No') >> log_completion_entry
        if_supervisor_id_present_60 >> rail.Label(
            'Yes') >> search_supervisor_in_replicon_61 >> if_supervisor_present

        if_supervisor_present >> rail.Label(
            'No') >> dummy_trigger_create_supervisor_dag >> get_supervisor_employee_details >> trigger_create_new_supervisor_in_replicon_71 \
            >> wait_for_completion_trigger_create_new_supervisor_in_replicon >> gather_result_from_supervisor_creation\
            >> put_supervisor_assignment_schedule_after_creation >> log_completion_entry
        if_supervisor_present >> rail.Label(
            'Yes') >> if_supervisor_permission_not_present_63

        if_supervisor_permission_not_present_63 >> rail.Label(
            'No') >> put_supervisor_assignment_schedule
        if_supervisor_permission_not_present_63 >> rail.Label(
            'Yes') >> assign_permission_set_to_user_68 >> put_supervisor_assignment_schedule

        put_supervisor_assignment_schedule >> log_completion_entry >> catch_and_log_error

        catch_and_log_error >> trigger_project_task_sync_to_replicon_78

    return dag


rail.for_each_instance(create_child_dag)
