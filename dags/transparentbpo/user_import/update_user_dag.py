"""
TransparentBPO Update User Child DAG
"""
from datetime import timedelta
from airflow.models import Variable
import rail
import json
from uuid import uuid4
from transparentbpo.user_import.utils import request_payload, custom_methods

null = None


def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.process_update_user_dag_id,
        description=f'TransparentBPO Update User Child Dag',
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
            no_task='is_status_not_active'
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
                "integrationaction": "update",
                'status': 'ignored',
                'details': f"Training billing type for the user '{dag_run.conf.get('customTrainingBillingType', '')}' is not allowed"
            }
        )

        is_status_not_active = rail.IfOperator(
            task_id='is_status_not_active',
            test=lambda dag_run: dag_run.conf.get('status') != 'Active',
            yes_task='get_my_actual_user_identity_7',
            no_task='get_user_details'
        )

        get_my_actual_user_identity_7 = rail.RepliconServiceOperator(
            task_id='get_my_actual_user_identity_7',
            endpoint="/services/UserAccessControlService1.svc/GetMyActualUserIdentity"
        )

        if_my_login_name_equals_workemail = rail.IfOperator(
            task_id='if_my_login_name_equals_workemail',
            test=lambda dag_run: rail.result('get_my_actual_user_identity_7')[
                'loginName'] == dag_run.conf['workEmail'],
            yes_task="log_admin_profile_cannot_be_disabled",
            no_task="disable_user",
        )

        log_admin_profile_cannot_be_disabled = rail.WriteLogOperator(
            task_id='log_admin_profile_cannot_be_disabled',
            log="{{dag_run.conf.user_log}}",
            severity='skipped',
            message='na',
            properties=lambda dag_run: {
                "employeenumber": dag_run.conf.get('employeeNumber', ''),
                "user_name": dag_run.conf.get('user_name', ''),
                'timelog': dag_run.conf['log_timestamp'],
                "integrationaction": "disable",
                'status': 'ignored',
                'details': "This user account is used by Replicon Integration"
            }
        )

        disable_user = rail.RepliconServiceOperator(
            task_id='disable_user',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: request_payload.disable_user_payload(
                dag_run, config.DATE_FORMAT)
        )

        log_user_disabled_success = rail.WriteLogOperator(
            task_id='log_user_disabled_success',
            log="{{dag_run.conf.user_log}}",
            severity='success',
            message='na',
            properties=lambda dag_run: {
                "employeenumber": dag_run.conf.get('employeeNumber', ''),
                "user_name": dag_run.conf.get('user_name', ''),
                'timelog': dag_run.conf['log_timestamp'],
                "integrationaction": "disable",
                'status': 'success',
                'details': ""
            }
        )

        get_user_details = rail.PythonOperator(
            task_id='get_user_details',
            python_callable=lambda dag_run: json.loads(
                rail.read_artifact(dag_run.conf['user_details_artifact']))
        )

        is_user_disabled_in_replicon = rail.IfOperator(
            task_id='is_user_disabled_in_replicon',
            test=lambda: rail.result('get_user_details')['userDetails']['isEnabled'] in [
                'false', False, 'False'],
            yes_task="enable_user_21_23",
            no_task="if_name_not_matching_24",
        )

        enable_user_21_23 = rail.RepliconServiceOperator(
            task_id="enable_user_21_23",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: request_payload.get_enable_user_payload(
                dag_run, config)
        )

        update_cost_center = rail.RepliconServiceOperator(
            task_id='update_cost_center',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "costCenterScheduleToApply": {
                        "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementCostCenterSchedule": [],
                        "updateCostCenterScheduleOverDateRange": {
                            "replacementCostCenterScheduleEntries": [
                                {
                                    "costCenter": {
                                        "uri": null,
                                        "parentUri": null,
                                        "name": "General Pay-rule"
                                    },
                                    "effectiveDate": rail.parse_date(dag_run.conf['job_run_date'], config.DATE_FORMAT)
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_name_not_matching_24 = rail.IfOperator(
            task_id='if_name_not_matching_24',
            test=lambda dag_run: rail.result('get_user_details')['userDetails']['firstName'] != (dag_run.conf.get('firstName', '') + ' ' + dag_run.conf.get(
                'middleName', '')).strip() or rail.result('get_user_details')['userDetails']['lastName'] != dag_run.conf.get('lastName', ''),
            yes_task="update_first_last_name_26",
            no_task="check_email_update",
        )

        update_first_last_name_26 = rail.RepliconServiceOperator(
            task_id='update_first_last_name_26',
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: {
                "target": {
                    "uri": dag_run.conf['useruri']
                },
                "template": null,
                "modifications": {
                    "firstName": {
                        "value": (dag_run.conf.get('firstName', '') + ' ' + dag_run.conf.get('middleName', '')).strip()
                    },
                    "lastName": {
                        "value": dag_run.conf.get('lastName', '')
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "unitOfWorkId": str(uuid4())
            }
        )

        # Step 27: Update email address
        check_email_update = rail.IfOperator(
            task_id='check_email_update',
            test=lambda dag_run: rail.result('get_user_details')[
                'userDetails']['emailAddress'] != dag_run.conf['workEmail'],
            yes_task='update_email_address',
            no_task='check_loginname'
        )

        update_email_address = rail.RepliconServiceOperator(
            task_id='update_email_address',
            endpoint='/services/ImportService1.svc/ApplyUserModifications3',
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}"
                },
                "modifications": {
                    "userDetailsToApply": {
                        "emailAddress": {
                            "emailAddress": "{{ dag_run.conf.workEmail }}"
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        check_loginname = rail.IfOperator(
            task_id='check_loginname',
            test=lambda dag_run: rail.result('get_user_details')[
                'userDetails']['emailAddress'] != dag_run.conf['workEmail'],
            yes_task='set_replicon_authentication_for_user_30',
            no_task='get_user_current_group_assignments'
        )

        set_replicon_authentication_for_user_30 = rail.RepliconServiceOperator(
            task_id='set_replicon_authentication_for_user_30',
            endpoint="/services/securityService1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ dag_run.conf.workEmail }}",
                "password": null,
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:do-not-change"
            }
        )

        get_user_current_group_assignments = rail.RepliconServiceOperator(
            task_id='get_user_current_group_assignments',
            endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": None
            },
            data_handler=lambda res: {
                'current_costcentre_name': (res['costCenters'][0]['costCenter']['costCenter']['displayText'] if res['costCenters'][0]['costCenter'] else '') if res['costCenters'] else '',
                'current_department_name': (res['departments'][0]['department']['department']['displayText'] if res['departments'][0]['department'] else '') if res['departments'] else '',
                'current_division_name': (res['divisions'][0]['division']['division']['displayText'] if res['divisions'][0]['division'] else '') if res['divisions'] else '',
                'current_employeetype_name': (res['employeeTypes'][0]['employeeType']['employeeType']['displayText'] if res['employeeTypes'][0]['employeeType'] else '') if res['employeeTypes'] else '',
                'current_location_name': (res['locations'][0]['location']['location']['displayText'] if res['locations'][0]['location'] else '') if res['locations'] else '',
                'current_servicecenter_name': (res['serviceCenters'][0]['serviceCenter']['serviceCenter']['displayText'] if res['serviceCenters'][0]['serviceCenter'] else '') if res['serviceCenters'] else '',
            }
        )

        check_relevant_group_assignments_to_update_32_70 = rail.PythonOperator(
            task_id='check_relevant_group_assignments_to_update_32_70',
            python_callable=lambda dag_run: custom_methods.get_relevant_group_assignments_to_update(
                dag_run, config)
        )

        if_updates_to_be_done = rail.IfOperator(
            task_id='if_updates_to_be_done',
            test=lambda: rail.result('check_relevant_group_assignments_to_update_32_70')[
                'modifications_payload'],
            yes_task='update_relevant_group_assignments_32_70',
            no_task='if_location_present'
        )

        update_relevant_group_assignments_32_70 = rail.RepliconServiceOperator(
            task_id='update_relevant_group_assignments_32_70',
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: {
                "target":  {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": rail.result('check_relevant_group_assignments_to_update_32_70')['modifications_payload'],
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "unitOfWorkId": str(uuid4())
            }
        )

        # Get Time Off Types to assign
        if_location_present = rail.IfOperator(
            task_id='if_location_present',
            test='''{{ dag_run.conf.location | is_truthy }}''',
            yes_task='if_location_initial_assignment_present',
            no_task='get_all_drop_down_options_for_overtime'
        )

        if_location_initial_assignment_present = rail.IfOperator(
            task_id='if_location_initial_assignment_present',
            test=lambda: rail.result('get_user_current_group_assignments')[
                'current_location_name'],
            yes_task='if_current_location_not_equal_to_new_location',
            no_task='get_all_drop_down_options_for_overtime'
        )

        if_current_location_not_equal_to_new_location = rail.IfOperator(
            task_id='if_current_location_not_equal_to_new_location',
            test=lambda dag_run: rail.result('get_user_current_group_assignments')[
                'current_location_name'] != dag_run.conf['location'],
            yes_task='get_all_timeoff_tpes_to_assign_from_mapper_71',
            no_task='get_all_drop_down_options_for_overtime'
        )

        get_all_timeoff_tpes_to_assign_from_mapper_71 = rail.PythonOperator(
            task_id='get_all_timeoff_tpes_to_assign_from_mapper_71',
            python_callable=lambda dag_run: list(filter(
                lambda x: x["location"] == dag_run.conf['location'], config.TIME_OFF_MAPPER))
        )

        # Get all time off types
        get_required_time_off_type_uris_to_assign = rail.RepliconServiceOperator(
            task_id='get_required_time_off_type_uris_to_assign',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=lambda response: custom_methods.get_time_off_type_uris_to_assign(
                rail.result('get_all_timeoff_tpes_to_assign_from_mapper_71'), response)
        )

        update_time_off_assignments_75 = rail.RepliconServiceOperator(
            task_id='update_time_off_assignments_75',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('get_required_time_off_type_uris_to_assign')
            }
        )

        get_all_drop_down_options_for_overtime = rail.RepliconServiceOperator(
            task_id='get_all_drop_down_options_for_overtime',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": dag_run.conf['custom_field_uris']['overtime_cf_uri']
            }
        )

        get_all_required_custom_fields_to_update_payload_79_102 = rail.PythonOperator(
            task_id='get_all_required_custom_fields_to_update_payload_79_102',
            python_callable=lambda dag_run: custom_methods.get_custom_fields_to_update_payload(
                dag_run, rail.result('get_user_details'), rail.result('get_all_drop_down_options_for_overtime'))
        )

        if_custom_field_payload_present = rail.IfOperator(
            task_id='if_custom_field_payload_present',
            test=lambda: bool(rail.result(
                'get_all_required_custom_fields_to_update_payload_79_102')),
            yes_task='put_custom_field_payload_79_102',
            no_task='if_supervisor_id_present_107'
        )

        put_custom_field_payload_79_102 = rail.RepliconServiceOperator(
            task_id='put_custom_field_payload_79_102',
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: {
                "target": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "customFields": rail.result('get_all_required_custom_fields_to_update_payload_79_102')
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "unitOfWorkId": str(uuid4())
            }
        )

        if_supervisor_id_present_107 = rail.IfOperator(
            task_id='if_supervisor_id_present_107',
            test=lambda dag_run: dag_run.conf.get('supervisorId'),
            yes_task='search_supervisor_in_replicon_108',
            no_task='log_update_completion_entry'
        )

        search_supervisor_in_replicon_108 = rail.RepliconServiceOperator(
            task_id="search_supervisor_in_replicon_108",
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

        get_current_supervisor = rail.RepliconServiceOperator(
            task_id='get_current_supervisor',
            endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "asOfDate": rail.parse_date(dag_run.conf['job_run_date'], config.DATE_FORMAT)
            },
            data_handler=lambda res: {
                'loginname': res['supervisor']['user']['loginName'],
                'uri': res['supervisor']['user']['uri']
            } if res else {}
        )

        if_new_supervisor_exists = rail.IfOperator(
            task_id='if_new_supervisor_exists',
            test=lambda: rail.result('search_supervisor_in_replicon_108'),
            yes_task='is_supervisor_assignment_present',
            no_task='is_existing_supervisor_schedule'
        )

        is_supervisor_assignment_present = rail.IfOperator(
            task_id='is_supervisor_assignment_present',
            test=lambda: rail.result('get_current_supervisor').get('uri'),
            yes_task='if_new_supervisor_not_equals_existing_supervisor',
            no_task='assign_initial_supervisor'
        )

        if_new_supervisor_not_equals_existing_supervisor = rail.IfOperator(
            task_id='if_new_supervisor_not_equals_existing_supervisor',
            test=lambda: rail.result('get_current_supervisor').get(
                'uri') != rail.result('search_supervisor_in_replicon_108').get('uri'),
            yes_task='if_supervisor_permission_present_115',
            no_task='log_update_completion_entry'
        )

        if_supervisor_permission_present_115 = rail.IfOperator(
            task_id='if_supervisor_permission_present_115',
            test=lambda: rail.result('search_supervisor_in_replicon_108')[
                'supervisor_permission'],
            yes_task='update_supervisor_assignment_schedule_117',
            no_task='assign_supervisor_permission_set_to_user'
        )

        assign_supervisor_permission_set_to_user = rail.RepliconServiceOperator(
            task_id='assign_supervisor_permission_set_to_user',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "permissionSetUri": dag_run.conf['supervisor_permission_set_uri']
            }
        )

        update_supervisor_assignment_schedule_117 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_117',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_supervisor_in_replicon_108')['uri'],
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf['job_run_date'], config.DATE_FORMAT)
                }
            }
        )

        assign_initial_supervisor = rail.RepliconServiceOperator(
            task_id='assign_initial_supervisor',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "initialSupervisorUri": rail.result('search_supervisor_in_replicon_108')['uri'],
                "scheduleEntries": []
            }
        )

        is_existing_supervisor_schedule = rail.IfOperator(
            task_id='is_existing_supervisor_schedule',
            test=lambda: rail.result('get_current_supervisor').get('uri'),
            yes_task='dummy_trigger_create_supervisor_dag',
            no_task='log_update_completion_entry'
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

        trigger_create_supervisor_dag_134 = rail.TriggerDagRunOperator(
            task_id='trigger_create_supervisor_dag_134',
            retries=0,
            trigger_dag_id=config.process_add_new_supervisor_dag_id,
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: request_payload.get_create_new_supervisor_payload(
                rail.result('get_supervisor_employee_details'), dag_run)
        )

        wait_for_completion_trigger_create_new_supervisor_in_replicon = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_create_new_supervisor_in_replicon',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_create_supervisor_dag_134") }}'
        )

        gather_result_from_supervisor_creation = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_result_from_supervisor_creation',
            dag_runs="{{result('trigger_create_supervisor_dag_134')}}",
            dagrun_task_id="get_created_user_uri",
            target='result'
        )

        update_supervisor_assignment_schedule_135 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_135',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('gather_result_from_supervisor_creation')[0],
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf['job_run_date'], config.DATE_FORMAT)
                }
            }
        )

        log_update_completion_entry = rail.WriteLogOperator(
            task_id='log_update_completion_entry',
            log="{{dag_run.conf.user_log}}",
            severity='success',
            message='na',
            properties=lambda dag_run: {
                "employeenumber": dag_run.conf.get('employeeNumber', ''),
                "user_name": dag_run.conf.get('user_name', ''),
                'timelog': dag_run.conf['log_timestamp'],
                "integrationaction": "update",
                'status': 'success',
                'details': ""
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
                "integrationaction": "update",
                'status': 'failed',
                'details': rail.render_template("{{ get_error_message() }}")
            }
        )

        trigger_project_task_sync_to_replicon_140 = rail.TriggerDagRunOperator(
            task_id='trigger_project_task_sync_to_replicon_140',
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
                "useruri": dag_run.conf.get('useruri', ''),
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
            'No') >> is_status_not_active
        check_custom_training_billing_type >> rail.Label(
            'Yes') >> log_invalid_training_type >> catch_and_log_error

        is_status_not_active >> rail.Label('No') >> get_user_details
        is_status_not_active >> rail.Label(
            'Yes') >> get_my_actual_user_identity_7

        get_my_actual_user_identity_7 >> if_my_login_name_equals_workemail

        if_my_login_name_equals_workemail >> rail.Label(
            'Yes') >> log_admin_profile_cannot_be_disabled >> catch_and_log_error

        if_my_login_name_equals_workemail >> rail.Label(
            'No') >> disable_user >> log_user_disabled_success >> catch_and_log_error

        get_user_details >> is_user_disabled_in_replicon

        is_user_disabled_in_replicon >> rail.Label(
            'Yes') >> enable_user_21_23 >> update_cost_center >> if_name_not_matching_24
        is_user_disabled_in_replicon >> rail.Label(
            'No') >> if_name_not_matching_24

        if_name_not_matching_24 >> rail.Label(
            'Yes') >> update_first_last_name_26 >> check_email_update
        if_name_not_matching_24 >> rail.Label('No') >> check_email_update

        check_email_update >> rail.Label(
            'Yes') >> update_email_address >> check_loginname
        check_email_update >> rail.Label('No') >> check_loginname

        check_loginname >> rail.Label(
            'Yes') >> set_replicon_authentication_for_user_30 >> get_user_current_group_assignments
        check_loginname >> rail.Label(
            'No') >> get_user_current_group_assignments

        get_user_current_group_assignments >> check_relevant_group_assignments_to_update_32_70 >> if_updates_to_be_done

        if_updates_to_be_done >> rail.Label(
            'Yes') >> update_relevant_group_assignments_32_70 >> if_location_present
        if_updates_to_be_done >> rail.Label('No') >> if_location_present

        if_location_present >> rail.Label(
            'No') >> get_all_drop_down_options_for_overtime
        if_location_present >> rail.Label(
            'Yes') >> if_location_initial_assignment_present

        if_location_initial_assignment_present >> rail.Label(
            'No') >> get_all_drop_down_options_for_overtime
        if_location_initial_assignment_present >> rail.Label(
            'Yes') >> if_current_location_not_equal_to_new_location

        if_current_location_not_equal_to_new_location >> rail.Label(
            'No') >> get_all_drop_down_options_for_overtime
        if_current_location_not_equal_to_new_location >> rail.Label(
            'Yes') >> get_all_timeoff_tpes_to_assign_from_mapper_71

        get_all_timeoff_tpes_to_assign_from_mapper_71 >> get_required_time_off_type_uris_to_assign \
            >> update_time_off_assignments_75 >> get_all_drop_down_options_for_overtime

        get_all_drop_down_options_for_overtime >> get_all_required_custom_fields_to_update_payload_79_102 >> if_custom_field_payload_present

        if_custom_field_payload_present >> rail.Label(
            'No') >> if_supervisor_id_present_107
        if_custom_field_payload_present >> rail.Label(
            'Yes') >> put_custom_field_payload_79_102 >> if_supervisor_id_present_107

        if_supervisor_id_present_107 >> rail.Label(
            'No') >> log_update_completion_entry
        if_supervisor_id_present_107 >> rail.Label(
            'Yes') >> search_supervisor_in_replicon_108

        search_supervisor_in_replicon_108 >> get_current_supervisor >> if_new_supervisor_exists

        if_new_supervisor_exists >> rail.Label(
            'No') >> is_existing_supervisor_schedule
        if_new_supervisor_exists >> rail.Label(
            'Yes') >> is_supervisor_assignment_present

        is_supervisor_assignment_present >> rail.Label(
            'No') >> assign_initial_supervisor
        is_supervisor_assignment_present >> rail.Label(
            'Yes') >> if_new_supervisor_not_equals_existing_supervisor

        if_new_supervisor_not_equals_existing_supervisor >> rail.Label(
            'No') >> log_update_completion_entry
        if_new_supervisor_not_equals_existing_supervisor >> rail.Label(
            'Yes') >> if_supervisor_permission_present_115

        if_supervisor_permission_present_115 >> rail.Label(
            'No') >> assign_supervisor_permission_set_to_user >> update_supervisor_assignment_schedule_117
        if_supervisor_permission_present_115 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_117

        update_supervisor_assignment_schedule_117 >> log_update_completion_entry

        is_existing_supervisor_schedule >> rail.Label(
            'No') >> log_update_completion_entry
        is_existing_supervisor_schedule >> rail.Label(
            'Yes') >> dummy_trigger_create_supervisor_dag

        dummy_trigger_create_supervisor_dag >> get_supervisor_employee_details >> trigger_create_supervisor_dag_134 >> wait_for_completion_trigger_create_new_supervisor_in_replicon \
            >> gather_result_from_supervisor_creation >> update_supervisor_assignment_schedule_135 >> log_update_completion_entry

        log_update_completion_entry >> catch_and_log_error
        catch_and_log_error >> trigger_project_task_sync_to_replicon_140

    return dag


rail.for_each_instance(create_child_dag)
