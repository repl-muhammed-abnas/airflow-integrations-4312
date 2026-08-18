import rail

from dxctechnology.workday_user_import_v1.user_import_philippines_v3.utils.custom_methods import (is_profile_enabled, get_user_uri)


null = None

def assign_supervisor(group_id, caller):

    with rail.TaskGroup(group_id = group_id, prefix_group_id=False) as supervisor_group:
        """
        Assigns a supervisor to the user based on the input data {dag_run.conf}
        """
        start_supervisor_assignment = rail.EmptyOperator(
            task_id = "start_supervisor_assignment"
        )

        is_profile_status_enabled = rail.IfOperator(
            task_id = "is_profile_status_enabled",
            test=is_profile_enabled,
            yes_task="is_supervisor_id_user_id_same",
            no_task="end_supervisor_assignment"
        )

        is_supervisor_id_user_id_same = rail.IfOperator(
            task_id = "is_supervisor_id_user_id_same",
            test= lambda dag_run: dag_run.conf['file_data']['supervisor_id'] == dag_run.conf['file_data']['emp_id'],
            yes_task="log_supervisor_id_user_id_same",
            no_task="is_caller_update"
        )

        log_supervisor_id_user_id_same = rail.WriteLogOperator(
            task_id = "log_supervisor_id_user_id_same",
            message = "User Add | Supervisor",
            log="{{dag_run.conf.user_log}}",
            severity = "Success",
            properties = lambda dag_run: {
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'],
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": "Add" if caller != "update" else "Update",
                "Status": "Exception",
                "Details": "Supervisor not updated - Supervisor is same as User"
            }
        )

        is_caller_update = rail.IfOperator(
            task_id = "is_caller_update",
            test=caller=="update",
            yes_task="get_current_supervisor_assignment",
            no_task="get_supervisor_details"
        )

        get_current_supervisor_assignment = rail.RepliconServiceOperator(
            task_id="get_current_supervisor_assignment",
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "asOfDate": dag_run.conf['json_formatted_dates']['supervisor_date']
            }
        )

        def can_update_supervisor_callable(dag_run):
            current_supervisor_assignment = rail.result("get_current_supervisor_assignment")
            if not current_supervisor_assignment:
                return True
            if current_supervisor_assignment.get('supervisor',{}).get('user', {}).get('loginName', '') != dag_run.conf['file_data']['supervisor_email_id']:
                return True
            return False

        can_update_supervisor = rail.IfOperator(
            task_id = "can_update_supervisor",
            test=can_update_supervisor_callable,
            yes_task="get_supervisor_details",
            no_task="end_supervisor_assignment"
        )

        def get_supervisor_details_request_payload(dag_run):
            return {
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:enabled",
                    "urn:replicon:user-list-column:division"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": dag_run.conf['file_data']['supervisor_id'],
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }

        def get_supervisor_details_data_handler(response, dag_run):
            if not response['rows']:
                return []
            return_response = list(filter(lambda emp_record: emp_record['emp_id'] == dag_run.conf['file_data']['supervisor_id']
                ,map(lambda record: {
                    "name": record['cells'][0].get('textValue'),
                    "login_name": record['cells'][1].get('textValue'),
                    "user_uri": record['cells'][0].get('uri'),
                    "emp_id": record['cells'][2].get('textValue'),
                    "status": record['cells'][3].get('textValue'),
                    "company": record['cells'][4].get('cellCollection', [{}])[0].get('textValue'),
                }, response['rows'])))
            return return_response[0] if return_response else []

        get_supervisor_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_details",
            endpoint="/services/UserListService1.svc/GetData",
            data=get_supervisor_details_request_payload,
            data_handler=get_supervisor_details_data_handler
        )

        is_supervisor_found = rail.IfOperator(
            task_id = "is_supervisor_found",
            test=lambda: bool(rail.result("get_supervisor_details")),
            yes_task="is_supervisor_company_present",
            no_task="log_supervisor_for_reassignment_check",
        )

        is_supervisor_company_present = rail.IfOperator(
            task_id = "is_supervisor_company_present",
            test=lambda: bool(rail.result("get_supervisor_details")["company"]),
            yes_task="is_supervisor_disabled",
            no_task="log_supervisor_for_reassignment_check",
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id = "is_supervisor_disabled",
            test=lambda: rail.result("get_supervisor_details")["status"] == "False",
            yes_task="log_supervisor_for_reassignment_check",
            no_task="get_supervisor_permission_details",
        )

        def get_supervisor_permission_details_data_handler(response, dag_run):
            rail.set_result(val=response, key="response")
            return {
                "end_user_permission": rail.find_first_by_attr_and_get_attr(filter(lambda permission: permission['policyUri'] == "urn:replicon:policy:user", response),
                    'permissionSet.name', dag_run.conf['user_permissions']['supervisor_end_user_permission']['name']),
                "supervisor_permission": rail.find_first_by_attr_and_get_attr(filter(lambda permission: permission['policyUri'] == "urn:replicon:policy:supervision", response),
                    'permissionSet.name', dag_run.conf['user_permissions']['supervisor_user_permission']['name'])
            }

        get_supervisor_permission_details = rail.RepliconServiceOperator(
            task_id = "get_supervisor_permission_details",
            endpoint="services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data=lambda: {
                "userUri":  rail.result('get_supervisor_details')['user_uri'],
            },
            data_handler=get_supervisor_permission_details_data_handler
        )

        is_supervisor_permission_missing = rail.IfOperator(
            task_id = "is_supervisor_permission_missing",
            test= lambda: not bool(rail.result("get_supervisor_permission_details")['supervisor_permission']),
            yes_task="assign_supervisor_permission",
            no_task="is_end_user_permission_missing"
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id = "assign_supervisor_permission",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda dag_run: {
                "userUri": rail.result('get_supervisor_details')['user_uri'],
                "permissionSetUri": dag_run.conf['user_permissions']['supervisor_user_permission']['uri']
            }
        )

        is_end_user_permission_missing = rail.IfOperator(
            task_id = "is_end_user_permission_missing",
            test= lambda: not bool(rail.result("get_supervisor_permission_details")['end_user_permission']),
            yes_task="assign_end_user_permission",
            no_task = "assign_supervisor_to_user"
        )

        assign_end_user_permission = rail.RepliconServiceOperator(
            task_id = "assign_end_user_permission",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda dag_run: {
                "userUri": rail.result('get_supervisor_details')['user_uri'],
                "permissionSetUri": dag_run.conf['user_permissions']['supervisor_end_user_permission']['uri']
            }
        )

        def get_data_access_scope_to_assign_callable(dag_run):
            rail.set_result(key = "test", val = rail.load_json_artifact(dag_run.conf['employee_type_data']))
            _employee_type = list(map(lambda employee_type: {
                                            "employeeTypeGroup": {
                                                "uri": employee_type['uri']
                                            }
                                        }
                                    , rail.load_json_artifact(dag_run.conf['employee_type_data'])['employee_data_for_assignment']
                                ))
            _division = list(map(lambda division: {
                        "division": {
                            "uri": division['uri']
                        }
                    }
                , [item for item in rail.load_json_artifact(dag_run.conf['division_data'])
                   if item['parent']==rail.result("get_supervisor_details")["company"]])
            )

            return {
                "division": _division,
                "employee_type": _employee_type
            }

        get_data_access_scope_to_assign = rail.PythonOperator(
            task_id = "get_data_access_scope_to_assign",
            python_callable=get_data_access_scope_to_assign_callable
        )

        put_policy_access_scope_for_supervisor = rail.RepliconServiceOperator(
            task_id="put_policy_access_scope_for_supervisor",
            endpoint="/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser",
            data=lambda: {
                "userUri": rail.result('get_supervisor_details')['user_uri'],
                "policyDataAccessScopes": [
                     {
                        "policyUri": "urn:replicon:policy:user",
                        "divisions": rail.result("get_data_access_scope_to_assign")['division'],
                        "employeeTypeGroups": rail.result("get_data_access_scope_to_assign")['employee_type']
                    }
                ]
            }
        )

        assign_supervisor_to_user = rail.RepliconServiceOperator(
            task_id = "assign_supervisor_to_user",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data = lambda dag_run:{
                "userUri": rail.result("create_user")['uri'] if caller=="add" else dag_run.conf['user_uri'],
                "supervisorUri": rail.result('get_supervisor_details')['user_uri'],
                "dateRange": null if caller=="add" else {"startDate": dag_run.conf['json_formatted_dates']['supervisor_date'], "endDate": null}
            }
        )

        log_supervisor_for_reassignment_check = rail.WriteLogOperator(
            task_id = "log_supervisor_for_reassignment_check",
            message = "User Add | Supervisor re-assignment",
            log="{{dag_run.conf.supervisor_user_log}}",
            severity = "Success",
            properties = lambda dag_run: {
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'],
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": caller,
                "Status": "pending",
                "Details": "Supervisor Reassignment",
                "login_name": dag_run.conf['file_data']['email_id'],
                "user_uri|country": f"{get_user_uri(dag_run, 'create_user')}|{dag_run.conf['file_data']['country']}",
                "user_name": f"{dag_run.conf['file_data']['first_name']} {dag_run.conf['file_data']['last_name']}",
                "supervisor_login_name": f'''{dag_run.conf['file_data']['supervisor_email_id']}|{dag_run.conf['file_data']['supervisor_id']}|{
                    dag_run.conf['file_data']['supervisor_f_name']}|{dag_run.conf['file_data']['supervisor_l_name']}''',
                "effective_date": dag_run.conf['json_formatted_dates']['supervisor_date'],
                "user_log": dag_run.conf['user_log'],
                "supervisor_end_user_permission": {
                    "name": dag_run.conf['user_permissions']['supervisor_end_user_permission']['name'],
                    "uri": dag_run.conf['user_permissions']['supervisor_end_user_permission']['uri']
                } if dag_run.conf['user_permissions']['supervisor_end_user_permission'] else {},
                "supervisor_user_permission":{
                    "name": dag_run.conf['user_permissions']['supervisor_user_permission']['name'],
                    "uri": dag_run.conf['user_permissions']['supervisor_user_permission']['uri']
                } if dag_run.conf['user_permissions']['supervisor_user_permission'] else {},
                'aus_supervisor_end_user_permission': {},
                'parent_company': dag_run.conf['file_data']['parent_company']
            }
        )

        end_supervisor_assignment = rail.EmptyOperator(
            task_id = "end_supervisor_assignment"
        )

        start_supervisor_assignment >> is_profile_status_enabled >> rail.Label("No") >> end_supervisor_assignment
        is_profile_status_enabled >> rail.Label("Yes") >> is_supervisor_id_user_id_same

        is_supervisor_id_user_id_same >> rail.Label("Yes") >> log_supervisor_id_user_id_same >> end_supervisor_assignment
        is_supervisor_id_user_id_same >> rail.Label("No") >> is_caller_update >> rail.Label("No") >> get_supervisor_details

        is_caller_update >> rail.Label("Yes") >> get_current_supervisor_assignment >> can_update_supervisor
        can_update_supervisor >> rail.Label("Yes") >> get_supervisor_details
        can_update_supervisor >> rail.Label("No") >> end_supervisor_assignment

        get_supervisor_details >> is_supervisor_found >> rail.Label("No") >> log_supervisor_for_reassignment_check  >> end_supervisor_assignment
        is_supervisor_found >> rail.Label("Yes") >> is_supervisor_company_present >> rail.Label("Yes") >> is_supervisor_disabled
        is_supervisor_company_present >> rail.Label("No") >> log_supervisor_for_reassignment_check  >> end_supervisor_assignment

        is_supervisor_disabled >> rail.Label("No") >> get_supervisor_permission_details >> is_supervisor_permission_missing
        is_supervisor_disabled >> rail.Label("Yes") >> log_supervisor_for_reassignment_check >> end_supervisor_assignment

        is_supervisor_permission_missing >> rail.Label("No") >> is_end_user_permission_missing
        is_supervisor_permission_missing >> rail.Label("Yes") >> assign_supervisor_permission >> is_end_user_permission_missing

        is_end_user_permission_missing >> rail.Label("Yes") >> assign_end_user_permission >> \
            get_data_access_scope_to_assign >> put_policy_access_scope_for_supervisor >> assign_supervisor_to_user
        is_end_user_permission_missing >> rail.Label("No") >> assign_supervisor_to_user >> end_supervisor_assignment

    return start_supervisor_assignment, end_supervisor_assignment
