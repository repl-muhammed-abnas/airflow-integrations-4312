from pendulum import datetime
import rail
from rail.lib.ecid import get_dagrun_ecid
from dxctechnology.workday_user_import_v1.user_import_philippines.utils.request_payload import get_todays_date_in_json
from dxctechnology.workday_user_import_v1.user_import_philippines.utils.custom_methods import convert_json_date_to_date
from airflow.models import Variable
from datetime import timedelta

null = None

# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_process_supervisor_assignment,
        description="dxctechnology workday user sync Master",
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        company_key=config.company_key,
        max_active_runs=config.max_active_run_process_each_users_philippines
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name_philippines, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="get_supervisor_details"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_supervisor_details",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        def get_supervisor_details_request_payload(dag_run):
            supervisor_id = dag_run.conf['supervisor_login_name'].split('|')[1]
            login_name = dag_run.conf['supervisor_login_name'].split('|')[0]
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
                            "text": supervisor_id,
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
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
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
                                "text": login_name,
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
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }

        def get_supervisor_details_data_handler(response, dag_run):
            if not response['rows']:
                return {"user_uri": None}
            rail.set_result(key="response", val=response)
            supervisor_id = dag_run.conf['supervisor_login_name'].split('|')[1]
            login_name = dag_run.conf['supervisor_login_name'].split('|')[0]

            return_response = list(filter(lambda emp_record: emp_record['emp_id'] == supervisor_id and\
                                            emp_record['login_name'] == login_name

                ,map(lambda record: {
                    "user": record['cells'][0],
                    "user_uri": record['cells'][0].get('uri'),
                    "login_name": record['cells'][1].get('textValue'),
                    "emp_id": record['cells'][2].get('textValue'),
                    "status": record['cells'][3].get('textValue'),
                    "division": record['cells'][4].get('textValue'),
                    "division_uri": record['cells'][4].get('uri'),
                    "parent_div": record['cells'][4].get('cellCollection', [{}])[0].get('textValue'),
                    "parent_div_uri": record['cells'][4].get('cellCollection', [{}])[0].get('uri'),
                    "division_full_path": rail.smartjoin_by_delim([item['textValue'] for item in record['cells'][4].get('cellCollection', [])], '*|*'),
                }, response['rows'])))
            return return_response[0] if return_response else {"user_uri": None}

        get_supervisor_details = rail.RepliconServiceOperator(
            task_id="get_supervisor_details",
            endpoint="/services/UserListService1.svc/GetData",
            data=get_supervisor_details_request_payload,
            data_handler=get_supervisor_details_data_handler
        )

        is_user_not_found = rail.IfOperator(
            task_id = "is_user_not_found",
            test=lambda: not bool(rail.result("get_supervisor_details")['user_uri']),
            yes_task="log_exception_user_not_found",
            no_task="is_user_disabled",
        )

        log_exception_user_not_found = rail.WriteLogOperator(
            task_id = "log_exception_user_not_found",
            log="{{dag_run.conf.user_log}}",
            message = "User Supervisor Exception",
            severity = "Exception",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['Userid'],
                "Email": dag_run.conf['Email'],
                "Action": dag_run.conf['Action'],
                "Status": "Exception",
                "Details": "Supervisor information doesn’t exist in Replicon or Supervisor login name and ID do not match"
            }
        )

        is_user_disabled = rail.IfOperator(
            task_id = "is_user_disabled",
            test=lambda: rail.result("get_supervisor_details")['status'] == "False",
            yes_task="enable_login",
            no_task="get_user_assigned_permissions"
        )

        enable_login = rail.RepliconServiceOperator(
            task_id="enable_login",
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data=lambda:{
                "userUri": rail.result("get_supervisor_details")['user_uri']
            }
        )

        def get_supervisor_permission_details_data_handler(response, dag_run):
            rail.set_result(val=response, key="response")
            return {
                "connect_supervisor_end_user_permission": rail.find_first_by_attr_and_get_attr(response,
                                        'permissionSet.name', dag_run.conf['aus_supervisor_end_user_permission'].get('name')),
                "end_user_permission": rail.find_first_by_attr_and_get_attr(response,
                                        'permissionSet.name', dag_run.conf['supervisor_end_user_permission'].get('name')),
                "supervisor_permission":rail.find_first_by_attr_and_get_attr(response,
                                        'permissionSet.name', dag_run.conf['supervisor_user_permission'].get('name'))
            }

        get_user_assigned_permissions = rail.RepliconServiceOperator(
            task_id="get_user_assigned_permissions",
            endpoint="services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data=lambda:{
                "userUri": rail.result("get_supervisor_details")['user_uri']
            },
            data_handler = get_supervisor_permission_details_data_handler
        )

        is_user_does_not_have_supervisor_permission = rail.IfOperator(
            task_id = "is_user_does_not_have_supervisor_permission",
            test=lambda: not bool(rail.result("get_user_assigned_permissions")['supervisor_permission']),
            yes_task="assign_supervisor_permission",
            no_task="is_user_does_not_have_end_user_permission"
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id = "assign_supervisor_permission",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda dag_run: {
                "userUri": rail.result('get_supervisor_details')['user_uri'],
                "permissionSetUri": dag_run.conf['supervisor_user_permission'].get('uri')
            }
        )

        is_user_does_not_have_end_user_permission = rail.IfOperator(
            task_id = "is_user_does_not_have_end_user_permission",
            test=lambda: not bool(rail.result("get_user_assigned_permissions")['end_user_permission']),
            yes_task="assign_end_user_permission",
            no_task="is_user_for_add"
        )

        assign_end_user_permission = rail.RepliconServiceOperator(
            task_id = "assign_end_user_permission",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda dag_run: {
                "userUri": rail.result('get_supervisor_details')['user_uri'],
                "permissionSetUri": dag_run.conf['supervisor_end_user_permission'].get('uri')
            }
        )

        def get_data_access_scope_to_assign_callable(dag_run):

            _employee_type = list(map(lambda employee_type: {
                                            "employeeTypeGroup": {
                                                "uri": employee_type['uri']
                                            }
                                        }
                                    , rail.load_json_artifact(dag_run.conf['employee_type_data'])['employee_data_for_assignment'])
                                )
            parent_company_code = dag_run.conf['parent_company']
            _division = []
            if rail.result("get_supervisor_details")['division']:
                get_all_division_under_parent = list(filter(lambda item: item['parent']==parent_company_code, rail.load_json_artifact(dag_run.conf['division_data'])))
                _division = list(map(lambda division: {
                                                "division": {
                                                    "uri": division['uri']
                                                }
                                            }
                                        , get_all_division_under_parent)
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

        is_user_for_add = rail.IfOperator(
            task_id = "is_user_for_add",
            test = lambda dag_run:  dag_run.conf['Action'].lower() == 'add',
            yes_task="assign_initial_supervisor",
            no_task="update_supervisor"
        )

        assign_initial_supervisor = rail.RepliconServiceOperator(
            task_id = "assign_initial_supervisor",
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "initialSupervisorUri": rail.result('get_supervisor_details')['user_uri'],
                "scheduleEntries": []
            }
        )

        def get_update_supervisor_payload(dag_run):

            supervisor_effective_date_to_apply = get_todays_date_in_json() if convert_json_date_to_date(dag_run.conf['effective_date']) < convert_json_date_to_date(get_todays_date_in_json())\
                                                                else dag_run.conf['effective_date']

            return {
                "userUri": dag_run.conf['user_uri'],
                "supervisorUri": rail.result('get_supervisor_details')['user_uri'],
                "dateRange": {
                    "startDate": supervisor_effective_date_to_apply,
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }

        update_supervisor = rail.RepliconServiceOperator(
            task_id = "update_supervisor",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=get_update_supervisor_payload
        )

        def has_any_exception_callable(dag_run):
            get_data_access_scope_to_assign = rail.result('get_data_access_scope_to_assign')
            if not get_data_access_scope_to_assign:
                return False
            if (get_data_access_scope_to_assign and not get_data_access_scope_to_assign['division']):
                return True

            return False

        has_any_exception = rail.IfOperator(
            task_id = "has_any_exception",
            test=has_any_exception_callable,
            yes_task="log_exception_user",
            no_task="catch_and_log_error"
        )

        log_exception_user = rail.WriteLogOperator(
            task_id = "log_exception_user",
            log="{{dag_run.conf.user_log}}",
            message = "User Supervisor Exception",
            severity = "Exception",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['Userid'],
                "Email": dag_run.conf['Email'],
                "Action": dag_run.conf['Action'],
                "Status": "Exception",
                "Details": f"Company code restriction not added for supervisor with id {rail.result('get_supervisor_details')['emp_id']} since no company code assigned on the user profile"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            trigger_rule = "one_failed",
            log="{{dag_run.conf.user_log}}",
            message = "User Add Error",
            severity = "Error",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['Userid'],
                "Email": dag_run.conf['Email'],
                "Action": dag_run.conf['Action'],
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> get_supervisor_details

        get_supervisor_details >> is_user_not_found >> rail.Label("Yes") >> log_exception_user_not_found >> rail.Label("On Error") >> catch_and_log_error
        is_user_not_found >> rail.Label("No") >> is_user_disabled >> rail.Label("Yes") >> enable_login >> get_user_assigned_permissions
        is_user_disabled >> rail.Label("No") >> get_user_assigned_permissions >> is_user_does_not_have_supervisor_permission
        is_user_does_not_have_supervisor_permission >> rail.Label("Yes") >> assign_supervisor_permission >> is_user_does_not_have_end_user_permission
        is_user_does_not_have_supervisor_permission >> rail.Label("No") >> is_user_does_not_have_end_user_permission

        is_user_does_not_have_end_user_permission >> rail.Label("Yes") >> assign_end_user_permission >>get_data_access_scope_to_assign >> put_policy_access_scope_for_supervisor
        put_policy_access_scope_for_supervisor >> is_user_for_add >> rail.Label("Yes") >> assign_initial_supervisor >> has_any_exception
        is_user_does_not_have_end_user_permission >> rail.Label("No")>> is_user_for_add
        is_user_for_add >> rail.Label("No") >> update_supervisor >> has_any_exception >> rail.Label("Yes") >> log_exception_user
        has_any_exception >> rail.Label("No") >> catch_and_log_error
        log_exception_user >> rail.Label("On Error") >> catch_and_log_error

        return dag

rail.for_each_instance(create_dag)
