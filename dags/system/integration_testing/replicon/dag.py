"""
### System Integration Testing Replicon Operators

#### Purpose:
- This DAG tests all the operators under the <u>[rail/operators/replicon](https://github.com/replicon/replicon-airflow-library/tree/main/rail/rail/operators/replicon)</u> folder
- PWD state link: <u>https://pwd.rplcn.co/plid=2812</u>

#### Test Cases:
- Added tests for list replicon operator
- Added tests for create resource replicon operator
- Added tests for data_handler
- Added tests for response_filter
- Added tests for target
- Added tests for response_check
"""

from datetime import datetime, timedelta
import rail
from system.integration_testing import config
from system.integration_testing.replicon import python_callable_method, response_filter

null = None

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing_replicon_operators",
    description="System Integration Testing Replicon Operators",
    company_key=config.company_key,
    replicon_conn_id=config.replicon_conn_id,
    start_date=datetime(2022, 1, 1),
    group="system",
    max_active_runs=10,
    is_paused_upon_creation=True,
    default_args={
        "owner": "system",
        "replicon_conn_id": config.replicon_conn_id,
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "doc": __doc__,
    },
) as dag:

    rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

    log_message = "add message for DAG Run ECID {{ dag_run_ecid() }}"

    batch_task_operator = rail.BatchTaskRunOperator(
        task_id="batch_task_operator",
        start_task="search_adminuser_list_operator",
        end_task="delete_this_dagrun",
        execution_timeout=timedelta(hours=config.execution_timeout_hours)
    )

    search_adminuser_list_operator = rail.RepliconServiceOperator(
        task_id="search_adminuser_list_operator",
        endpoint="/services/UserListService1.svc/GetData",
        method="POST",
        data=lambda dag_run: {
            "page": "1",
            "pagesize": "10",
            "columnUris": ["urn:replicon:user-list-column:login-name"],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text",
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": dag_run.conf["admin_loginname"]
                    }
                }
            },
        },
        response_filter=lambda response: response.json()["d"][
            "rows"][0]["cells"][0]["uri"]
    )

    error_message = "Method Not Allowed for POST for run id: {{ dag_run_ecid() }}"
    assert_searchuser_response = rail.PythonOperator(
        task_id="assert_searchuser_response",
        python_callable=python_callable_method.assert_searchuser_method,
        op_args=[error_message]
    )

    create_user = rail.RepliconServiceOperator(
        task_id="create_user",
        endpoint="/services/importService1.svc/PutUser3",
        data=lambda dag_run: {
            "user": {
                "target": {
                    "loginName": dag_run.conf["loginname"],
                    "employeeId": dag_run.conf["employeeid"]
                },
                "firstname": dag_run.conf["firstname"],
                "lastname": dag_run.conf["lastname"],
                "employeeId": dag_run.conf["employeeid"],
                "securityConfiguration": {
                    "enabledAuthenticationTypeUris": [],
                    "isLoginEnabled": "true",
                    "loginName": dag_run.conf["loginname"],
                    "SSOName": str(dag_run.conf["firstname"])
                    + str(dag_run.conf["lastname"]),
                    "password": dag_run.conf["password"],
                }
            }
        }
    )

    error_message = "Mismatch in the create user data for run id: {{ dag_run_ecid() }}"
    assert_createuser_data = rail.PythonOperator(
        task_id="assert_createuser_data",
        python_callable=python_callable_method.assert_createuser_data,
        op_args=[error_message]
    )

    def page_handler(request, result):
        if len(result['rows']) > 0:
            request['page'] += 1
            return request
        return None

    search_createduser = rail.RepliconServicePageOperator(
        task_id="search_createduser",
        endpoint="/services/UserListService1.svc/GetData",
        data=lambda dag_run: {
            "page": 1,
            "pagesize": 10,
            "columnUris": [
                "urn:replicon:user-list-column:login-name",
                "urn:replicon:user-list-column:employee-id",
            ],
            "filterExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": dag_run.conf['loginname']
                    }
                }
            }
        },
        page_handler=page_handler,
        all_result_data_handler=response_filter.get_empid_by_loginname
    )

    error_message = "Incorrect data for run id: {{ dag_run_ecid() }}"
    assert_repliconpage_operator = rail.PythonOperator(
        task_id="assert_repliconpage_operator",
        python_callable=python_callable_method.assert_repliconpage_operator,
        op_args=[error_message]
    )

    get_user_details = rail.RepliconServiceOperator(
        task_id="get_user_details",
        endpoint="/services/UserService1.svc/GetUserDetails",
        target='artifact',
        data={
            "userUri": "{{ result('create_user').uri }}"
        }
    )

    error_message = "Target must be result or artifact for run id: {{ dag_run_ecid() }}"
    assert_target = rail.PythonOperator(
        task_id="assert_target",
        python_callable=python_callable_method.assert_target_value,
        op_args=[error_message]
    )

    get_all_employeetypegrouplist_data = rail.RepliconServicePageOperator(
        task_id="get_all_employeetypegrouplist_data",
        endpoint="services/EmployeeTypeGroupListService1.svc/GetData",
        data=lambda: {
            "page": 1,
            "pagesize": 10,
            "columnUris": [
                "urn:replicon:employee-type-group-list-column:employee-type-group",
                "urn:replicon:employee-type-group-list-column:code",
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:employee-type-group-list-filter:effectively-enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "bool": "true"
                    }
                }
            },
        },
        page_handler=page_handler,
        all_result_data_handler=response_filter.get_filtered_employee_grp
    )

    error_message = "incorrect response_filter for run id: {{ dag_run_ecid() }}"
    assert_employeetype_filter_data = rail.PythonOperator(
        task_id="assert_employeetype_filter_data",
        python_callable=python_callable_method.assert_employeetypegrouplist_value,
        op_args=[error_message]
    )

    def check_response(response):
        if hasattr(response, 'status_code'):
            return response.status_code == 200
        if hasattr(response, 'ok'):
            return response.ok
        if hasattr(response, 'status'):
            return response.status == 200
        raise ValueError("Unknown response object")
    bulk_get_users = rail.RepliconServiceOperator(
        task_id="bulk_get_users",
        endpoint="/services/ImportService1.svc/BulkGetUsers3",
        data={
            "users": [
                {
                    "uri": "{{ result('create_user').uri }}"
                }
            ],
            "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission",
        },
        response_check=check_response
    )

    error_message = "Unknown response object for run id: {{ dag_run_ecid() }}"
    assert_response_check = rail.PythonOperator(
        task_id="assert_response_check",
        python_callable=python_callable_method.assert_response_check,
        op_args=[error_message]
    )

    get_repliconusers = rail.RepliconServiceOperator(
        task_id="get_repliconusers",
        endpoint="/services/UserService1.svc/GetAllUsers",
        data_handler=lambda response: [user['loginName'] for user in response]
    )

    get_user_uris = rail.RepliconServiceCallForEachItemOperator(
        task_id="get_user_uris",
        endpoint="/services/UserService1.svc/GetUser2",
        items=lambda: rail.result("get_repliconusers"),
        execution_timeout=timedelta(hours=config.execution_timeout_hours),
        flatten=True,
        data={
            "user": {
                "loginName": "{{ items | first_or_default }}"
            }
        },
        data_handler=lambda resp: resp['uri'],
        batch_size=1
    )

    error_message = "Mismatch in the data for run id: {{ dag_run_ecid() }}"
    assert_input_data = rail.PythonOperator(
        task_id="assert_input_data",
        python_callable=python_callable_method.assert_input_data,
        op_args=[error_message]
    )

    def check_response_status(response):
        if hasattr(response, "status_code"):
            return response.status_code == 200
        elif hasattr(response, "ok"):
            return response.ok
        elif hasattr(response, "status"):
            return response.status == 200
        else:
            raise ValueError("Unknown response object")

    check_response_data = rail.RepliconServiceCallForEachItemOperator(
        task_id="check_response_data",
        endpoint="/services/UserService1.svc/GetUser2",
        items=lambda: rail.result("get_repliconusers"),
        execution_timeout=timedelta(hours=config.execution_timeout_hours),
        flatten=True,
        data=lambda item: {
            "user": {
                "loginName": item
            }
        },
        response_check=check_response_status
    )

    error_message = "Unknown response objectTrigger Failed for run id: {{ dag_run_ecid() }}"
    assert_responsecheck = rail.PythonOperator(
        task_id="assert_responsecheck",
        python_callable=python_callable_method.assert_responsecheck,
        op_args=[error_message]
    )

    create_user_delete_batch = rail.RepliconServiceOperator(
        task_id='create_user_delete_batch',
        endpoint="services/UserService1.svc/CreateUserDeleteBatch",
        data=lambda: {
            "userUris": [
                rail.result('create_user')['uri']
            ]
        }
    )
    execute_user_delete_batch = rail.RepliconServiceOperator(
        task_id='execute_user_delete_batch',
        endpoint="services/UserService1.svc/ExecuteUserDeleteBatch",
        data={
            "userDeleteBatchUri": "{{ result('create_user_delete_batch') }}"
        }
    )

    wait_for_delete_batch = rail.RepliconBatchExecutionSensor(
        task_id='wait_for_delete_batch',
        batch_uri='{{ result("create_user_delete_batch") }}',
        replicon_conn_id=config.replicon_conn_id,
        tasks_to_retry=['create_user_delete_batch',
                        'execute_user_delete_batch'],
        retries=3,
        execution_timeout=timedelta(hours=config.execution_timeout_hours)
    )

    error_message = "Batch execution trigger failed for run id: {{ dag_run_ecid() }}"
    assert_wait_for_delete_batch = rail.PythonOperator(
        task_id='assert_wait_for_delete_batch',
        python_callable=python_callable_method.assert_batch_response,
        op_args=[error_message]
    )

    delete_user = rail.RepliconServiceOperator(
        task_id="delete_user",
        endpoint="/services/UserService1.svc/Delete",
        data={
            "userUri": "{{ result('create_user').uri }}"
        }
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id="delete_this_dagrun",
        trigger_rule="none_failed"
    )

    (
        batch_task_operator
        >> rail.Label("Test Replicon List Operator")
        >> search_adminuser_list_operator
        >> assert_searchuser_response
        >> rail.Label("Test Replicon Operator, User creation")
        >> create_user
        >> assert_createuser_data
        >> rail.Label("Test Replicon Service Page Operator")
        >> search_createduser
        >> assert_repliconpage_operator
        >> rail.Label("Test Replicon Operator, store as artifact")
        >> get_user_details
        >> assert_target
        >> rail.Label("Test Replicon Service Page Operator")
        >> get_all_employeetypegrouplist_data
        >> assert_employeetype_filter_data
        >> rail.Label("Test Replicon Operator, check response")
        >> bulk_get_users
        >> assert_response_check
        >> rail.Label("Test Loop Operators")
        >> get_repliconusers
        >> get_user_uris
        >> assert_input_data
        >> check_response_data
        >> assert_responsecheck
        >> rail.Label("Create user delete batch operator")
        >> create_user_delete_batch
        >> execute_user_delete_batch
        >> wait_for_delete_batch
        >> rail.Label("Test response values")
        >> assert_wait_for_delete_batch
        >> delete_user
        >> delete_this_dagrun
    )

    batch_task_operator >> rail.Label(
        "Mark DAGRun for deletion") >> delete_this_dagrun
