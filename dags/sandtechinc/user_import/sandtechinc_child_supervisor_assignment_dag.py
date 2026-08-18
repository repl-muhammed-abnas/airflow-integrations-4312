"""
Sand Tech Inc - Child DAG for Supervisor Assignment
Handles pending supervisor assignments when supervisor wasn't found during initial processing
"""

from datetime import timedelta, datetime
import itertools
import pendulum
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.supervisor_child_dagid,
        description=f'Sand Tech Inc - Child Supervisor Assignment {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_supervisor'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_supervisor',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ========== SEARCH SUPERVISOR BY EMAIL ==========
        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def all_result_data_handler(result, email):
            flattened_rows = list(itertools.chain(*[x['rows'] for x in result]))
            existing_user = [
                {
                    'username': row['cells'][0].get('textValue'),
                    'employeeid': row['cells'][2].get('textValue'),
                    'status': row['cells'][3].get('textValue'),
                    'loginname': row['cells'][1].get('textValue'),
                    'useruri': row['cells'][1].get('uri')
                }
                for row in flattened_rows
                if row['cells'][1].get('textValue', '').lower() == email.lower()
            ]
            return existing_user[0] if existing_user else {}

        search_supervisor = rail.RepliconServicePageOperator(
            task_id="search_supervisor",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['manager_email']
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result, dag_run: all_result_data_handler(
                result, dag_run.conf['manager_email'])
        )

        # ========== PARSE EFFECTIVE DATE ==========
        def parse_date(date_str, date_format):
            if not date_str:
                return None
            try:
                parsed = datetime.strptime(date_str.strip(), date_format)
                return {
                    'year': parsed.year,
                    'month': parsed.month,
                    'day': parsed.day
                }
            except ValueError:
                return None

        parse_effective_date = rail.PythonOperator(
            task_id='parse_effective_date',
            python_callable=lambda dag_run: parse_date(
                dag_run.conf.get('effective_date'), dag_run.conf.get('date_format', '%d/%m/%Y'))
        )

        # ========== CHECK SUPERVISOR FOUND ==========
        supervisor_found = rail.IfOperator(
            task_id='supervisor_found',
            test='{{ result("search_supervisor") | is_truthy }}',
            yes_task="check_supervisor_not_self",
            no_task="log_supervisor_not_found",
        )

        log_supervisor_not_found = rail.WriteLogOperator(
            task_id='log_supervisor_not_found',
            message="Exception - Supervisor not found",
            severity="Exception",
            properties={
                "Empid": "{{ dag_run.conf.employee_id }}",
                "Username": "{{ dag_run.conf.username }}",
                "Action": "Supervisor Assignment",
                "Status": "Exception",
                "Details": "Supervisor not assigned - Manager {{ dag_run.conf.manager_email }} not found in Replicon",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        # ========== CHECK SUPERVISOR IS NOT SELF ==========
        check_supervisor_not_self = rail.IfOperator(
            task_id='check_supervisor_not_self',
            test='{{ result("search_supervisor").useruri != dag_run.conf.useruri }}',
            yes_task="get_supervisor_details",
            no_task="log_supervisor_is_self",
        )

        log_supervisor_is_self = rail.WriteLogOperator(
            task_id='log_supervisor_is_self',
            message="Exception - Supervisor is self",
            severity="Exception",
            properties={
                "Empid": "{{ dag_run.conf.employee_id }}",
                "Username": "{{ dag_run.conf.username }}",
                "Action": "Supervisor Assignment",
                "Status": "Exception",
                "Details": "Supervisor not assigned - User and Supervisor are the same person",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        # ========== GET SUPERVISOR DETAILS ==========
        get_supervisor_details = rail.RepliconServiceOperator(
            task_id='get_supervisor_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [{
                    "uri": "{{ result('search_supervisor').useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                }],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else None
        )

        # ========== CHECK SUPERVISOR IS ENABLED ==========
        supervisor_is_enabled = rail.IfOperator(
            task_id='supervisor_is_enabled',
            test='{{ result("get_supervisor_details").userDetails.isEnabled == true }}',
            yes_task="check_supervisor_permission",
            no_task="log_supervisor_disabled",
        )

        log_supervisor_disabled = rail.WriteLogOperator(
            task_id='log_supervisor_disabled',
            message="Exception - Supervisor disabled",
            severity="Exception",
            properties={
                "Empid": "{{ dag_run.conf.employee_id }}",
                "Username": "{{ dag_run.conf.username }}",
                "Action": "Supervisor Assignment",
                "Status": "Exception",
                "Details": "Supervisor not assigned - Manager {{ dag_run.conf.manager_email }} is disabled in Replicon",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        # ========== CHECK SUPERVISOR HAS PERMISSION ==========
        def get_supervisor_permission():
            supervisor_data = rail.result('get_supervisor_details')
            if supervisor_data and supervisor_data.get('permissionSets'):
                for perm in supervisor_data['permissionSets']:
                    if perm.get('name') == 'Supervisor':
                        return perm.get('uri')
            return None

        check_supervisor_permission = rail.PythonOperator(
            task_id='check_supervisor_permission',
            python_callable=get_supervisor_permission
        )

        needs_supervisor_permission = rail.IfOperator(
            task_id='needs_supervisor_permission',
            test='{{ result("check_supervisor_permission") | is_falsy }}',
            yes_task="assign_supervisor_permission",
            no_task="assign_supervisor_to_user",
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id='assign_supervisor_permission',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('get_supervisor_details').userDetails.uri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisor_permission_uri }}"
            }
        )

        # ========== ASSIGN SUPERVISOR ==========
        assign_supervisor_to_user = rail.IfOperator(
            task_id='assign_supervisor_to_user',
            test='{{ dag_run.conf.action == "Add" }}',
            yes_task="assign_initial_supervisor",
            no_task="update_supervisor_schedule",
        )

        assign_initial_supervisor = rail.RepliconServiceOperator(
            task_id='assign_initial_supervisor',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('get_supervisor_details').userDetails.uri }}",
                "scheduleEntries": []
            }
        )

        update_supervisor_schedule = rail.RepliconServiceOperator(
            task_id='update_supervisor_schedule',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('get_supervisor_details')['userDetails']['uri'],
                "dateRange": {
                    "startDate": rail.result('parse_effective_date') if rail.result('parse_effective_date') else {
                        "year": pendulum.now(config.est_timezone).year,
                        "month": pendulum.now(config.est_timezone).month,
                        "day": pendulum.now(config.est_timezone).day
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        # ========== LOG SUCCESS ==========
        log_success = rail.WriteLogOperator(
            task_id='log_success',
            message="Success",
            severity="Success",
            properties={
                "Empid": "{{ dag_run.conf.employee_id }}",
                "Username": "{{ dag_run.conf.username }}",
                "Action": "Supervisor Assignment",
                "Status": "Success",
                "Details": "Supervisor {{ dag_run.conf.manager_email }} assigned successfully",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        log_error = rail.WriteLogOperator(
            task_id='log_error',
            message="{{ get_error_message() }}",
            severity="Error",
            trigger_rule='one_failed',
            properties={
                "Empid": "{{ dag_run.conf.employee_id }}",
                "Username": "{{ dag_run.conf.username }}",
                "Action": "Supervisor Assignment",
                "Status": "Error",
                "Details": "{{ get_error_message() }}",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        # ========== TASK DEPENDENCIES ==========
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> search_supervisor >> parse_effective_date >> supervisor_found

        supervisor_found >> rail.Label('Yes') >> check_supervisor_not_self
        supervisor_found >> rail.Label('No') >> log_supervisor_not_found >> log_to_sumo

        check_supervisor_not_self >> rail.Label('Yes') >> get_supervisor_details >> supervisor_is_enabled
        check_supervisor_not_self >> rail.Label('No') >> log_supervisor_is_self >> log_to_sumo

        supervisor_is_enabled >> rail.Label('Yes') >> check_supervisor_permission >> needs_supervisor_permission
        supervisor_is_enabled >> rail.Label('No') >> log_supervisor_disabled >> log_to_sumo

        needs_supervisor_permission >> rail.Label('Yes') >> assign_supervisor_permission >> assign_supervisor_to_user
        needs_supervisor_permission >> rail.Label('No') >> assign_supervisor_to_user

        assign_supervisor_to_user >> rail.Label('Yes') >> assign_initial_supervisor >> log_success
        assign_supervisor_to_user >> rail.Label('No') >> update_supervisor_schedule >> log_success

        log_success >> log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)