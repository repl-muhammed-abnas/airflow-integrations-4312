from datetime import datetime, timedelta
import rail
from airflow.models import Variable
from bamboohr.user_import.utils import bamboohr_utils


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/bamboohr/main_dag/config.py


# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_bamboohr_{config.region.replace('-', '_')}_disable_user_{config.instance}",
        description=f'BambooHR {config.region} Disable User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_lastsync_time_and_current_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_lastsync_time_and_current_time',
            end_task='should_log_history',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_lastsync_time_and_current_time = rail.GetLastSyncTimeOperator(
            task_id='get_lastsync_time_and_current_time',
            workflow_name=config.workflow,
            date_format='%Y-%m-%dT%H:%M:%SZ',
            initial_sync_time=lambda: bamboohr_utils.get_initial_sync_time(60),
            provider=config.provider
        )

        def get_inactive_employees(response):
            """
            Backward-compatible handler that maintains existing response structure.
            Works with new Datasets API while preserving downstream compatibility.
            """
            # Handle both old format (response['employees']) and new format (direct array)
            employees_data = response.get('employees', response) if isinstance(
                response, dict) else response

            # Apply the exact same transformation logic as before
            return list(filter(lambda x: x['status'].lower() == "inactive", list(map(lambda item: {
                "workemail": item['workEmail'],
                "enddate": item['terminationDate'],
                "status": item['status']
            }, employees_data)))) if employees_data else []

        bamboohr_changed_employees = rail.BambooHROperator(
            task_id='bamboohr_changed_employees',
            company_domain='{{ dag_run.conf.company_domain }}',
            request_method='GET',
            endpoint="/employees/changed?since={{ result('get_lastsync_time_and_current_time').last_synctime }}",
            bamboohr_conn_id='{{ dag_run.conf.bamboohr_conn_id }}',
            data_handler=lambda response: bamboohr_utils.extract_changed_employee_ids(response, action_filter='Updated')
        )

        has_changed_employees = rail.IfOperator(
            task_id='has_changed_employees',
            test=lambda: len(rail.result('bamboohr_changed_employees')) > 0,
            yes_task='fetch_employee_details',
            no_task='should_log_history'
        )

        declare_inactive_employee_list = rail.SetVariableOperator(
            task_id='declare_inactive_employee_list',
            append=False,
            name='inactive_employee_list',
            value=[]
        )

        foreach_changed_employees = rail.ForEachOperator(
            task_id='foreach_changed_employees',
            items=lambda: rail.result('bamboohr_changed_employees'),
            start_task='fetch_single_employee',
            end_task='foreach_changed_employees_end'
        )

        fetch_single_employee = rail.BambooHROperator(
            task_id='fetch_single_employee',
            company_domain='{{ dag_run.conf.company_domain }}',
            request_method='GET',
            endpoint="/employees/{{ result('foreach_changed_employees').id }}?fields=" +
            ",".join(bamboohr_utils.DISABLE_USER_FIELDS),
            bamboohr_conn_id='{{ dag_run.conf.bamboohr_conn_id }}',
            data_handler=lambda response: bamboohr_utils.process_single_employee_response(
                response, bamboohr_utils.DISABLE_USER_FIELD_MAPPING)
        )

        filter_inactive_employee = rail.IfOperator(
            task_id='filter_inactive_employee',
            test=lambda: (rail.result('fetch_single_employee') and
                          rail.result('fetch_single_employee').get('status', '').lower() == 'inactive'),
            yes_task='add_inactive_employee_to_list',
            no_task='foreach_changed_employees_end'
        )

        add_inactive_employee_to_list = rail.SetVariableOperator(
            task_id='add_inactive_employee_to_list',
            append=True,
            name='inactive_employee_list',
            value=lambda: rail.result('fetch_single_employee')
        )

        foreach_changed_employees_end = rail.EmptyOperator(
            task_id='foreach_changed_employees_end'
        )

        fetch_employee_details = rail.EmptyOperator(
            task_id='fetch_employee_details'
        )

        get_inactive_employees = rail.GetVariableOperator(
            task_id='get_inactive_employees',
            name='inactive_employee_list'
        )

        foreach_inactive_users = rail.ForEachOperator(
            task_id='foreach_inactive_users',
            items=lambda: rail.result('get_inactive_employees')['value'],
            start_task='search_user',
            end_task='foreach_inactive_users_end'
        )

        def get_userdetails_by_loginname(response):
            return list(filter(lambda x: x['loginname'] == rail.result('foreach_inactive_users')['workemail'], list(map(lambda item: {
                "employeeid": item['cells'][1].get('textValue'),
                "useruri": item['cells'][0]['uri'],
                "loginname": item['cells'][0]['textValue'],
                "startdate": item['cells'][2]['dateValue']
            }, response['rows'])))) if response['rows'] else []

        search_user = rail.RepliconServiceOperator(
            task_id='search_user',
            endpoint='/services/UserListService1.svc/GetData',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                'page': '1',
                'pagesize': '100',
                'columnUris': [
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    "urn:replicon:user-list-column:start-date"
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:login-name'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': "{{ result('foreach_inactive_users').workemail }}"
                        }
                    }
                },
            },
            data_handler=get_userdetails_by_loginname
        )

        if_login_name_uri_present = rail.IfOperator(
            task_id='if_login_name_uri_present',
            test=lambda: bool(len(rail.result('search_user'))
                              > 0 and rail.result('search_user')[0]['useruri']),
            yes_task="if_employee_terminationdate_present",
            no_task="foreach_inactive_users_end",
        )

        if_employee_terminationdate_present = rail.IfOperator(
            task_id='if_employee_terminationdate_present',
            test=lambda: rail.result('foreach_inactive_users')['enddate'] and rail.result(
                'foreach_inactive_users')['enddate'] != '0000-00-00',
            yes_task="update_user_terminationdate",
            no_task="disable_user",
        )

        def get_datetime_obj(date_str, fmt='%Y-%m-%d'):
            datetime_obj = datetime.strptime(date_str, fmt)
            return {
                'year': datetime_obj.year,
                'month': datetime_obj.month,
                'day': datetime_obj.day
            }
        update_user_terminationdate = rail.RepliconServiceOperator(
            task_id='update_user_terminationdate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "userUri": rail.result('search_user')[0]['useruri'],
                "dateRange": {
                    "startDate": rail.result('search_user')[0]['startdate'],
                    "endDate": get_datetime_obj(rail.result('foreach_inactive_users')['enddate'])
                }
            }
        )

        disable_user = rail.RepliconServiceOperator(
            task_id='disable_user',
            endpoint="/services/securityService1.svc/DisableLogin",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                "userUri": "{{ result('search_user')[0].useruri }}"
            }
        )

        foreach_inactive_users_end = rail.EmptyOperator(
            task_id='foreach_inactive_users_end',
        )

        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ not(get_task_state('has_changed_employees') == 'success' and \
                result('has_changed_employees') != 'fetch_employee_details') }}",
            trigger_rule='all_done',
            yes_task='log_dagrun_details_to_table',
            no_task='delete_this_dagrun'
        )

        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id='log_dagrun_details_to_table',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.hmac_secret
            },
            company_key='{{ dag_run.conf.company_key }}',
            connector_name='bamboohr',
            integration_type='disable_user'
        )

        update_lastsync_time = rail.SetLastSyncTimeOperator(
            task_id='update_lastsync_time',
            workflow_name=config.workflow,
            provider=config.provider,
            value_to_set='{{result("get_lastsync_time_and_current_time").current_time}}'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> should_log_history
        can_run_batch_task >> rail.Label(
            'No') >> get_lastsync_time_and_current_time >> bamboohr_changed_employees >> update_lastsync_time
        update_lastsync_time >> has_changed_employees
        has_changed_employees >> rail.Label('Yes') >> fetch_employee_details
        fetch_employee_details >> declare_inactive_employee_list >> foreach_changed_employees >> fetch_single_employee >> filter_inactive_employee
        filter_inactive_employee >> rail.Label(
            'Yes') >> add_inactive_employee_to_list >> foreach_changed_employees_end
        filter_inactive_employee >> rail.Label(
            'No') >> foreach_changed_employees_end
        foreach_changed_employees >> foreach_changed_employees_end >> get_inactive_employees >> foreach_inactive_users
        foreach_inactive_users >> search_user >> if_login_name_uri_present
        if_login_name_uri_present >> rail.Label(
            'Yes') >> if_employee_terminationdate_present
        if_login_name_uri_present >> rail.Label(
            'No') >> foreach_inactive_users_end
        if_employee_terminationdate_present >> rail.Label(
            'Yes') >> update_user_terminationdate >> disable_user >> foreach_inactive_users_end >> should_log_history
        if_employee_terminationdate_present >> rail.Label(
            'No') >> disable_user
        foreach_inactive_users >> foreach_inactive_users_end
        has_changed_employees >> rail.Label('No') >> should_log_history
        should_log_history >> rail.Label(
            'Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label(
            'No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
