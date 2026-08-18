from datetime import timedelta
import rail
from airflow.models import Variable
from bamboohr.user_import.utils import bamboohr_utils

null = None


# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_bamboohr_{config.region.replace('-', '_')}_user_import_{config.instance}",
        description=f'BambooHR {config.region} User Import {config.instance}',
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

        def get_employees_details(response):
            """
            Backward-compatible handler that maintains existing response structure.
            Works with new Datasets API while preserving downstream compatibility.
            """
            # Handle both old format (response['employees']) and new format (direct array)
            employees_data = response.get('employees', response) if isinstance(
                response, dict) else response

            # Apply the exact same transformation logic as before
            return list(filter(lambda x: x['status'].lower() == "active", list(map(lambda item: {
                "id": item["id"],
                "firstname": item['firstName'],
                "lastname": item['lastName'],
                "employeenumber": item['employeeNumber'],
                "startdate": item['hireDate'],
                "workemail": item['workEmail'],
                "status": item['status'],
                "jobtitle": item['jobTitle'],
                "location": item['location']
            }, employees_data)))) if employees_data else []

        bamboohr_changed_employees = rail.BambooHROperator(
            task_id='bamboohr_changed_employees',
            company_domain='{{ dag_run.conf.company_domain }}',
            request_method='GET',
            endpoint="/employees/changed?since={{ result('get_lastsync_time_and_current_time').last_synctime }}",
            bamboohr_conn_id='{{ dag_run.conf.bamboohr_conn_id }}',
            data_handler=lambda response: bamboohr_utils.extract_changed_employee_ids(response)
        )

        has_bamboohr_employees_data = rail.IfOperator(
            task_id='has_bamboohr_employees_data',
            test=lambda: len(rail.result('bamboohr_changed_employees')) > 0,
            yes_task='fetch_employee_details',
            no_task='should_log_history'
        )

        declare_employee_details_list = rail.SetVariableOperator(
            task_id='declare_employee_details_list',
            append=False,
            name='employee_details_list',
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
            ",".join(bamboohr_utils.USER_IMPORT_FIELDS),
            bamboohr_conn_id='{{ dag_run.conf.bamboohr_conn_id }}',
            data_handler=lambda response: bamboohr_utils.process_single_employee_response(
                response, bamboohr_utils.USER_IMPORT_FIELD_MAPPING)
        )

        filter_active_employee = rail.IfOperator(
            task_id='filter_active_employee',
            test=lambda: (rail.result('fetch_single_employee') and
                          rail.result('fetch_single_employee').get('status', '').lower() == 'active'),
            yes_task='add_employee_to_list',
            no_task='foreach_changed_employees_end'
        )

        add_employee_to_list = rail.SetVariableOperator(
            task_id='add_employee_to_list',
            append=True,
            name='employee_details_list',
            value=lambda: rail.result('fetch_single_employee')
        )

        foreach_changed_employees_end = rail.EmptyOperator(
            task_id='foreach_changed_employees_end'
        )

        fetch_employee_details = rail.EmptyOperator(
            task_id='fetch_employee_details'
        )

        check_is_legacy_employee_type = rail.RepliconServiceOperator(
            task_id='check_is_legacy_employee_type',
            endpoint="/services/EmployeeTypeService1.svc/GetIsLegacyEmployeeTypeEnabled",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
        )

        declare_employee_list = rail.SetVariableOperator(
            task_id='declare_employee_list',
            append=False,
            name='employee_list',
            value=[]
        )

        get_filtered_employees = rail.GetVariableOperator(
            task_id='get_filtered_employees',
            name='employee_details_list'
        )

        foreach_employees = rail.ForEachOperator(
            task_id='foreach_employees',
            items=lambda: rail.result('get_filtered_employees')['value'],
            start_task='search_users',
            end_task='foreach_employees_end'
        )

        def get_userdetails_by_loginname(response):
            # pylint: disable=line-too-long
            return list(filter(lambda x: rail.result('foreach_employees')['workemail'] and x['loginname'] == rail.result('foreach_employees')['workemail'], list(map(lambda item: {
                "employeeid": item['cells'][1].get('textValue'),
                "useruri": item['cells'][0]['uri'],
                "loginname": item['cells'][0]['textValue'],
                "startdate": item['cells'][2]['dateValue']
            }, response['rows'])))) if response['rows'] else []

        search_users = rail.RepliconServiceOperator(
            task_id='search_users',
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
                            'text': "{{ result('foreach_employees').workemail }}"
                        }
                    }
                },
            },
            data_handler=get_userdetails_by_loginname
        )

        if_login_name_not_equals_to_bamboohr_workemail = rail.IfOperator(
            task_id='if_login_name_not_equals_to_bamboohr_workemail',
            test=lambda: rail.result('foreach_employees')['workemail'] and len(
                rail.result('search_users')) == 0,
            yes_task="update_employee_list",
            no_task="foreach_employees_end",
        )

        update_employee_list = rail.SetVariableOperator(
            task_id='update_employee_list',
            append=True,
            name='{{ result("declare_employee_list").name }}',
            value=lambda: rail.result('foreach_employees')
        )

        foreach_employees_end = rail.EmptyOperator(
            task_id='foreach_employees_end',
        )

        get_employee_list = rail.GetVariableOperator(
            task_id='get_employee_list',
            name='employee_list'
        )

        declare_employee_table_records_list = rail.SetVariableOperator(
            task_id='declare_employee_table_records_list',
            append=False,
            name='employee_table_list',
            value=[]
        )

        foreach_employees_table_record = rail.ForEachOperator(
            task_id='foreach_employees_table_record',
            items=lambda: rail.result('get_employee_list')['value'],
            start_task='get_endpoint',
            end_task='foreach_employees_table_record_end'
        )

        def get_endpoint_detail():
            return str("/employees/" + rail.result("foreach_employees_table_record")['id'] + "/tables/jobInfo")

        get_endpoint = rail.PythonOperator(
            task_id="get_endpoint",
            python_callable=get_endpoint_detail
        )

        get_table_records = rail.BambooHROperator(
            task_id='get_table_records',
            company_domain='{{ dag_run.conf.company_domain }}',
            request_method='GET',
            endpoint='{{ result("get_endpoint") }}',
            bamboohr_conn_id='{{ dag_run.conf.bamboohr_conn_id }}'
        )

        if_get_table_records_is_not_empty = rail.IfOperator(
            task_id='if_get_table_records_is_not_empty',
            test=lambda: len(rail.result('get_table_records')) > 0,
            yes_task="update_employee_table_list",
            no_task="update_employee_table_list_jobinfo_empty",
        )

        update_employee_table_list = rail.SetVariableOperator(
            task_id='update_employee_table_list',
            append=True,
            name='{{ result("declare_employee_table_records_list").name }}',
            value=lambda: rail.result('get_table_records')[0]
        )

        update_employee_table_list_jobinfo_empty = rail.SetVariableOperator(
            task_id='update_employee_table_list_jobinfo_empty',
            append=True,
            name='{{ result("declare_employee_table_records_list").name }}',
            # pylint: disable=line-too-long
            value=lambda: {
                "id": rail.result("foreach_employees_table_record")['employeeNumber'] if rail.result("foreach_employees_table_record").get('employeeNumber') else null,
                "employeeId": rail.result("foreach_employees_table_record")['id'],
                "date": rail.result("foreach_employees_table_record")['hiredate'] if rail.result("foreach_employees_table_record").get('hiredate') else '0000-00-00',
                "location": rail.result("foreach_employees_table_record")['location'] if rail.result("foreach_employees_table_record").get('location') else null,
                "department": rail.result("foreach_employees_table_record")['department'] if rail.result("foreach_employees_table_record").get('department') else null,
                "division": rail.result("foreach_employees_table_record")['division'] if rail.result("foreach_employees_table_record").get('division') else null,
                "jobTitle": rail.result("foreach_employees_table_record")['jobTitle'] if rail.result("foreach_employees_table_record").get('jobTitle') else null,
                "reportsTo": rail.result("foreach_employees_table_record")['supervisor'] if rail.result("foreach_employees_table_record").get('supervisor') else null,
            }
        )

        foreach_employees_table_record_end = rail.EmptyOperator(
            task_id='foreach_employees_table_record_end',
        )

        get_user_create_list = rail.GetVariableOperator(
            task_id='get_user_create_list',
            name='employee_table_list'
        )

        def get_merge_user_list():
            merged_list = []
            for employee in rail.result('get_employee_list')['value']:
                for user in rail.result('get_user_create_list')['value']:
                    if employee['id'] == user['employeeId']:
                        merged_employee = {**employee, **user}
                        merged_list.append(merged_employee)
            return merged_list
        get_merge_user_create_list = rail.PythonOperator(
            task_id='get_merge_user_create_list',
            python_callable=get_merge_user_list
        )

        ifadhoc_http_action_2_d_is_true_8 = rail.IfOperator(
            task_id='ifadhoc_http_action_2_d_is_true_8',
            test="{{ result('check_is_legacy_employee_type') | is_truthy }}",
            yes_task="trigger_user_create_legacy_child_dag",
            no_task="trigger_user_create_child_dag",
        )

        trigger_user_create_legacy_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_user_create_legacy_child_dag',
            retries=0,
            items=lambda: rail.result('get_merge_user_create_list'),
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f"standard_bamboohr_{config.region.replace('-', '_')}_user_create_legacy_child_dag_{config.instance}",
            conf=lambda dag_run, item: {
                'id': item.get('id'),
                'firstname': item.get('firstname'),
                'lastname': item.get('lastname'),
                'employeenumber': item.get('employeenumber'),
                'startdate': item.get('startdate'),
                'status': item.get('status'),
                'workemail': item.get('workemail'),
                'date': item.get('date'),
                'department': item.get('department'),
                'division': item.get('division'),
                'employeeid': item.get('employeeId'),
                'jobtitle': item.get('jobTitle'),
                'location': item.get('location'),
                'supervisor': item.get('reportsTo'),
                'company_key': dag_run.conf['company_key'],
                'bamboohr_conn_id': dag_run.conf['bamboohr_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id'],
            }
        )

        wait_for_user_create_legacy_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_user_create_legacy_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_user_create_legacy_child_dag") }}'
        )

        gather_user_legacy_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_legacy_error',
            dag_runs="{{ result('trigger_user_create_legacy_child_dag') }}",
            dagrun_task_id='catch_client_error',
            flatten=True
        )

        is_user_legacy_error = rail.IfOperator(
            task_id='is_user_legacy_error',
            # pylint: disable=line-too-long
            test="{{ (get_task_state('gather_user_legacy_error') == 'success' and result('gather_user_legacy_error') | length > 0)}}",
            yes_task='fail_user_legacy_error',
            no_task='should_log_history'
        )

        fail_user_legacy_error = rail.FailOperator(
            task_id='fail_user_legacy_error',
            message="{{ result('gather_user_legacy_error') | map_to_attr('error') | join('|') }}"
        )

        trigger_user_create_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_user_create_child_dag',
            retries=0,
            items=lambda: rail.result('get_merge_user_create_list'),
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f"standard_bamboohr_{config.region.replace('-', '_')}_user_create_child_dag_{config.instance}",
            conf=lambda dag_run, item: {
                'id': item.get('id'),
                'firstname': item.get('firstname'),
                'lastname': item.get('lastname'),
                'employeenumber': item.get('employeenumber'),
                'startdate': item.get('startdate'),
                'status': item.get('status'),
                'workemail': item.get('workemail'),
                'date': item.get('date'),
                'department': item.get('department'),
                'division': item.get('division'),
                'employeeid': item.get('employeeId'),
                'jobtitle': item.get('jobTitle'),
                'location': item.get('location'),
                'supervisor': item.get('reportsTo'),
                'company_key': dag_run.conf['company_key'],
                'bamboohr_conn_id': dag_run.conf['bamboohr_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id'],
            }
        )

        wait_for_user_create_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_user_create_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_user_create_child_dag") }}'
        )

        gather_user_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_error',
            dag_runs="{{ result('trigger_user_create_child_dag') }}",
            dagrun_task_id='catch_client_error',
            flatten=True
        )

        is_user_error = rail.IfOperator(
            task_id='is_user_error',
            test="{{ (get_task_state('gather_user_error') == 'success' and result('gather_user_error') | length > 0) }}",
            yes_task='fail_user_error',
            no_task='should_log_history'
        )

        fail_user_error = rail.FailOperator(
            task_id='fail_user_error',
            message="{{ result('gather_user_error') | map_to_attr('error') | join('|') }}"
        )

        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ not(get_task_state('has_bamboohr_employees_data') == 'success' and \
                result('has_bamboohr_employees_data') != 'fetch_employee_details') }}",
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
            integration_type='user_import'
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
        update_lastsync_time >> has_bamboohr_employees_data
        has_bamboohr_employees_data >> rail.Label(
            'Yes') >> fetch_employee_details
        fetch_employee_details >> declare_employee_details_list >> foreach_changed_employees >> fetch_single_employee >> filter_active_employee
        filter_active_employee >> rail.Label(
            'Yes') >> add_employee_to_list >> foreach_changed_employees_end
        filter_active_employee >> rail.Label(
            'No') >> foreach_changed_employees_end
        foreach_changed_employees >> foreach_changed_employees_end >> get_filtered_employees >> check_is_legacy_employee_type >> declare_employee_list >> foreach_employees >> search_users \
            >> if_login_name_not_equals_to_bamboohr_workemail
        if_login_name_not_equals_to_bamboohr_workemail >> rail.Label(
            'Yes') >> update_employee_list >> foreach_employees_end
        if_login_name_not_equals_to_bamboohr_workemail >> rail.Label(
            'No') >> foreach_employees_end
        foreach_employees >> foreach_employees_end >> get_employee_list >> declare_employee_table_records_list \
            >> foreach_employees_table_record >> get_endpoint >> get_table_records >> if_get_table_records_is_not_empty
        if_get_table_records_is_not_empty >> rail.Label(
            'Yes') >> update_employee_table_list >> foreach_employees_table_record_end
        if_get_table_records_is_not_empty >> rail.Label(
            'No') >> update_employee_table_list_jobinfo_empty >> foreach_employees_table_record_end
        foreach_employees_table_record >> foreach_employees_table_record_end >> get_user_create_list \
            >> get_merge_user_create_list >> ifadhoc_http_action_2_d_is_true_8
        ifadhoc_http_action_2_d_is_true_8 >> rail.Label(
            'Yes') >> trigger_user_create_legacy_child_dag >> wait_for_user_create_legacy_child_dag >> gather_user_legacy_error >> is_user_legacy_error
        is_user_legacy_error >> rail.Label(
            'Yes') >> fail_user_legacy_error >> should_log_history
        is_user_legacy_error >> rail.Label(
            'No') >> should_log_history
        ifadhoc_http_action_2_d_is_true_8 >> rail.Label(
            'No') >> trigger_user_create_child_dag >> wait_for_user_create_child_dag >> gather_user_error >> is_user_error
        is_user_error >> rail.Label(
            'Yes') >> fail_user_error >> should_log_history
        is_user_error >> rail.Label(
            'No') >> should_log_history
        has_bamboohr_employees_data >> rail.Label(
            'No') >> should_log_history
        should_log_history >> rail.Label(
            'Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label(
            'No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
