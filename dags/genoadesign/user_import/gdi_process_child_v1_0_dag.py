
from datetime import timedelta
import itertools
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'genoadesign_user_import_process_child_v1_0_{config.instance}',
        description=f'Live|GDI_Process_Child_V1.0 {config.instance}',
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
            no_task='search_users_1'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_users_1',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def compose_user_details(response, loginname):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            users_info = list(filter(lambda x: x['loginname'] == loginname, map(lambda row: {
                'loginname': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'useruri': row['cells'][1]['uri']
            }, flaten_rows)))
            return users_info[0] if users_info else None

        search_users_1 = rail.RepliconServicePageOperator(
            task_id='search_users_1',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                "sort": [],
                "filterExpression": {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['loginname'],
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda response, dag_run: compose_user_details(
                response, dag_run.conf['loginname'])
        )

        if_log_checkiftheuserisavaialble_blank_3 = rail.IfOperator(
            task_id='if_log_checkiftheuserisavaialble_blank_3',
            test='''{{ result('search_users_1') | is_falsy }}''',
            yes_task="if_new_changed_profiles_loginstatus_blank_enabled_4",
            no_task="trigger_dag_run_genoadesign_user_import_gdi_child_update_user_v1_0async_7",
        )

        if_new_changed_profiles_loginstatus_blank_enabled_4 = rail.IfOperator(
            task_id='if_new_changed_profiles_loginstatus_blank_enabled_4',
            test='''{{ dag_run.conf.loginstatus | is_falsy or ( dag_run.conf.loginstatus | is_truthy and dag_run.conf.loginstatus | lower != 'enabled') }}''',
            yes_task="genoadi_user_import_logs_add_entry_5",
            no_task="trigger_dag_run_genoadesign_user_import_gdi_child_add_user_v1_0async_6",
        )

        genoadi_user_import_logs_add_entry_5 = rail.WriteLogOperator(
            task_id='genoadi_user_import_logs_add_entry_5',
            message="na",
            severity="Skipped",
            properties={
                "username|loginname": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}|{{ dag_run.conf.loginname }}",
                "status": "Skipped",
                "details": '\
                    {%- if dag_run.conf.loginstatus | is_truthy -%} \
                        Login status is not enabled\
                    {%- else -%} \
                        Login Status is not present\
                    {%- endif -%}'
            }
        )

        trigger_dag_run_genoadesign_user_import_gdi_child_add_user_v1_0async_6 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_genoadesign_user_import_gdi_child_add_user_v1_0async_6',
            retries=0,
            items=[-1],
            trigger_dag_id=f'genoadesign_user_import_gdi_child_add_user_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                "email": dag_run.conf['email'],
                "employeeid": dag_run.conf['employeeid'],
                "team": dag_run.conf['team'],
                "startdate": dag_run.conf['startdate'],
                "loginname": dag_run.conf['loginname'],
                "departmentname": dag_run.conf['departmentname'],
                "supervisor": dag_run.conf['supervisor'],
                "supervisoreffectivedate": dag_run.conf['supervisoreffectivedate'],
                "department": dag_run.conf['department'],
                "employeehourlycost": dag_run.conf['employeehourlycost'],
                "employeehourlycosteffectivedate": dag_run.conf['employeehourlycosteffectivedate'],
                "userhourlycostcurrency": dag_run.conf['userhourlycostcurrency'],
                "employeetype": dag_run.conf['employeetype'],
                "loginstatus": dag_run.conf['loginstatus'],
                "location": dag_run.conf['location'],
                "timezone": dag_run.conf['timezone'],
                "holidaycalendar": dag_run.conf['holidaycalendar'],
                "supervisor_processing_log": dag_run.conf['supervisor_processing_log']
            }
        )

        wait_for_completion_trigger_dag_run_genoadesign_user_import_gdi_child_add_user_v1_0async_6 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_genoadesign_user_import_gdi_child_add_user_v1_0async_6',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_genoadesign_user_import_gdi_child_add_user_v1_0async_6") }}'
        )

        trigger_dag_run_genoadesign_user_import_gdi_child_update_user_v1_0async_7 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_genoadesign_user_import_gdi_child_update_user_v1_0async_7',
            retries=0,
            items=[-1],
            trigger_dag_id=f'genoadesign_user_import_gdi_child_update_user_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                "email": dag_run.conf['email'],
                "employeeid": dag_run.conf['employeeid'],
                "team": dag_run.conf['team'],
                "startdate": dag_run.conf['startdate'],
                "loginname": dag_run.conf['loginname'],
                "departmentname": dag_run.conf['departmentname'],
                "supervisor": dag_run.conf['supervisor'],
                "supervisoreffectivedate": dag_run.conf['supervisoreffectivedate'],
                "department": dag_run.conf['department'],
                "employeehourlycost": dag_run.conf['employeehourlycost'],
                "employeehourlycosteffectivedate": dag_run.conf['employeehourlycosteffectivedate'],
                "userhourlycostcurrency": dag_run.conf['userhourlycostcurrency'],
                "employeetype": dag_run.conf['employeetype'],
                "loginstatus": dag_run.conf['loginstatus'],
                "location": dag_run.conf['location'],
                "timezone": dag_run.conf['timezone'],
                "holidaycalendar": dag_run.conf['holidaycalendar'],
                "useruri": rail.result('search_users_1')['useruri'],
                "supervisor_processing_log": dag_run.conf['supervisor_processing_log']
            }
        )

        wait_for_completion_trigger_dag_run_genoadesign_user_import_gdi_child_update_user_v1_0async_7 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_genoadesign_user_import_gdi_child_update_user_v1_0async_7',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_genoadesign_user_import_gdi_child_update_user_v1_0async_7") }}'
        )

        genoadi_user_import_logs_add_entry = rail.WriteLogOperator(
            task_id='genoadi_user_import_logs_add_entry',
            message="{{ get_error_message() }}",
            severity="Error",
            trigger_rule='one_failed',
            properties={
                "username|loginname": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}|{{ dag_run.conf.loginname }}",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        process_users = rail.EmptyOperator(
            task_id="process_users"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> search_users_1 >> if_log_checkiftheuserisavaialble_blank_3
        if_log_checkiftheuserisavaialble_blank_3 >> rail.Label(
            'No') >> trigger_dag_run_genoadesign_user_import_gdi_child_update_user_v1_0async_7 >> \
            wait_for_completion_trigger_dag_run_genoadesign_user_import_gdi_child_update_user_v1_0async_7 >> \
            process_users >> genoadi_user_import_logs_add_entry >> log_to_sumo
        if_log_checkiftheuserisavaialble_blank_3 >> rail.Label(
            'Yes') >> if_new_changed_profiles_loginstatus_blank_enabled_4
        if_new_changed_profiles_loginstatus_blank_enabled_4 >> rail.Label('No') >> \
            trigger_dag_run_genoadesign_user_import_gdi_child_add_user_v1_0async_6 >> \
            wait_for_completion_trigger_dag_run_genoadesign_user_import_gdi_child_add_user_v1_0async_6 >> process_users >> genoadi_user_import_logs_add_entry
        if_new_changed_profiles_loginstatus_blank_enabled_4 >> rail.Label('Yes') >> \
            genoadi_user_import_logs_add_entry_5 >> genoadi_user_import_logs_add_entry

    return dag


rail.for_each_instance(create_dag)
