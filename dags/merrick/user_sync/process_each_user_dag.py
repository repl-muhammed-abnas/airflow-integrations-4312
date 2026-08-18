from datetime import datetime, timedelta
from pytz import timezone
import math
import itertools
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.process_each_user_dag_id,
        description=f'Deltek Costpoint Process Each User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
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
            no_task='if_employeeid_not_present_skip_processing'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_employeeid_not_present_skip_processing',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        # The login name is sourced from EMPL_ID or EMAIL_ID depending on the
        # instance config (loginname_source_field, resolved in the master DAG).
        # Skip processing when the key identifier is missing: always require the
        # employee id, and additionally require the email id when it drives the
        # login name (EMAIL_ID), since the user cannot be created without it.
        _loginname_source = getattr(config, 'loginname_source_field', 'EMPL_ID')
        if _loginname_source == 'EMAIL_ID':
            _skip_processing_test = '''{{ not (dag_run.conf.employeeId and dag_run.conf.emailaddress) }}'''
            _skip_processing_reason = "Employee Id or Email Id not present in feed file"
        else:
            _skip_processing_test = '''{{ dag_run.conf.employeeId | is_falsy }}'''
            _skip_processing_reason = "Employee Id not present in feed file"

        if_employeeid_not_present_skip_processing = rail.IfOperator(
            task_id='if_employeeid_not_present_skip_processing',
            test=_skip_processing_test,
            yes_task="user_import_logs_add_entry_1",
            no_task="if_superuser_not_present_skip_processing",
        )

        user_import_logs_add_entry_1 = rail.WriteLogOperator(
            task_id='user_import_logs_add_entry_1',
            message="na",
            severity="Skipped",
            properties={
                "employeeid": "{{ dag_run.conf.employeeId }}",
                "action": "Add/Update",
                "status": "Skipped",
                "reason": _skip_processing_reason
            }
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def joinFilter(leftExpression, rightExpression, operatorUri):
            return {
                "leftExpression": leftExpression,
                "operatorUri": operatorUri,
                "rightExpression": rightExpression
            }

        def getFilterExpression(employeeId):
            return {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": employeeId
                    }
                }
            }

        def combineLeaves(leaves):
            if leaves:
                if len(leaves) == 0:
                    return None
                if len(leaves) == 1:
                    return leaves[0]
                if len(leaves) == 2:
                    return joinFilter(leaves[0], leaves[1], "urn:replicon:filter-operator:or")
                if len(leaves) > 2:
                    midpoint = math.ceil(len(leaves) / 2)
                    return joinFilter(combineLeaves(leaves[:midpoint]), combineLeaves(leaves[midpoint:]), "urn:replicon:filter-operator:or")

        def get_filter_super_user_request(dag_run, columnUris):
            leaves = []
            super_users = dag_run.conf['employeehistory']
            for userid in super_users:
                filterExpression = getFilterExpression(userid['supervisor'])
                leaves.append(filterExpression)

            finalFilterExpression = combineLeaves(leaves)
            return {
                "page": 1,
                "pagesize": 10000,
                "columnUris": columnUris,
                "sort": [],
                "filterExpression": finalFilterExpression
            }

        def compose_user_infs(response):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            users_info = list(map(lambda row: {
                'loginname': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
                'employeeid': row['cells'][2]['textValue'] if 'textValue' in row['cells'][2] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'useruri': row['cells'][1]['uri']
            }, flaten_rows))
            return users_info if users_info else None

        if_superuser_not_present_skip_processing = rail.IfOperator(
            task_id='if_superuser_not_present_skip_processing',
            test='''{{ dag_run.conf.employeehistory | length > 0 }}''',
            yes_task="search_supervisor_users",
            no_task="search_users",
        )

        search_supervisor_users = rail.RepliconServicePageOperator(
            task_id='search_supervisor_users',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: get_filter_super_user_request(dag_run, [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
            ]),
            page_handler=page_handler,
            all_result_data_handler=lambda response: compose_user_infs(
                response)
        )

        def get_user_info(dag_run):
            if dag_run.conf.get('modified_users'):
                return rail.find_first_by_attr_and_get_attr(dag_run.conf['modified_users'], 'employeeId', dag_run.conf['employeeId'], 'uri', None)
            return None

        search_users = rail.PythonOperator(
            task_id='search_users',
            python_callable=get_user_info
        )

        if_user_uri_present_update_user = rail.IfOperator(
            task_id='if_user_uri_present_update_user',
            test='''{{ result('search_users') | is_truthy }}''',
            yes_task="if_supervisor_assignment_missing",
            no_task="trigger_dag_run_add_user",
        )

        def is_new_supervior_present():
            super_user_history = rail.result('search_supervisor_users')
            new_user_info = rail.find_first_by_attr_and_get_attr(
                super_user_history, 'superuseruri', None) if super_user_history else None
            return bool(new_user_info)

        if_supervisor_assignment_missing = rail.IfOperator(
            task_id='if_supervisor_assignment_missing',
            test=is_new_supervior_present,
            yes_task="add_supervisor_assignment_table",
            no_task="trigger_dag_run_user_update",
        )

        def get_missing_supervisor_user(dag_run):
            mapper_configurations = config.get_mapper_details(dag_run.conf)
            super_user_assignment_history = []
            super_user_history = rail.result('search_supervisor_users')
            if super_user_history:
                for employee in dag_run.conf['employeehistory']:
                    if employee['supervisor']:
                        supervisor_uri = rail.find_first_by_attr_and_get_attr(super_user_history, 'employeeid', employee['supervisor'], 'useruri') \
                            if employee['supervisor'] else None
                        super_user_assignment_history.append({
                            "supervisor": employee['supervisor'],
                            "effectivedate": employee['effectivedate'],
                            "superuseruri": supervisor_uri,
                        })
            return {
                "loginname": dag_run.conf['loginname'],
                "employeeid": dag_run.conf['employeeId'],
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(dag_run.conf['all_permissions'],
                                                                                'name', mapper_configurations['supervisor_permission'], 'uri'),
                "useruri": rail.result('search_users'),
                "supervisorassignment": super_user_assignment_history,
                "action": "Update",
                "status": "queued"
            }

        add_supervisor_assignment_table = rail.WriteLogOperator(
            task_id='add_supervisor_assignment_table',
            log="{{ dag_run.conf.supervisor_processing_log }}",
            message="na",
            severity="queued",
            properties=get_missing_supervisor_user
        )

        def get_employee_history(dag_run):
            emp_history = []
            super_user_history = rail.result('search_supervisor_users')
            for employee in dag_run.conf['employeehistory']:
                supervisor_uri = rail.find_first_by_attr_and_get_attr(super_user_history, 'employeeid', employee['supervisor'], 'useruri') \
                    if employee['supervisor'] else None
                emp_history.append({
                    "departmenturi": employee['departmenturi'],
                    "employeetypeuri": employee['employeetypeuri'],
                    "locationuri": employee['locationuri'],
                    "divisionuri": employee['divisionuri'],
                    "superuseruri": supervisor_uri,
                    "effectivedate": employee['effectivedate'],
                    "supervisor": employee['supervisor'],
                    "workschedule_uri": employee['workschedule'],
                    # Discipline sync: Pass discipline role URI per history entry
                    "disciplinecode": employee.get('disciplinecode'),
                    "disciplinename": employee.get('disciplinename'),
                    "disciplineroleuri": employee.get('disciplineroleuri')
                })
            return emp_history

        def get_mapper_configurations(dag_run):
            mapper_configurations = config.get_mapper_details(dag_run.conf)
            return {
                "reportuserpermissionuri": rail.find_first_by_attr_and_get_attr(dag_run.conf['all_permissions'],
                                                                                'name', mapper_configurations['user_permission'], 'uri'),
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(dag_run.conf['all_permissions'],
                                                                                'name', mapper_configurations['supervisor_permission'], 'uri'),
                "office_schedule": mapper_configurations['office_schedule'],
                "workweek": mapper_configurations['workweek'],
                "default_password": mapper_configurations['default_password'],
                "holiday_calendar": mapper_configurations['holiday_calendar'],
                "user_permission": mapper_configurations['user_permission'],
                "time_off_template": mapper_configurations['time_off_template'],
                "timesheet_template": mapper_configurations['timesheet_template'],
                "timesheet_period_type": mapper_configurations['timesheet_period_type'],
                "timesheet_approval_path": mapper_configurations['timesheet_approval_path'],
                "timeoff_approval_path": mapper_configurations['timeoff_approval_path'],
                "timeZone": mapper_configurations['timeZone'],
                "payrule": mapper_configurations['payrule'],
                "activities": mapper_configurations.get('activities', []),
                "expenses_approval_path": mapper_configurations.get('expenses_approval_path')
            }

        trigger_dag_run_user_update = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_user_update',
            retries=0,
            items=[1],
            trigger_dag_id=config.update_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda dag_run: {
                **get_mapper_configurations(dag_run),
                **dag_run.conf,
                **{
                    "useruri": rail.result('search_users'),
                    "userhistory": get_employee_history(dag_run)
                }
            }
        )

        wait_for_completion_trigger_dag_run_user_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_user_update',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_user_update") }}'
        )

        trigger_dag_run_add_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_add_user',
            retries=0,
            items=[1],
            trigger_dag_id=config.add_user_dag_id,
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                **get_mapper_configurations(dag_run),
                **dag_run.conf,
                **{
                    # "superuseruri": rail.result('search_super_users')['useruri'] if rail.result('search_super_users') else None,
                    "userhistory": get_employee_history(dag_run)
                }
            }
        )

        wait_for_completion_trigger_dag_run_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_add_user',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_add_user") }}'
        )

        process_users = rail.EmptyOperator(
            task_id="process_users"
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.employeeId }}",
                "action": "Add/Update",
                "status": "Error",
                "reason": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> \
            if_employeeid_not_present_skip_processing
        if_employeeid_not_present_skip_processing >> rail.Label(
            'No') >> if_superuser_not_present_skip_processing
        if_superuser_not_present_skip_processing >> rail.Label(
            'No') >> search_users
        if_superuser_not_present_skip_processing >> rail.Label('Yes') >> search_supervisor_users >> search_users >> \
            if_user_uri_present_update_user
        if_user_uri_present_update_user >> rail.Label('No') >> \
            trigger_dag_run_add_user >> wait_for_completion_trigger_dag_run_add_user >> process_users
        if_user_uri_present_update_user >> rail.Label('Yes') >> \
            if_supervisor_assignment_missing
        if_supervisor_assignment_missing >> rail.Label(
            'No') >> trigger_dag_run_user_update
        if_supervisor_assignment_missing >> rail.Label('Yes') >> add_supervisor_assignment_table >> trigger_dag_run_user_update >> \
            wait_for_completion_trigger_dag_run_user_update >> \
            process_users >> catch_error
        if_employeeid_not_present_skip_processing >> rail.Label('Yes') >> user_import_logs_add_entry_1 >> \
            catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
