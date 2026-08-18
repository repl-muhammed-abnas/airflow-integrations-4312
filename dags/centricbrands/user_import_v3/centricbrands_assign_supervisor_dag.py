
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.child_assign_supervisor_dag_id,
        description=f'CentricBrands User Import - Assign Supervisor Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_supervisor_user'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_supervisor_user',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_supervisor_uri_and_status(response, dag_run):
            users_found = response['rows']
            matching_user = list(filter(
                lambda user: user['cells'][0]['textValue'] == dag_run.conf['supervisorloginname'], users_found))
            return {
                'uri': matching_user[0]['cells'][0]['uri'] if matching_user else '',
                'status': matching_user[0]['cells'][1]['textValue'] if matching_user else ''
            }

        search_supervisor_user = rail.RepliconServiceOperator(
            task_id='search_supervisor_user',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled",
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{dag_run.conf.supervisorloginname}}"
                        }
                    }
                }
            },
            data_handler=get_supervisor_uri_and_status
        )

        if_supervisorloginname_unequal_userloginname = rail.IfOperator(
            task_id='if_supervisorloginname_unequal_userloginname',
            test='''{{ dag_run.conf.supervisorloginname != dag_run.conf.loginname }}''',
            yes_task="if_supervisor_uri_present",
            no_task="log_loginname_for_user_and_supervisor_same",
        )

        if_supervisor_uri_present = rail.IfOperator(
            task_id='if_supervisor_uri_present',
            test='''{{ result('search_supervisor_user').uri | is_truthy }}''',
            yes_task="if_supervisor_status_contains_true",
            no_task="if_supervisor_uri_not_present",
        )

        if_supervisor_status_contains_true = rail.IfOperator(
            task_id='if_supervisor_status_contains_true',
            test=lambda: 'True' in rail.result(
                'search_supervisor_user')['status'],
            yes_task="get_assigned_premissionsets_for_user",
            no_task="log_supervisor_is_disabled_in_replicon",
        )

        get_assigned_premissionsets_for_user = rail.RepliconServiceOperator(
            task_id='get_assigned_premissionsets_for_user',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_supervisor_user').uri }}"
            }
        )

        check_for_supervision_permissionset = rail.PythonOperator(
            task_id='check_for_supervision_permissionset',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_assigned_premissionsets_for_user'), 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.uri') if rail.result(
                'get_assigned_premissionsets_for_user')[0]['policyUri'] else null
        )

        if_permission_not_present = rail.IfOperator(
            task_id='if_permission_not_present',
            test='''{{ result('check_for_supervision_permissionset') | is_falsy }}''',
            yes_task="log_supervisor_permission_not_present",
            no_task="if_permission_present",
        )

        log_supervisor_permission_not_present = rail.PythonOperator(
            task_id='log_supervisor_permission_not_present',
            python_callable=lambda dag_run:  "Supervisor assignment/update is not done for user '" +
            dag_run.conf['loginname'] + "' as Supervisor with login name '" +
            dag_run.conf['supervisorloginname'] +
            "' does not have supervisor permissions;"
        )

        if_permission_present = rail.IfOperator(
            task_id='if_permission_present',
            test='''{{ result('check_for_supervision_permissionset') | is_truthy }}''',
            yes_task="if_action_equals_new_user",
            no_task="if_supervisor_uri_not_present",
        )

        if_action_equals_new_user = rail.IfOperator(
            task_id='if_action_equals_new_user',
            test='''{{ dag_run.conf.action == 'new user' }}''',
            yes_task="put_supervisor_assignment_schedule",
            no_task="get_supervisor_effectivedate_object",
        )

        put_supervisor_assignment_schedule = rail.RepliconServiceOperator(
            task_id='put_supervisor_assignment_schedule',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('search_supervisor_user').uri }}",
                "scheduleEntries": []
            }
        )

        log_supervisor_assigned = rail.PythonOperator(
            task_id='log_supervisor_assigned',
            python_callable=lambda dag_run:  "Initial Supervisor assigned as '" +
            dag_run.conf['supervisorloginname'] + "';"
        )

        def get_date_object(datestring):
            dateobj = datetime.strptime(datestring, '%m/%d/%Y')
            return {
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year,
                'datestring': datestring
            }

        get_supervisor_effectivedate_object = rail.PythonOperator(
            task_id='get_supervisor_effectivedate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['supervisorstartdate'] if dag_run.conf['supervisorstartdate'] else datetime.now().strftime("%m/%d/%Y"))
        )

        update_supervisor_assignment_schedule_over_date_range = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('search_supervisor_user').uri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('get_supervisor_effectivedate_object').year }}",
                        "month": "{{ result('get_supervisor_effectivedate_object').month }}",
                        "day": "{{ result('get_supervisor_effectivedate_object').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        log_supervisor_is_assigned = rail.PythonOperator(
            task_id='log_supervisor_is_assigned',
            python_callable=lambda dag_run:  "Supervisor assigned as '" +
            dag_run.conf['supervisorloginname'] + "';"
        )

        delete_entry_from_supervisor_lookup = rail.FilterLogEntriesOperator(
            task_id='delete_entry_from_supervisor_lookup',
            log="{{dag_run.conf.supervisorlookuptable}}",
            properties={
                'jobid': "{{dag_run.conf.parentjobid}}",
                "childjobid": "{{dag_run.conf.childjobid}}"
            },
            remove_filtered_entries=True
        )

        load_found_supervisor_entry = rail.PythonOperator(
            task_id='load_found_supervisor_entry',
            python_callable=lambda: rail.load_all_records(
                rail.result('delete_entry_from_supervisor_lookup'))
        )

        if_entry_was_present = rail.IfOperator(
            task_id='if_entry_was_present',
            test='''{{ result('delete_entry_from_supervisor_lookup','length') > 0 | is_truthy }}''',
            yes_task="update_entry_in_supervisorlookup",
            no_task="if_supervisor_uri_not_present",
        )

        update_entry_in_supervisorlookup = rail.WriteLogOperator(
            task_id='update_entry_in_supervisorlookup',
            log="{{dag_run.conf.supervisorlookuptable}}",
            message='na',
            properties=lambda dag_run: {
                "jobid": rail.result('load_found_supervisor_entry')[0]['properties']['jobid'],
                "loginname": rail.result('load_found_supervisor_entry')[0]['properties']['loginname'],
                "useruri": rail.result('load_found_supervisor_entry')[0]['properties']['useruri'],
                "supervisorloginname": rail.result('load_found_supervisor_entry')[0]['properties']['supervisorloginname'],
                "supervisorstartdate": rail.result('load_found_supervisor_entry')[0]['properties']['supervisorstartdate'],
                "childjobid": rail.result('load_found_supervisor_entry')[0]['properties']['childjobid'],
                "assignedstatus": "Updated",
                "action": rail.result('load_found_supervisor_entry')[0]['properties']['action']
            }
        )

        log_supervisor_is_disabled_in_replicon = rail.PythonOperator(
            task_id='log_supervisor_is_disabled_in_replicon',
            python_callable=lambda dag_run:  "Supervisor assignment/update is not done for user '" +
            dag_run.conf['loginname'] + "' as Supervisor with login name '" +
            dag_run.conf['supervisorloginname'] + "' is disabled in Replicon;"
        )

        if_supervisor_uri_not_present = rail.IfOperator(
            task_id='if_supervisor_uri_not_present',
            test='''{{ result('search_supervisor_user').uri | is_falsy }}''',
            yes_task="log_supervisor_not_available",
            no_task="search_userimport_logs_for_user_and_delete_to_update",
        )

        log_supervisor_not_available = rail.PythonOperator(
            task_id='log_supervisor_not_available',
            python_callable=lambda dag_run:  "Supervisor is not assigned/updated as the Supervisor  with login name '" +
            dag_run.conf['supervisorloginname'] +
            "' is not available in Replicon;"
        )

        log_loginname_for_user_and_supervisor_same = rail.PythonOperator(
            task_id='log_loginname_for_user_and_supervisor_same',
            python_callable=lambda:  'Supervisor is not updated as' +
            ' the "Login name" for user and supervisor is same on the input file;'
        )

        search_userimport_logs_for_user_and_delete_to_update = rail.FilterLogEntriesOperator(
            task_id='search_userimport_logs_for_user_and_delete_to_update',
            log="{{dag_run.conf.userimportlogslookuptable}}",
            properties={
                "jobid": "{{dag_run.conf.parentjobid}}",
                "childjobid": "{{dag_run.conf.childjobid}}"
            },
            remove_filtered_entries=True
        )

        load_found_logs_entry = rail.PythonOperator(
            task_id='load_found_logs_entry',
            python_callable=lambda: rail.load_all_records(rail.result(
                'search_userimport_logs_for_user_and_delete_to_update'))
        )

        if_entry_is_present = rail.IfOperator(
            task_id='if_entry_is_present',
            test='''{{ result('search_userimport_logs_for_user_and_delete_to_update','length') > 0 | is_truthy }}''',
            yes_task="add_updated_log",
            no_task="catch_error",
        )

        def get_exceptions(only_exceptions=True):
            exceptions = (rail.result('log_loginname_for_user_and_supervisor_same') if rail.result(
                'log_loginname_for_user_and_supervisor_same') else '') + (rail.result('log_supervisor_not_available') if rail.result(
                    'log_supervisor_not_available') else '') + (rail.result('log_supervisor_is_disabled_in_replicon') if rail.result(
                        'log_supervisor_is_disabled_in_replicon') else '') + (rail.result('log_supervisor_permission_not_present') if rail.result(
                            'log_supervisor_permission_not_present') else '')
            success_logs = (rail.result('log_supervisor_assigned') if rail.result('log_supervisor_assigned') else '') + (rail.result(
                'log_supervisor_is_assigned') if rail.result('log_supervisor_is_assigned') else '')
            return exceptions if only_exceptions else exceptions + success_logs

        add_updated_log = rail.WriteLogOperator(
            task_id='add_updated_log',
            log="{{dag_run.conf.userimportlogslookuptable}}",
            message='na',
            properties=lambda: {
                "loginname": rail.result('load_found_logs_entry')[0]['properties']['loginname'],
                "empid": rail.result('load_found_logs_entry')[0]['properties']['empid'],
                "email": rail.result('load_found_logs_entry')[0]['properties']['email'],
                "isloginenabled": rail.result('load_found_logs_entry')[0]['properties']['isloginenabled'],
                "status": 'Error' if 'Error' in rail.result('load_found_logs_entry')[0]['properties']['status'] else (
                    'Exception' if get_exceptions() else rail.result('load_found_logs_entry')[0]['properties']['status']),
                "details": rail.result('load_found_logs_entry')[0]['properties']['details'] + get_exceptions(False),
                "jobid": rail.result('load_found_logs_entry')[0]['properties']['jobid'],
                "childjobid": rail.result('load_found_logs_entry')[0]['properties']['childjobid'] + "|" + rail.render_template("{{dag_run_ecid()}}"),
                "department|location|team": "||"
            }
        )

        catch_error = rail.FilterLogEntriesOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            log="{{dag_run.conf.userimportlogslookuptable}}",
            properties={
                "jobid": "{{dag_run.conf.parentjobid}}",
                "childjobid": "{{dag_run.conf.childjobid}}"
            },
            remove_filtered_entries=True
        )

        load_found_entry_on_error = rail.PythonOperator(
            task_id='load_found_entry_on_error',
            python_callable=lambda: rail.load_all_records(
                rail.result('load_found_entry_on_error'))
        )

        if_entry_found = rail.IfOperator(
            task_id='if_entry_found',
            test='''{{ result('catch_error','length') > 0 | is_truthy }}''',
            yes_task="update_userimport_log_entry"
        )

        update_userimport_log_entry = rail.WriteLogOperator(
            task_id='update_userimport_log_entry',
            message='na',
            log="{{dag_run.conf.userimportlogslookuptable}}",
            properties=lambda: {
                "loginname": rail.result('load_found_entry_on_error')[0]['properties']['loginname'],
                "empid": rail.result('load_found_entry_on_error')[0]['properties']['empid'],
                "email": rail.result('load_found_entry_on_error')[0]['properties']['email'],
                "isloginenabled": rail.result('load_found_entry_on_error')[0]['properties']['isloginenabled'],
                "status": "Error",
                "details": rail.render_template("{{get_error_message()}}") + rail.result('load_found_entry_on_error')[0]['properties']['details'],
                "jobid": rail.result('load_found_entry_on_error')[0]['properties']['jobid'],
                "childjobid": rail.result('load_found_entry_on_error')[0]['properties']['childjobid'] + rail.render_template("{{ dag_run_ecid() }}"),
                "department|location|team": "||"
            }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> search_supervisor_user
        search_supervisor_user >> if_supervisorloginname_unequal_userloginname
        if_supervisorloginname_unequal_userloginname >> rail.Label(
            'Yes') >> if_supervisor_uri_present
        if_supervisor_uri_present >> rail.Label(
            'Yes') >> if_supervisor_status_contains_true
        if_supervisor_status_contains_true >> rail.Label(
            'Yes') >> get_assigned_premissionsets_for_user >> check_for_supervision_permissionset >> if_permission_not_present
        if_permission_not_present >> rail.Label(
            'Yes') >> log_supervisor_permission_not_present >> if_permission_present
        if_permission_not_present >> rail.Label('No') >> if_permission_present
        if_permission_present >> rail.Label('Yes') >> if_action_equals_new_user
        if_action_equals_new_user >> rail.Label(
            'Yes') >> put_supervisor_assignment_schedule >> log_supervisor_assigned >> if_supervisor_uri_not_present
        if_action_equals_new_user >> rail.Label(
            'No') >> get_supervisor_effectivedate_object >> update_supervisor_assignment_schedule_over_date_range >> log_supervisor_is_assigned
        log_supervisor_is_assigned >> delete_entry_from_supervisor_lookup >> load_found_supervisor_entry >> if_entry_was_present
        if_entry_was_present >> rail.Label(
            'Yes') >> update_entry_in_supervisorlookup >> if_supervisor_uri_not_present
        if_entry_was_present >> rail.Label(
            'No') >> if_supervisor_uri_not_present
        if_permission_present >> rail.Label(
            'No') >> if_supervisor_uri_not_present
        if_supervisor_status_contains_true >> rail.Label(
            'No') >> log_supervisor_is_disabled_in_replicon >> if_supervisor_uri_not_present
        if_supervisor_uri_present >> rail.Label(
            'No') >> if_supervisor_uri_not_present
        if_supervisor_uri_not_present >> rail.Label(
            'Yes') >> log_supervisor_not_available >> search_userimport_logs_for_user_and_delete_to_update
        if_supervisor_uri_not_present >> rail.Label(
            'No') >> search_userimport_logs_for_user_and_delete_to_update
        if_supervisorloginname_unequal_userloginname >> rail.Label(
            'No') >> log_loginname_for_user_and_supervisor_same >> search_userimport_logs_for_user_and_delete_to_update >> load_found_logs_entry
        load_found_logs_entry >> if_entry_is_present
        if_entry_is_present >> rail.Label(
            'Yes') >> add_updated_log >> catch_error
        if_entry_is_present >> rail.Label(
            'No') >> catch_error >> load_found_entry_on_error >> if_entry_found
        if_entry_found >> rail.Label(
            'Yes') >> update_userimport_log_entry

    return dag


rail.for_each_instance(create_dag)
