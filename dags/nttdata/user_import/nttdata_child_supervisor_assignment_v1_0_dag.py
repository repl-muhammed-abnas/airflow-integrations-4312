
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nttdata_user_import_supervisor_assignment_child_{config.instance}',
        description=f'NTTData_Child_Supervisor Assignment_ {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_user_by_loginname'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_user_by_loginname',
            end_task='catch_error_and_search_log_for_user',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_supervisordetails(response,dag_run):
            supervisor_user = {}
            users = response['rows']
            for user in users:
                #pylint: disable = line-too-long
                if user['cells'][0]['textValue'] and dag_run.conf['supervisorloginname'] and (user['cells'][0]['textValue']).lower() == dag_run.conf['supervisorloginname'].lower():
                    supervisor_user = user
                    break
            dateobj = datetime.now()
            return {
                "urioutput": supervisor_user['cells'][0]['uri'] if supervisor_user else null,
                "nameoutput": supervisor_user['cells'][1]['textValue'] if supervisor_user else null,
                "statusoutput": supervisor_user['cells'][2]['textValue'] if supervisor_user else null,
                "todaydayoutput": dateobj.day,
                "todaymonthoutput": dateobj.month,
                "todayyearoutput": dateobj.year,
                "todayoutput": dateobj.strftime('%Y-%m-%d')
            } if supervisor_user else {}

        search_user_by_loginname=rail.RepliconServiceOperator(
            task_id='search_user_by_loginname',
            endpoint="/services/UserListService1.svc/GetData",
            data={
              "page": "1",
              "pagesize": "100",
              "columnUris": [
                  "urn:replicon:user-list-column:login-name",
                  "urn:replicon:user-list-column:employee-id",
                  "urn:replicon:user-list-column:enabled",
                  "urn:replicon:user-list-column:hourly-cost"
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
            data_handler=get_supervisordetails
        )

        if_supervisor_found = rail.IfOperator(
            task_id = 'if_supervisor_found',
            test=lambda: bool(rail.result('search_user_by_loginname') and rail.result('search_user_by_loginname')['urioutput']),
            yes_task='if_supervisorloginname_unequal_loginname',
            no_task='catch_error_and_search_log_for_user'
        )

        if_supervisorloginname_unequal_loginname=rail.IfOperator(
            task_id='if_supervisorloginname_unequal_loginname',
            test='''{{ dag_run.conf.supervisorloginname != dag_run.conf.loginname }}''',
            yes_task="if_supervisoruri_present",
            no_task="log_user_and_supervisor_same_in_inputfile",
        )

        if_supervisoruri_present=rail.IfOperator(
            task_id='if_supervisoruri_present',
            test='''{{ result('search_user_by_loginname').urioutput | is_truthy }}''',
            yes_task="get_assigned_permissionsets_foruser",
            no_task="if_supervisoruri_not_present",
        )

        def check_manager_permission(response):
            return {
                'supervisorpermission': rail.find_first_by_attr_and_get_attr(response,'permissionSet.name',
                    'Manager','permissionSet.uri','') if response[0]['policyUri'] else null,
                'enduserpermission': rail.find_first_by_attr_and_get_attr(response,'permissionSet.name',
                    'End user with reports view','permissionSet.uri','') if response[0]['policyUri'] else null
            }
        get_assigned_permissionsets_foruser=rail.RepliconServiceOperator(
            task_id='get_assigned_permissionsets_foruser',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_user_by_loginname').urioutput }}"
            },
            data_handler=check_manager_permission
        )

        if_supervisorpermission_not_assigned=rail.IfOperator(
            task_id='if_supervisorpermission_not_assigned',
            test='''{{ result('get_assigned_permissionsets_foruser').supervisorpermission | is_falsy }}''',
            yes_task="assign_supervsior_permissionset_to_user_manager",
            no_task="if_enduser_permission_not_assigned",
        )

        assign_supervsior_permissionset_to_user_manager=rail.RepliconServiceOperator(
            task_id='assign_supervsior_permissionset_to_user_manager',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_user_by_loginname').urioutput }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        if_enduser_permission_not_assigned=rail.IfOperator(
            task_id='if_enduser_permission_not_assigned',
            test='''{{ result('get_assigned_permissionsets_foruser').enduserpermission | is_falsy }}''',
            yes_task="assign_supervsior_permission_set_touser_enduser_with_reportsview",
            no_task="if_action_equals_add",
        )

        assign_supervsior_permission_set_touser_enduser_with_reportsview=rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_touser_enduser_with_reportsview',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_user_by_loginname').urioutput }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        if_action_equals_add=rail.IfOperator(
            task_id='if_action_equals_add',
            test='''{{ dag_run.conf.action == 'Add' }}''',
            yes_task="update_initial_supervisor",
            no_task="if_action_equals_update",
        )

        update_initial_supervisor=rail.RepliconServiceOperator(
            task_id='update_initial_supervisor',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('search_user_by_loginname').urioutput }}",
                "scheduleEntries": []
            }
        )

        if_action_equals_update=rail.IfOperator(
            task_id='if_action_equals_update',
            test='''{{ dag_run.conf.action == 'Update' }}''',
            yes_task="update_supervisor_assignment_schedule_over_daterange",
            no_task="if_supervisoruri_not_present",
        )

        def get_date_object(datestring):
            dateobj = datetime.strptime(datestring,'%Y-%m-%d')
            return {
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year
            }

        update_supervisor_assignment_schedule_over_daterange=rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_daterange',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run:{
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_user_by_loginname')['urioutput'],
                "dateRange": {
                    "startDate": get_date_object(dag_run.conf['supeffectivedate']),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_supervisoruri_not_present=rail.IfOperator(
            task_id='if_supervisoruri_not_present',
            test='''{{ result('search_user_by_loginname').urioutput | is_falsy }}''',
            yes_task="log_error_supervisor_is_notavailable",
            no_task="search_existing_logs_for_this_user",
        )

        log_error_supervisor_is_notavailable=rail.PythonOperator(
            task_id='log_error_supervisor_is_notavailable',
            python_callable= lambda dag_run:  "Supervisor is not updated as the supervisor with login name " +
                                dag_run.conf['supervisorloginname'] + " is not available"
        )

        log_user_and_supervisor_same_in_inputfile=rail.PythonOperator(
            task_id='log_user_and_supervisor_same_in_inputfile',
            python_callable= lambda: 'Supervisor is not updated ' + 'as the "Login name" for user and supervisor is same on the input file'
        )

        search_existing_logs_for_this_user=rail.FilterLogEntriesOperator(
            task_id='search_existing_logs_for_this_user',
            log="{{dag_run.conf.logslookuptable}}",
            properties={
                "childjobis": "{{dag_run.conf.childjobid}}"
            }
        )

        load_found_log_entry = rail.PythonOperator(
            task_id = 'load_found_log_entry',
            python_callable=lambda: rail.load_all_records(rail.result('search_existing_logs_for_this_user'))
        )

        if_log_present=rail.IfOperator(
            task_id='if_log_present',
            test='''{{ result('search_existing_logs_for_this_user','length') > 0 }}''',
            yes_task="delete_previous_log_to_update_logs",
            no_task="delete_entry_to_update_supervisor_checkup",
        )

        delete_previous_log_to_update_logs = rail.FilterLogEntriesOperator(
            task_id = 'delete_previous_log_to_update_logs',
            log="{{dag_run.conf.logslookuptable}}",
            properties={
                "childjobis": "{{result('load_found_log_entry')[0].properties.childjobis}}",
                "userid": "{{result('load_found_log_entry')[0].properties.userid}}"
            },
            remove_filtered_entries=True
        )

        update_log_in_lookuptable=rail.WriteLogOperator(
            task_id='update_log_in_lookuptable',
            log="{{dag_run.conf.logslookuptable}}",
            message='na',
            properties=lambda dag_run:{
                "userid": rail.result('load_found_log_entry')[0]['properties']['userid'],
                "username": rail.result('load_found_log_entry')[0]['properties']['username'],
                "action": rail.result('load_found_log_entry')[0]['properties']['action'],
                "status": 'Error' if 'Error' in rail.result('load_found_log_entry')[0]['properties']['status'] else (
                    'Exception' if rail.result('log_user_and_supervisor_same_in_inputfile') or rail.result('log_error_supervisor_is_notavailable') else
                    rail.result('load_found_log_entry')[0]['properties']['status']),
                "details": rail.result('load_found_log_entry')[0]['properties']['details'] + (';' +
                    rail.result('log_user_and_supervisor_same_in_inputfile') if rail.result('log_user_and_supervisor_same_in_inputfile') else '') +
                    (';' + rail.result('log_error_supervisor_is_notavailable') if rail.result('log_error_supervisor_is_notavailable') else ''),
                "childjobis": rail.result('load_found_log_entry')[0]['properties']['childjobis'],
                "parentjobid": rail.result('load_found_log_entry')[0]['properties']['parentjobid']
            }
        )

        delete_entry_to_update_supervisor_checkup = rail.FilterLogEntriesOperator(
            task_id = 'delete_entry_to_update_supervisor_checkup',
            log="{{dag_run.conf.supervisorchecklookup}}",
            properties={
                'childjobid': "{{dag_run.conf.childjobid}}",
                'userloginname': "{{dag_run.conf.loginname}}"
            },
            remove_filtered_entries=True
        )

        update_entry_in_supervisorcheckup_lookup=rail.WriteLogOperator(
            task_id='update_entry_in_supervisorcheckup_lookup',
            log="{{dag_run.conf.supervisorchecklookup}}",
            message='na',
            properties=lambda dag_run:{
              "jobid": dag_run.conf['jobid'],
              "userloginname": dag_run.conf['loginname'],
              "useruri": dag_run.conf['useruri'],
              "username": dag_run.conf['username'],
              "supervisorloginname": dag_run.conf['supervisorloginname'],
              "childjobid": dag_run.conf['childjobid'],
              "action": dag_run.conf['action'],
              "status": 'Completed',
              "effectivedate": dag_run.conf['supeffectivedate']
            }
        )

        catch_error_and_search_log_for_user=rail.FilterLogEntriesOperator(
            task_id='catch_error_and_search_log_for_user',
            trigger_rule='one_failed',
            log="{{dag_run.conf.logslookuptable}}",
            properties={
                "childjobis": "{{dag_run.conf.childjobid}}"
            }
        )

        load_found_entry = rail.PythonOperator(
            task_id = 'load_found_entry',
            python_callable=lambda: rail.load_all_records(rail.result('catch_error_and_search_log_for_user'))
        )

        if_log_for_user_present=rail.IfOperator(
            task_id='if_log_for_user_present',
            test='''{{ result('catch_error_and_search_log_for_user','length') > 0 }}''',
            yes_task="delete_log_to_update_logs",
            no_task="delete_entry_to_update_supervisorcheckup",
        )

        delete_log_to_update_logs = rail.FilterLogEntriesOperator(
            task_id = 'delete_log_to_update_logs',
            log="{{dag_run.conf.logslookuptable}}",
            properties={
                "childjobis": "{{result('load_found_entry')[0].properties.childjobis}}",
                "userid": "{{result('load_found_entry')[0].properties.userid}}"
            },
            remove_filtered_entries=True
        )

        update_logs_with_error=rail.WriteLogOperator(
            task_id='update_logs_with_error',
            log="{{dag_run.conf.logslookuptable}}",
            message='na',
            properties=lambda dag_run:{
                "userid": rail.result('load_found_entry')[0]['properties']['userid'],
                "username": rail.result('load_found_entry')[0]['properties']['username'],
                "action": rail.result('load_found_entry')[0]['properties']['action'],
                "status": 'Error',
                "details": rail.result('load_found_entry')[0]['properties']['details'] + ';' + rail.render_template("{{get_error_message()}}"),
                "childjobis": rail.result('load_found_entry')[0]['properties']['childjobis'],
                "parentjobid": rail.result('load_found_entry')[0]['properties']['parentjobid']
            }
        )

        delete_entry_to_update_supervisorcheckup  = rail.FilterLogEntriesOperator(
            task_id = 'delete_entry_to_update_supervisorcheckup',
            log="{{dag_run.conf.supervisorchecklookup}}",
            properties={
                'childjobid': "{{dag_run.conf.childjobid}}",
                'userloginname': "{{dag_run.conf.loginname}}"
            },
            remove_filtered_entries=True
        )

        update_supervisor_checkup_lookup=rail.WriteLogOperator(
            task_id='update_supervisor_checkup_lookup',
            log="{{dag_run.conf.supervisorchecklookup}}",
            message='na',
            properties=lambda dag_run:{
              "jobid": dag_run.conf['jobid'],
              "userloginname": dag_run.conf['loginname'],
              "useruri": dag_run.conf['useruri'],
              "username": dag_run.conf['username'],
              "supervisorloginname": dag_run.conf['supervisorloginname'],
              "childjobid": dag_run.conf['childjobid'],
              "action": dag_run.conf['action'],
              "status": 'Completed',
              "effectivedate": dag_run.conf['supeffectivedate']
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error_and_search_log_for_user
        can_run_batch_task >> rail.Label('No') >> search_user_by_loginname
        search_user_by_loginname >> if_supervisor_found >> rail.Label('Yes') >> if_supervisorloginname_unequal_loginname
        if_supervisor_found >> rail.Label('No') >> catch_error_and_search_log_for_user
        if_supervisorloginname_unequal_loginname >> rail.Label('Yes')  >> if_supervisoruri_present
        if_supervisoruri_present >> rail.Label(
            'Yes')  >> get_assigned_permissionsets_foruser >> if_supervisorpermission_not_assigned
        if_supervisorpermission_not_assigned >> rail.Label(
            'Yes')  >> assign_supervsior_permissionset_to_user_manager >> if_enduser_permission_not_assigned
        if_supervisorpermission_not_assigned >> rail.Label('No') >> if_enduser_permission_not_assigned
        if_enduser_permission_not_assigned >> rail.Label('Yes')  >> assign_supervsior_permission_set_touser_enduser_with_reportsview >> if_action_equals_add
        if_enduser_permission_not_assigned >> rail.Label('No') >> if_action_equals_add
        if_action_equals_add >> rail.Label('Yes')  >> update_initial_supervisor >> if_action_equals_update
        if_action_equals_add >> rail.Label('No') >> if_action_equals_update
        if_action_equals_update >> rail.Label(
            'Yes')  >> update_supervisor_assignment_schedule_over_daterange >> if_supervisoruri_not_present
        if_action_equals_update >> rail.Label('No') >> if_supervisoruri_not_present
        if_supervisoruri_present >> rail.Label('No') >> if_supervisoruri_not_present
        if_supervisoruri_not_present >> rail.Label('Yes')  >> log_error_supervisor_is_notavailable >> search_existing_logs_for_this_user
        if_supervisoruri_not_present >> rail.Label('No') >> search_existing_logs_for_this_user
        if_supervisorloginname_unequal_loginname >> rail.Label(
            'No') >> log_user_and_supervisor_same_in_inputfile >> search_existing_logs_for_this_user >> load_found_log_entry >> if_log_present
        if_log_present >> rail.Label('Yes')  >> delete_previous_log_to_update_logs >> update_log_in_lookuptable >> delete_entry_to_update_supervisor_checkup
        if_log_present >> rail.Label('No') >> delete_entry_to_update_supervisor_checkup >> update_entry_in_supervisorcheckup_lookup
        update_entry_in_supervisorcheckup_lookup >> catch_error_and_search_log_for_user >> load_found_entry >> if_log_for_user_present
        if_log_for_user_present >> rail.Label('Yes')  >> delete_log_to_update_logs >> update_logs_with_error >> delete_entry_to_update_supervisorcheckup
        if_log_for_user_present >> rail.Label('No') >> delete_entry_to_update_supervisorcheckup >> update_supervisor_checkup_lookup >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
