import rail
from gee.user_import.utils import request_payload, response_filter

def create_child_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.gee_supervisor_assignment_child,
        description=f'GEE Supervisor assignment child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        get_user_details_with_supervisorloginname = rail.RepliconServiceOperator(
            task_id="get_user_details_with_supervisorloginname",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_supervisordetails,
            response_filter=response_filter.get_filtered_user_details
        )

        if_supervisor_loginname_not_equals = rail.IfOperator(
            task_id='if_supervisor_loginname_not_equals',
            test=lambda dag_run: dag_run.conf['supervisorloginname'] != dag_run.conf['loginname'],
            yes_task='if_urioutput_present',
            no_task='log_error_supervisor_and_loginname_is_same'
        )

        if_urioutput_present = rail.IfOperator(
            task_id='if_urioutput_present',
            test=lambda: bool(rail.result('get_user_details_with_supervisorloginname')['urioutput']),
            yes_task='if_statusoutput_true',
            no_task='if_urioutput_not_present'
        )

        if_statusoutput_true = rail.IfOperator(
            task_id='if_statusoutput_true',
            test="{{ result('get_user_details_with_supervisorloginname').statusoutput | matches('True') }}",
            yes_task='get_missing_supervisor_permission',
            no_task='log_error_when_supervisor_disabled'
        )

        get_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                'userUri': "{{ result('get_user_details_with_supervisorloginname').urioutput }}"
            },
            data_handler=response_filter.is_assign_supervisorpermission
        )

        if_manager_permissionset_not_present = rail.IfOperator(
            task_id='if_manager_permissionset_not_present',
            test=lambda: not bool(rail.result('get_missing_supervisor_permission')['managerpermissionset']),
            yes_task='add_missing_supervisor_permission',
            no_task='if_enduser_permissionset_not_present'
        )

        add_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('get_user_details_with_supervisorloginname').urioutput }}",
                'permissionSetUri': "{{ result('get_missing_supervisor_permission').managerpermissionset }}"
            }
        )

        if_enduser_permissionset_not_present = rail.IfOperator(
            task_id='if_enduser_permissionset_not_present',
            test=lambda: not bool(rail.result('get_missing_supervisor_permission')['enduserpermissionset']),
            yes_task='add_missing_enduser_permission',
            no_task='if_action_equals_add'
        )

        add_missing_enduser_permission = rail.RepliconServiceOperator(
            task_id='add_missing_enduser_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('get_user_details_with_supervisorloginname').urioutput }}",
                'permissionSetUri': "{{ dag_run.conf.enduserpermissionformanager }}"
            }
        )

        if_action_equals_add = rail.IfOperator(
            task_id='if_action_equals_add',
            test=lambda dag_run: dag_run.conf['action'] == 'Add',
            yes_task='update_initial_supervisor',
            no_task='if_action_equals_update'
        )

        update_initial_supervisor = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('get_user_details_with_supervisorloginname').urioutput }}",
                "scheduleEntries": []
            }
        )

        if_action_equals_update = rail.IfOperator(
            task_id='if_action_equals_update',
            test=lambda dag_run: dag_run.conf['action'] == 'Update',
            yes_task='update_supervisor_assignment_schedule_over_date_range',
            no_task='user_import_logs_search_entries'
        )

        update_supervisor_assignment_schedule_over_date_range = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('get_user_details_with_supervisorloginname').urioutput }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('get_user_details_with_supervisorloginname').todayyearoutput }}",
                        "month": "{{ result('get_user_details_with_supervisorloginname').todaymonthoutput }}",
                        "day": "{{ result('get_user_details_with_supervisorloginname').todaydayoutput }}"
                    },
                    "endDate": None,
                    "relativeDateRangeUri": None,
                    "relativeDateRangeAsOfDate": None
                }
            }
        )

        log_error_when_supervisor_disabled = rail.PythonOperator(
            task_id='log_error_when_supervisor_disabled',
            python_callable=lambda dag_run: 'Supervsior assignment/update is not done for user "' +
            dag_run.conf['loginname'] + '" as supervsior with loginname "' + dag_run.conf['supervisorloginname'] +
            '" is disabled in Replicon.'
        )

        if_urioutput_not_present = rail.IfOperator(
            task_id='if_urioutput_not_present',
            test=lambda dag_run: dag_run.conf['action'] == 'Update',
            yes_task='log_error_when_supervisor_not_available',
            no_task='user_import_logs_search_entries'
        )

        log_error_when_supervisor_not_available = rail.PythonOperator(
            task_id='log_error_when_supervisor_not_available',
            python_callable=lambda dag_run: 'Supervisor is not updated as the supervisor with login name "' +
            dag_run.conf['supervisorloginname'] + '" is not available'
        )

        log_error_supervisor_and_loginname_is_same = rail.PythonOperator(
            task_id='log_error_supervisor_and_loginname_is_same',
            python_callable=lambda: 'Supervisor is not updated as the "Login name" for user' +
            'and supervisor is same on the input file'
        )

        user_import_logs_search_entries = rail.FilterLogEntriesOperator(
            task_id='user_import_logs_search_entries',
            log="{{dag_run.conf.userimport_lookup_table}}",
            properties={
                "jobid": "{{dag_run.conf.parentjobid}}"
            },
            remove_filtered_entries=True
        )

        load_found_entry = rail.PythonOperator(
            task_id='load_found_entry',
            python_callable=lambda: rail.load_all_records(rail.result(
                'user_import_logs_search_entries'))
        )

        if_log_entry_found = rail.IfOperator(
            task_id='if_log_entry_found',
            test=lambda dag_run: bool(rail.result('load_found_entry')),
            yes_task='add_to_lookup_table',
            no_task='update_supervisor_log_table'
        )

        add_to_lookup_table = rail.WriteLogOperator(
            task_id='add_to_lookup_table',
            log = "{{ dag_run.conf.gee_user_lookup_table }}",
            items= "{{ result('load_found_entry') }}",
            message="na",
            severity="Success",
            properties={
                "loginname" : "{{ item.properties.loginname }}",
                "empid" : "{{ item.properties.empid }}",
                "action": "{{ item.properties.action }}",
                "status" : "{{ item.properties.status }}",
                "details" : "{{ item.properties.details }}",
                "jobid" : "",
                "childjobid" : ""
            }
        )

        update_supervisor_log_table = rail.WriteLogOperator(
            task_id='update_supervisor_log_table',
            log = "{{ dag_run.conf.gee_supervisor_lookup_table }}",
            message="na",
            severity="Success",
            properties={
                "jobid" : "",
                "userloginname" : "",
                "useruri" : "",
                "username" : "",
                "supervisorloginname" : "",
                "action": "",
                "empid": "",
                "childjobid" : "",
                "status" : "completed"
            }
        )

        get_user_details_with_supervisorloginname >> if_supervisor_loginname_not_equals >> rail.Label(
            "Yes") >> if_urioutput_present >> rail.Label(
            "Yes") >> if_statusoutput_true >> rail.Label(
            "Yes") >> get_missing_supervisor_permission >> if_manager_permissionset_not_present >> rail.Label(
            "Yes") >> add_missing_supervisor_permission >> if_enduser_permissionset_not_present
        if_manager_permissionset_not_present >> rail.Label(
            "No") >> if_enduser_permissionset_not_present >> rail.Label(
            "Yes") >> add_missing_enduser_permission >> if_action_equals_add
        if_enduser_permissionset_not_present >> rail.Label(
            "No") >> if_action_equals_add >> rail.Label(
            "Yes") >> update_initial_supervisor >> if_action_equals_update
        if_action_equals_add >> rail.Label(
            "No") >> if_action_equals_update >> rail.Label(
            "Yes") >> update_supervisor_assignment_schedule_over_date_range >> user_import_logs_search_entries
        if_action_equals_update >> rail.Label(
            "No") >> user_import_logs_search_entries
        if_statusoutput_true >> rail.Label(
            "No") >> log_error_when_supervisor_disabled >> user_import_logs_search_entries
        if_urioutput_present >> rail.Label(
            "No") >> if_urioutput_not_present >> rail.Label(
            "Yes") >> log_error_when_supervisor_not_available >> user_import_logs_search_entries
        if_urioutput_not_present >> rail.Label(
            "No") >> user_import_logs_search_entries
        if_supervisor_loginname_not_equals >> rail.Label(
            "No") >> log_error_supervisor_and_loginname_is_same >> user_import_logs_search_entries
        user_import_logs_search_entries >> load_found_entry >> if_log_entry_found >> rail.Label(
            "Yes") >> add_to_lookup_table >> update_supervisor_log_table
        if_log_entry_found >> rail.Label(
            "No") >> update_supervisor_log_table

        return dag


rail.for_each_instance(create_child_dag)
