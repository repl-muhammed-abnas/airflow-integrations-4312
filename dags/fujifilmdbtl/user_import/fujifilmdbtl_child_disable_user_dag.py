from datetime import timedelta, datetime
from pendulum import now
import json
from airflow.models import Variable
import rail
from fujifilmdbtl.user_import.utils import request_payload, python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'fujifilmdbtl_child_disable_user_{config.instance}',
        description=f'FDT_Child Workflow to disable user {config.instance}',
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
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_startdate_to_time_greater_than_todayto_time_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_startdate_to_time_greater_than_todayto_time_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_startdate_to_time_greater_than_todayto_time_3 = rail.IfOperator(
            task_id='if_startdate_to_time_greater_than_todayto_time_3',
            test=lambda dag_run: datetime.strptime(
                dag_run.conf['startdate'], "%d/%m/%Y").date() > now().date(),
            yes_task="fdt_user_import_logs_add_entry_4",
            no_task="get_assigned_permission_sets_for_user2_6",
        )

        fdt_user_import_logs_add_entry_4 = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_add_entry_4',
            log="{{dag_run.conf.userimportlogtable}}",
            message="na",
            severity="Skipped",
            properties={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "username": "{{ dag_run.conf.username }}",
                "loginname": "{{ dag_run.conf.userloginname }}",
                "emplid": "{{ dag_run.conf.emplid }}",
                "action": "Disable",
                "status": "Skipped",
                "details": "User's start date {{ dag_run.conf.startdate }} is in future",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        get_assigned_permission_sets_for_user2_6 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_6',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_checkif_superadmin_permissionisassigned_7 = rail.PythonOperator(
            task_id='log_checkif_superadmin_permissionisassigned_7',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_assigned_permission_sets_for_user2_6'), 'displayText', "Superadmin", 'name', '') if rail.result('get_assigned_permission_sets_for_user2_6') else null
        )

        if_log_checkif_superadmin_permissionisassigned_7_present_8 = rail.IfOperator(
            task_id='if_log_checkif_superadmin_permissionisassigned_7_present_8',
            test='{{ result("log_checkif_superadmin_permissionisassigned_7") | is_truthy }}',
            yes_task="fdt_user_import_logs_add_entry_9",
            no_task="log_end_date_day_11",
        )

        fdt_user_import_logs_add_entry_9 = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_add_entry_9',
            log="{{dag_run.conf.userimportlogtable}}",
            message="na",
            severity="Skipped",
            properties={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "username": "{{ dag_run.conf.username }}",
                "loginname": "{{ dag_run.conf.userloginname }}",
                "emplid": "{{ dag_run.conf.emplid }}",
                "action": "Disable",
                "status": "Skipped",
                "details": "User has Superadmin permission. Hence user is not disabled",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_end_date_day_11 = rail.PythonOperator(
            task_id='log_end_date_day_11',
            python_callable=lambda:  now().strftime("%d")
        )

        log_end_date_month_12 = rail.PythonOperator(
            task_id='log_end_date_month_12',
            python_callable=lambda:  now().strftime("%m")
        )

        log_end_date_year_13 = rail.PythonOperator(
            task_id='log_end_date_year_13',
            python_callable=lambda:  now().strftime("%Y")
        )

        get_direct_reports_for_user_14 = rail.RepliconServiceOperator(
            task_id='get_direct_reports_for_user_14',
            endpoint="/services/UserService1.svc/GetDirectReportsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "asOfDate": {
                    "year": "{{ result('log_end_date_year_13') }}",
                    "month": "{{ result('log_end_date_month_12') }}",
                    "day": "{{ result('log_end_date_day_11') }}"
                },
                "userStatusOptionUri": "urn:replicon:user-status-option:include-only-enabled-users"
            }
        )

        if_first_loginname_present_15 = rail.IfOperator(
            task_id='if_first_loginname_present_15',
            test=lambda: rail.result('get_direct_reports_for_user_14') and (rail.result('get_direct_reports_for_user_14')[
                0]['loginName']),
            yes_task="search_users_16",
            no_task="disable_login_29",
        )

        search_users_16 = rail.RepliconServiceOperator(
            task_id='search_users_16',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_search_user_payload_for_supervisor(
                "candi.nelson@fujifilm.com"),
            data_handler=lambda response: response['rows']
        )

        get_search_user_detail = rail.PythonOperator(
            task_id='get_search_user_detail',
            python_callable=lambda: python_callable.get_search_user_details(
                rail.result('search_users_16'))
        )

        log_checkifmultipleusershavethesameemailid_17 = rail.PythonOperator(
            task_id='log_checkifmultipleusershavethesameemailid_17',
            python_callable=lambda: len(rail.result('search_users_16'))
        )

        if_log_checkifmultipleusershavethesameemailid_17_present_18 = rail.IfOperator(
            task_id='if_log_checkifmultipleusershavethesameemailid_17_present_18',
            test='{{ result("log_checkifmultipleusershavethesameemailid_17") > 1 }}',
            yes_task="fdt_user_import_logs_add_entry_19",
            no_task="log_get_requiredusersuri_22",
        )

        fdt_user_import_logs_add_entry_19 = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_add_entry_19',
            log="{{dag_run.conf.userimportlogtable}}",
            message="na",
            severity="Skipped",
            properties={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "username": "{{ dag_run.conf.username }}",
                "loginname": "{{ dag_run.conf.userloginname }}",
                "emplid": "{{ dag_run.conf.emplid }}",
                "action": "Disable",
                "status": "Skipped",
                "details": "The user is not disabled as the supervisor update cannot be done to Candi Nelson. Multiple users have same email id as candi.nelson@fujifilm.com.",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_get_requiredusersuri_22 = rail.PythonOperator(
            task_id='log_get_requiredusersuri_22',
            python_callable=lambda dag_run: (rail.find_first_by_attr_and_get_attr(rail.result(
                'get_search_user_detail'), 'email', 'candi.nelson@fujifilm.com', 'uri', "")) if rail.result('get_search_user_detail') else null
        )

        foreach_response_23 = rail.ForEachOperator(
            task_id='foreach_response_23',
            items=lambda: rail.result('get_direct_reports_for_user_14'),
            start_task='get_assigned_policy_sets_for_user_24',
            end_task='foreach_response_23_end'
        )

        get_assigned_policy_sets_for_user_24 = rail.RepliconServiceOperator(
            task_id='get_assigned_policy_sets_for_user_24',
            endpoint="/services/PolicySetService1.svc/GetAssignedPolicySetsForUser",
            data={
                "userUri": "{{ result('foreach_response_23').uri }}"
            }
        )

        log_checkif_timesheet_templateisassignedfordirectreport_25 = rail.PythonOperator(
            task_id='log_checkif_timesheet_templateisassignedfordirectreport_25',
            python_callable=lambda: "Timesheet policy present" if rail.find_first_by_attr_and_get_attr(rail.result(
                'get_assigned_policy_sets_for_user_24'), 'policyUri', 'urn:replicon:policy:timesheet', 'uri', '') else null
        )

        log_checkif_timeoff_templateisassignedfordirectreport_26 = rail.PythonOperator(
            task_id='log_checkif_timeoff_templateisassignedfordirectreport_26',
            python_callable=lambda:  "Timesheet policy present" if rail.find_first_by_attr_and_get_attr(rail.result(
                'get_assigned_policy_sets_for_user_24'), 'policyUri', 'urn:replicon:policy:time-off', 'uri', '') else null
        )

        if_log_checkif_timesheet_templateisassignedfordirectreport_25_present_27 = rail.IfOperator(
            task_id='if_log_checkif_timesheet_templateisassignedfordirectreport_25_present_27',
            test=lambda: rail.result('log_checkif_timesheet_templateisassignedfordirectreport_25') != null or rail.result(
                'log_checkif_timeoff_templateisassignedfordirectreport_26') != null,
            yes_task="update_supervisor_assignment_schedule_over_date_range_28",
            no_task="foreach_response_23_end",
        )

        update_supervisor_assignment_schedule_over_date_range_28 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_28',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('foreach_response_23').uri }}",
                "supervisorUri": "{{ result('log_get_requiredusersuri_22') }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('log_end_date_year_13') }}",
                        "month": "{{ result('log_end_date_month_12') }}",
                        "day": "{{ result('log_end_date_day_11') }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        foreach_response_23_end = rail.EmptyOperator(
            task_id='foreach_response_23_end'
        )

        disable_login_29 = rail.RepliconServiceOperator(
            task_id='disable_login_29',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        update_employment_date_rangeforenddate_33 = rail.RepliconServiceOperator(
            task_id='update_employment_date_rangeforenddate_33',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf['startdate'], "%d/%m/%Y"),
                    "endDate": {
                        "year": rail.result('log_end_date_year_13'),
                        "month": rail.result('log_end_date_month_12'),
                        "day": rail.result('log_end_date_day_11')
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        get_user_time_off_type_policy_summary_34 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_34',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        # Initialize variable to collect DAG runs from foreach loop
        init_timeoff_dag_runs_list = rail.SetVariableOperator(
            task_id='init_timeoff_dag_runs_list',
            name='timeoff_dag_runs',
            append=False,
            value=[]
        )

        foreach_d_35 = rail.ForEachOperator(
            task_id="foreach_d_35",
            items=lambda: rail.result('get_user_time_off_type_policy_summary_34')[
                'policiesByTimeOffType'],
            start_task='if_foreach_d_35_istimeoffallowedagainstthistimeofftype_is_true_36',
            end_task='foreach_d_35_end'
        )

        if_foreach_d_35_istimeoffallowedagainstthistimeofftype_is_true_36 = rail.IfOperator(
            task_id='if_foreach_d_35_istimeoffallowedagainstthistimeofftype_is_true_36',
            test='''{{ result('foreach_d_35').isTimeOffAllowedAgainstThisTimeOffType | is_truthy }}''',
            yes_task="get_balance_summary_for_account_37",
            no_task="foreach_d_35_end",
        )

        get_balance_summary_for_account_37 = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account_37',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data={
                "account": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_d_35').timeOffType.uri }}"
                },
                "asOfDate": {
                    "year": "{{ result('log_end_date_year_13') }}",
                    "month": "{{ result('log_end_date_month_12') }}",
                    "day": "{{ result('log_end_date_day_11') }}"
                }
            }
        )

        log_policy_set_schedule_38 = rail.PythonOperator(
            task_id='log_policy_set_schedule_38',
            python_callable=lambda:  rail.result(
                'foreach_d_35')['policySetSchedule']
        )

        if_log_policy_set_schedule_38_present_39 = rail.IfOperator(
            task_id='if_log_policy_set_schedule_38_present_39',
            test='{{ result("log_policy_set_schedule_38") | is_truthy }}',
            yes_task="trigger_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_41",
            no_task="foreach_d_35_end",
        )

        trigger_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_41 = rail.TriggerDagRunOperator(
            task_id='trigger_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_41',
            retries=0,
            trigger_dag_id=f'fujifilmdbtl_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "parentjobid": rail.render_template("{{ dag_run.conf.parentjobid }}"),
                "useruri": rail.render_template("{{ dag_run.conf.useruri }}"),
                "timeoffuri": rail.result('foreach_d_35')['timeOffType']['uri'],
                "policyset": json.dumps(rail.result('log_policy_set_schedule_38')),
                "companykey": rail.render_template("{{get_company_key()}}"),
                "newschedulebalance": rail.render_template("{{ result('get_balance_summary_for_account_37').timeRemaining }}"),
                "enddate": datetime.strftime(now(), "%m/%d/%Y")
            }
        )

        # Append the triggered DAG run to the collection variable
        append_timeoff_dag_run = rail.SetVariableOperator(
            task_id='append_timeoff_dag_run',
            name='timeoff_dag_runs',
            append=True,
            value='{{ result("trigger_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_41") }}'
        )

        foreach_d_35_end = rail.EmptyOperator(
            task_id='foreach_d_35_end',
        )

        # Process the collected DAG run IDs
        get_timeoff_child_dag_ids = rail.PythonOperator(
            task_id='get_timeoff_child_dag_ids',
            python_callable=lambda: [
                int(item) for item in rail.get_dag_run_var('timeoff_dag_runs')] if rail.get_dag_run_var('timeoff_dag_runs') else []
        )

        # Wait for all collected DAG runs after the foreach loop completes
        wait_for_all_timeoff_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_all_timeoff_dag_runs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_timeoff_child_dag_ids') | to_json }}"
        )

        fdt_user_import_logs_add_entry_44 = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_add_entry_44',
            log="{{dag_run.conf.userimportlogtable}}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "username": dag_run.conf['username'],
                "loginname": dag_run.conf['userloginname'],
                "emplid": dag_run.conf['emplid'],
                "action": "Disable",
                "status": "Success",
                "childjobid": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{dag_run.conf.userimportlogtable}}",
            message="na",
            severity="Error",
            properties={
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "username": "{{ dag_run.conf.username }}",
                "loginname": "{{ dag_run.conf.userloginname }}",
                "emplid": "{{ dag_run.conf.emplid }}",
                "action": "Disable",
                "status": "Error",
                'details': "{{get_error_message()}}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> if_startdate_to_time_greater_than_todayto_time_3
        if_startdate_to_time_greater_than_todayto_time_3
        if_startdate_to_time_greater_than_todayto_time_3 >> rail.Label(
            'Yes') >> fdt_user_import_logs_add_entry_4 >> catch_and_log_error
        if_startdate_to_time_greater_than_todayto_time_3 >> rail.Label(
            'No') >> get_assigned_permission_sets_for_user2_6 >> log_checkif_superadmin_permissionisassigned_7 \
            >> if_log_checkif_superadmin_permissionisassigned_7_present_8
        if_log_checkif_superadmin_permissionisassigned_7_present_8 >> rail.Label(
            'Yes') >> fdt_user_import_logs_add_entry_9 >> catch_and_log_error
        if_log_checkif_superadmin_permissionisassigned_7_present_8 >> rail.Label(
            'No') >> log_end_date_day_11 >> log_end_date_month_12 >> log_end_date_year_13 >> get_direct_reports_for_user_14 >> if_first_loginname_present_15
        if_first_loginname_present_15 >> rail.Label(
            'Yes') >> search_users_16 >> get_search_user_detail >> log_checkifmultipleusershavethesameemailid_17 \
            >> if_log_checkifmultipleusershavethesameemailid_17_present_18
        if_log_checkifmultipleusershavethesameemailid_17_present_18 >> rail.Label(
            'Yes') >> fdt_user_import_logs_add_entry_19 >> catch_and_log_error
        if_log_checkifmultipleusershavethesameemailid_17_present_18 >> rail.Label(
            'No') >> log_get_requiredusersuri_22 >> foreach_response_23 >> get_assigned_policy_sets_for_user_24 \
            >> log_checkif_timesheet_templateisassignedfordirectreport_25 >> log_checkif_timeoff_templateisassignedfordirectreport_26 \
            >> if_log_checkif_timesheet_templateisassignedfordirectreport_25_present_27
        if_log_checkif_timesheet_templateisassignedfordirectreport_25_present_27 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_28 >> foreach_response_23_end
        if_log_checkif_timesheet_templateisassignedfordirectreport_25_present_27 >> rail.Label(
            'No') >> foreach_response_23_end
        foreach_response_23 >> foreach_response_23_end >> disable_login_29
        if_first_loginname_present_15 >> rail.Label(
            'No') >> disable_login_29 >> update_employment_date_rangeforenddate_33

        update_employment_date_rangeforenddate_33 >> get_user_time_off_type_policy_summary_34 >> init_timeoff_dag_runs_list >> foreach_d_35

        foreach_d_35 >> if_foreach_d_35_istimeoffallowedagainstthistimeofftype_is_true_36
        if_foreach_d_35_istimeoffallowedagainstthistimeofftype_is_true_36 >> rail.Label(
            'Yes') >> get_balance_summary_for_account_37 >> log_policy_set_schedule_38 >> if_log_policy_set_schedule_38_present_39
        if_log_policy_set_schedule_38_present_39 >> rail.Label(
            'Yes') >> trigger_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_41 \
            >> append_timeoff_dag_run \
            >> foreach_d_35_end
        if_log_policy_set_schedule_38_present_39 >> rail.Label(
            'No') >> foreach_d_35_end
        if_foreach_d_35_istimeoffallowedagainstthistimeofftype_is_true_36 >> rail.Label(
            'No') >> foreach_d_35_end

        foreach_d_35 >> foreach_d_35_end >> get_timeoff_child_dag_ids >> wait_for_all_timeoff_dag_runs >> fdt_user_import_logs_add_entry_44 >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
