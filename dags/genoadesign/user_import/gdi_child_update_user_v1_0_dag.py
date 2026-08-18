
from datetime import timedelta, datetime
import json
import itertools
import pendulum
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'genoadesign_user_import_gdi_child_update_user_v1_0_{config.instance}',
        description=f'Live|GDI_Child_Update User V1.0 {config.instance}',
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
            no_task='bulk_get_users3_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='bulk_get_users3_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        bulk_get_users3_3 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3_3',
            endpoint="/services/importservice1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ dag_run.conf.useruri }}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            }
        )

        if_userdetails_isenabled_is_true_4 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_4',
            test='''{{ result('bulk_get_users3_3')[0].userDetails.isEnabled | is_truthy  and dag_run.conf.loginstatus | is_truthy and dag_run.conf.loginstatus != 'Enabled' }}''',
            yes_task="trigger_dag_run_live_gdi_child_workflow_to_disable_user_v1_05",
            no_task="if_userdetails_isenabled_is_not_true_7",
        )

        trigger_dag_run_live_gdi_child_workflow_to_disable_user_v1_05 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_gdi_child_workflow_to_disable_user_v1_05',
            retries=0,
            items=[-1],
            trigger_dag_id=f'genoadesign_user_import_gdi_child_workflow_to_disable_user_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "userloginname": "{{ dag_run.conf.loginname }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        wait_for_completion_trigger_dag_run_live_gdi_child_workflow_to_disable_user_v1_05 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_gdi_child_workflow_to_disable_user_v1_05',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_gdi_child_workflow_to_disable_user_v1_05") }}'
        )

        if_userdetails_isenabled_is_not_true_7 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_7',
            test='''{{ result('bulk_get_users3_3')[0].userDetails.isEnabled | is_falsy  and dag_run.conf.loginstatus | is_truthy and dag_run.conf.loginstatus != 'Enabled' }}''',
            yes_task="genoadi_user_import_logs_add_entry_8",
            no_task="if_userdetails_isenabled_is_not_true_10",
        )

        genoadi_user_import_logs_add_entry_8 = rail.WriteLogOperator(
            task_id='genoadi_user_import_logs_add_entry_8',
            message="na",
            severity="Skipped",
            properties={
                "username|loginname": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}|{{ dag_run.conf.loginname }}",
                "status": "Skipped",
                "details": "User is already disabled in Replicon",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_userdetails_isenabled_is_not_true_10 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_10',
            test='''{{ result('bulk_get_users3_3')[0].userDetails.isEnabled | is_falsy  and dag_run.conf.loginstatus == 'Enabled' }}''',
            yes_task="enable_login_11",
            no_task="if_request_firstname_present_13",
        )

        enable_login_11 = rail.RepliconServiceOperator(
            task_id='enable_login_11',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        update_employment_date_rangetoremoveenddate_12 = rail.RepliconServiceOperator(
            task_id='update_employment_date_rangetoremoveenddate_12',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": '''{{ result('bulk_get_users3_3')[0].userDetails.employmentDateRange.startDate.year }}''',
                        "month": '''{{ result('bulk_get_users3_3')[0].userDetails.employmentDateRange.startDate.month }}''',
                        "day": '''{{ result('bulk_get_users3_3')[0].userDetails.employmentDateRange.startDate.day }}'''
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_firstname_present_13 = rail.IfOperator(
            task_id='if_request_firstname_present_13',
            test='''{{ dag_run.conf.firstname | is_truthy  and result('bulk_get_users3_3')[0].userDetails.firstName != dag_run.conf.firstname }}''',
            yes_task="update_first_name_14",
            no_task="if_request_lastname_present_15",
        )

        update_first_name_14 = rail.RepliconServiceOperator(
            task_id='update_first_name_14',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        if_request_lastname_present_15 = rail.IfOperator(
            task_id='if_request_lastname_present_15',
            test='''{{ dag_run.conf.lastname | is_truthy  and dag_run.conf.lastname != result('bulk_get_users3_3')[0].userDetails.lastName }}''',
            yes_task="update_last_name_16",
            no_task="if_request_email_contains_17",
        )

        update_last_name_16 = rail.RepliconServiceOperator(
            task_id='update_last_name_16',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        if_request_email_contains_17 = rail.IfOperator(
            task_id='if_request_email_contains_17',
            test='''{{ dag_run.conf.email | matches('@')  and result('bulk_get_users3_3')[0].userDetails.emailAddress != dag_run.conf.email }}''',
            yes_task="update_email_18",
            no_task="if_request_employeeid_present_19",
        )

        update_email_18 = rail.RepliconServiceOperator(
            task_id='update_email_18',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.email }}"
            }
        )

        if_request_employeeid_present_19 = rail.IfOperator(
            task_id='if_request_employeeid_present_19',
            test='''{{ dag_run.conf.employeeid | is_truthy and result('bulk_get_users3_3')[0].userDetails.employeeId != dag_run.conf.employeeid }}''',
            yes_task="update_employee_id_20",
            no_task="if_request_startdate_present_21",
        )

        update_employee_id_20 = rail.RepliconServiceOperator(
            task_id='update_employee_id_20',
            endpoint="/services/UserService1.svc/UpdateEmployeeId",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeId": "{{ dag_run.conf.employeeid }}"
            }
        )

        if_request_startdate_present_21 = rail.IfOperator(
            task_id='if_request_startdate_present_21',
            test='''{{ dag_run.conf.startdate | is_truthy }}''',
            yes_task="log_start_dateasper_repliconprofile_26",
            no_task="if_request_employeetype_present_fulltimehourly_33",
        )

        log_start_dateasper_repliconprofile_26 = rail.PythonOperator(
            task_id='log_start_dateasper_repliconprofile_26',
            python_callable=lambda:  rail.render_template(
                "{{ result('bulk_get_users3_3')[0].userDetails.employmentDateRange.startDate.year }}{{ result('bulk_get_users3_3')[0].userDetails.employmentDateRange.startDate.month }}{{ result('bulk_get_users3_3')[0].userDetails.employmentDateRange.startDate.day }}")
        )

        if_to_date_not_equals_to_dataloggerlog_start_dateasper_repliconprofile_26messageto_date_27 = rail.IfOperator(
            task_id='if_to_date_not_equals_to_dataloggerlog_start_dateasper_repliconprofile_26messageto_date_27',
            test='''{{ dag_run.conf.startdate != result('log_start_dateasper_repliconprofile_26') }}''',
            yes_task="update_employment_date_rangeforstartdate_28",
            no_task="if_request_employeetype_present_fulltimehourly_33",
        )

        update_employment_date_rangeforstartdate_28 = rail.RepliconServiceOperator(
            task_id='update_employment_date_rangeforstartdate_28',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": datetime.strptime(dag_run.conf['startdate'], '%Y%m%d').year,
                        "month": datetime.strptime(dag_run.conf['startdate'], '%Y%m%d').month,
                        "day": datetime.strptime(dag_run.conf['startdate'], '%Y%m%d').day
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_employeetype_present_fulltimehourly_33 = rail.IfOperator(
            task_id='if_request_employeetype_present_fulltimehourly_33',
            test='''{{ dag_run.conf.employeetype | is_truthy and result('bulk_get_users3_3')[0].employeeType.name | lower | replace("-", " ") != dag_run.conf.employeetype | lower }}''',
            yes_task="_adhoc_http_action_34",
            no_task="if_request_departmentname_present_86",
        )

        _adhoc_http_action_34 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_34',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data=None
        )

        def get_employee_type_uri(dag_run):
            employee_type_info = list(filter(lambda x: x['name'] and x['name'].replace('-', ' ').lower(
            ) == dag_run.conf['employeetype'].lower(), rail.result('_adhoc_http_action_34')))
            return employee_type_info[0]['uri'] if employee_type_info else None

        log_employee_type_uri_37 = rail.PythonOperator(
            task_id='log_employee_type_uri_37',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda dag_run: get_employee_type_uri(dag_run)
        )

        if_log_employee_type_uri_37_present_38 = rail.IfOperator(
            task_id='if_log_employee_type_uri_37_present_38',
            test='''{{ result('log_employee_type_uri_37') | is_truthy }}''',
            yes_task="update_employee_type_for_user_39",
            no_task="_adhoc_http_action_40",
        )

        update_employee_type_for_user_39 = rail.RepliconServiceOperator(
            task_id='update_employee_type_for_user_39',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeTypeUri": "{{ result('log_employee_type_uri_37') }}"
            }
        )

        _adhoc_http_action_40 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_40',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data=None
        )

        def is_fulltime_employee(dag_run):
            employee_type_input = dag_run.conf['employeetype'].lower(
            ) if dag_run.conf['employeetype'] else ""
            existing_employee_type = rail.result('bulk_get_users3_3')[
                0]['employeeType']['name'].lower().replace('-', ' ')
            return bool(employee_type_input in "full time hourly,full time salaried" and existing_employee_type not in "full time hourly,full time salaried")

        if_fulltimehourlyfulltimesalaried_contains_dataworkato_service3cd9c331requestemployeetype_41 = rail.IfOperator(
            task_id='if_fulltimehourlyfulltimesalaried_contains_dataworkato_service3cd9c331requestemployeetype_41',
            test=is_fulltime_employee,
            yes_task="trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_042",
            no_task="if_request_employeetype_equals_to_fulltimehourly_43",
        )

        trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_042 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_042',
            retries=0,
            items=[-1],
            trigger_dag_id=f'genoadesign_user_import_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "userloginname": "{{ dag_run.conf.loginname }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "employeetype": "{{ dag_run.conf.employeetype }}"
            }
        )

        wait_for_completion_trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_042 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_042',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_042") }}'
        )

        if_request_employeetype_equals_to_fulltimehourly_43 = rail.IfOperator(
            task_id='if_request_employeetype_equals_to_fulltimehourly_43',
            test='''{{ dag_run.conf.employeetype | lower == 'full time hourly' and result('bulk_get_users3_3')[0].employeeType.name | lower | replace("-", " ") != 'full time hourly' }}''',
            yes_task="trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_044",
            no_task="if_request_employeetype_equals_to_fulltimesalaried_45",
        )

        trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_044 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_044',
            retries=0,
            items=[-1],
            trigger_dag_id=f'genoadesign_user_import_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "userloginname": "{{ dag_run.conf.loginname }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "employeetype": "{{ dag_run.conf.employeetype }}"
            }
        )

        wait_for_completion_trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_044 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_044',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_044") }}'
        )

        if_request_employeetype_equals_to_fulltimesalaried_45 = rail.IfOperator(
            task_id='if_request_employeetype_equals_to_fulltimesalaried_45',
            test='''{{ dag_run.conf.employeetype | lower == 'full time salaried' and result('bulk_get_users3_3')[0].employeeType.name | lower | replace("-", " ") != 'full time salaried' }}''',
            yes_task="trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_046",
            no_task="if_fulltimehourlyfulltimesalaried_not_contains_dataworkato_service3cd9c331requestemployeetype_47",
        )

        trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_046 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_046',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            trigger_dag_id=f'genoadesign_user_import_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "userloginname": "{{ item.loginname }}",
                "useruri": "{{ item.useruri }}",
                "employeetype": "{{ item.employeetype }}"
            }
        )

        wait_for_completion_trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_046 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_046',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_046") }}'
        )

        def is_not_fulltime_employee(dag_run):
            employee_type_input = dag_run.conf['employeetype'].lower(
            ) if dag_run.conf['employeetype'] else ""
            existing_employee_type = rail.result('bulk_get_users3_3')[
                0]['employeeType']['name'].lower().replace("-", " ")
            return bool(employee_type_input not in "full time hourly,full time salaried" and existing_employee_type in "full time hourly,full time salaried")

        if_fulltimehourlyfulltimesalaried_not_contains_dataworkato_service3cd9c331requestemployeetype_47 = rail.IfOperator(
            task_id='if_fulltimehourlyfulltimesalaried_not_contains_dataworkato_service3cd9c331requestemployeetype_47',
            test=is_not_fulltime_employee,
            yes_task="remove_timeoffassignmentsforusers_48",
            no_task="if_request_employeetype_equals_to_fulltimehourly_49",
        )

        remove_timeoffassignmentsforusers_48 = rail.RepliconServiceOperator(
            task_id='remove_timeoffassignmentsforusers_48',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeOffTypeUris": []
            }
        )

        if_request_employeetype_equals_to_fulltimehourly_49 = rail.IfOperator(
            task_id='if_request_employeetype_equals_to_fulltimehourly_49',
            test='''{{ dag_run.conf.employeetype | lower == 'full time hourly' and result('bulk_get_users3_3')[0].employeeType.name | lower | replace("-", " ") != 'full time hourly' }}''',
            yes_task="existing_payrule_data",
            no_task="if_request_employeetype_not_equals_to_fulltimehourly_69",
        )

        def get_datetime_obj(effectiveDate):
            year = effectiveDate['year']
            month = effectiveDate['month']
            day = effectiveDate['day']
            return datetime.strptime(f"{year}/{month}/{day}", '%Y/%m/%d')

        def payrule_script_data():
            pay_schedules = []
            payrule_schedules = rail.result('bulk_get_users3_3')[
                0]['payRuleScriptSchedule']
            for payrule_schedule in payrule_schedules:
                if payrule_schedule['effectiveDate']:
                    effective_date = get_datetime_obj(
                        payrule_schedule['effectiveDate'])
                    if effective_date.date() > pendulum.now(config.pacific_timezone).date():
                        pay_schedules.append({
                            "payRuleScript": {
                                "uri": payrule_schedule['payRuleScript']['uri'],
                                "name": null
                            },
                            "effectiveDate": payrule_schedule['effectiveDate']
                        })
                else:
                    pay_schedules.append({
                        "payRuleScript": {
                            "uri": payrule_schedule['payRuleScript']['uri'],
                            "name": null
                        },
                        "effectiveDate": null
                    })

            return pay_schedules

        existing_payrule_data = rail.PythonOperator(
            task_id='existing_payrule_data',
            python_callable=payrule_script_data
        )

        log_pluckif_pay_ruleispresent_59 = rail.PythonOperator(
            task_id='log_pluckif_pay_ruleispresent_59',
            python_callable=lambda dag_run:  "Genoa Design - Overtime rule" if "full time hourly" in dag_run.conf['employeetype'].lower(
            ) else "No Payrule"
        )

        log_get_pay_rule_script_uri_60 = rail.PythonOperator(
            task_id='log_get_pay_rule_script_uri_60',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_40'), 'displayText', rail.result('log_pluckif_pay_ruleispresent_59'), 'uri')
        )

        if_declare_list_50_list_items_less_than_1_61 = rail.IfOperator(
            task_id='if_declare_list_50_list_items_less_than_1_61',
            test='''{{ result('existing_payrule_data') | length < 1 }}''',
            yes_task="if_log_get_pay_rule_script_uri_60_present_enabled_62",
            no_task="if_log_get_pay_rule_script_uri_60_present_65",
        )

        if_log_get_pay_rule_script_uri_60_present_enabled_62 = rail.IfOperator(
            task_id='if_log_get_pay_rule_script_uri_60_present_enabled_62',
            test='''{{ result('log_get_pay_rule_script_uri_60') | is_truthy }}''',
            yes_task="put_payroll_assignment_63",
            no_task="if_request_employeetype_not_equals_to_fulltimehourly_69",
        )

        put_payroll_assignment_63 = rail.RepliconServiceOperator(
            task_id='put_payroll_assignment_63',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri": "{{ result('log_get_pay_rule_script_uri_60') }}",
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_log_get_pay_rule_script_uri_60_present_65 = rail.IfOperator(
            task_id='if_log_get_pay_rule_script_uri_60_present_65',
            test='''{{ result('log_get_pay_rule_script_uri_60') | is_truthy }}''',
            yes_task="put_payroll_assignment_68",
            no_task="if_request_employeetype_not_equals_to_fulltimehourly_69",
        )

        def get_payrule_schedules(payrule_task, script_uri_task):
            existing_payrule_data = rail.result(payrule_task)
            existing_payrule_data.append({
                "payRuleScript": {
                    "uri": rail.result(script_uri_task),
                    "name": null
                },
                "effectiveDate": {
                    "year": pendulum.now(config.pacific_timezone).year,
                    "month": pendulum.now(config.pacific_timezone).month,
                    "day": pendulum.now(config.pacific_timezone).day
                }
            })

            return existing_payrule_data

        put_payroll_assignment_68 = rail.RepliconServiceOperator(
            task_id='put_payroll_assignment_68',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": get_payrule_schedules('existing_payrule_data', 'log_get_pay_rule_script_uri_60')
            }
        )

        if_request_employeetype_not_equals_to_fulltimehourly_69 = rail.IfOperator(
            task_id='if_request_employeetype_not_equals_to_fulltimehourly_69',
            test='''{{ dag_run.conf.employeetype | lower != 'full time hourly' and result('bulk_get_users3_3')[0].employeeType.name | lower | replace("-", " ") == 'full time hourly' }}''',
            yes_task="existing_payrule_data_not_fulltime",
            no_task="if_request_departmentname_present_86",
        )

        existing_payrule_data_not_fulltime = rail.PythonOperator(
            task_id='existing_payrule_data_not_fulltime',
            python_callable=payrule_script_data
        )

        log_pluckif_pay_ruleispresent_79 = rail.PythonOperator(
            task_id='log_pluckif_pay_ruleispresent_79',
            python_callable=lambda dag_run:  "Genoa Design - Overtime rule" if "full time hourly" in dag_run.conf['employeetype'].lower(
            ) else "Blank Payrule - in case Employee type changes"
        )

        log_get_pay_rule_script_uri_80 = rail.PythonOperator(
            task_id='log_get_pay_rule_script_uri_80',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_40'), 'displayText', rail.result('log_pluckif_pay_ruleispresent_79'), 'uri')
        )

        if_declare_list_70_list_items_greater_than_0_81 = rail.IfOperator(
            task_id='if_declare_list_70_list_items_greater_than_0_81',
            test='''{{ result('existing_payrule_data_not_fulltime') | length > 0 }}''',
            yes_task="if_log_get_pay_rule_script_uri_80_present_82",
            no_task="if_request_departmentname_present_86",
        )

        if_log_get_pay_rule_script_uri_80_present_82 = rail.IfOperator(
            task_id='if_log_get_pay_rule_script_uri_80_present_82',
            test='''{{ result('log_get_pay_rule_script_uri_80') | is_truthy }}''',
            yes_task="put_payroll_assignment_85",
            no_task="if_request_departmentname_present_86",
        )

        put_payroll_assignment_85 = rail.RepliconServiceOperator(
            task_id='put_payroll_assignment_85',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": get_payrule_schedules('existing_payrule_data_not_fulltime', 'log_get_pay_rule_script_uri_80')
            }
        )

        if_request_departmentname_present_86 = rail.IfOperator(
            task_id='if_request_departmentname_present_86',
            test='''{{ dag_run.conf.departmentname | is_truthy and dag_run.conf.departmentname | lower != result('bulk_get_users3_3')[0].userDetails.department.name }}''',
            yes_task="_adhoc_http_action_87",
            no_task="get_datafortherequireduser_99",
        )

        _adhoc_http_action_87 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_87',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
            data=None
        )

        def get_department_uri(dag_run, department_task):
            if dag_run.conf['departmentname']:
                existing_deparment = rail.result(department_task)
                department_info = list(filter(lambda x: x['displayText'] and x['displayText'].lower(
                ) == dag_run.conf['departmentname'].lower(), existing_deparment))
                return department_info[0]['uri'] if department_info else None
            return None

        log_departmenturi_88 = rail.PythonOperator(
            task_id='log_departmenturi_88',
            python_callable=lambda dag_run: get_department_uri(
                dag_run, '_adhoc_http_action_87')
        )

        if_log_departmenturi_88_present_89 = rail.IfOperator(
            task_id='if_log_departmenturi_88_present_89',
            test='''{{ result('log_departmenturi_88') | is_truthy }}''',
            yes_task="update_department_for_user_90",
            no_task="log_error_logfordepartmentnotpresent_92",
        )

        update_department_for_user_90 = rail.RepliconServiceOperator(
            task_id='update_department_for_user_90',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "departmentUri": "{{ result('log_departmenturi_88') }}"
            }
        )

        log_error_logfordepartmentnotpresent_92 = rail.PythonOperator(
            task_id='log_error_logfordepartmentnotpresent_92',
            python_callable=lambda dag_run:  "Department not updated for User " +
            dag_run.conf['firstname'] + " " + dag_run.conf['lastname']+". " +
                    dag_run.conf['departmentname'] +
            " is not available in Replicon."
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def compose_activity_details(response, department):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            activity_info = list(filter(lambda x: x['name'] == department, map(lambda row: {
                'name': row['cells'][0]['textValue'] if 'textValue' in row['cells'][0] else None,
                'code': row['cells'][1]['textValue'] if 'textValue' in row['cells'][0] else None,
                'uri': row['cells'][0]['uri']
            }, flaten_rows)))
            return activity_info[0] if activity_info else None

        get_activity_dataforthedepartment_93 = rail.RepliconServicePageOperator(
            task_id='get_activity_dataforthedepartment_93',
            endpoint="/services/ActivityListService1.svc/GetData",
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 1000000,
                "columnUris": [
                    "urn:replicon:activity-list-column:activity",
                    "urn:replicon:activity-list-column:code"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:activity-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": dag_run.conf['departmentname'],
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda response, dag_run: compose_activity_details(
                response, dag_run.conf['departmentname'])
        )

        if_log_activity_uristobeassigned_96_present_97 = rail.IfOperator(
            task_id='if_log_activity_uristobeassigned_96_present_97',
            test='''{{ result('get_activity_dataforthedepartment_93') | is_truthy }}''',
            yes_task="put_activity_assignments_for_user_98",
            no_task="get_datafortherequireduser_99",
        )

        put_activity_assignments_for_user_98 = rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_98',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "activityUris": [
                    "{{ result('get_activity_dataforthedepartment_93').uri }}"
                ]
            }
        )

        def compose_super_details(response):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            user_info = list(map(lambda row: {
                'supervisor': row['cells'][0]['textValue'] if 'textValue' in row['cells'][0] else None,
                'location': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
                'costcenter': row['cells'][2]['textValue'] if 'textValue' in row['cells'][2] else None,
                'hourlycost': row['cells'][3]['numberValue'] if 'numberValue' in row['cells'][3] else None
            }, flaten_rows))
            return user_info[0] if user_info else None

        get_datafortherequireduser_99 = rail.RepliconServicePageOperator(
            task_id='get_datafortherequireduser_99',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    "urn:replicon:user-list-column:supervisor",
                    "urn:replicon:user-list-column:location",
                    "urn:replicon:user-list-column:cost-center",
                    "urn:replicon:user-list-column:hourly-cost"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:user"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": dag_run.conf['useruri'],
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            page_handler=page_handler,
            all_result_data_handler=compose_super_details
        )

        if_request_supervisor_present_101 = rail.IfOperator(
            task_id='if_request_supervisor_present_101',
            test='''{{ dag_run.conf.supervisor | is_truthy }}''',
            yes_task="search_users_102",
            no_task="if_request_employeehourlycost_present_141",
        )

        def compose_user_details(response, loginname):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            users_info = list(filter(lambda x: x['loginname'] == loginname, map(lambda row: {
                'loginname': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'useruri': row['cells'][1]['uri']
            }, flaten_rows)))
            return users_info[0] if users_info else None

        search_users_102 = rail.RepliconServicePageOperator(
            task_id='search_users_102',
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
                            'text': dag_run.conf['supervisor'],
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda response, dag_run: compose_user_details(
                response, dag_run.conf['supervisor'])
        )

        if_request_loginname_equals_to_dataloggerlog_getsupervisor_login_name_104message_105 = rail.IfOperator(
            task_id='if_request_loginname_equals_to_dataloggerlog_getsupervisor_login_name_104message_105',
            test='''{{ result('search_users_102') | is_truthy and dag_run.conf.loginname == result('search_users_102').loginname }}''',
            yes_task="log_error_messagefor_supervisoranduserloginnamesame_106",
            no_task="if_request_loginname_not_equals_to_dataloggerlog_getsupervisor_login_name_104message_107",
        )

        log_error_messagefor_supervisoranduserloginnamesame_106 = rail.PythonOperator(
            task_id='log_error_messagefor_supervisoranduserloginnamesame_106',
            python_callable=lambda: rail.render_template(
                "Supervsior not updated for {{ dag_run.conf.loginname }} as user's and supervsior's login name are same")
        )

        if_request_loginname_not_equals_to_dataloggerlog_getsupervisor_login_name_104message_107 = rail.IfOperator(
            task_id='if_request_loginname_not_equals_to_dataloggerlog_getsupervisor_login_name_104message_107',
            test='''{{ result('search_users_102') | is_truthy and dag_run.conf.loginname != result('search_users_102').loginname }}''',
            yes_task="_adhoc_http_action_108",
            no_task="if_request_employeehourlycost_present_141",
        )

        _adhoc_http_action_108 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_108',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data=None
        )

        if_log_getsupervisor_uri_103_present_110 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_103_present_110',
            test='''{{ result('search_users_102') | is_truthy and result('search_users_102').useruri | is_truthy }}''',
            yes_task="_adhoc_http_action_111",
            no_task="if_log_checkifsupervsorisassigned_100_equals_to_urnrepliconlisttypenull_116",
        )

        _adhoc_http_action_111 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_111',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_102').useruri }}"
            }
        )

        def get_supervision_permission(permission_task):
            permissionset = rail.find_first_by_attr_and_get_attr(rail.result(
                permission_task), 'policyUri', "urn:replicon:policy:supervision", 'permissionSet')
            return permissionset['uri'] if permissionset else None

        log_checkifsupervisorhassupervisorpermission_112 = rail.PythonOperator(
            task_id='log_checkifsupervisorhassupervisorpermission_112',
            python_callable=lambda: get_supervision_permission(
                '_adhoc_http_action_111')
        )

        if_log_checkifsupervisorhassupervisorpermission_112_blank_113 = rail.IfOperator(
            task_id='if_log_checkifsupervisorhassupervisorpermission_112_blank_113',
            test='''{{ result('log_checkifsupervisorhassupervisorpermission_112') | is_falsy }}''',
            yes_task="log_get_supervisor_permission_114",
            no_task="if_log_checkifsupervsorisassigned_100_equals_to_urnrepliconlisttypenull_116",
        )

        log_get_supervisor_permission_114 = rail.PythonOperator(
            task_id='log_get_supervisor_permission_114',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_108'), 'displayText', "Supervisor", 'uri')
        )

        assign_supervsior_permission_set_to_user_115 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_115',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users_102').useruri }}",
                "permissionSetUri": "{{ result('log_get_supervisor_permission_114') }}"
            }
        )

        if_log_checkifsupervsorisassigned_100_equals_to_urnrepliconlisttypenull_116 = rail.IfOperator(
            task_id='if_log_checkifsupervsorisassigned_100_equals_to_urnrepliconlisttypenull_116',
            test='''{{ result('search_users_102') | is_falsy or result('search_users_102').useruri | is_falsy }}''',
            yes_task="if_log_getsupervisor_status_109_equals_to_true_117",
            no_task="if_log_checkifsupervsorisassigned_100_not_equals_to_urnrepliconlisttypenull_124",
        )

        if_log_getsupervisor_status_109_equals_to_true_117 = rail.IfOperator(
            task_id='if_log_getsupervisor_status_109_equals_to_true_117',
            test='''{{ result('search_users_102') | is_truthy and result('search_users_102').status == 'True' }}''',
            yes_task="if_log_getsupervisor_uri_103_present_118",
            no_task="genoadi_supervisor_assignment_table_add_entry_123",
        )

        if_log_getsupervisor_uri_103_present_118 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_103_present_118',
            test='''{{ result('search_users_102') | is_truthy and result('search_users_102').useruri | is_truthy }}''',
            yes_task="update_initial_supervisor_119",
            no_task="genoadi_supervisor_assignment_table_add_entry_121",
        )

        update_initial_supervisor_119 = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor_119',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('search_users_102').useruri }}",
                "scheduleEntries": []
            }
        )

        genoadi_supervisor_assignment_table_add_entry_121 = rail.WriteLogOperator(
            task_id='genoadi_supervisor_assignment_table_add_entry_121',
            log="{{ dag_run.conf.supervisor_processing_log }}",
            message="na",
            severity="fixme",
            properties={
                "userloginname": "{{ dag_run.conf.loginname }}|{{ dag_run.conf.useruri }}",
                "supervisorloginname": "{{ dag_run.conf.supervisor }}|{{ dag_run.conf.supervisoreffectivedate }}",
                "action": "Add"
            }
        )

        genoadi_supervisor_assignment_table_add_entry_123 = rail.WriteLogOperator(
            task_id='genoadi_supervisor_assignment_table_add_entry_123',
            log="{{ dag_run.conf.supervisor_processing_log }}",
            message="na",
            severity="fixme",
            properties={
                "userloginname": "{{ dag_run.conf.loginname }}|{{ dag_run.conf.useruri }}",
                "supervisorloginname": "{{ dag_run.conf.supervisor }}|{{ dag_run.conf.supervisoreffectivedate }}",
                "action": "Add"
            }
        )

        if_log_checkifsupervsorisassigned_100_not_equals_to_urnrepliconlisttypenull_124 = rail.IfOperator(
            task_id='if_log_checkifsupervsorisassigned_100_not_equals_to_urnrepliconlisttypenull_124',
            test='''{{ result('search_users_102') | is_truthy and result('search_users_102').useruri | is_truthy }}''',
            yes_task="log_getthesupervisorloginname_126",
            no_task="if_request_employeehourlycost_present_141",
        )

        def get_supervision_loginname(permission_task):
            existing_supervisor_schedules = rail.result(permission_task)[
                0]['supervisorAssignmentSchedule'] if rail.result(permission_task)[
                0]['supervisorAssignmentSchedule'] else []
            superuser = list(filter(lambda data: data['supervisor']['displayText'].lower() == rail.result(
                'get_datafortherequireduser_99')['supervisor'].lower(), existing_supervisor_schedules))
            return superuser[0]['supervisor']['user']['loginName'] if superuser else ""

        log_getthesupervisorloginname_126 = rail.PythonOperator(
            task_id='log_getthesupervisorloginname_126',
            python_callable=lambda:  get_supervision_loginname(
                'bulk_get_users3_3')
        )

        if_request_supervisor_present_129 = rail.IfOperator(
            task_id='if_request_supervisor_present_129',
            test='''{{ dag_run.conf.supervisor | is_truthy  and dag_run.conf.supervisor | lower != result('log_getthesupervisorloginname_126') | lower }}''',
            yes_task="if_log_getsupervisor_uri_103_present_130",
            no_task="if_request_employeehourlycost_present_141",
        )

        if_log_getsupervisor_uri_103_present_130 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_103_present_130',
            test='''{{ result('search_users_102') | is_truthy and result('search_users_102').useruri | is_truthy }}''',
            yes_task="if_log_getsupervisor_status_109_equals_to_true_131",
            no_task="genoadi_supervisor_assignment_table_add_entry_139",
        )

        if_log_getsupervisor_status_109_equals_to_true_131 = rail.IfOperator(
            task_id='if_log_getsupervisor_status_109_equals_to_true_131',
            test='''{{ result('search_users_102') | is_truthy and result('search_users_102').status == 'True' }}''',
            yes_task="update_supervisor_assignment_schedule_over_date_range_135",
            no_task="genoadi_supervisor_assignment_table_add_entry_137",
        )

        update_supervisor_assignment_schedule_over_date_range_135 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_135',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_users_102')['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": datetime.strptime(dag_run.conf['supervisoreffectivedate'], '%Y%m%d').year,
                        "month": datetime.strptime(dag_run.conf['supervisoreffectivedate'], '%Y%m%d').month,
                        "day": datetime.strptime(dag_run.conf['supervisoreffectivedate'], '%Y%m%d').day,
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        genoadi_supervisor_assignment_table_add_entry_137 = rail.WriteLogOperator(
            task_id='genoadi_supervisor_assignment_table_add_entry_137',
            message="na",
            log="{{ dag_run.conf.supervisor_processing_log }}",
            severity="supervisor",
            properties={
                "userloginname": "{{ dag_run.conf.loginname }}||{{ dag_run.conf.useruri }}|{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "supervisorloginname": "{{ dag_run.conf.supervisor }}|{{ dag_run.conf.supervisoreffectivedate }}",
                "action": "Update"
            }
        )

        genoadi_supervisor_assignment_table_add_entry_139 = rail.WriteLogOperator(
            task_id='genoadi_supervisor_assignment_table_add_entry_139',
            message="na",
            log="{{ dag_run.conf.supervisor_processing_log }}",
            severity="supervisor",
            properties={
                "userloginname": "{{ dag_run.conf.loginname }}|{{ dag_run.conf.useruri }}|{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "supervisorloginname": "{{ dag_run.conf.supervisor }}|{{ dag_run.conf.supervisoreffectivedate }}",
                "action": "Update"
            }
        )

        if_request_employeehourlycost_present_141 = rail.IfOperator(
            task_id='if_request_employeehourlycost_present_141',
            test='''{{ dag_run.conf.employeehourlycost | is_truthy and dag_run.conf.employeehourlycost != result('get_datafortherequireduser_99').hourlycost }}''',
            yes_task="if_request_userhourlycostcurrency_present_142",
            no_task="if_request_timezone_present_153",
        )

        if_request_userhourlycostcurrency_present_142 = rail.IfOperator(
            task_id='if_request_userhourlycostcurrency_present_142',
            test='''{{ dag_run.conf.userhourlycostcurrency | is_truthy }}''',
            yes_task="_adhoc_http_action_143",
            no_task="_adhoc_http_action_146",
        )

        _adhoc_http_action_143 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_143',
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies",
            data=None
        )

        log_get_currency_uri_144 = rail.PythonOperator(
            task_id='log_get_currency_uri_144',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_143'), 'displayText', dag_run.conf['userhourlycostcurrency'], 'uri')
        )

        _adhoc_http_action_146 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_146',
            endpoint="/services/CurrencyService2.svc/GetBaseCurrency",
            data=None
        )

        log_get_currency_uri_147 = rail.PythonOperator(
            task_id='log_get_currency_uri_147',
            python_callable=lambda:  rail.result(
                '_adhoc_http_action_146')['uri']
        )

        log_required_currency_uri_148 = rail.PythonOperator(
            task_id='log_required_currency_uri_148',
            python_callable=lambda:  rail.result(
                'log_get_currency_uri_144') or rail.result('log_get_currency_uri_147')
        )

        update_userhoulycostschedulewitheffectivedate_152 = rail.RepliconServiceOperator(
            task_id='update_userhoulycostschedulewitheffectivedate_152',
            endpoint="/services/ResourceService1.svc/UpdateUserCostRateScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "hourlyRate": {
                    "amount": dag_run.conf['employeehourlycost'],
                    "currencyUri": rail.result('log_required_currency_uri_148')
                },
                "dateRange": {
                    "startDate": {
                        "year": datetime.strptime(dag_run.conf['employeehourlycosteffectivedate'], '%Y%m%d').year,
                        "month": datetime.strptime(dag_run.conf['employeehourlycosteffectivedate'], '%Y%m%d').month,
                        "day": datetime.strptime(dag_run.conf['employeehourlycosteffectivedate'], '%Y%m%d').day
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_timezone_present_153 = rail.IfOperator(
            task_id='if_request_timezone_present_153',
            test='''{{ dag_run.conf.timezone | is_truthy }}''',
            yes_task="log_get_time_zone_161",
            no_task="if_request_holidaycalendar_present_166",
        )

        def get_timezone(dag_run):
            timezone = ""
            if dag_run.conf['timezone'] == 'NL':
                timezone = "(UTC-3:30) Newfoundland Standard Time"
            if dag_run.conf['timezone'] == 'BC':
                timezone = "(UTC-8:00) Pacific Standard Time"
            if dag_run.conf['timezone'] == 'New Orleans':
                timezone = "(UTC-6:00) Central Standard Time"
            return timezone

        log_get_time_zone_161 = rail.PythonOperator(
            task_id='log_get_time_zone_161',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda dag_run: get_timezone(dag_run)
        )

        if_timezone_displaytext_not_equals_to_dataworkato_variabledeclare_variable_154value_161 = rail.IfOperator(
            task_id='if_timezone_displaytext_not_equals_to_dataworkato_variabledeclare_variable_154value_161',
            test='''{{ result('bulk_get_users3_3')[0].timeZone.displayText != result('log_get_time_zone_161') }}''',
            yes_task="_adhoc_http_action_162",
            no_task="if_request_holidaycalendar_present_166",
        )

        _adhoc_http_action_162 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_162',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
            data=None
        )

        log_get_time_zone_uri_163 = rail.PythonOperator(
            task_id='log_get_time_zone_uri_163',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_162'), 'displayText', rail.result('log_get_time_zone_161'), 'uri')
        )

        if_log_get_time_zone_uri_163_present_164 = rail.IfOperator(
            task_id='if_log_get_time_zone_uri_163_present_164',
            test='''{{ result('log_get_time_zone_uri_163') | is_truthy }}''',
            yes_task="update_time_zone_for_user_165",
            no_task="if_request_holidaycalendar_present_166",
        )

        update_time_zone_for_user_165 = rail.RepliconServiceOperator(
            task_id='update_time_zone_for_user_165',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeZoneUri": "{{ result('log_get_time_zone_uri_163') }}"
            }
        )

        if_request_holidaycalendar_present_166 = rail.IfOperator(
            task_id='if_request_holidaycalendar_present_166',
            test='''{{ dag_run.conf.holidaycalendar | is_truthy  and dag_run.conf.holidaycalendar != result('bulk_get_users3_3')[0].holidayCalendar }}''',
            yes_task="_adhoc_http_action_167",
            no_task="_adhoc_http_action_172",
        )

        _adhoc_http_action_167 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_167',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data=None
        )

        log_get_holiday_calendar_uri_168 = rail.PythonOperator(
            task_id='log_get_holiday_calendar_uri_168',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_167'), 'displayText', dag_run.conf['holidaycalendar'], 'uri')
        )

        if_log_get_holiday_calendar_uri_168_present_169 = rail.IfOperator(
            task_id='if_log_get_holiday_calendar_uri_168_present_169',
            test='''{{ result('log_get_holiday_calendar_uri_168') | is_truthy }}''',
            yes_task="update_holiday_calendar_for_user_170",
            no_task="_adhoc_http_action_172",
        )

        update_holiday_calendar_for_user_170 = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user_170',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": "{{ result('log_get_holiday_calendar_uri_168') }}"
            }
        )

        _adhoc_http_action_172 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_172',
            endpoint="/services/LocationService1.svc/GetAllLocations",
            data=None
        )

        if_log_checkifanylocationisassigned_171_equals_to_urnrepliconlisttypenull_173 = rail.IfOperator(
            task_id='if_log_checkifanylocationisassigned_171_equals_to_urnrepliconlisttypenull_173',
            test='''{{ result('get_datafortherequireduser_99').location | is_falsy }}''',
            yes_task="if_request_location_present_174",
            no_task="if_log_checkifanylocationisassigned_171_not_equals_to_urnrepliconlisttypenull_180",
        )

        if_request_location_present_174 = rail.IfOperator(
            task_id='if_request_location_present_174',
            test='''{{ dag_run.conf.location | is_truthy }}''',
            yes_task="log_get_required_location_uri_175",
            no_task="if_log_checkifanylocationisassigned_171_not_equals_to_urnrepliconlisttypenull_180",
        )

        log_get_required_location_uri_175 = rail.PythonOperator(
            task_id='log_get_required_location_uri_175',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_172'), 'displayText', dag_run.conf['location'], 'uri')
        )

        if_log_get_required_location_uri_175_present_176 = rail.IfOperator(
            task_id='if_log_get_required_location_uri_175_present_176',
            test='''{{ result('log_get_required_location_uri_175') | is_truthy }}''',
            yes_task="put_location_schedule_for_user_177",
            no_task="if_log_get_required_location_uri_175_blank_178",
        )

        put_location_schedule_for_user_177 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_177',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "location": {
                            "uri": "{{ result('log_get_required_location_uri_175') }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_log_get_required_location_uri_175_blank_178 = rail.IfOperator(
            task_id='if_log_get_required_location_uri_175_blank_178',
            test='''{{ result('log_get_required_location_uri_175') | is_falsy }}''',
            yes_task="log_errormessageincasewhenlocationisnotavailable_179",
            no_task="if_log_checkifanylocationisassigned_171_not_equals_to_urnrepliconlisttypenull_180",
        )

        log_errormessageincasewhenlocationisnotavailable_179 = rail.PythonOperator(
            task_id='log_errormessageincasewhenlocationisnotavailable_179',
            python_callable=lambda dag_run: "Location not updated for User " +
            dag_run.conf['firstname'] + " "+dag_run.conf['lastname']+" as " +
                    dag_run.conf['location'] + " is not available in Replicon"
        )

        if_log_checkifanylocationisassigned_171_not_equals_to_urnrepliconlisttypenull_180 = rail.IfOperator(
            task_id='if_log_checkifanylocationisassigned_171_not_equals_to_urnrepliconlisttypenull_180',
            test='''{{ result('get_datafortherequireduser_99').location | is_truthy }}''',
            yes_task="log_getthecurrent_location_181",
            no_task="log_checkifanycostcenterisassigned_202",
        )

        log_getthecurrent_location_181 = rail.PythonOperator(
            task_id='log_getthecurrent_location_181',
            python_callable=lambda:  rail.result(
                'get_datafortherequireduser_99')['location']
        )

        if_request_location_present_184 = rail.IfOperator(
            task_id='if_request_location_present_184',
            test='''{{ dag_run.conf.location | is_truthy  and dag_run.conf.location != result('log_getthecurrent_location_181') }}''',
            yes_task="log_get_required_location_uri_185",
            no_task="log_checkifanycostcenterisassigned_202",
        )

        log_get_required_location_uri_185 = rail.PythonOperator(
            task_id='log_get_required_location_uri_185',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_172'), 'displayText', dag_run.conf['location'], 'uri')
        )

        if_log_get_required_location_uri_185_blank_186 = rail.IfOperator(
            task_id='if_log_get_required_location_uri_185_blank_186',
            test='''{{ result('log_get_required_location_uri_185') | is_falsy }}''',
            yes_task="log_errormessageincasewhenlocationisnotavailable_187",
            no_task="if_log_get_required_location_uri_185_present_188",
        )

        log_errormessageincasewhenlocationisnotavailable_187 = rail.PythonOperator(
            task_id='log_errormessageincasewhenlocationisnotavailable_187',
            python_callable=lambda dag_run:  "Location not updated for User " + dag_run.conf['firstname'] +
            " "+dag_run.conf['lastname'] + " as " +
                    dag_run.conf['location'] + " is not available in Replicon"
        )

        if_log_get_required_location_uri_185_present_188 = rail.IfOperator(
            task_id='if_log_get_required_location_uri_185_present_188',
            test='''{{ result('log_get_required_location_uri_185') | is_truthy }}''',
            yes_task="log_location_schedule_199",
            no_task="log_checkifanycostcenterisassigned_202",
        )

        def location_schedule_data():
            derived_location_schedules = []
            location_schedules = rail.result('bulk_get_users3_3')[
                0]['locationSchedule']
            for location_schedule in location_schedules:
                if location_schedule['effectiveDate']:
                    effective_date = get_datetime_obj(
                        location_schedule['effectiveDate'])
                    if effective_date.date() > pendulum.now(config.pacific_timezone).date():
                        derived_location_schedules.append({
                            "location": {
                                "uri": location_schedule['location']['uri'],
                                "name": null
                            },
                            "effectiveDate": location_schedule['effectiveDate']
                        })
                else:
                    derived_location_schedules.append({
                        "location": {
                            "uri": location_schedule['location']['uri'],
                            "name": null
                        },
                        "effectiveDate": null
                    })

            derived_location_schedules.append({
                "location": {
                    "uri": rail.result('log_get_required_location_uri_185'),
                    "name": null
                },
                "effectiveDate": {
                    "year": pendulum.now(config.pacific_timezone).year,
                    "month": pendulum.now(config.pacific_timezone).month,
                    "day": pendulum.now(config.pacific_timezone).day
                }
            })

            return derived_location_schedules

        log_location_schedule_199 = rail.PythonOperator(
            task_id='log_location_schedule_199',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda:  location_schedule_data()
        )

        if_log_location_schedule_199_present_200 = rail.IfOperator(
            task_id='if_log_location_schedule_199_present_200',
            test='''{{ result('log_location_schedule_199') | is_truthy }}''',
            yes_task="put_location_schedule_for_user_201",
            no_task="log_checkifanycostcenterisassigned_202",
        )

        put_location_schedule_for_user_201 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_201',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": json.loads(json.dumps(
                    rail.result('log_location_schedule_199'), indent=2))
                # '''{{ result('log_location_schedule_199') }}'''
            }
        )

        log_checkifanycostcenterisassigned_202 = rail.PythonOperator(
            task_id='log_checkifanycostcenterisassigned_202',
            python_callable=lambda:  rail.result(
                'get_datafortherequireduser_99')['costcenter']
        )

        _adhoc_http_action_203 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_203',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
            data=None
        )

        if_log_checkifanycostcenterisassigned_202_equals_to_urnrepliconlisttypenull_204 = rail.IfOperator(
            task_id='if_log_checkifanycostcenterisassigned_202_equals_to_urnrepliconlisttypenull_204',
            test='''{{ result('log_checkifanycostcenterisassigned_202') | is_falsy }}''',
            yes_task="if_request_team_present_205",
            no_task="if_log_checkifanycostcenterisassigned_202_not_equals_to_urnrepliconlisttypenull_211",
        )

        if_request_team_present_205 = rail.IfOperator(
            task_id='if_request_team_present_205',
            test='''{{ dag_run.conf.team | is_truthy }}''',
            yes_task="log_get_required_team_uri_206",
            no_task="if_log_checkifanycostcenterisassigned_202_not_equals_to_urnrepliconlisttypenull_211",
        )

        log_get_required_team_uri_206 = rail.PythonOperator(
            task_id='log_get_required_team_uri_206',
            python_callable=lambda dag_run:  rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_203'), 'displayText', dag_run.conf['team'], 'uri')
        )

        if_log_get_required_team_uri_206_present_207 = rail.IfOperator(
            task_id='if_log_get_required_team_uri_206_present_207',
            test='''{{ result('log_get_required_team_uri_206') | is_truthy }}''',
            yes_task="put_cost_center_schedule_for_user_208",
            no_task="log_errormessageincasewhenteamisnotavailable_210",
        )

        put_cost_center_schedule_for_user_208 = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user_208',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "costCenter": {
                            "uri": "{{ result('log_get_required_team_uri_206') }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        log_errormessageincasewhenteamisnotavailable_210 = rail.PythonOperator(
            task_id='log_errormessageincasewhenteamisnotavailable_210',
            python_callable=lambda dag_run:  "Team not added for User " + dag_run.conf['firstname'] +
            " "+dag_run.conf['lastname']+" as " +
            dag_run.conf['team']+" is not available in Replicon"
        )

        if_log_checkifanycostcenterisassigned_202_not_equals_to_urnrepliconlisttypenull_211 = rail.IfOperator(
            task_id='if_log_checkifanycostcenterisassigned_202_not_equals_to_urnrepliconlisttypenull_211',
            test='''{{ result('log_checkifanycostcenterisassigned_202') | is_truthy }}''',
            yes_task="log_getthecurrent_team_212",
            no_task="genoadi_user_import_logs_add_entry_233",
        )

        log_getthecurrent_team_212 = rail.PythonOperator(
            task_id='log_getthecurrent_team_212',
            python_callable=lambda:  rail.result(
                'get_datafortherequireduser_99')['costcenter']
        )

        if_request_team_present_215 = rail.IfOperator(
            task_id='if_request_team_present_215',
            test='''{{ dag_run.conf.team | is_truthy  and dag_run.conf.team != result('log_getthecurrent_team_212') }}''',
            yes_task="log_get_required_team_uri_216",
            no_task="genoadi_user_import_logs_add_entry_233",
        )

        log_get_required_team_uri_216 = rail.PythonOperator(
            task_id='log_get_required_team_uri_216',
            python_callable=lambda dag_run:  rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_203'), 'displayText', dag_run.conf['team'], 'uri')
        )

        if_log_get_required_team_uri_216_blank_218 = rail.IfOperator(
            task_id='if_log_get_required_team_uri_216_blank_218',
            test='''{{ result('log_get_required_team_uri_216') | is_falsy }}''',
            yes_task="log_errormessageincasewhen_teamisnotavailable_219",
            no_task="if_log_get_required_team_uri_216_present_220",
        )

        log_errormessageincasewhen_teamisnotavailable_219 = rail.PythonOperator(
            task_id='log_errormessageincasewhen_teamisnotavailable_219',
            python_callable=lambda dag_run:  "Team  not updated for User "+dag_run.conf['firstname'] +
            " " + dag_run.conf['lastname']+" as " +
                    dag_run.conf['team']+" is not available in Replicon"
        )

        if_log_get_required_team_uri_216_present_220 = rail.IfOperator(
            task_id='if_log_get_required_team_uri_216_present_220',
            test='''{{ result('log_get_required_team_uri_216') | is_truthy }}''',
            yes_task="log_team_schedule_230",
            no_task="genoadi_user_import_logs_add_entry_233",
        )

        def costcenter_schedule_data():
            derived_costcenter_schedules = []
            costcenter_schedules = rail.result('bulk_get_users3_3')[
                0]['costCenterSchedule']
            for costcenter_schedule in costcenter_schedules:
                if costcenter_schedule['effectiveDate']:
                    effective_date = get_datetime_obj(
                        costcenter_schedule['effectiveDate'])
                    if effective_date.date() > pendulum.now(config.pacific_timezone).date():
                        derived_costcenter_schedules.append({
                            "costcenter": {
                                "uri": costcenter_schedule['costCenter']['uri'],
                                "name": null
                            },
                            "effectiveDate": costcenter_schedule['effectiveDate']
                        })
                else:
                    derived_costcenter_schedules.append({
                        "costcenter": {
                            "uri": costcenter_schedule['costCenter']['uri'],
                            "name": null
                        },
                        "effectiveDate": null
                    })

            derived_costcenter_schedules.append({
                "costcenter": {
                    "uri": rail.result('log_get_required_team_uri_216'),
                    "name": null
                },
                "effectiveDate": {
                    "year": pendulum.now(config.pacific_timezone).year,
                    "month": pendulum.now(config.pacific_timezone).month,
                    "day": pendulum.now(config.pacific_timezone).day
                }
            })

            return derived_costcenter_schedules

        log_team_schedule_230 = rail.PythonOperator(
            task_id='log_team_schedule_230',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda: costcenter_schedule_data()
        )

        if_log_team_schedule_230_present_231 = rail.IfOperator(
            task_id='if_log_team_schedule_230_present_231',
            test='''{{ result('log_team_schedule_230') | is_truthy }}''',
            yes_task="put_cost_center_schedule_for_user_232",
            no_task="genoadi_user_import_logs_add_entry_233",
        )

        put_cost_center_schedule_for_user_232 = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user_232',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": costcenter_schedule_data()
            }
        )

        def get_status_details():
            details = ["Updated Successfully",
                       rail.result(
                           'log_errormessageincasewhen_teamisnotavailable_219'),
                       rail.result('log_error_logfordepartmentnotpresent_92'),
                       rail.result(
                           'log_errormessageincasewhenlocationisnotavailable_187'),
                       rail.result(
                           'log_errormessageincasewhenteamisnotavailable_210'),
                       rail.result('log_errormessageincasewhenlocationisnotavailable_179')]
            return rail.smartjoin_by_delim(details, ';')

        genoadi_user_import_logs_add_entry_233 = rail.WriteLogOperator(
            task_id='genoadi_user_import_logs_add_entry_233',
            message="na",
            severity=lambda: "Failed" if rail.result('log_errormessageincasewhen_teamisnotavailable_219') or rail.result('log_error_logfordepartmentnotpresent_92') or rail.result(
                'log_errormessageincasewhenlocationisnotavailable_187') or rail.result('log_errormessageincasewhenteamisnotavailable_210') or rail.result('log_errormessageincasewhenlocationisnotavailable_179') else "Success",
            properties=lambda dag_run: {
                "username|loginname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'] + "|" + dag_run.conf['loginname'],
                "status": "Failed" if rail.result('log_errormessageincasewhen_teamisnotavailable_219') or rail.result('log_error_logfordepartmentnotpresent_92') or rail.result('log_errormessageincasewhenlocationisnotavailable_187') or rail.result('log_errormessageincasewhenteamisnotavailable_210') or rail.result('log_errormessageincasewhenlocationisnotavailable_179') else "Success",
                "details": get_status_details(),
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        genoadi_user_import_logs_add_entry_235 = rail.WriteLogOperator(
            task_id='genoadi_user_import_logs_add_entry_235',
            message="na",
            severity="Error",
            trigger_rule='one_failed',
            properties={
                "username|loginname": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}|{{ dag_run.conf.loginname }}",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        disable_users = rail.EmptyOperator(
            task_id="disable_users"
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> bulk_get_users3_3
        bulk_get_users3_3 >> if_userdetails_isenabled_is_true_4
        if_userdetails_isenabled_is_true_4 >> rail.Label(
            'Yes') >> trigger_dag_run_live_gdi_child_workflow_to_disable_user_v1_05 >> \
            wait_for_completion_trigger_dag_run_live_gdi_child_workflow_to_disable_user_v1_05 >> disable_users >> genoadi_user_import_logs_add_entry_235
        if_userdetails_isenabled_is_true_4 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_7
        if_userdetails_isenabled_is_not_true_7 >> rail.Label(
            'Yes') >> genoadi_user_import_logs_add_entry_8 >> genoadi_user_import_logs_add_entry_235
        if_userdetails_isenabled_is_not_true_7 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_10
        if_userdetails_isenabled_is_not_true_10 >> rail.Label(
            'Yes') >> enable_login_11 >> update_employment_date_rangetoremoveenddate_12 >> if_request_firstname_present_13
        if_userdetails_isenabled_is_not_true_10 >> rail.Label(
            'No') >> if_request_firstname_present_13
        if_request_firstname_present_13 >> rail.Label(
            'Yes') >> update_first_name_14 >> if_request_lastname_present_15
        if_request_firstname_present_13 >> rail.Label(
            'No') >> if_request_lastname_present_15
        if_request_lastname_present_15 >> rail.Label(
            'Yes') >> update_last_name_16 >> if_request_email_contains_17
        if_request_lastname_present_15 >> rail.Label(
            'No') >> if_request_email_contains_17
        if_request_email_contains_17 >> rail.Label(
            'Yes') >> update_email_18 >> if_request_employeeid_present_19
        if_request_email_contains_17 >> rail.Label(
            'No') >> if_request_employeeid_present_19
        if_request_employeeid_present_19 >> rail.Label(
            'Yes') >> update_employee_id_20 >> if_request_startdate_present_21
        if_request_employeeid_present_19 >> rail.Label(
            'No') >> if_request_startdate_present_21
        if_request_startdate_present_21 >> rail.Label(
            'Yes') >> log_start_dateasper_repliconprofile_26 >> \
            if_to_date_not_equals_to_dataloggerlog_start_dateasper_repliconprofile_26messageto_date_27
        if_to_date_not_equals_to_dataloggerlog_start_dateasper_repliconprofile_26messageto_date_27 >> rail.Label(
            'Yes') >> update_employment_date_rangeforstartdate_28 >> if_request_employeetype_present_fulltimehourly_33
        if_to_date_not_equals_to_dataloggerlog_start_dateasper_repliconprofile_26messageto_date_27 >> rail.Label(
            'No') >> if_request_employeetype_present_fulltimehourly_33
        if_request_startdate_present_21 >> rail.Label(
            'No') >> if_request_employeetype_present_fulltimehourly_33
        if_request_employeetype_present_fulltimehourly_33 >> rail.Label(
            'Yes') >> _adhoc_http_action_34 >> log_employee_type_uri_37 >> if_log_employee_type_uri_37_present_38
        if_log_employee_type_uri_37_present_38 >> rail.Label(
            'Yes') >> update_employee_type_for_user_39 >> _adhoc_http_action_40
        if_log_employee_type_uri_37_present_38 >> rail.Label(
            'No') >> _adhoc_http_action_40 >> if_fulltimehourlyfulltimesalaried_contains_dataworkato_service3cd9c331requestemployeetype_41
        if_fulltimehourlyfulltimesalaried_contains_dataworkato_service3cd9c331requestemployeetype_41 >> rail.Label(
            'Yes') >> trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_042 >> \
            wait_for_completion_trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_042 >> \
            if_request_employeetype_equals_to_fulltimehourly_43
        if_fulltimehourlyfulltimesalaried_contains_dataworkato_service3cd9c331requestemployeetype_41 >> rail.Label(
            'No') >> if_request_employeetype_equals_to_fulltimehourly_43
        if_request_employeetype_equals_to_fulltimehourly_43 >> rail.Label(
            'Yes') >> trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_044 >> \
            wait_for_completion_trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_044 >> \
            if_request_employeetype_equals_to_fulltimesalaried_45
        if_request_employeetype_equals_to_fulltimehourly_43 >> rail.Label(
            'No') >> if_request_employeetype_equals_to_fulltimesalaried_45
        if_request_employeetype_equals_to_fulltimesalaried_45 >> rail.Label(
            'Yes') >> trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_046 >> \
            wait_for_completion_trigger_dag_run_live_gdi_child_workflow_to_add_remove_timeoff_type_for_existing_user_employee_type_change_v1_046 >> \
            if_fulltimehourlyfulltimesalaried_not_contains_dataworkato_service3cd9c331requestemployeetype_47
        if_request_employeetype_equals_to_fulltimesalaried_45 >> rail.Label(
            'No') >> if_fulltimehourlyfulltimesalaried_not_contains_dataworkato_service3cd9c331requestemployeetype_47
        if_fulltimehourlyfulltimesalaried_not_contains_dataworkato_service3cd9c331requestemployeetype_47 >> rail.Label(
            'Yes') >> remove_timeoffassignmentsforusers_48 >> if_request_employeetype_equals_to_fulltimehourly_49
        if_fulltimehourlyfulltimesalaried_not_contains_dataworkato_service3cd9c331requestemployeetype_47 >> rail.Label(
            'No') >> if_request_employeetype_equals_to_fulltimehourly_49
        if_request_employeetype_equals_to_fulltimehourly_49 >> rail.Label(
            'Yes') >> existing_payrule_data >> log_pluckif_pay_ruleispresent_59 >> log_get_pay_rule_script_uri_60 >> if_declare_list_50_list_items_less_than_1_61
        if_declare_list_50_list_items_less_than_1_61 >> rail.Label(
            'No') >> if_log_get_pay_rule_script_uri_60_present_65
        if_declare_list_50_list_items_less_than_1_61 >> rail.Label(
            'Yes') >> if_log_get_pay_rule_script_uri_60_present_enabled_62
        if_log_get_pay_rule_script_uri_60_present_enabled_62 >> rail.Label(
            'Yes') >> put_payroll_assignment_63 >> if_request_employeetype_not_equals_to_fulltimehourly_69
        if_log_get_pay_rule_script_uri_60_present_enabled_62 >> rail.Label(
            'No') >> if_request_employeetype_not_equals_to_fulltimehourly_69
        if_log_get_pay_rule_script_uri_60_present_65 >> rail.Label(
            'Yes') >> put_payroll_assignment_68 >> if_request_employeetype_not_equals_to_fulltimehourly_69
        if_log_get_pay_rule_script_uri_60_present_65 >> rail.Label(
            'No') >> if_request_employeetype_not_equals_to_fulltimehourly_69
        if_request_employeetype_equals_to_fulltimehourly_49 >> rail.Label(
            'No') >> if_request_employeetype_not_equals_to_fulltimehourly_69
        if_request_employeetype_not_equals_to_fulltimehourly_69 >> rail.Label(
            'Yes') >> existing_payrule_data_not_fulltime >> log_pluckif_pay_ruleispresent_79 >> log_get_pay_rule_script_uri_80 >> if_declare_list_70_list_items_greater_than_0_81
        if_declare_list_70_list_items_greater_than_0_81 >> rail.Label(
            'Yes') >> if_log_get_pay_rule_script_uri_80_present_82
        if_log_get_pay_rule_script_uri_80_present_82 >> rail.Label(
            'Yes') >> put_payroll_assignment_85 >> if_request_departmentname_present_86
        if_log_get_pay_rule_script_uri_80_present_82 >> rail.Label(
            'No') >> if_request_departmentname_present_86
        if_declare_list_70_list_items_greater_than_0_81 >> rail.Label(
            'No') >> if_request_departmentname_present_86
        if_request_employeetype_not_equals_to_fulltimehourly_69 >> rail.Label(
            'No') >> if_request_departmentname_present_86
        if_request_employeetype_present_fulltimehourly_33 >> rail.Label(
            'No') >> if_request_departmentname_present_86
        if_request_departmentname_present_86 >> rail.Label(
            'Yes') >> _adhoc_http_action_87 >> log_departmenturi_88 >> if_log_departmenturi_88_present_89
        if_log_departmenturi_88_present_89 >> rail.Label(
            'Yes') >> update_department_for_user_90 >> get_activity_dataforthedepartment_93
        if_log_departmenturi_88_present_89 >> rail.Label(
            'No') >> log_error_logfordepartmentnotpresent_92 >> get_activity_dataforthedepartment_93 >> \
            if_log_activity_uristobeassigned_96_present_97
        if_log_activity_uristobeassigned_96_present_97 >> rail.Label(
            'Yes') >> put_activity_assignments_for_user_98 >> get_datafortherequireduser_99
        if_log_activity_uristobeassigned_96_present_97 >> rail.Label(
            'No') >> get_datafortherequireduser_99
        if_request_departmentname_present_86 >> rail.Label(
            'No') >> get_datafortherequireduser_99 >> if_request_supervisor_present_101
        if_request_supervisor_present_101 >> rail.Label(
            'Yes') >> search_users_102 >> if_request_loginname_equals_to_dataloggerlog_getsupervisor_login_name_104message_105
        if_request_loginname_equals_to_dataloggerlog_getsupervisor_login_name_104message_105 >> rail.Label(
            'Yes') >> log_error_messagefor_supervisoranduserloginnamesame_106 >> \
            if_request_loginname_not_equals_to_dataloggerlog_getsupervisor_login_name_104message_107
        if_request_loginname_equals_to_dataloggerlog_getsupervisor_login_name_104message_105 >> rail.Label(
            'No') >> if_request_loginname_not_equals_to_dataloggerlog_getsupervisor_login_name_104message_107
        if_request_loginname_not_equals_to_dataloggerlog_getsupervisor_login_name_104message_107 >> rail.Label(
            'Yes') >> _adhoc_http_action_108 >> if_log_getsupervisor_uri_103_present_110
        if_log_getsupervisor_uri_103_present_110 >> rail.Label(
            'Yes') >> _adhoc_http_action_111 >> log_checkifsupervisorhassupervisorpermission_112 >> \
            if_log_checkifsupervisorhassupervisorpermission_112_blank_113
        if_log_checkifsupervisorhassupervisorpermission_112_blank_113 >> rail.Label(
            'Yes') >> log_get_supervisor_permission_114 >> assign_supervsior_permission_set_to_user_115 >> \
            if_log_checkifsupervsorisassigned_100_equals_to_urnrepliconlisttypenull_116
        if_log_checkifsupervisorhassupervisorpermission_112_blank_113 >> rail.Label(
            'No') >> if_log_checkifsupervsorisassigned_100_equals_to_urnrepliconlisttypenull_116
        if_log_getsupervisor_uri_103_present_110 >> rail.Label(
            'No') >> if_log_checkifsupervsorisassigned_100_equals_to_urnrepliconlisttypenull_116
        if_log_checkifsupervsorisassigned_100_equals_to_urnrepliconlisttypenull_116 >> rail.Label(
            'Yes') >> if_log_getsupervisor_status_109_equals_to_true_117
        if_log_getsupervisor_status_109_equals_to_true_117 >> rail.Label(
            'No') >> genoadi_supervisor_assignment_table_add_entry_123 >> \
            if_log_checkifsupervsorisassigned_100_not_equals_to_urnrepliconlisttypenull_124
        if_log_getsupervisor_status_109_equals_to_true_117 >> rail.Label(
            'Yes') >> if_log_getsupervisor_uri_103_present_118
        if_log_getsupervisor_uri_103_present_118 >> rail.Label(
            'Yes') >> update_initial_supervisor_119 >> \
            if_log_checkifsupervsorisassigned_100_not_equals_to_urnrepliconlisttypenull_124
        if_log_getsupervisor_uri_103_present_118 >> rail.Label(
            'No') >> genoadi_supervisor_assignment_table_add_entry_121 >> \
            if_log_checkifsupervsorisassigned_100_not_equals_to_urnrepliconlisttypenull_124
        if_log_checkifsupervsorisassigned_100_equals_to_urnrepliconlisttypenull_116 >> rail.Label(
            'No') >> if_log_checkifsupervsorisassigned_100_not_equals_to_urnrepliconlisttypenull_124
        if_log_checkifsupervsorisassigned_100_not_equals_to_urnrepliconlisttypenull_124 >> rail.Label(
            'Yes') >> log_getthesupervisorloginname_126 >> if_request_supervisor_present_129
        if_request_supervisor_present_129 >> rail.Label(
            'Yes') >> if_log_getsupervisor_uri_103_present_130
        if_log_getsupervisor_uri_103_present_130 >> rail.Label(
            'No') >> genoadi_supervisor_assignment_table_add_entry_139 >> if_request_employeehourlycost_present_141
        if_log_getsupervisor_uri_103_present_130 >> rail.Label(
            'Yes') >> if_log_getsupervisor_status_109_equals_to_true_131
        if_log_getsupervisor_status_109_equals_to_true_131 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_135 >> \
            if_request_employeehourlycost_present_141
        if_log_getsupervisor_status_109_equals_to_true_131 >> rail.Label(
            'No') >> genoadi_supervisor_assignment_table_add_entry_137 >> if_request_employeehourlycost_present_141
        if_request_supervisor_present_129 >> rail.Label(
            'No') >> if_request_employeehourlycost_present_141
        if_log_checkifsupervsorisassigned_100_not_equals_to_urnrepliconlisttypenull_124 >> rail.Label(
            'No') >> if_request_employeehourlycost_present_141
        if_request_loginname_not_equals_to_dataloggerlog_getsupervisor_login_name_104message_107 >> rail.Label(
            'No') >> if_request_employeehourlycost_present_141
        if_request_supervisor_present_101 >> rail.Label(
            'No') >> if_request_employeehourlycost_present_141
        if_request_employeehourlycost_present_141 >> rail.Label(
            'Yes') >> if_request_userhourlycostcurrency_present_142
        if_request_userhourlycostcurrency_present_142 >> rail.Label(
            'Yes') >> _adhoc_http_action_143 >> log_get_currency_uri_144 >> log_required_currency_uri_148
        if_request_userhourlycostcurrency_present_142 >> rail.Label(
            'No') >> _adhoc_http_action_146 >> log_get_currency_uri_147 >> log_required_currency_uri_148 >> \
            update_userhoulycostschedulewitheffectivedate_152 >> if_request_timezone_present_153
        if_request_employeehourlycost_present_141 >> rail.Label(
            'No') >> if_request_timezone_present_153
        if_request_timezone_present_153 >> rail.Label(
            'Yes') >> log_get_time_zone_161 >> if_timezone_displaytext_not_equals_to_dataworkato_variabledeclare_variable_154value_161
        if_timezone_displaytext_not_equals_to_dataworkato_variabledeclare_variable_154value_161 >> rail.Label(
            'Yes') >> _adhoc_http_action_162 >> log_get_time_zone_uri_163 >> if_log_get_time_zone_uri_163_present_164
        if_log_get_time_zone_uri_163_present_164 >> rail.Label(
            'Yes') >> update_time_zone_for_user_165 >> if_request_holidaycalendar_present_166
        if_log_get_time_zone_uri_163_present_164 >> rail.Label(
            'No') >> if_request_holidaycalendar_present_166
        if_timezone_displaytext_not_equals_to_dataworkato_variabledeclare_variable_154value_161 >> rail.Label(
            'No') >> if_request_holidaycalendar_present_166
        if_request_timezone_present_153 >> rail.Label(
            'No') >> if_request_holidaycalendar_present_166
        if_request_holidaycalendar_present_166 >> rail.Label(
            'Yes') >> _adhoc_http_action_167 >> log_get_holiday_calendar_uri_168 >> if_log_get_holiday_calendar_uri_168_present_169
        if_log_get_holiday_calendar_uri_168_present_169 >> rail.Label(
            'Yes') >> update_holiday_calendar_for_user_170 >> _adhoc_http_action_172
        if_log_get_holiday_calendar_uri_168_present_169 >> rail.Label(
            'No') >> _adhoc_http_action_172
        if_request_holidaycalendar_present_166 >> rail.Label(
            'No') >> _adhoc_http_action_172 >> if_log_checkifanylocationisassigned_171_equals_to_urnrepliconlisttypenull_173
        if_log_checkifanylocationisassigned_171_equals_to_urnrepliconlisttypenull_173 >> rail.Label(
            'Yes') >> if_request_location_present_174
        if_request_location_present_174 >> rail.Label(
            'Yes') >> log_get_required_location_uri_175 >> if_log_get_required_location_uri_175_present_176
        if_log_get_required_location_uri_175_present_176 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_177 >> if_log_get_required_location_uri_175_blank_178
        if_log_get_required_location_uri_175_present_176 >> rail.Label(
            'No') >> if_log_get_required_location_uri_175_blank_178
        if_log_get_required_location_uri_175_blank_178 >> rail.Label(
            'Yes') >> log_errormessageincasewhenlocationisnotavailable_179 >> \
            if_log_checkifanylocationisassigned_171_not_equals_to_urnrepliconlisttypenull_180
        if_log_get_required_location_uri_175_blank_178 >> rail.Label('No') >> \
            if_log_checkifanylocationisassigned_171_not_equals_to_urnrepliconlisttypenull_180
        if_request_location_present_174 >> rail.Label(
            'No') >> if_log_checkifanylocationisassigned_171_not_equals_to_urnrepliconlisttypenull_180
        if_log_checkifanylocationisassigned_171_equals_to_urnrepliconlisttypenull_173 >> rail.Label(
            'No') >> if_log_checkifanylocationisassigned_171_not_equals_to_urnrepliconlisttypenull_180
        if_log_checkifanylocationisassigned_171_not_equals_to_urnrepliconlisttypenull_180 >> rail.Label(
            'Yes') >> log_getthecurrent_location_181 >> if_request_location_present_184
        if_request_location_present_184 >> rail.Label(
            'Yes') >> log_get_required_location_uri_185 >> if_log_get_required_location_uri_185_blank_186
        if_log_get_required_location_uri_185_blank_186 >> rail.Label(
            'Yes') >> log_errormessageincasewhenlocationisnotavailable_187 >> if_log_get_required_location_uri_185_present_188
        if_log_get_required_location_uri_185_blank_186 >> rail.Label(
            'No') >> if_log_get_required_location_uri_185_present_188
        if_log_get_required_location_uri_185_present_188 >> rail.Label(
            'Yes') >> log_location_schedule_199 >> if_log_location_schedule_199_present_200
        if_log_location_schedule_199_present_200 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_201 >> log_checkifanycostcenterisassigned_202
        if_log_location_schedule_199_present_200 >> rail.Label(
            'No') >> log_checkifanycostcenterisassigned_202
        if_log_get_required_location_uri_185_present_188 >> rail.Label(
            'No') >> log_checkifanycostcenterisassigned_202
        if_request_location_present_184 >> rail.Label(
            'No') >> log_checkifanycostcenterisassigned_202
        if_log_checkifanylocationisassigned_171_not_equals_to_urnrepliconlisttypenull_180 >> rail.Label(
            'No') >> log_checkifanycostcenterisassigned_202 >> _adhoc_http_action_203 >> \
            if_log_checkifanycostcenterisassigned_202_equals_to_urnrepliconlisttypenull_204
        if_log_checkifanycostcenterisassigned_202_equals_to_urnrepliconlisttypenull_204 >> rail.Label(
            'Yes') >> if_request_team_present_205
        if_request_team_present_205 >> rail.Label(
            'Yes') >> log_get_required_team_uri_206 >> if_log_get_required_team_uri_206_present_207
        if_log_get_required_team_uri_206_present_207 >> rail.Label(
            'Yes') >> put_cost_center_schedule_for_user_208 >> if_log_checkifanycostcenterisassigned_202_not_equals_to_urnrepliconlisttypenull_211
        if_log_get_required_team_uri_206_present_207 >> rail.Label(
            'No') >> log_errormessageincasewhenteamisnotavailable_210 >> if_log_checkifanycostcenterisassigned_202_not_equals_to_urnrepliconlisttypenull_211
        if_request_team_present_205 >> rail.Label(
            'No') >> if_log_checkifanycostcenterisassigned_202_not_equals_to_urnrepliconlisttypenull_211
        if_log_checkifanycostcenterisassigned_202_equals_to_urnrepliconlisttypenull_204 >> rail.Label(
            'No') >> if_log_checkifanycostcenterisassigned_202_not_equals_to_urnrepliconlisttypenull_211
        if_log_checkifanycostcenterisassigned_202_not_equals_to_urnrepliconlisttypenull_211 >> rail.Label(
            'Yes') >> log_getthecurrent_team_212 >> if_request_team_present_215
        if_request_team_present_215 >> rail.Label(
            'Yes') >> log_get_required_team_uri_216 >> if_log_get_required_team_uri_216_blank_218
        if_log_get_required_team_uri_216_blank_218 >> rail.Label(
            'Yes') >> log_errormessageincasewhen_teamisnotavailable_219 >> if_log_get_required_team_uri_216_present_220
        if_log_get_required_team_uri_216_blank_218 >> rail.Label(
            'No') >> if_log_get_required_team_uri_216_present_220
        if_log_get_required_team_uri_216_present_220 >> rail.Label(
            'Yes') >> log_team_schedule_230 >> if_log_team_schedule_230_present_231
        if_log_team_schedule_230_present_231 >> rail.Label(
            'Yes') >> put_cost_center_schedule_for_user_232 >> genoadi_user_import_logs_add_entry_233
        if_log_team_schedule_230_present_231 >> rail.Label(
            'No') >> genoadi_user_import_logs_add_entry_233
        if_log_get_required_team_uri_216_present_220 >> rail.Label(
            'No') >> genoadi_user_import_logs_add_entry_233
        if_request_team_present_215 >> rail.Label(
            'No') >> genoadi_user_import_logs_add_entry_233
        if_log_checkifanycostcenterisassigned_202_not_equals_to_urnrepliconlisttypenull_211 >> rail.Label(
            'No') >> genoadi_user_import_logs_add_entry_233 >> genoadi_user_import_logs_add_entry_235 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
