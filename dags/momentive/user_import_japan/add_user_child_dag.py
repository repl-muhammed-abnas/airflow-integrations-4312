from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail
from pendulum import now, datetime as dt
from rail.lib.ecid import get_dagrun_ecid
from momentive.user_import_japan.mappers.momentive_user_import_mapper import momentive_user_import_mapper
from momentive.user_import_japan.utils import python_callable, request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_japan_user_sync_child_add_user_dag_id,
        description=f'Momentive Japan User Sync Add Child {config.instance}',
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
            no_task='create_exceptionlogger_list'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_exceptionlogger_list',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_exceptionlogger_list = rail.SetVariableOperator(
            task_id='create_exceptionlogger_list',
            append=False,
            name='exceptionlogger_list',
            value=[]
        )

        get_input_validation_log = rail.PythonOperator(
            task_id="get_input_validation_log",
            python_callable=python_callable.get_input_validationlog
        )

        if_input_validation_log_present = rail.IfOperator(
            task_id='if_input_validation_log_present',
            test="{{ result('get_input_validation_log').exc_present | is_truthy }}",
            yes_task="log_user_import_not_created",
            no_task="if_gender_not_present_8",
        )

        log_user_import_not_created = rail.WriteLogOperator(
            task_id="log_user_import_not_created",
            log='{{ dag_run.conf.user_import_logs}}',
            message="na",
            severity="Warning",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['userid'],
                "username": dag_run.conf['firstname'] + "|" + dag_run.conf['lastname'],
                "action": "Add",
                "status": "Warning",
                "details": "User not created, " + rail.result('get_input_validation_log')['exc_value'],
                "childjobid": get_dagrun_ecid(dag_run),
            }
        )

        if_gender_not_present_8 = rail.IfOperator(
            task_id='if_gender_not_present_8',
            test=lambda dag_run: dag_run.conf['gender'] is not None and dag_run.conf['gender'] != '',
            yes_task= "get_all_employee_type_details_11",
            no_task= "log_user_not_created_gender_not_present_9"
        )

        log_user_not_created_gender_not_present_9 = rail.WriteLogOperator(
            task_id="log_user_not_created_gender_not_present_9",
            log='{{ dag_run.conf.user_import_logs}}',
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['userid'],
                "username": dag_run.conf['firstname'] + "|" + dag_run.conf['lastname'],
                "action": "Add",
                "status": "Exception",
                "details": "User not created, gender must be present for users with employee worker type",
                "childjobid": get_dagrun_ecid(dag_run)
            }
        )

        get_all_employee_type_details_11 = rail.RepliconServiceOperator(
            task_id="get_all_employee_type_details_11",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups"
        )

        get_all_time_zones_13 = rail.RepliconServiceOperator(
            task_id='get_all_time_zones_13',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones"
        )

        create_location_lookup_var = rail.SetVariableOperator(
            task_id='create_location_lookup_var',
            append=False,
            name='location_lookup',
            value=''
        )

        if_req_location_equals_jpohta_17 = rail.IfOperator(
            task_id='if_req_location_equals_jpohta_17',
            test='''{{ dag_run.conf.location == 'JP Ohta' }}''',
            yes_task="update_location_lookup_var_with_jpohta_18",
            no_task="update_location_lookup_var_with_nil_20"
        )

        update_location_lookup_var_with_jpohta_18 = rail.SetVariableOperator(
            task_id='update_location_lookup_var_with_jpohta_18',
            append=False,
            name='{{ result("create_location_lookup_var").name }}',
            value='JP Ohta'
        )

        update_location_lookup_var_with_nil_20 = rail.SetVariableOperator(
            task_id='update_location_lookup_var_with_nil_20',
            append=False,
            name='{{ result("create_location_lookup_var").name }}',
            value=''
        )

        create_shift_lookup_var_21 = rail.SetVariableOperator(
            task_id='create_shift_lookup_var_21',
            append=False,
            name='shift_lookup',
            value=''
        )

        if_req_workshift_equals_shift_a_b_c_d_or_day_22 = rail.IfOperator(
            task_id='if_req_workshift_equals_shift_a_b_c_d_or_day_22',
            test='''{{ dag_run.conf.workshift == 'Shift A' or dag_run.conf.workshift == 'Shift B' or dag_run.conf.workshift == 'Shift C' or dag_run.conf.workshift == 'Shift D' or dag_run.conf.workshift == 'Day' }}''',
            yes_task="update_shift_lookup_var_23",
            no_task="update_shift_lookup_var_with_nil_25"
        )

        update_shift_lookup_var_23 = rail.SetVariableOperator(
            task_id='update_shift_lookup_var_23',
            append=False,
            name='{{ result("create_shift_lookup_var_21").name }}',
            value='{{ dag_run.conf.workshift }}'
        )

        update_shift_lookup_var_with_nil_25 = rail.SetVariableOperator(
            task_id='update_shift_lookup_var_with_nil_25',
            append=False,
            name='{{ result("create_shift_lookup_var_21").name }}',
            value=''
        )

        create_workersubshift_lookup_var_26 = rail.SetVariableOperator(
            task_id='create_workersubshift_lookup_var_26',
            append=False,
            name='workersubshift_lookup',
            value='{{ dag_run.conf.worker_subtype }}'
        )

        create_timesheetapprovalpath_var_27 = rail.SetVariableOperator(
            task_id='create_timesheetapprovalpath_var_27',
            append=False,
            name='timesheetapprovalpath',
            value=''
        )

        create_timeoffapprovalpath_var_28 = rail.SetVariableOperator(
            task_id='create_timeoffapprovalpath_var_28',
            append=False,
            name='timeoffapprovalpath',
            value=''
        )

        create_legalentity_division_var_29 = rail.SetVariableOperator(
            task_id='create_legalentity_division_var_29',
            append=False,
            name='legalentity_division',
            value=[]
        )

        create_paygroup_servicecenter_var_30 = rail.SetVariableOperator(
            task_id='create_paygroup_servicecenter_var_30',
            append=False,
            name='paygroup_servicecenter',
            value=[]
        )

        create_costcenter_var_31 = rail.SetVariableOperator(
            task_id='create_costcenter_var_31',
            append=False,
            name='costcenter',
            value=[]
        )

        create_schedule_var_32 = rail.SetVariableOperator(
            task_id='create_schedule_var_32',
            append=False,
            name='schedule',
            value=[]
        )

        create_holiday_calendar_var_33 = rail.SetVariableOperator(
            task_id='create_holiday_calendar_var_33',
            append=False,
            name='holidaycalendar',
            value=''
        )

        create_payruletoassign_var_34 = rail.SetVariableOperator(
            task_id='create_payruletoassign_var_34',
            append=False,
            name='payruletoassign',
            value=[]
        )

        create_loginstatus_var_35 = rail.SetVariableOperator(
            task_id='create_loginstatus_var_35',
            append=False,
            name='loginstatus',
            value=''
        )

        update_timeoffapprovalpath_var_36 = rail.SetVariableOperator(
            task_id='update_timeoffapprovalpath_var_36',
            append=False,
            name='{{ result("create_timeoffapprovalpath_var_28").name }}',
            value={ "uri": null, "name": "JPN_Time Off Approval"}
        )

        create_activity_list_37 = rail.SetVariableOperator(
            task_id='create_activity_list_37',
            append=True,
            name='activity_list',
            value=[]
        )

        momentive_userimport_mapper_search_entries_38 = rail.PythonOperator(
            task_id='momentive_userimport_mapper_search_entries_38',
            python_callable=lambda dag_run:  list(filter(lambda x: x["type"] == "Employee type" and x["workertype"] == dag_run.conf['workertype'] and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and (
                x['shift'] == rail.get_dag_run_var("shift_lookup")) and x["japan_flag"] == None, momentive_user_import_mapper))
        )

        if_mapper_search_entry_present_39 = rail.IfOperator(
            task_id='if_mapper_search_entry_present_39',
            test='''{{ result('momentive_userimport_mapper_search_entries_38')| is_truthy }}''',
            yes_task="get_required_employeetype_uri_40",
            no_task="if_get_required_employeetype_uri_not_present_or_deptgrpuri_not_present_41"
        )

        get_required_employeetype_uri_40 = rail.PythonOperator(
            task_id="get_required_employeetype_uri_40",
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_employee_type_details_11'), 'displayText', rail.result(
                    'momentive_userimport_mapper_search_entries_38')[0]['value'], 'uri', '')
        )

        if_get_required_employeetype_uri_not_present_or_deptgrpuri_not_present_41 = rail.IfOperator(
            task_id='if_get_required_employeetype_uri_not_present_or_deptgrpuri_not_present_41',
            test="{{ result('get_required_employeetype_uri_40')| is_falsy or \
                dag_run.conf.departmentgroupuri | is_falsy }}",
            yes_task="details_employeetype_and_departmentygrpuri_not_exist",
            no_task="if_req_emp_type_uri_and_deptgrpuri_present_44",
        )

        details_employeetype_and_departmentygrpuri_not_exist = rail.PythonOperator(
            task_id='details_employeetype_and_departmentygrpuri_not_exist',
            python_callable=python_callable.get_details_for_employeetype_and_departmentygrpuri_not_exist
        )

        log_user_import_employeetype_dept_not_exist_42 = rail.WriteLogOperator(
            task_id="log_user_import_employeetype_dept_not_exist_42",
            log='{{ dag_run.conf.user_import_logs}}',
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['userid'],
                "username": dag_run.conf['firstname'] + "|" + dag_run.conf['lastname'],
                "action": "Add",
                "status": "Exception",
                "details": rail.smartjoin_by_delim(rail.result('details_employeetype_and_departmentygrpuri_not_exist').split(";"), ";"),
                "childjobid": get_dagrun_ecid(dag_run),
            }
        )

        if_req_emp_type_uri_and_deptgrpuri_present_44 = rail.IfOperator(
            task_id='if_req_emp_type_uri_and_deptgrpuri_present_44',
            test="{{ result('get_required_employeetype_uri_40')| is_truthy and \
                dag_run.conf.departmentgroupuri | is_truthy }}",
            yes_task="momentive_userimport_mapper_search_entries_45",
            no_task="catch_and_log_error"
        )

        momentive_userimport_mapper_search_entries_45 = rail.PythonOperator(
            task_id='momentive_userimport_mapper_search_entries_45',
            python_callable=lambda dag_run:  list(filter(lambda x: x["workertype"] == dag_run.conf['workertype'], momentive_user_import_mapper))
        )

        log_hiredate_47 = rail.PythonOperator(
            task_id='log_hiredate_47',
            python_callable=lambda dag_run: python_callable.split_date_string(dag_run.conf['hiredate'], split_type='int')
        )

        get_all_pay_rule_scripts_49 = rail.RepliconServiceOperator(
            task_id='get_all_pay_rule_scripts_49',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
        )

        log_timesheetapprovalpathtobeassigned_50 = rail.PythonOperator(
            task_id='log_timesheetapprovalpathtobeassigned_50',
            python_callable=lambda dag_run: next((x['value'] for x in filter(lambda x: x["type"] == "Timesheet approval path" and x["workertype"] == dag_run.conf['workertype'] and
                 x["location"] == rail.get_dag_run_var("location_lookup") and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and 
                 x['shift'] == rail.get_dag_run_var("shift_lookup") and x['worker_subtype'] == rail.get_dag_run_var("workersubshift_lookup") and 
                 x['japan_flag'] == dag_run.conf['Japan_flag'], rail.result('momentive_userimport_mapper_search_entries_45') or [])), '')
        )

        if_timesheetapprovalpathtobeassigned_present_51 = rail.IfOperator(
            task_id='if_timesheetapprovalpathtobeassigned_present_51',
            test='''{{ result('log_timesheetapprovalpathtobeassigned_50') | is_truthy }}''',
            yes_task="update_timesheetapprovalpath_var_52",
            no_task="log_timesheettemplatetobeassigned_53"
        )

        update_timesheetapprovalpath_var_52 = rail.SetVariableOperator(
            task_id='update_timesheetapprovalpath_var_52',
            append=False,
            name='{{ result("create_timesheetapprovalpath_var_27").name }}',
            value={ "uri": null, "name": "{{ result('log_timesheetapprovalpathtobeassigned_50') }}" }
        )

        log_timesheettemplatetobeassigned_53 = rail.PythonOperator(
            task_id='log_timesheettemplatetobeassigned_53',
            python_callable=lambda dag_run: next((x['value'] for x in filter(lambda x: x["type"] == "Timesheet Template" and x["workertype"] == dag_run.conf['workertype'] and
                 x["location"] == rail.get_dag_run_var("location_lookup") and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and 
                 x['shift'] == rail.get_dag_run_var("shift_lookup") and x['worker_subtype'] == rail.get_dag_run_var("workersubshift_lookup") and 
                 x['japan_flag'] == dag_run.conf['Japan_flag'], rail.result('momentive_userimport_mapper_search_entries_45') or [])), '')
        )

        log_payruletobeassigned_54 = rail.PythonOperator(
            task_id='log_payruletobeassigned_54',
            python_callable=lambda dag_run: next((x['value'] for x in filter(lambda x: x["type"] == "Payrule" and x["workertype"] == dag_run.conf['workertype'] and
                 x["location"] == rail.get_dag_run_var("location_lookup") and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and 
                 x['shift'] == rail.get_dag_run_var("shift_lookup") and x['worker_subtype'] == rail.get_dag_run_var("workersubshift_lookup") and 
                 x['japan_flag'] == dag_run.conf['Japan_flag'], rail.result('momentive_userimport_mapper_search_entries_45') or [])), '')
        )

        log_pay_rule_uri_55 = rail.PythonOperator(
            task_id='log_pay_rule_uri_55',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_pay_rule_scripts_49'), 'displayText', rail.result('log_payruletobeassigned_54'), 'uri', '')
        )

        update_payruletoassign_var_56 = rail.SetVariableOperator(
            task_id='update_payruletoassign_var_56',
            append=False,
            name='{{ result("create_payruletoassign_var_34").name }}',
            value=[{"payRuleScript": {"uri": "{{ result('log_pay_rule_uri_55') }}", "name": null}, "effectiveDate": null}]
        )

        log_scheduletobeassigned_57 = rail.PythonOperator(
            task_id='log_scheduletobeassigned_57',
            python_callable=lambda dag_run: next((x['value'] for x in filter(lambda x: x["type"] == "Schedule" and x["workertype"] == dag_run.conf['workertype'] and
                 x["location"] == rail.get_dag_run_var("location_lookup") and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and 
                 x['shift'] == rail.get_dag_run_var("shift_lookup") and x['worker_subtype'] == rail.get_dag_run_var("workersubshift_lookup") and 
                 x['japan_flag'] == dag_run.conf['Japan_flag'], rail.result('momentive_userimport_mapper_search_entries_45') or [])), '')
        )

        if_scheduletobeassigned_present_58 = rail.IfOperator(
            task_id='if_scheduletobeassigned_present_58',
            test='''{{ result('log_scheduletobeassigned_57') | is_truthy }}''',
            yes_task="if_scheduletobeassigned_equals_shift_59",
            no_task="if_legalentity_present_and_legalentityuristartswithurn_72"
        )

        if_scheduletobeassigned_equals_shift_59 = rail.IfOperator(
            task_id='if_scheduletobeassigned_equals_shift_59',
            test='''{{ result('log_scheduletobeassigned_57') == 'Shift'}}''',
            yes_task="update_schedule_var_60",
            no_task="get_req_office_schedules_62"
        )

        update_schedule_var_60 = rail.SetVariableOperator(
            task_id='update_schedule_var_60',
            append=False,
            name='{{ result("create_schedule_var_32").name }}',
            value=[{"schedulePolicy": {"officeScheduleUri": null, "name": null, "officeSchedule": {"officeScheduleUri": null, "name": null}, "scheduleTypeUri": "urn:replicon:schedule-type:shift"}, "effectiveDate": null}]
        )

        get_req_office_schedules_62 = rail.RepliconServiceOperator(
            task_id='get_req_office_schedules_62',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda response: {
                'default_office_schedule_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('log_scheduletobeassigned_57'), 'uri'),
                '0hrs_schedule': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', "0 hrs. Schedule", 'uri')
            }
        )

        if_hire_date_not_equals_begofmonth_65 = rail.IfOperator(
            task_id='if_hire_date_not_equals_begofmonth_65',
            test=lambda dag_run: datetime.strptime(dag_run.conf['hiredate'], "%Y-%m-%d").day != 1,
            yes_task= "update_schedule_var_66",
            no_task="if_default_office_schedule_present_68"
        )

        update_schedule_var_66 = rail.SetVariableOperator(
            task_id='update_schedule_var_66',
            append=False,
            name='{{ result("create_schedule_var_32").name }}',
            value=lambda: [{"schedulePolicy": {"officeScheduleUri": rail.result('get_req_office_schedules_62')['0hrs_schedule'], "name": None, "officeSchedule": {"officeScheduleUri": rail.result('get_req_office_schedules_62')['0hrs_schedule'], "name": None}, "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"}, "effectiveDate": None}, {"schedulePolicy": {"officeScheduleUri": rail.result('get_req_office_schedules_62')['default_office_schedule_uri'], "officeSchedule": {"officeScheduleUri": rail.result('get_req_office_schedules_62')['default_office_schedule_uri'], "name": None}, "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"}, "effectiveDate": rail.result('log_hiredate_47')}]
        )

        if_default_office_schedule_present_68 = rail.IfOperator(
            task_id='if_default_office_schedule_present_68',
            test='''{{ result('get_req_office_schedules_62').default_office_schedule_uri | is_truthy }}''',
            yes_task="update_schedule_var_69",
            no_task="log_exception_schedule_not_found_71"
        )

        update_schedule_var_69 = rail.SetVariableOperator(
            task_id='update_schedule_var_69',
            append=False,
            name='{{ result("create_schedule_var_32").name }}',
            value=[{"schedulePolicy": {"officeScheduleUri": "{{ result('get_req_office_schedules_62').default_office_schedule_uri }}", "name": null, "officeSchedule": {"officeScheduleUri": "{{ result('get_req_office_schedules_62').default_office_schedule_uri }}", "name": null}, "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"}, "effectiveDate": null}]
        )

        log_exception_schedule_not_found_71 =rail.SetVariableOperator(
            task_id='log_exception_schedule_not_found_71',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Schedule: {{ result('log_scheduletobeassigned_57') }} not found in the instance/ disabled hence schedule not assigned."
            }
        )

        if_legalentity_present_and_legalentityuristartswithurn_72 = rail.IfOperator(
            task_id='if_legalentity_present_and_legalentityuristartswithurn_72',
            test=lambda dag_run: dag_run.conf['legalentity'] and dag_run.conf['legalentityuri'] and dag_run.conf['legalentityuri'].startswith('urn'),
            yes_task="update_legalentity_division_var_73",
            no_task="log_exception_legalentity_invalid_75"
        )

        update_legalentity_division_var_73 = rail.SetVariableOperator(
            task_id='update_legalentity_division_var_73',
            append=False,
            name='{{ result("create_legalentity_division_var_29").name }}',
            value=[{"division": {"uri": "{{ dag_run.conf['legalentityuri'] }}", "parentUri": null, "name": null}, "effectiveDate": null}]
        )

        log_exception_legalentity_invalid_75 = rail.SetVariableOperator(
            task_id='log_exception_legalentity_invalid_75',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Legal entity: {{ dag_run.conf['legalentity'] }} not found or is disabled in Replicon."
            }
        )

        if_payruletobeassigned_not_present_76 = rail.IfOperator(
            task_id='if_payruletobeassigned_not_present_76',
            test='''{{ result('log_payruletobeassigned_54') | is_falsy }}''',
            yes_task="log_exception_payrule_not_found_77",
            no_task="if_exceptionlogger_list_present_79"
        )

        log_exception_payrule_not_found_77 = rail.SetVariableOperator(
            task_id='log_exception_payrule_not_found_77',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "No mapper found for the provided combination ( Workertype = {{ dag_run.conf['workertype'] }} |Exemptionstatus = {{ dag_run.conf['exemptionstatus'] }} |Location = {{ dag_run.conf['location'] }} |Work shift= {{ dag_run.conf['workshift'] }} |WorkerSubtype= {{ dag_run.conf['worker_subtype'] }} )"
            }
        )

        if_exceptionlogger_list_present_79 = rail.IfOperator(
            task_id='if_exceptionlogger_list_present_79',
            test='''{{ result('create_exceptionlogger_list').value | length > 0 }}''',
            yes_task="log_user_import_not_created_with_exceptionlogger_list_80",
            no_task="if_paygroupuri_present_and_startswith_urn_82"
        )

        log_user_import_not_created_with_exceptionlogger_list_80 = rail.WriteLogOperator(
            task_id="log_user_import_not_created_with_exceptionlogger_list_80",
            log='{{ dag_run.conf.user_import_logs}}',
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['userid'],
                "username": dag_run.conf['firstname'] + "|" + dag_run.conf['lastname'],
                "action": "Add",
                "status": "Exception",
                "details": "User not created due to following reasons: " + rail.smartjoin_by_delim([log['log'] for log in rail.get_dag_run_var("exceptionlogger_list")], ","),
                "childjobid": get_dagrun_ecid(dag_run),
            }
        )

        if_paygroupuri_present_and_startswith_urn_82 = rail.IfOperator(
            task_id='if_paygroupuri_present_and_startswith_urn_82',
            test=lambda dag_run: dag_run.conf['paygroupuri'] and dag_run.conf['paygroupuri'].startswith('urn'),
            yes_task="update_paygroup_servicecenter_var_83",
            no_task="log_exception_paygroup_invalid_85"
        )

        update_paygroup_servicecenter_var_83 = rail.SetVariableOperator(
            task_id='update_paygroup_servicecenter_var_83',
            append=False,
            name='{{ result("create_paygroup_servicecenter_var_30").name }}',
            value=[{"serviceCenter": {"uri": "{{ dag_run.conf['paygroupuri'] }}", "parentUri": null, "name": null}, "effectiveDate": null}]
        )

        log_exception_paygroup_invalid_85 = rail.SetVariableOperator(
            task_id='log_exception_paygroup_invalid_85',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Pay group: {{ dag_run.conf['paygroup'] }} not found or is disabled in Replicon."
            }
        )

        if_costcenteruri_present_and_startswith_urn_86 = rail.IfOperator(
            task_id='if_costcenteruri_present_and_startswith_urn_86',
            test=lambda dag_run: dag_run.conf['costcenteruri'] and dag_run.conf['costcenteruri'].startswith('urn'),
            yes_task="update_costcenter_var_87",
            no_task="log_exception_costcenter_invalid_89"
        )

        update_costcenter_var_87 = rail.SetVariableOperator(
            task_id='update_costcenter_var_87',
            append=False,
            name='{{ result("create_costcenter_var_31").name }}',
            value=[{"costCenter": {"uri": "{{ dag_run.conf['costcenteruri'] }}", "parentUri": null, "name": null}, "effectiveDate": null}]
        )

        log_exception_costcenter_invalid_89 = rail.SetVariableOperator(
            task_id='log_exception_costcenter_invalid_89',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Cost center: {{ dag_run.conf['costcenter'] }} not found or is disabled in Replicon."
            }
        )

        log_holidaycalendartobeassigned_90 = rail.PythonOperator(
            task_id='log_holidaycalendartobeassigned_90',
            python_callable=lambda dag_run: next((x['value'] for x in filter(lambda x: x["type"] == "Holiday Calendar" and x["workertype"] == dag_run.conf['workertype'] and
                 x["location"] == rail.get_dag_run_var("location_lookup") and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and 
                 x['shift'] == rail.get_dag_run_var("shift_lookup") and x['worker_subtype'] == rail.get_dag_run_var("workersubshift_lookup") and 
                 x['japan_flag'] == dag_run.conf['Japan_flag'], rail.result('momentive_userimport_mapper_search_entries_45') or [])), '')
        )

        if_holidaycalendartobeassigned_present_91 = rail.IfOperator(
            task_id='if_holidaycalendartobeassigned_present_91',
            test='''{{ result('log_holidaycalendartobeassigned_90') | is_truthy }}''',
            yes_task="get_all_holiday_calendars_92",
            no_task="log_punch_entrypolicy_tobeassigned_98"
        )

        get_all_holiday_calendars_92 = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars_92',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response: {
                'holiday_calendar_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'name', rail.result('log_holidaycalendartobeassigned_90'), 'uri')
            }
        )

        if_holiday_calendar_uri_present_94 = rail.IfOperator(
            task_id='if_holiday_calendar_uri_present_94',
            test='''{{ result('get_all_holiday_calendars_92').holiday_calendar_uri | is_truthy }}''',
            yes_task="update_holiday_calendar_var_95",
            no_task="log_exception_holiday_calendar_not_found_97"
        )

        update_holiday_calendar_var_95 = rail.SetVariableOperator(
            task_id='update_holiday_calendar_var_95',
            append=False,
            name='{{ result("create_holiday_calendar_var_33").name }}',
            value={"uri": "{{ result('get_all_holiday_calendars_92').holiday_calendar_uri }}", "name": null}
        )

        log_exception_holiday_calendar_not_found_97 = rail.SetVariableOperator(
            task_id='log_exception_holiday_calendar_not_found_97',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Holiday Calendar: {{ result('log_holidaycalendartobeassigned_90') }} not found in the instance hence holiday calendar not assigned."
            }
        )

        log_punch_entrypolicy_tobeassigned_98 = rail.PythonOperator(
            task_id='log_punch_entrypolicy_tobeassigned_98',
            python_callable=lambda dag_run: next((x['value'] for x in filter(lambda x: x["type"] == "Punch entry policy" and x["workertype"] == dag_run.conf['workertype'], rail.result('momentive_userimport_mapper_search_entries_45') or [])), '')
        )

        create_policysets_var_99 = rail.SetVariableOperator(
            task_id='create_policysets_var_99',
            append=False,
            name='policysets',
            value=[]
        )

        update_policysets_var_100 = rail.SetVariableOperator(
            task_id='update_policysets_var_100',
            append=False,
            name='{{ result("create_policysets_var_99").name }}',
            value=[{"uri": null, "name": "{{ result('log_timesheettemplatetobeassigned_53') }}" }, {"uri": null, "name": "{{ result('log_punch_entrypolicy_tobeassigned_98') }}" }, {"uri": null, "name": "Time Off"}]
        )

        create_user_105 = rail.RepliconServiceOperator(
            task_id='create_user_105',
            endpoint="/services/ImportService1.svc/PutUser3",
            data=request_payload.create_user_payload
        )

        put_timesheetperiodschedule_106 = rail.RepliconServiceOperator(
            task_id='put_timesheetperiodschedule_106',
            endpoint="/services/TimesheetPeriodService2.svc/PutTimesheetPeriodScheduleForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user_105')['uri'],
                "scheduleEntries": [
                {
                    "timesheetPeriod": {
                        "uri": null,
                        "name": "Monthly"
                    },
                    "effectiveDate": null
                }
            ]
            }
        )

        put_policy_data_access_scopes_for_userdepartmentrestricted_113 = rail.RepliconServiceOperator(
            task_id='put_policy_data_access_scopes_for_userdepartmentrestricted_113',
            endpoint="/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user_105')['uri'],
                "policyDataAccessScopes": [
                    {
                        "policyUri": "urn:replicon:policy:time-off",
                        "locations": [],
                        "divisions": [],
                        "costCenters": [],
                        "serviceCenters": [],
                        "departmentGroups": [
                            {
                                "departmentGroup": {
                                    "uri": dag_run.conf['departmentgroupuri'],
                                    "parent": null,
                                    "name": null,
                                    "parameterCorrelationId": null
                                },
                                "groupSpecificationModeUri": null,
                                "groupDescendantModeUri": null
                            }
                        ],
                        "employeeTypeGroups": []
                    }
                ]
            }
        )

        remove_all_timeoffs_114 = rail.RepliconServiceOperator(
            task_id='remove_all_timeoffs_114',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=lambda :{
                'userUri': rail.result('create_user_105')['uri'],
                'timeOffTypeUris': []
            }
        )

        if_cfdob_present_116 = rail.IfOperator(
            task_id='if_cfdob_present_116',
            test="{{ dag_run.conf.CF_Date_of_Birth_MM_DD_YYYY | is_truthy }}",
            yes_task="update_cfdob_field_120",
            no_task="if_businesstitle_present_121"
        )

        update_cfdob_field_120 = rail.RepliconServiceOperator(
            task_id='update_cfdob_field_120',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_105')['uri'],
                "customFieldUri": dag_run.conf['date_of_birth_uri'],
                "value": python_callable.split_date_string(dag_run.conf['CF_Date_of_Birth_MM_DD_YYYY']),
            }
        )

        if_businesstitle_present_121 = rail.IfOperator(
            task_id='if_businesstitle_present_121',
            test="{{ dag_run.conf.businesstitle | is_truthy }}",
            yes_task="update_businesstitle_field_124",
            no_task="if_worker_subtype_present_125"
        )

        update_businesstitle_field_124 = rail.RepliconServiceOperator(
            task_id='update_businesstitle_field_124',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_105')['uri'],
                "customFieldUri": dag_run.conf['title_uri'],
                "value": dag_run.conf['businesstitle'],
            }
        )

        if_worker_subtype_present_125 = rail.IfOperator(
            task_id='if_worker_subtype_present_125',
            test="{{ dag_run.conf.worker_subtype | is_truthy }}",
            yes_task="get_req_customfielddropdown_options_128",
            no_task="if_workshift_present_132"
        )

        get_req_customfielddropdown_options_128 = rail.RepliconServiceOperator(
            task_id='get_req_customfielddropdown_options_128',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data= lambda dag_run: {
                "customFieldUri": dag_run.conf['worker_subtypeuri']
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['worker_subtype'], 'uri')
        )

        update_workersubtype_field_131 = rail.RepliconServiceOperator(
            task_id='update_workersubtype_field_129',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_105')['uri'],
                "customFieldUri": dag_run.conf['worker_subtypeuri'],
                "customFieldDropDownOptionUri": rail.result('get_req_customfielddropdown_options_128')
            }
        )

        if_workshift_present_132 = rail.IfOperator(
            task_id='if_workshift_present_132',
            test="{{ dag_run.conf.workshift | is_truthy }}",
            yes_task="get_req_customfielddropdown_options_135",
            no_task="if_workertype_present_139"
        )

        get_req_customfielddropdown_options_135 = rail.RepliconServiceOperator(
            task_id='get_req_customfielddropdown_options_135',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data= lambda dag_run: {
                "customFieldUri": dag_run.conf['workshift_uri']
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['workshift'], 'uri')
        )

        update_workshift_field_138 = rail.RepliconServiceOperator(
            task_id='update_workshift_field_138',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_105')['uri'],
                "customFieldUri": dag_run.conf['workshift_uri'],
                "customFieldDropDownOptionUri": rail.result('get_req_customfielddropdown_options_135')
            }
        )

        if_workertype_present_139 = rail.IfOperator(
            task_id='if_workertype_present_139',
            test="{{ dag_run.conf.workertype | is_truthy }}",
            yes_task="get_req_customfielddropdown_options_142",
            no_task="if_years_of_service_present_146"
        )

        get_req_customfielddropdown_options_142 = rail.RepliconServiceOperator(
            task_id='get_req_customfielddropdown_options_142',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data= lambda dag_run: {
                "customFieldUri": dag_run.conf['workertype_uri']
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['workertype'], 'uri')
        )

        update_workertype_field_145 = rail.RepliconServiceOperator(
            task_id='update_workertype_field_145',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_105')['uri'],
                "customFieldUri": dag_run.conf['workertype_uri'],
                "customFieldDropDownOptionUri": rail.result('get_req_customfielddropdown_options_142')
            }
        )

        if_years_of_service_present_146 = rail.IfOperator(
            task_id='if_years_of_service_present_146',
            test="{{ dag_run.conf.years_of_service | is_truthy }}",
            yes_task="update_years_of_service_field_149",
            no_task="if_fieldhr_present_150"
        )

        update_years_of_service_field_149 = rail.RepliconServiceOperator(
            task_id='update_years_of_service_field_149',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_105')['uri'],
                "customFieldUri": dag_run.conf['years_of_service_uri'],
                "value": dag_run.conf['years_of_service']
            }
        )

        if_fieldhr_present_150 = rail.IfOperator(
            task_id='if_fieldhr_present_150',
            test="{{ dag_run.conf.fieldhr | is_truthy }}",
            yes_task="update_fieldhr_field_153",
            no_task="if_continuos_service_date_present_154"
        )

        update_fieldhr_field_153 = rail.RepliconServiceOperator(
            task_id='update_fieldhr_field_153',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_105')['uri'],
                "customFieldUri": dag_run.conf['hrm_uri'],
                "value": dag_run.conf['fieldhr']
            }
        )

        if_continuos_service_date_present_154 = rail.IfOperator(
            task_id='if_continuos_service_date_present_154',
            test="{{ dag_run.conf.continous_service_date | is_truthy }}",
            yes_task="update_continuos_service_date_field_157",
            no_task="if_timeoff_service_date_present_158"
        )

        update_continuos_service_date_field_157 = rail.RepliconServiceOperator(
            task_id='update_continuos_service_date_field_157',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_105')['uri'],
                "customFieldUri": dag_run.conf['continous_years_of_service_uri'],
                "value": dag_run.conf['continous_service_date']
            }
        )

        if_timeoff_service_date_present_158 = rail.IfOperator(
            task_id='if_timeoff_service_date_present_158',
            test="{{ dag_run.conf.timeoff_service_date | is_truthy }}",
            yes_task="update_timeoff_service_date_field_161",
            no_task="if_gender_present_162"
        )

        update_timeoff_service_date_field_161 = rail.RepliconServiceOperator(
            task_id='update_timeoff_service_date_field_161',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_105')['uri'],
                "customFieldUri": dag_run.conf['timeoff_service_date_uri'],
                "value": dag_run.conf['timeoff_service_date']
            }
        )

        if_gender_present_162 = rail.IfOperator(
            task_id='if_gender_present_162',
            test="{{ dag_run.conf.gender | is_truthy }}",
            yes_task="update_gender_field_165",
            no_task="if_manager_id_present_166"
        )

        update_gender_field_165 = rail.RepliconServiceOperator(
            task_id='update_gender_field_165',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user_105')['uri'],
                "customFieldUri": dag_run.conf['gender_uri'],
                "value": dag_run.conf['gender']
            }
        )

        if_manager_id_present_166 = rail.IfOperator(
            task_id='if_manager_id_present_166',
            test="{{ dag_run.conf.manager_id | is_truthy }}",
            yes_task="search_for_user_with_empid_167",
            no_task="get_timesheetfordate2_189"
        )

        search_for_user_with_empid_167 = rail.RepliconServiceOperator(
            task_id='search_for_user_with_empid_167',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.search_supervisor_payload,
            data_handler=python_callable.get_userdata_list_for_managerid
        )

        if_multiple_users_with_same_empid_169 = rail.IfOperator(
            task_id='if_multiple_users_with_same_empid_169',
            test='''{{ result('search_for_user_with_empid_167') | length > 1 }}''',
            yes_task="log_exception_users_with_same_empid_170",
            no_task="if_login_name_uri_present_172"
        )

        log_exception_users_with_same_empid_170 = rail.SetVariableOperator(
            task_id='log_exception_users_with_same_empid_170',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Supervisor not assigned for user {{ dag_run.conf['firstname'] }} {{ dag_run.conf['lastname'] }} as multiple users have same Employee ID: {{ dag_run.conf.manager_id }}."
            }
        )

        if_login_name_uri_present_172 = rail.IfOperator(
            task_id='if_login_name_uri_present_172',
            test=lambda: bool(rail.result('search_for_user_with_empid_167') and rail.result('search_for_user_with_empid_167')[0]['uri']),
            yes_task="get_manager_details_174",
            no_task="log_supervisor_assignment_186",
        )

        get_manager_details_174 = rail.RepliconServiceOperator(
            task_id='get_manager_details_174',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda: {
                "users": [
                    {
                        "uri": rail.result('search_for_user_with_empid_167')[0]['uri']
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_manager_details_present_and_enabled = rail.IfOperator(
            task_id='if_manager_details_present_and_enabled',
            test="{{ result('get_manager_details_174') | is_truthy and result('get_manager_details_174')[0]['userDetails']['isEnabled'] | is_truthy }}",
            yes_task="get_assigned_permissionset_foruser_176",
            no_task="log_supervisor_assignment_186",
        )

        get_assigned_permissionset_foruser_176 = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionset_foruser_176',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_for_user_with_empid_167')[0].uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.uri', '')
        )

        if_supervisor_permission_not_assigned_182 = rail.IfOperator(
            task_id='if_supervisor_permission_not_assigned_182',
            test="{{ result('get_assigned_permissionset_foruser_176') | is_falsy }}",
            yes_task="add_missing_supervisor_permission_183",
            no_task="update_initial_supervisor_184",
        )

        add_missing_supervisor_permission_183 = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission_183',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=request_payload.add_missing_supervisor_permission_payload
        )

        update_initial_supervisor_184 = rail.RepliconServiceOperator(
            task_id="update_initial_supervisor_184",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user_105').uri }}",
                "supervisorUri": "{{ result('search_for_user_with_empid_167')[0].uri }}",
                "dateRange": null
            }
        )

        log_supervisor_assignment_186 = rail.WriteLogOperator(
            task_id="log_supervisor_assignment_186",
            log='{{ dag_run.conf.supervisor_assignment_logs}}',
            message="Exception",
            severity="Exception",
            properties=request_payload.supervisor_assignment_log_payload
        )

        get_timesheetfordate2_189 =rail.RepliconServiceOperator(
            task_id='get_timesheetfordate2_189',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run: {
                "userUri": rail.result('create_user_105')['uri'],
                "date": rail.result('log_hiredate_47'),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        log_activity_tobeassigned_190 = rail.PythonOperator(
            task_id='log_activity_tobeassigned_190',
            python_callable=lambda dag_run: next((x['value'] for x in filter(lambda x: x["type"] == "Activity" and x["workertype"] == dag_run.conf['workertype'] and
                 x['shift'] == rail.get_dag_run_var("shift_lookup") and x['worker_subtype'] == rail.get_dag_run_var("workersubshift_lookup"), rail.result('momentive_userimport_mapper_search_entries_45') or [])), '')
        )

        if_activity_tobeassigned_present_191 = rail.IfOperator(
            task_id='if_activity_tobeassigned_present_191',
            test='''{{ result('log_activity_tobeassigned_190') | is_truthy }}''',
            yes_task="split_activities_bydelim_192",
            no_task="log_language_tobeassigned_203"
        )

        split_activities_bydelim_192 = rail.PythonOperator(
            task_id='split_activities_bydelim_192',
            python_callable=lambda: [activity.strip() for activity in rail.result('log_activity_tobeassigned_190').split("|")]
        )

        get_all_enabled_activities_194 = rail.RepliconServiceOperator(
            task_id='get_all_enabled_activities_194',
            endpoint="/services/ActivityService1.svc/GetEnabledActivities"
        )

        update_activity_list_197 = rail.SetVariableOperator(
            task_id='update_activity_list_197',
            append=False,
            name='{{ result("create_activity_list_37").name }}',
            value=lambda: [uri for uri in [rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_activities_194'), 'displayText', activity, 'uri') for activity in rail.result('split_activities_bydelim_192') or []] if uri]
        )

        if_activity_uris_present_199 = rail.IfOperator(
            task_id='if_activity_uris_present_199',
            test=lambda dag_run: len(rail.get_dag_run_var("activity_list") or []) > 0,
            yes_task="assign_activities_to_user_200",
            no_task="log_exception_activity_not_found_202"
        )

        assign_activities_to_user_200 = rail.RepliconServiceOperator(
            task_id='assign_activities_to_user_200',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user_105')['uri'],
                "activityUris": rail.get_dag_run_var("activity_list")
            }
        )

        log_exception_activity_not_found_202 = rail.SetVariableOperator(
            task_id='log_exception_activity_not_found_202',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Activity not assigned since {{ result('log_activity_tobeassigned_190') }} is not avialble in Replicon."
            }
        )

        log_language_tobeassigned_203 = rail.PythonOperator(
            task_id='log_language_tobeassigned_203',
            python_callable=lambda dag_run: next((x['value'] for x in filter(lambda x: x["type"] == "Language" and x["workertype"] == dag_run.conf['workertype'], rail.result('momentive_userimport_mapper_search_entries_45') or [])), '')
        )

        if_language_tobeassigned_present_204 = rail.IfOperator(
            task_id='if_language_tobeassigned_present_204',
            test='''{{ result('log_language_tobeassigned_203') | is_truthy }}''',
            yes_task="update_language_foruser_205",
            no_task="log_timofftypes_tobeassigned_206"
        )

        update_language_foruser_205 = rail.RepliconServiceOperator(
            task_id='update_language_foruser_205',
            endpoint="/services/InternationalizationService1.svc/UpdateLanguageForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user_105')['uri'],
                "languageUri": rail.result('log_language_tobeassigned_203')
            }
        )

        log_timofftypes_tobeassigned_206 = rail.PythonOperator(
            task_id= "log_timofftypes_tobeassigned_206",
            python_callable=lambda dag_run: next((x['value'] for x in filter(lambda x: x["type"] == "Time off types" and x["workertype"] == dag_run.conf['workertype'] and
                 x["location"] == rail.get_dag_run_var("location_lookup") and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and 
                 x['shift'] == rail.get_dag_run_var("shift_lookup") and x['worker_subtype'] == rail.get_dag_run_var("workersubshift_lookup") and
                 x['japan_flag'] == dag_run.conf['Japan_flag'] and x['gender'] == dag_run.conf['gender'], rail.result('momentive_userimport_mapper_search_entries_45') or [])), '')
        )

        if_timeofftypes_tobeassigned_present_and_active_equals_1_207 = rail.IfOperator(
            task_id='if_timeofftypes_tobeassigned_present_and_active_equals_1_207',
            test=lambda dag_run: bool(rail.result('log_timofftypes_tobeassigned_206')) and str(dag_run.conf['active']) == '1',
            yes_task="trigger_timeoff_add_new_user_208",
            no_task="momentive_user_import_logs_add_entry_209"
        )

        trigger_timeoff_add_new_user_208 = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_add_new_user_208',
            trigger_dag_id=config.momentive_japan_user_sync_child_add_timeoff_new_user_dag_id,
            conf=request_payload.trigger_timeoff_add_new_user,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_timeoff_add_new_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_timeoff_add_new_user',
            dag_runs='{{ result("trigger_timeoff_add_new_user_208") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_result_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_result_from_child',
            dag_runs='''{{result('trigger_timeoff_add_new_user_208')}}''',
            dagrun_task_id='final_response_from_dag',
            target='result'
        )

        if_error_in_gather_result_from_child = rail.IfOperator(
            task_id='if_error_in_gather_result_from_child',
            test=lambda: bool(rail.result("gather_result_from_child")) and "Error" in json.dumps(rail.result(
                "gather_result_from_child")[0]),
            yes_task='stop_processing_due_to_error_in_child',
            no_task='momentive_user_import_logs_add_entry_209'
        )

        stop_processing_due_to_error_in_child = rail.FailOperator(
            task_id='stop_processing_due_to_error_in_child',
            message='''Error in adding timeoff type for new user'''
        )

        momentive_user_import_logs_add_entry_209 = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_209',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity=lambda: "Exception" if rail.get_dag_run_var(
                'exceptionlogger_list') else "Success",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['userid'],
                "username": rail.render_template("{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"),
                "action": "Add",
                "status": "Exception" if rail.get_dag_run_var('exceptionlogger_list') else "Success",
                "details": "User created with exception, " + ",".join(log['log'] for log in rail.get_dag_run_var('exceptionlogger_list')) if rail.get_dag_run_var(
                    'exceptionlogger_list') else "User created successfully",
                "childjobid": get_dagrun_ecid(dag_run),
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            trigger_rule='one_failed',
            severity="Error",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['userid'],
                "username": dag_run.conf['firstname'] + dag_run.conf['lastname'],
                "action": "Add",
                "status": "Error",
                "details": rail.render_template("User created, but partially updated ; {{ get_error_message() }}") if rail.result(
                    "create_user_105") else rail.render_template("User not created ;{{ get_error_message() }}"),
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> create_exceptionlogger_list >> get_input_validation_log

        get_input_validation_log >> if_input_validation_log_present

        if_input_validation_log_present >> rail.Label(
            'Yes') >> log_user_import_not_created >> catch_and_log_error
        if_input_validation_log_present >> rail.Label(
            'No') >> if_gender_not_present_8 
        
        if_gender_not_present_8 >> rail.Label('Yes') >> log_user_not_created_gender_not_present_9 >> catch_and_log_error  
        if_gender_not_present_8 >> rail.Label('No') >> get_all_employee_type_details_11 >> get_all_time_zones_13 >> create_location_lookup_var
        
        create_location_lookup_var >> if_req_location_equals_jpohta_17 >> rail.Label('Yes') >> update_location_lookup_var_with_jpohta_18 >> create_shift_lookup_var_21
        create_location_lookup_var >> if_req_location_equals_jpohta_17 >> rail.Label('No') >> update_location_lookup_var_with_nil_20 >> create_shift_lookup_var_21 
        
        create_shift_lookup_var_21 >> if_req_workshift_equals_shift_a_b_c_d_or_day_22

        if_req_workshift_equals_shift_a_b_c_d_or_day_22 >> rail.Label('Yes') >> update_shift_lookup_var_23 >> create_workersubshift_lookup_var_26
        if_req_workshift_equals_shift_a_b_c_d_or_day_22 >> rail.Label('No') >> update_shift_lookup_var_with_nil_25 >> create_workersubshift_lookup_var_26 
        
        create_workersubshift_lookup_var_26 >> create_timesheetapprovalpath_var_27 >> create_timeoffapprovalpath_var_28 >> create_legalentity_division_var_29 >> create_paygroup_servicecenter_var_30 >> create_costcenter_var_31

        create_costcenter_var_31 >> create_schedule_var_32 >> create_holiday_calendar_var_33 >> create_payruletoassign_var_34 >> create_loginstatus_var_35 >> update_timeoffapprovalpath_var_36 >> create_activity_list_37 >> momentive_userimport_mapper_search_entries_38 >> if_mapper_search_entry_present_39

        if_mapper_search_entry_present_39 >> rail.Label('Yes') >> get_required_employeetype_uri_40 >> if_get_required_employeetype_uri_not_present_or_deptgrpuri_not_present_41
        if_mapper_search_entry_present_39 >> rail.Label('No') >> if_get_required_employeetype_uri_not_present_or_deptgrpuri_not_present_41

        if_get_required_employeetype_uri_not_present_or_deptgrpuri_not_present_41 >> rail.Label('Yes') >> details_employeetype_and_departmentygrpuri_not_exist >> log_user_import_employeetype_dept_not_exist_42 >> catch_and_log_error
        if_get_required_employeetype_uri_not_present_or_deptgrpuri_not_present_41 >> rail.Label('No') >> if_req_emp_type_uri_and_deptgrpuri_present_44 

        if_req_emp_type_uri_and_deptgrpuri_present_44 >> rail.Label('Yes') >> momentive_userimport_mapper_search_entries_45 >> log_hiredate_47 >> get_all_pay_rule_scripts_49 >> log_timesheetapprovalpathtobeassigned_50 >> if_timesheetapprovalpathtobeassigned_present_51
        if_req_emp_type_uri_and_deptgrpuri_present_44 >> rail.Label('No') >> catch_and_log_error

        if_timesheetapprovalpathtobeassigned_present_51 >> rail.Label('Yes') >> update_timesheetapprovalpath_var_52 >> log_timesheettemplatetobeassigned_53 
        if_timesheetapprovalpathtobeassigned_present_51 >> rail.Label('No') >> log_timesheettemplatetobeassigned_53 >> log_payruletobeassigned_54 >> log_pay_rule_uri_55 >> update_payruletoassign_var_56 >> log_scheduletobeassigned_57 >> if_scheduletobeassigned_present_58
        
        if_scheduletobeassigned_present_58 >> rail.Label('Yes') >> if_scheduletobeassigned_equals_shift_59 
        if_scheduletobeassigned_present_58 >> rail.Label('No') >> if_legalentity_present_and_legalentityuristartswithurn_72
        
        if_scheduletobeassigned_equals_shift_59 >> rail.Label('Yes') >> update_schedule_var_60 >> if_legalentity_present_and_legalentityuristartswithurn_72
        if_scheduletobeassigned_equals_shift_59 >> rail.Label('No') >> get_req_office_schedules_62 >> if_hire_date_not_equals_begofmonth_65 
        
        if_hire_date_not_equals_begofmonth_65 >> rail.Label('Yes') >> update_schedule_var_66 >> if_legalentity_present_and_legalentityuristartswithurn_72
        if_hire_date_not_equals_begofmonth_65 >> rail.Label('No') >> if_default_office_schedule_present_68

        if_default_office_schedule_present_68 >> rail.Label('Yes') >> update_schedule_var_69 >> if_legalentity_present_and_legalentityuristartswithurn_72
        if_default_office_schedule_present_68 >> rail.Label('No') >>  log_exception_schedule_not_found_71 >> if_legalentity_present_and_legalentityuristartswithurn_72

        if_legalentity_present_and_legalentityuristartswithurn_72 >> rail.Label('Yes') >> update_legalentity_division_var_73 >> if_payruletobeassigned_not_present_76
        if_legalentity_present_and_legalentityuristartswithurn_72 >> rail.Label('No') >> log_exception_legalentity_invalid_75 >> if_payruletobeassigned_not_present_76

        if_payruletobeassigned_not_present_76 >> rail.Label('Yes') >> log_exception_payrule_not_found_77 >> if_exceptionlogger_list_present_79
        if_payruletobeassigned_not_present_76 >> rail.Label('No') >> if_exceptionlogger_list_present_79

        if_exceptionlogger_list_present_79 >> rail.Label('Yes') >> log_user_import_not_created_with_exceptionlogger_list_80 >> catch_and_log_error
        if_exceptionlogger_list_present_79 >> rail.Label('No') >> if_paygroupuri_present_and_startswith_urn_82

        if_paygroupuri_present_and_startswith_urn_82 >> rail.Label('Yes') >> update_paygroup_servicecenter_var_83 >> if_costcenteruri_present_and_startswith_urn_86
        if_paygroupuri_present_and_startswith_urn_82 >> rail.Label('No') >> log_exception_paygroup_invalid_85 >> if_costcenteruri_present_and_startswith_urn_86

        if_costcenteruri_present_and_startswith_urn_86 >> rail.Label('Yes') >> update_costcenter_var_87 >> log_holidaycalendartobeassigned_90
        if_costcenteruri_present_and_startswith_urn_86 >> rail.Label('No') >> log_exception_costcenter_invalid_89 >> log_holidaycalendartobeassigned_90 >> if_holidaycalendartobeassigned_present_91

        if_holidaycalendartobeassigned_present_91 >> rail.Label('Yes') >> get_all_holiday_calendars_92 >> if_holiday_calendar_uri_present_94
        if_holidaycalendartobeassigned_present_91 >> rail.Label('No') >> log_punch_entrypolicy_tobeassigned_98

        if_holiday_calendar_uri_present_94 >> rail.Label('Yes') >> update_holiday_calendar_var_95 >> log_punch_entrypolicy_tobeassigned_98
        if_holiday_calendar_uri_present_94 >> rail.Label('No') >> log_exception_holiday_calendar_not_found_97 >> log_punch_entrypolicy_tobeassigned_98

        log_punch_entrypolicy_tobeassigned_98 >> create_policysets_var_99 >> update_policysets_var_100 >> create_user_105 >> put_timesheetperiodschedule_106 >> put_policy_data_access_scopes_for_userdepartmentrestricted_113 >> \
        remove_all_timeoffs_114 >> if_cfdob_present_116 >> rail.Label('Yes') >> update_cfdob_field_120 >> if_businesstitle_present_121

        if_cfdob_present_116 >> rail.Label('No') >> if_businesstitle_present_121 >> rail.Label('Yes') >> update_businesstitle_field_124 >> if_worker_subtype_present_125

        if_businesstitle_present_121 >> rail.Label('No') >> if_worker_subtype_present_125 >> rail.Label('Yes') >> get_req_customfielddropdown_options_128 >> update_workersubtype_field_131 >> if_workshift_present_132

        if_worker_subtype_present_125 >> rail.Label('No') >> if_workshift_present_132 >> rail.Label('Yes') >> get_req_customfielddropdown_options_135 >> update_workshift_field_138 >> if_workertype_present_139

        if_workshift_present_132 >> rail.Label('No') >> if_workertype_present_139 >> rail.Label('Yes') >> get_req_customfielddropdown_options_142 >> update_workertype_field_145 >> if_years_of_service_present_146

        if_workertype_present_139 >> rail.Label('No') >> if_years_of_service_present_146 >> rail.Label('Yes') >> update_years_of_service_field_149 >> if_fieldhr_present_150

        if_years_of_service_present_146 >> rail.Label('No') >> if_fieldhr_present_150 >> rail.Label('Yes') >> update_fieldhr_field_153 >> if_continuos_service_date_present_154

        if_fieldhr_present_150 >> rail.Label('No') >> if_continuos_service_date_present_154 >> rail.Label('Yes') >> update_continuos_service_date_field_157 >> if_timeoff_service_date_present_158

        if_continuos_service_date_present_154 >> rail.Label('No') >> if_timeoff_service_date_present_158 >> rail.Label('Yes') >> update_timeoff_service_date_field_161 >> if_gender_present_162

        if_timeoff_service_date_present_158 >> rail.Label('No') >> if_gender_present_162 >> rail.Label('Yes') >> update_gender_field_165 >> if_manager_id_present_166

        if_gender_present_162 >> rail.Label('No') >> if_manager_id_present_166 >> rail.Label('Yes') >> search_for_user_with_empid_167 >> if_multiple_users_with_same_empid_169
        if_manager_id_present_166 >> rail.Label('No') >> get_timesheetfordate2_189

        if_multiple_users_with_same_empid_169 >> rail.Label('Yes') >> log_exception_users_with_same_empid_170 >> get_timesheetfordate2_189
        if_multiple_users_with_same_empid_169 >> rail.Label('No') >> if_login_name_uri_present_172

        if_login_name_uri_present_172 >> rail.Label('Yes') >> get_manager_details_174 >> if_manager_details_present_and_enabled
        if_login_name_uri_present_172 >> rail.Label('No') >> log_supervisor_assignment_186 >> get_timesheetfordate2_189

        if_manager_details_present_and_enabled >> rail.Label('Yes') >> get_assigned_permissionset_foruser_176
        if_manager_details_present_and_enabled >> rail.Label('No') >> log_supervisor_assignment_186 >> get_timesheetfordate2_189

        get_assigned_permissionset_foruser_176 >> if_supervisor_permission_not_assigned_182 >> rail.Label('Yes') >> add_missing_supervisor_permission_183 >> update_initial_supervisor_184
        if_supervisor_permission_not_assigned_182 >> rail.Label('No') >> update_initial_supervisor_184 >> get_timesheetfordate2_189

        get_timesheetfordate2_189 >> log_activity_tobeassigned_190 >> if_activity_tobeassigned_present_191
        
        if_activity_tobeassigned_present_191 >> rail.Label('Yes') >> split_activities_bydelim_192 >> get_all_enabled_activities_194
        if_activity_tobeassigned_present_191 >> rail.Label('No') >> log_language_tobeassigned_203

        get_all_enabled_activities_194 >> update_activity_list_197 >> if_activity_uris_present_199

        if_activity_uris_present_199 >> rail.Label('Yes') >> assign_activities_to_user_200 >> log_language_tobeassigned_203
        if_activity_uris_present_199 >> rail.Label('No') >> log_exception_activity_not_found_202 >> log_language_tobeassigned_203

        log_language_tobeassigned_203 >> if_language_tobeassigned_present_204 >> rail.Label('Yes') >> update_language_foruser_205 >> log_timofftypes_tobeassigned_206
        if_language_tobeassigned_present_204 >> rail.Label('No') >> log_timofftypes_tobeassigned_206

        log_timofftypes_tobeassigned_206 >> if_timeofftypes_tobeassigned_present_and_active_equals_1_207 >> rail.Label('Yes') >> trigger_timeoff_add_new_user_208 >> wait_for_timeoff_add_new_user >> gather_result_from_child >> if_error_in_gather_result_from_child >> rail.Label("Yes") >> stop_processing_due_to_error_in_child >> momentive_user_import_logs_add_entry_209 
        if_error_in_gather_result_from_child >> rail.Label("No") >> momentive_user_import_logs_add_entry_209
        
        if_timeofftypes_tobeassigned_present_and_active_equals_1_207 >> rail.Label('No') >> momentive_user_import_logs_add_entry_209

        momentive_user_import_logs_add_entry_209 >> catch_and_log_error

        return dag
    
rail.for_each_instance(create_dag)














        
















































        










