
from datetime import timedelta, datetime
from airflow.models import Variable
from centricbrands.user_import_v2.mappers.centric_brands_time_off_type_assignment_mapper import centric_brands_time_off_type_assignment
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'centricbrands_update_user_time_off_type_assignment_{config.instance}_v2',
        description=f'CentricBrands_Update User - Time Off Type assignment {config.instance}_v2',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
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
            no_task='check_location_hk'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_location_hk',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        check_location_hk = rail.IfOperator(
            task_id='check_location_hk',
            test='''{{dag_run.conf.location | lower == 'hong kong'}}''',
            yes_task='search_matching_timeoff_hk',
            no_task='check_location_china'
        )

        search_matching_timeoff_hk = rail.PythonOperator(
            task_id='search_matching_timeoff_hk',
            python_callable=lambda dag_run: list(filter(lambda entry: entry['country'] == (
                dag_run.conf['location']).lower() and entry['hong_kong_levels'] == (
                dag_run.conf['hongkonglevels']).lower(), centric_brands_time_off_type_assignment))
        )

        check_location_china = rail.IfOperator(
            task_id='check_location_china',
            test='''{{dag_run.conf.location | lower == 'china'}}''',
            yes_task='search_matching_timeoff_china',
            no_task='search_timeoff_type_for_user'
        )

        search_matching_timeoff_china = rail.PythonOperator(
            task_id='search_matching_timeoff_china',
            python_callable=lambda dag_run: list(filter(lambda entry: entry['country'] == (
                dag_run.conf['location']).lower() and entry['state'] == (
                dag_run.conf['stateprovince']).lower(), centric_brands_time_off_type_assignment))
        )

        search_timeoff_type_for_user = rail.PythonOperator(
            task_id='search_timeoff_type_for_user',
            python_callable=lambda dag_run: list(filter(lambda entry: entry['country'] == (dag_run.conf['location']).lower() and entry['state'] == (
                dag_run.conf['stateprovince']).lower() and entry['employeetype'] == (
                dag_run.conf['employeetype']).lower(), centric_brands_time_off_type_assignment))
        )

        required_mapper_record_with_timeoff_type = rail.PythonOperator(
            task_id='required_mapper_record_with_timeoff_type',
            python_callable=lambda: rail.result('search_matching_timeoff_hk') or rail.result(
                'search_matching_timeoff_china') or rail.result('search_timeoff_type_for_user')
        )

        if_timeoffype_present = rail.IfOperator(
            task_id='if_timeoffype_present',
            test='''{{ result('required_mapper_record_with_timeoff_type') | is_truthy }}''',
            yes_task="get_time_off_type_assignments_for_user",
            no_task="catch_error",
        )

        get_time_off_type_assignments_for_user = rail.RepliconServiceOperator(
            task_id='get_time_off_type_assignments_for_user',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.uri }}"
            }
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        get_required_time_off_types = rail.PythonOperator(
            task_id='get_required_time_off_types',
            python_callable=lambda: ((rail.result('required_mapper_record_with_timeoff_type')[0])[
                                     'timeofftypes']).split('|')
        )

        declare_timeoffnameswithpreviousbalance_list = rail.SetVariableOperator(
            task_id='declare_timeoffnameswithpreviousbalance_list',
            append=False,
            name='timeoffnameswithpreviousbalance',
            value=[]
        )

        def get_date_object(datestring):
            date_obj = datetime.strptime(datestring, '%m/%d/%Y')
            return {
                'day': date_obj.day,
                'month': date_obj.month,
                'year': date_obj.year,
                'date': date_obj.strftime("%Y-%m-%d"),
            }

        get_today_date_object = rail.PythonOperator(
            task_id='get_today_date_object',
            python_callable=lambda: get_date_object(
                datetime.now().strftime("%m/%d/%Y"))
        )

        foreach_timeofftype_assignment = rail.ForEachOperator(
            task_id='foreach_timeofftype_assignment',
            items="{{ result('get_time_off_type_assignments_for_user') | to_json }}",
            start_task='get_balance_summary_for_account',
            end_task='foreach_timeofftype_assignment_end'
        )

        get_balance_summary_for_account = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data={
                "account": {
                    "userUri": "{{ dag_run.conf.uri }}",
                    "timeOffTypeUri": "{{ result('foreach_timeofftype_assignment').uri }}"
                },
                "asOfDate": {
                    "year": "{{ result('get_today_date_object').year }}",
                    "month": "{{ result('get_today_date_object').month }}",
                    "day": "{{ result('get_today_date_object').day }}"
                }
            }
        )

        insertto_timeoffnameswithbalance_list = rail.SetVariableOperator(
            task_id='insertto_timeoffnameswithbalance_list',
            append=True,
            name='{{ result("declare_timeoffnameswithpreviousbalance_list").name }}',
            value={
                "name": "{{ result('foreach_timeofftype_assignment').name }}",
                "uri": "{{ result('foreach_timeofftype_assignment').uri }}",
                "balance": "{{ result('get_balance_summary_for_account').timeRemaining }}"
            }
        )

        foreach_timeofftype_assignment_end = rail.EmptyOperator(
            task_id='foreach_timeofftype_assignment_end',
        )

        get_timeoff_types_to_assign_with_uri = rail.PythonOperator(
            task_id='get_timeoff_types_to_assign_with_uri',
            python_callable=lambda: [{
                "name": timeoff,
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'), 'displayText', timeoff.strip(), 'uri')
            } for timeoff in rail.result('get_required_time_off_types')]
        )

        assign_timeofftypes = rail.RepliconServiceOperator(
            task_id='assign_timeofftypes',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['uri'],
                "timeOffTypeUris": [timeoff['uri'] for timeoff in rail.result('get_timeoff_types_to_assign_with_uri')]
            }
        )

        if_location_china_hongkong = rail.IfOperator(
            task_id='if_location_china_hongkong',
            test='''{{dag_run.conf.location | lower == 'china' or dag_run.conf.location | lower == 'hong kong'}}''',
            yes_task='catch_error',
            no_task='get_startingbalancesetto_uri'
        )

        get_startingbalancesetto_uri = rail.RepliconServiceOperator(
            task_id='get_startingbalancesetto_uri',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Starting Balance Set To', 'uri', '')
        )

        get_preventionbalanceoverdraw_uri = rail.RepliconServiceOperator(
            task_id='get_preventionbalanceoverdraw_uri',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Prevent balance overdraw', 'uri', '')
        )

        create_list_child_triggered = rail.SetVariableOperator(
            task_id='create_list_child_triggered',
            name='childtriggered',
            append=False,
            value=[]
        )

        foreach_timeoff_assigned = rail.ForEachOperator(
            task_id='foreach_timeoff_assigned',
            items=lambda: rail.result('get_timeoff_types_to_assign_with_uri'),
            start_task='create_previousbalance_variable',
            end_task='foreach_timeoff_assigned_end'
        )

        create_previousbalance_variable = rail.SetVariableOperator(
            task_id='create_previousbalance_variable',
            append=False,
            name='previousbalance',
            value=None
        )

        if_previouscountry_equal_location_equal_usa = rail.IfOperator(
            task_id='if_previouscountry_equal_location_equal_usa',
            test='''{{ dag_run.conf.previouscountry == dag_run.conf.location  and dag_run.conf.location == 'USA' }}''',
            yes_task="update_previousbalance",
            no_task="if_previouscountry_unequal_location",
        )

        def get_balance_value(timeofftype1, timeofftype2):
            timeoffnames = rail.get_dag_run_var(
                'timeoffnameswithpreviousbalance')
            balance_for_timeofftype1 = rail.find_first_by_attr_and_get_attr(
                timeoffnames, 'name', timeofftype1, 'balance', '')
            balance_for_timeofftype2 = rail.find_first_by_attr_and_get_attr(
                timeoffnames, 'name', timeofftype2, 'balance', '')
            return balance_for_timeofftype1 if balance_for_timeofftype1 else (balance_for_timeofftype2 if balance_for_timeofftype2 else 'NA')

        update_previousbalance = rail.SetVariableOperator(
            task_id='update_previousbalance',
            append=False,
            name='{{ result("create_previousbalance_variable").name }}',
            value=lambda: get_balance_value(
                '1: USA - PTO', '1: USA - PTO (California)')
        )

        if_previouscountry_unequal_location = rail.IfOperator(
            task_id='if_previouscountry_unequal_location',
            test='''{{ dag_run.conf.previouscountry != dag_run.conf.location }}''',
            yes_task="if_location_equal_usa",
            no_task="if_previousemployeetype_unequal_employeetype",
        )

        if_location_equal_usa = rail.IfOperator(
            task_id='if_location_equal_usa',
            test='''{{ dag_run.conf.location == 'USA' }}''',
            yes_task="update_previous_balance",
            no_task="update_previous_balance_variable",
        )

        update_previous_balance = rail.SetVariableOperator(
            task_id='update_previous_balance',
            append=False,
            name='{{ result("create_previousbalance_variable").name }}',
            value=lambda: get_balance_value(
                '1: Canada - Vacation Days (Hourly)', '1: Canada - Vacation Days (Salaried)')
        )

        update_previous_balance_variable = rail.SetVariableOperator(
            task_id='update_previous_balance_variable',
            append=False,
            name='{{ result("create_previousbalance_variable").name }}',
            value=lambda: get_balance_value(
                '1: USA - PTO', '1: USA - PTO (California)')
        )

        if_previousemployeetype_unequal_employeetype = rail.IfOperator(
            task_id='if_previousemployeetype_unequal_employeetype',
            test='''{{ dag_run.conf.previousemployeetype != dag_run.conf.employeetype }}''',
            yes_task="if_location_is_usa",
            no_task="get_value_of_balance",
        )

        if_location_is_usa = rail.IfOperator(
            task_id='if_location_is_usa',
            test='''{{ dag_run.conf.location == 'USA' }}''',
            yes_task="update_previousbalance_variable",
            no_task="update_variable_previousbalance",
        )

        update_previousbalance_variable = rail.SetVariableOperator(
            task_id='update_previousbalance_variable',
            append=False,
            name='{{ result("create_previousbalance_variable").name }}',
            value=lambda: get_balance_value(
                '1: USA - PTO', '1: USA - PTO (California)')
        )

        update_variable_previousbalance = rail.SetVariableOperator(
            task_id='update_variable_previousbalance',
            append=False,
            name='{{ result("create_previousbalance_variable").name }}',
            value=lambda: get_balance_value(
                '1: Canada - Vacation Days (Hourly)', '1: Canada - Vacation Days (Salaried)')
        )

        get_value_of_balance = rail.PythonOperator(
            task_id='get_value_of_balance',
            python_callable=lambda: null if 'NA' in rail.get_dag_run_var(
                'previousbalance') else rail.get_dag_run_var('previousbalance')
        )

        trigger_child_updateuser_timeoffpolicy_assignment = rail.TriggerDagRunOperator(
            task_id='trigger_child_updateuser_timeoffpolicy_assignment',
            retries=0,
            trigger_dag_id=f'centricbrands_user_import_update_user_time_off_policy_assignment_{config.instance}_v2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "uri": "{{ dag_run.conf.uri }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "email": "{{ dag_run.conf.email }}",
                "employeetype": "{{ dag_run.conf.employeetype }}",
                "authenticationtype": "{{ dag_run.conf.authenticationtype }}",
                "departmentname": "{{ dag_run.conf.departmentname }}",
                "licenseseats": "{{ dag_run.conf.licenseseats }}",
                "isloginenabled": "{{ dag_run.conf.isloginenabled }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "integrationdate": "{{ dag_run.conf.integrationdate }}",
                "enddate": "{{ dag_run.conf.enddate }}",
                "userpermission": "{{ dag_run.conf.userpermission }}",
                "supervisorpermission": "{{ dag_run.conf.supervisorpermission }}",
                "teammanager": "{{ dag_run.conf.teammanager }}",
                "payrollmanager": "{{ dag_run.conf.payrollmanager }}",
                "administratorpermission": "{{ dag_run.conf.administratorpermission }}",
                "location": "{{ dag_run.conf.location }}",
                "locationeffectivedate": "{{ dag_run.conf.locationeffectivedate }}",
                "team": "{{ dag_run.conf.team }}",
                "teameffectivedate": "{{ dag_run.conf.teameffectivedate }}",
                "stateprovince": "{{ dag_run.conf.stateprovince }}",
                "supervisorloginname": "{{ dag_run.conf.supervisorloginname }}",
                "supervisorstartdate": "{{ dag_run.conf.supervisorstartdate }}",
                "timeofftemplate": "{{ dag_run.conf.timeofftemplate }}",
                "timeoffapprovalpath": "{{ dag_run.conf.timeoffapprovalpath }}",
                "holidaycalendar": "{{ dag_run.conf.holidaycalendar }}",
                "schedule": "{{ dag_run.conf.schedule }}",
                "scheduleeffectivedate": "{{ dag_run.conf.scheduleeffectivedate }}",
                "timeofftypename": "{{ result('foreach_timeoff_assigned').name }}",
                "timeoffuri": "{{ result('foreach_timeoff_assigned').uri }}",
                "startingbalancesettouri": "{{ result('get_startingbalancesetto_uri') }}",
                "preventbalanceoverdrawuri": "{{ result('get_preventionbalanceoverdraw_uri') }}",
                "previousbalance": "{{result('get_value_of_balance')}}"
            }
        )

        insert_to_wait_for_child_list = rail.SetVariableOperator(
            task_id='insert_to_wait_for_child_list',
            name="{{result('create_list_child_triggered').name}}",
            append=True,
            value="{{result('trigger_child_updateuser_timeoffpolicy_assignment')}}"
        )

        foreach_timeoff_assigned_end = rail.EmptyOperator(
            task_id='foreach_timeoff_assigned_end',
        )

        if_child_triggered = rail.IfOperator(
            task_id='if_child_triggered',
            test=lambda: bool(rail.get_dag_run_var('childtriggered')),
            yes_task='waitfor_child_updateuser_timeoffpolicy_assignment',
            no_task='get_user_time_off_type_policy_summary'
        )

        waitfor_child_updateuser_timeoffpolicy_assignment = rail.WaitForDagRunsSensor(
            task_id='waitfor_child_updateuser_timeoffpolicy_assignment',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_updateuser_timeoffpolicy_assignment") }}'
        )

        get_user_time_off_type_policy_summary = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.uri }}"
            }
        )

        def get_timeoffs_not_needed(dag_run):
            timeoffsalreadyassigned = rail.get_dag_run_var(
                'timeoffnameswithpreviousbalance')
            timeoff_to_assign = rail.result(
                'get_timeoff_types_to_assign_with_uri')
            timeoff_to_reset_balance = [timeoff for timeoff in timeoffsalreadyassigned if (not (
                rail.find_first_by_attr_and_get_attr(timeoff_to_assign, 'uri', timeoff['uri'], 'uri', '')))]
            return timeoff_to_reset_balance if timeoff_to_reset_balance and dag_run.conf['previousloginstatus'] == 'True' else []

        get_timeoffs_already_present_and_not_tobe_added = rail.PythonOperator(
            task_id='get_timeoffs_already_present_and_not_tobe_added',
            python_callable=get_timeoffs_not_needed
        )

        trigger_child_put_balance_as0 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_put_balance_as0',
            retries=0,
            items=lambda: rail.result(
                'get_timeoffs_already_present_and_not_tobe_added'),
            trigger_dag_id=f'centricbrands_user_import_put_0_balance_{config.instance}_v2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "useruri": dag_run.conf['uri'],
                "timeoffuri": item['uri'],
                "terminationdate": (datetime.now() - timedelta(days=1)).strftime("%m/%d/%Y"),
                "startingbalancesettouri": rail.result('get_startingbalancesetto_uri'),
                "preventbalanceoverdrawuri": rail.result('get_preventionbalanceoverdraw_uri'),
                "balance": 0
            }
        )

        waitfor_child_put_balance_as0 = rail.WaitForDagRunsSensor(
            task_id='waitfor_child_put_balance_as0',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_put_balance_as0") }}'
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "{{get_error_message()}}")
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> check_location_hk
        check_location_hk >> rail.Label(
            'Yes') >> search_matching_timeoff_hk >> required_mapper_record_with_timeoff_type
        check_location_hk >> rail.Label('No') >> check_location_china
        check_location_china >> rail.Label(
            'Yes') >> search_matching_timeoff_china >> required_mapper_record_with_timeoff_type
        check_location_china >> rail.Label(
            'No') >> search_timeoff_type_for_user >> required_mapper_record_with_timeoff_type
        required_mapper_record_with_timeoff_type >> if_timeoffype_present
        if_timeoffype_present >> rail.Label(
            'Yes') >> get_time_off_type_assignments_for_user >> get_all_time_off_types >> get_required_time_off_types
        get_required_time_off_types >> declare_timeoffnameswithpreviousbalance_list >> get_today_date_object
        get_today_date_object >> foreach_timeofftype_assignment >> get_balance_summary_for_account >> insertto_timeoffnameswithbalance_list
        insertto_timeoffnameswithbalance_list >> foreach_timeofftype_assignment_end
        foreach_timeofftype_assignment >> foreach_timeofftype_assignment_end >> get_timeoff_types_to_assign_with_uri >> assign_timeofftypes
        assign_timeofftypes >> if_location_china_hongkong
        if_location_china_hongkong >> rail.Label('Yes') >> catch_error
        if_location_china_hongkong >> rail.Label(
            'No') >> get_startingbalancesetto_uri >> get_preventionbalanceoverdraw_uri >> create_list_child_triggered
        create_list_child_triggered >> foreach_timeoff_assigned >> create_previousbalance_variable >> if_previouscountry_equal_location_equal_usa
        if_previouscountry_equal_location_equal_usa >> rail.Label(
            'Yes') >> update_previousbalance >> if_previouscountry_unequal_location
        if_previouscountry_equal_location_equal_usa >> rail.Label(
            'No') >> if_previouscountry_unequal_location
        if_previouscountry_unequal_location >> rail.Label(
            'Yes') >> if_location_equal_usa
        if_location_equal_usa >> rail.Label(
            'Yes') >> update_previous_balance >> get_value_of_balance
        if_location_equal_usa >> rail.Label(
            'No') >> update_previous_balance_variable >> get_value_of_balance
        if_previouscountry_unequal_location >> rail.Label(
            'No') >> if_previousemployeetype_unequal_employeetype
        if_previousemployeetype_unequal_employeetype >> rail.Label(
            'Yes') >> if_location_is_usa
        if_location_is_usa >> rail.Label(
            'Yes') >> update_previousbalance_variable >> get_value_of_balance
        if_location_is_usa >> rail.Label(
            'No') >> update_variable_previousbalance >> get_value_of_balance
        if_previousemployeetype_unequal_employeetype >> rail.Label(
            'No') >> get_value_of_balance >> trigger_child_updateuser_timeoffpolicy_assignment >> insert_to_wait_for_child_list >> foreach_timeoff_assigned_end
        foreach_timeoff_assigned >> foreach_timeoff_assigned_end >> if_child_triggered >> rail.Label(
            'Yes') >> waitfor_child_updateuser_timeoffpolicy_assignment >> get_user_time_off_type_policy_summary
        if_child_triggered >> rail.Label(
            'No') >> get_user_time_off_type_policy_summary >> get_timeoffs_already_present_and_not_tobe_added >> trigger_child_put_balance_as0
        trigger_child_put_balance_as0 >> waitfor_child_put_balance_as0 >> catch_error
        if_timeoffype_present >> rail.Label('No') >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
