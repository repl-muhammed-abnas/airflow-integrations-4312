
from datetime import timedelta, datetime
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nttdatabc_seniority_udf_update_child_{config.instance}_v1',
        description=f'NTTDATABC Seniority UDF Update Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='load_records_approved_timesheet_collection'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='load_records_approved_timesheet_collection',
            end_task='catch_error_and_log_entry',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        load_records_approved_timesheet_collection = rail.PythonOperator(
            task_id = 'load_records_approved_timesheet_collection',
            python_callable= lambda dag_run: rail.load_all_records(dag_run.conf['inputdata1'])
        )

        get_sum_for_timeoff_hours=rail.QueryCollectionOperator(
            task_id='get_sum_for_timeoff_hours',
            query="""SELECT approvedtimesheetdata.timeofftype, SUM( approvedtimesheetdata.timeoffhours) as sum FROM  approvedtimesheetdata""",
        )

        load_records_employee_pay_code_collection = rail.PythonOperator(
            task_id = 'load_records_employee_pay_code_collection',
            python_callable= lambda dag_run: rail.load_all_records(dag_run.conf['inputdata2'])
        )

        get_sum_for_regular_hours=rail.QueryCollectionOperator(
            task_id='get_sum_for_regular_hours',
            query="""SELECT SUM( paycodehoursdata.paycodehours) as sum FROM  paycodehoursdata""",
        )

        def get_paycode_timeoff_hours():
            timeoffhours = rail.load_all_records(rail.result('get_sum_for_timeoff_hours'))[0]['sum']
            paycodehours = rail.load_all_records(rail.result('get_sum_for_regular_hours'))[0]['sum']
            return {
                'timeoffhours': float(timeoffhours) if timeoffhours else 0,
                'paycodehours': float(paycodehours) if paycodehours else 0
            }

        get_paycode_and_timeoff_hours_value = rail.PythonOperator(
            task_id = 'get_paycode_and_timeoff_hours_value',
            python_callable= get_paycode_timeoff_hours
        )

        if_paycode_or_timeoff_hours_present=rail.IfOperator(
            task_id='if_paycode_or_timeoff_hours_present',
            test=lambda: bool( rail.result('get_paycode_and_timeoff_hours_value')['timeoffhours'] or rail.result(
                            'get_paycode_and_timeoff_hours_value')['paycodehours']),
            yes_task="get_final_hours_to_consider",
            no_task="catch_error_and_log_entry",
        )

        def get_hours_to_consider():
            hours = rail.result('get_paycode_and_timeoff_hours_value')
            return (hours['timeoffhours'] if hours['timeoffhours'] else 0) + (hours['paycodehours'] if hours['paycodehours'] else 0)

        get_final_hours_to_consider=rail.PythonOperator(
            task_id='get_final_hours_to_consider',
            python_callable= get_hours_to_consider
        )

        get_timesheet_mapper = rail.CreateLogOperator(
            task_id = 'get_timesheet_mapper',
            tenant_wide_name="ntt_timesheet_mapper",
            existing_log_mode="append",
        )

        get_filter_values = rail.PythonOperator(
            task_id = 'get_filter_values',
            python_callable = lambda: {
                "loginname": (rail.result('load_records_approved_timesheet_collection')[0]['loginname']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['loginname']
                    else rail.result('load_records_employee_pay_code_collection')[0]['loginname']),
                "username": (
                    rail.result('load_records_approved_timesheet_collection')[0]['username']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['username']
                    else rail.result('load_records_employee_pay_code_collection')[0]['username']
                    ),
                "timesheetperiod": (
                    rail.result('load_records_approved_timesheet_collection')[0]['timesheetperiod']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['timesheetperiod']
                    else rail.result('load_records_employee_pay_code_collection')[0]['timesheetperiod']
                    ),
                "useruri": (
                    rail.result('load_records_approved_timesheet_collection')[0]['useruri']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['useruri']
                    else rail.result('load_records_employee_pay_code_collection')[0]['useruri'])
            }
        )

        search_entries_in_timesheet_mapper=rail.FilterLogEntriesOperator(
            task_id='search_entries_in_timesheet_mapper',
            log="{{ result('get_timesheet_mapper') }}",
            properties={
                'loginname': "{{ result('get_filter_values').loginname }}",
                'username': "{{ result('get_filter_values').username }}", 
                'timesheetperiod': "{{ result('get_filter_values').timesheetperiod }}"
            }
        )

        if_entry_present=rail.IfOperator(
            task_id='if_entry_present',
            test='''{{ result('search_entries_in_timesheet_mapper','length') > 0 }}''',
            yes_task="if_hour_in_entry_not_equal_final_hours",
            no_task="if_there_are_final_hours",
        )

        if_hour_in_entry_not_equal_final_hours=rail.IfOperator(
            task_id='if_hour_in_entry_not_equal_final_hours',
            test=lambda: float(rail.load_all_records(
                rail.result('search_entries_in_timesheet_mapper'))[0]['properties']['totalduration']) != float(rail.result('get_final_hours_to_consider')),
            yes_task="trigger_subchild_dag",
            no_task="add_entry_its_already_updated",
        )

        def get_subchild_payload(dag_run):
            ntt_mapper_entry = rail.load_all_records(rail.result('search_entries_in_timesheet_mapper'))[0]
            return {
                    "timesheeturi": (rail.result('load_records_approved_timesheet_collection')[0]['timesheeturi']
                        if rail.result('load_records_approved_timesheet_collection') and \
                            rail.result('load_records_approved_timesheet_collection')[0]['timesheeturi']
                        else rail.result('load_records_employee_pay_code_collection')[0]['timesheeturi']),
                    "approvalstatus": (rail.result('load_records_approved_timesheet_collection')[0]['approvalstatus']
                        if rail.result('load_records_approved_timesheet_collection') and \
                            rail.result('load_records_approved_timesheet_collection')[0]['approvalstatus']
                        else rail.result('load_records_employee_pay_code_collection')[0]['approvalstatus']),
                    "user": (rail.result('load_records_approved_timesheet_collection')[0]['username']
                        if rail.result('load_records_approved_timesheet_collection') and \
                            rail.result('load_records_approved_timesheet_collection')[0]['username']
                        else rail.result('load_records_employee_pay_code_collection')[0]['username']),
                    "useruri": (rail.result('load_records_approved_timesheet_collection')[0]['useruri']
                        if rail.result('load_records_approved_timesheet_collection') and \
                            rail.result('load_records_approved_timesheet_collection')[0]['useruri']
                        else rail.result('load_records_employee_pay_code_collection')[0]['useruri']),
                    "timesheetperiod": (rail.result('load_records_approved_timesheet_collection')[0]['timesheetperiod']
                        if rail.result('load_records_approved_timesheet_collection') and \
                            rail.result('load_records_approved_timesheet_collection')[0]['timesheetperiod']
                        else rail.result('load_records_employee_pay_code_collection')[0]['timesheetperiod']),
                    "totalhours": rail.result('get_final_hours_to_consider'),
                    "loginname": (rail.result('load_records_approved_timesheet_collection')[0]['loginname']
                        if rail.result('load_records_approved_timesheet_collection') and \
                            rail.result('load_records_approved_timesheet_collection')[0]['loginname']
                        else rail.result('load_records_employee_pay_code_collection')[0]['loginname']),
                    "customfielduri": dag_run.conf['senorityudfuri'],
                    "previousvalue": ntt_mapper_entry['properties']['earlierudfvalue'],
                    "previoustotalhours": ntt_mapper_entry['properties']['totalduration'],
                    "check": ntt_mapper_entry['properties']['check'],
                    "parentjobid": dag_run.conf['callerjobid'],
                    'ntttimesheetmapper': rail.result('get_timesheet_mapper'),
                    'lookuptable': dag_run.conf['lookuptable'],
                    'employeetype': dag_run.conf['employeetype']
            }

        trigger_subchild_dag=rail.TriggerDagRunOperator(
            task_id='trigger_subchild_dag',
            retries=0,
            trigger_dag_id=f'nttdatabc_seniority_udf_update_subchild_{config.instance}_v1',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=get_subchild_payload
        )

        wait_for_subchild_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_subchild_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_subchild_dag") }}'
        )

        add_entry_its_already_updated=rail.WriteLogOperator(
            task_id='add_entry_its_already_updated',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity="Skipped",
            properties=lambda dag_run:{
                "jobid": dag_run.conf['callerjobid'],
                "loginname": (rail.result('load_records_approved_timesheet_collection')[0]['loginname']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['loginname']
                    else rail.result('load_records_employee_pay_code_collection')[0]['loginname']),
                "timesheetperiod": (rail.result('load_records_approved_timesheet_collection')[0]['timesheetperiod']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['timesheetperiod']
                    else rail.result('load_records_employee_pay_code_collection')[0]['timesheetperiod']),
                "totalhours": rail.result('get_final_hours_to_consider'),
                "finalvalue": rail.result('get_final_seniority_value'),
                "approvalstatus": (rail.result('load_records_approved_timesheet_collection')[0]['approvalstatus']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['approvalstatus']
                    else rail.result('load_records_employee_pay_code_collection')[0]['approvalstatus']),
                "status": "Skipped",
                "details": "Already updated - No change to the reapproved timesheet",
                "childjob":  get_dagrun_ecid(rail.get_current_context()['dag_run']),
                "retrycount": "0"
            }
        )

        if_there_are_final_hours=rail.IfOperator(
            task_id='if_there_are_final_hours',
            test='''{{ result('get_final_hours_to_consider') > 0 }}''',
            yes_task="get_seniority_custom_field_value",
            no_task="add_entry_zero_hours_to_be_added",
        )

        get_seniority_custom_field_value=rail.RepliconServiceOperator(
            task_id='get_seniority_custom_field_value',
            endpoint="/services/CustomFieldService1.svc/GetValue",
            data={
                "objectUri": "{{ result('get_filter_values').useruri }}",
                "customFieldUri": "{{ dag_run.conf.senorityudfuri }}"
            }
        )

        create_final_senority_variable=rail.SetVariableOperator(
            task_id='create_final_senority_variable',
            append=False,
            name='final_senority',
            value=None
        )

        if_employeetype_is_hourly = rail.IfOperator(
            task_id='if_employeetype_is_hourly',
            test='''{{ dag_run.conf.employeetype == 'Hourly' }}''',
            yes_task="get_final_seniority_value_27",
            no_task="if_employeetype_is_aux_hourly",
        )

        get_final_seniority_value_27=rail.SetVariableOperator(
            task_id='get_final_seniority_value_27',
            append=False,
            name='{{ result("create_final_senority_variable").name }}',
            value=lambda: ( 0 if rail.result('get_seniority_custom_field_value')['text'] == '' else
                                float(rail.result('get_seniority_custom_field_value')['text'])) + 37.5
        )

        if_employeetype_is_aux_hourly = rail.IfOperator(
            task_id='if_employeetype_is_aux_hourly',
            test='''{{ dag_run.conf.employeetype == 'Auxiliary Hourly' }}''',
            yes_task="if_finalhours_to_consoder_lessthan_equal_37_5",
            no_task="add_success_logentry_to_lookup",
        )

        if_finalhours_to_consoder_lessthan_equal_37_5 = rail.IfOperator(
            task_id='if_finalhours_to_consoder_lessthan_equal_37_5',
            test=lambda: bool(float(rail.result('get_final_hours_to_consider')) <= 37.5),
            yes_task="get_final_seniority_value_32",
            no_task="get_final_seniority_value_36",
        )

        get_final_seniority_value_32=rail.SetVariableOperator(
            task_id='get_final_seniority_value_32',
            append=False,
            name='{{ result("create_final_senority_variable").name }}',
            value=lambda: ( 0 if rail.result('get_seniority_custom_field_value')['text'] == '' else
                                float(rail.result('get_seniority_custom_field_value')['text'])) + float(rail.result('get_final_hours_to_consider'))
        )

        get_final_seniority_value_36=rail.SetVariableOperator(
            task_id='get_final_seniority_value_36',
            append=False,
            name='{{ result("create_final_senority_variable").name }}',
            value=lambda: ( 0 if rail.result('get_seniority_custom_field_value')['text'] == '' else
                                float(rail.result('get_seniority_custom_field_value')['text'])) + 37.5
        )

        get_final_seniority_value = rail.PythonOperator(
            task_id= "get_final_seniority_value",
            python_callable=lambda : rail.get_dag_run_var(rail.result('create_final_senority_variable')['name'])
        )

        update_seniority_custom_field_value=rail.RepliconServiceOperator(
            task_id='update_seniority_custom_field_value',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ result('get_filter_values').useruri }}",
                "customFieldUri": "{{ dag_run.conf.senorityudfuri }}",
                "value": "{{ result('get_final_seniority_value') }}"
            }
        )

        add_success_logentry_to_lookup=rail.WriteLogOperator(
            task_id='add_success_logentry_to_lookup',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity="Success",
            properties=lambda dag_run:{
                "jobid": dag_run.conf['callerjobid'],
                "loginname": (rail.result('load_records_approved_timesheet_collection')[0]['loginname']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['loginname']
                    else rail.result('load_records_employee_pay_code_collection')[0]['loginname']),
                "timesheetperiod": (rail.result('load_records_approved_timesheet_collection')[0]['timesheetperiod']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['timesheetperiod']
                    else rail.result('load_records_employee_pay_code_collection')[0]['timesheetperiod']),
                "totalhours": "{{ result('get_final_hours_to_consider') }}",
                "finalvalue": "{{ result('get_final_seniority_value') }}" if dag_run.conf['employeetype'] in ['Hourly','Auxiliary Hourly'] else '',
                "approvalstatus": (rail.result('load_records_approved_timesheet_collection')[0]['approvalstatus']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['approvalstatus']
                    else rail.result('load_records_employee_pay_code_collection')[0]['approvalstatus']),
                "status": "Success",
                "details": "Added Successfully",
                "childjob":  get_dagrun_ecid(rail.get_current_context()['dag_run']),
                "retrycount": "0"
            }
        )

        add_entry_to_ntt_mapper=rail.WriteLogOperator(
            task_id='add_entry_to_ntt_mapper',
            log="{{result('get_timesheet_mapper') }}",
            message="na",
            properties=lambda dag_run:{
                "jobid": get_dagrun_ecid(rail.get_current_context()['dag_run']),
                "loginname": (rail.result('load_records_approved_timesheet_collection')[0]['loginname']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['loginname']
                    else rail.result('load_records_employee_pay_code_collection')[0]['loginname']),
                "username": (rail.result('load_records_approved_timesheet_collection')[0]['username']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['username']
                    else rail.result('load_records_employee_pay_code_collection')[0]['username']),
                "timesheetperiod": (rail.result('load_records_approved_timesheet_collection')[0]['timesheetperiod']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['timesheetperiod']
                    else rail.result('load_records_employee_pay_code_collection')[0]['timesheetperiod']),
                "earlierudfvalue": rail.result('get_seniority_custom_field_value')['text'],
                "totalduration": rail.result('get_final_hours_to_consider'),
                "finalvalue": rail.result('get_final_seniority_value') if dag_run.conf['employeetype'] in ['Hourly','Auxiliary Hourly'] else '',
                "date": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
                "check": (datetime.now()+timedelta(days=46)).strftime('%Y-%m-%d')
            }
        )

        add_entry_zero_hours_to_be_added=rail.WriteLogOperator(
            task_id='add_entry_zero_hours_to_be_added',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity="Skipped",
            properties= lambda dag_run:{
                "jobid": dag_run.conf['callerjobid'],
                "loginname": (rail.result('load_records_approved_timesheet_collection')[0]['loginname']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['loginname']
                    else rail.result('load_records_employee_pay_code_collection')[0]['loginname']),
                "timesheetperiod": (rail.result('load_records_approved_timesheet_collection')[0]['timesheetperiod']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['timesheetperiod']
                    else rail.result('load_records_employee_pay_code_collection')[0]['timesheetperiod']),
                "totalhours": "{{ result('get_final_hours_to_consider') }}",
                "finalvalue": "{{ result('get_final_seniority_value') }}" if dag_run.conf['employeetype'] in ['Hourly','Auxiliary Hourly'] else '',
                "approvalstatus": (rail.result('load_records_approved_timesheet_collection')[0]['approvalstatus']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['approvalstatus']
                    else rail.result('load_records_employee_pay_code_collection')[0]['approvalstatus']),
                "status": "Skipped",
                "details": 'Total Hours 0 for a new entry to be  updated',
                "childjob": get_dagrun_ecid(rail.get_current_context()['dag_run']),
                "retrycount": "0"
            }
        )

        catch_error_and_log_entry=rail.WriteLogOperator(
            task_id='catch_error_and_log_entry',
            log="{{ dag_run.conf.lookuptable }}",
            trigger_rule='one_failed',
            message="{{get_error_message}}",
            severity="Error",
            properties=lambda dag_run:{
                "jobid": dag_run.conf['callerjobid'],
                "loginname": (rail.result('load_records_approved_timesheet_collection')[0]['loginname']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['loginname']
                    else rail.result('load_records_employee_pay_code_collection')[0]['loginname']),
                "timesheetperiod": (rail.result('load_records_approved_timesheet_collection')[0]['timesheetperiod']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['timesheetperiod']
                    else rail.result('load_records_employee_pay_code_collection')[0]['timesheetperiod']),
                "totalhours": "{{ result('get_final_hours_to_consider') }}",
                "finalvalue": "{{ result('get_final_seniority_value') }}" if dag_run.conf['employeetype'] in ['Hourly','Auxiliary Hourly'] else '',
                "approvalstatus": (rail.result('load_records_approved_timesheet_collection')[0]['approvalstatus']
                    if rail.result('load_records_approved_timesheet_collection') and \
                        rail.result('load_records_approved_timesheet_collection')[0]['approvalstatus']
                    else rail.result('load_records_employee_pay_code_collection')[0]['approvalstatus']),
                "status": "Error",
                "details": rail.render_template('{{ get_error_message()}}'),
                "childjob": get_dagrun_ecid(rail.get_current_context()['dag_run']),
                "retrycount": "0"
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error_and_log_entry
        can_run_batch_task >> rail.Label(
            'No') >> load_records_approved_timesheet_collection
        load_records_approved_timesheet_collection >> get_sum_for_timeoff_hours >> load_records_employee_pay_code_collection
        load_records_employee_pay_code_collection >> get_sum_for_regular_hours
        get_sum_for_regular_hours >> get_paycode_and_timeoff_hours_value >> if_paycode_or_timeoff_hours_present
        if_paycode_or_timeoff_hours_present >> rail.Label('No')  >> catch_error_and_log_entry
        if_paycode_or_timeoff_hours_present >> rail.Label(
            'Yes') >> get_final_hours_to_consider >> get_timesheet_mapper >> get_filter_values >> search_entries_in_timesheet_mapper >> if_entry_present
        if_entry_present >> rail.Label('Yes')  >> if_hour_in_entry_not_equal_final_hours
        if_hour_in_entry_not_equal_final_hours >> rail.Label('Yes')  >> trigger_subchild_dag >> wait_for_subchild_dag >> catch_error_and_log_entry
        if_hour_in_entry_not_equal_final_hours >> rail.Label('No') >> add_entry_its_already_updated >> catch_error_and_log_entry
        if_entry_present >> rail.Label('No') >> if_there_are_final_hours
        if_there_are_final_hours >> rail.Label(
            'Yes') >> get_seniority_custom_field_value >> create_final_senority_variable >> if_employeetype_is_hourly
        if_employeetype_is_hourly >> rail.Label('Yes')  >> get_final_seniority_value_27 >> get_final_seniority_value
        if_employeetype_is_hourly >> rail.Label('No')  >> if_employeetype_is_aux_hourly
        if_employeetype_is_aux_hourly >> rail.Label('Yes')  >> if_finalhours_to_consoder_lessthan_equal_37_5
        if_employeetype_is_aux_hourly >> rail.Label('No')  >> add_success_logentry_to_lookup
        if_finalhours_to_consoder_lessthan_equal_37_5 >> rail.Label('Yes')  >> get_final_seniority_value_32 >> get_final_seniority_value
        if_finalhours_to_consoder_lessthan_equal_37_5 >> rail.Label('No')  >> get_final_seniority_value_36 >> get_final_seniority_value
        get_final_seniority_value >> update_seniority_custom_field_value >> add_success_logentry_to_lookup
        add_success_logentry_to_lookup >> add_entry_to_ntt_mapper >> catch_error_and_log_entry
        if_there_are_final_hours >> rail.Label('No') >> add_entry_zero_hours_to_be_added >> catch_error_and_log_entry >> dagrun_log_to_sumo

    return dag

rail.for_each_instance(create_dag)
