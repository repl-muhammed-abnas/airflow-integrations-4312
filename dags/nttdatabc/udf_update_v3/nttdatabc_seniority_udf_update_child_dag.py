
from datetime import timedelta, datetime
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nttdatabc_seniority_udf_update_child_{config.instance}_v3',
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

        create_final_senority_variable=rail.SetVariableOperator(
            task_id='create_final_senority_variable',
            append=False,
            name='final_senority',
            value=None
        )

        if_employeetype_is_hourly = rail.IfOperator(
            task_id='if_employeetype_is_hourly',
            test='''{{ dag_run.conf.employeetype == 'Hourly'}}''',
            yes_task="get_final_seniority_value_27",
            no_task="if_employeetype_is_aux_hourly",
        )

        get_final_seniority_value_27=rail.SetVariableOperator(
            task_id='get_final_seniority_value_27',
            append=False,
            name='{{ result("create_final_senority_variable").name }}',
            value=37.5
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
            value=lambda: float(rail.result('get_final_hours_to_consider'))
        )

        get_final_seniority_value_36=rail.SetVariableOperator(
            task_id='get_final_seniority_value_36',
            append=False,
            name='{{ result("create_final_senority_variable").name }}',
            value= 37.5
        )

        get_final_seniority_value = rail.PythonOperator(
            task_id= "get_final_seniority_value",
            python_callable=lambda : rail.get_dag_run_var(rail.result('create_final_senority_variable')['name'])
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
                "employeetype":dag_run.conf['employeetype'],
                "finalvalue": "{{ result('get_final_seniority_value') }}" if dag_run.conf['employeetype'] in [
                    'Hourly','Auxiliary Hourly'] else '',
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
                "employeetype":dag_run.conf['employeetype'],
                "finalvalue": "{{ result('get_final_seniority_value') }}" if dag_run.conf['employeetype'] in [
                    'Hourly','Auxiliary Hourly'] else '',
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
            'Yes') >> get_final_hours_to_consider >> create_final_senority_variable >> if_employeetype_is_hourly

        if_employeetype_is_hourly >> rail.Label('Yes')  >> get_final_seniority_value_27 >> get_final_seniority_value
        if_employeetype_is_hourly >> rail.Label('No')  >> if_employeetype_is_aux_hourly
        if_employeetype_is_aux_hourly >> rail.Label('Yes')  >> if_finalhours_to_consoder_lessthan_equal_37_5
        if_employeetype_is_aux_hourly >> rail.Label('No')  >> add_success_logentry_to_lookup
        if_finalhours_to_consoder_lessthan_equal_37_5 >> rail.Label('Yes')  >> get_final_seniority_value_32 >> get_final_seniority_value
        if_finalhours_to_consoder_lessthan_equal_37_5 >> rail.Label('No')  >> get_final_seniority_value_36 >> get_final_seniority_value

        get_final_seniority_value >> add_success_logentry_to_lookup >> catch_error_and_log_entry >> dagrun_log_to_sumo

    return dag

rail.for_each_instance(create_dag)
