
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nttdata_cost_rate_import_child_{config.instance}',
        description=f'NTT Data - Cost rate import-child {config.instance}',
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
            no_task='create_status_variable'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_status_variable',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_status_variable=rail.SetVariableOperator(
            task_id='create_status_variable',
            append=False,
            name='status',
            value=None
        )

        if_effectivedate_invalid=rail.IfOperator(
            task_id='if_effectivedate_invalid',
            test=lambda dag_run: '-' not in dag_run.conf['effectivedate'],
            yes_task="log_invalid_date_fromat",
            no_task="get_enabled_currencies",
        )

        log_invalid_date_fromat=rail.WriteLogOperator(
            task_id='log_invalid_date_fromat',
            log="{{ dag_run.conf.loglookuptable }}",
            message="Invalid date format|{{dag_run_ecid()}}",
            severity="Exception",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "employeeid": "{{dag_run.conf.empid}}",
                "hourlycost": "{{dag_run.conf.hourlyrate}}",
                "effectivedate": "{{dag_run.conf.effectivedate}}",
                "status": "Exception",
                "details": "Invalid date format|{{dag_run_ecid()}}"
            }
        )

        update_status_variable=rail.SetVariableOperator(
            task_id='update_status_variable',
            append=False,
            name='{{ result("create_status_variable").name }}',
            value='Exception'
        )

        get_enabled_currencies=rail.RepliconServiceOperator(
            task_id='get_enabled_currencies',
            endpoint="/services/CurrencyService2.svc/GetEnabledCurrencies",
        )

        get_currency_number_tobe_added=rail.PythonOperator(
            task_id='get_currency_number_tobe_added',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(
                                rail.result('get_enabled_currencies'),'symbol',dag_run.conf['currency'],'uri','') if
                                rail.result('get_enabled_currencies')[0]['symbol'] else null
        )

        if_currency_number_not_found=rail.IfOperator(
            task_id='if_currency_number_not_found',
            test='''{{ result('get_currency_number_tobe_added') | is_falsy }}''',
            yes_task="log_incorrect_currency",
            no_task="get_user_details",
        )

        log_incorrect_currency=rail.WriteLogOperator(
            task_id='log_incorrect_currency',
            log="{{ dag_run.conf.loglookuptable }}",
            message="Appropriate currency must be present |{{dag_run_ecid()}}",
            severity="Exception",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "employeeid": "{{dag_run.conf.empid}}",
                "hourlycost": "{{dag_run.conf.hourlyrate}}",
                "effectivedate": "{{dag_run.conf.effectivedate}}",
                "status": "Exception",
                "details": "Appropriate currency must be present |{{dag_run_ecid()}}"
            }
        )

        update_variable_status=rail.SetVariableOperator(
            task_id='update_variable_status',
            append=False,
            name='{{ result("create_status_variable").name }}',
            value='Exception'
        )

        get_user_details=rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/UserService1.svc/GetUserDetails",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_current_value_of_annual_hours=rail.PythonOperator(
            task_id='log_current_value_of_annual_hours',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(
                                rail.result('get_user_details')['customFieldValues'],'customField.displayText','Annual Hours','text',null) if(
                                rail.result('get_user_details')['customFieldValues'] and
                                rail.result('get_user_details')['customFieldValues'][0]['customField']['uri']) else null
        )

        if_annual_hours_present_and_unequal_current=rail.IfOperator(
            task_id='if_annual_hours_present_and_unequal_current',
            test=lambda dag_run: bool(dag_run.conf['annualhours'] and float(dag_run.conf['annualhours'])!= rail.result('log_current_value_of_annual_hours')),
            yes_task="get_enabled_custom_fields",
            no_task="get_date",
        )

        get_enabled_custom_fields=rail.RepliconServiceOperator(
            task_id='get_enabled_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            }
        )

        log_annual_hours_customfield_uri=rail.PythonOperator(
            task_id='log_annual_hours_customfield_uri',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_custom_fields'),'displayText','Annual Hours','uri','')
        )

        if_annual_hours_customfield_uri_present=rail.IfOperator(
            task_id='if_annual_hours_customfield_uri_present',
            test='''{{ result('log_annual_hours_customfield_uri') | is_truthy }}''',
            yes_task="update_annual_hours_numeric_value",
            no_task="get_date",
        )

        update_annual_hours_numeric_value=rail.RepliconServiceOperator(
            task_id='update_annual_hours_numeric_value',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_annual_hours_customfield_uri') }}",
                "value": "{{ dag_run.conf.annualhours }}"
            }
        )

        def get_date_in_format(dag_run):
            effectivedate = datetime.strptime(dag_run.conf['effectivedate'],'%Y-%m-%d')
            return{
                'year': effectivedate.strftime('%Y'),
                'month': effectivedate.strftime('%m'),
                'day': effectivedate.strftime('%d')
            }

        get_date=rail.PythonOperator(
            task_id='get_date',
            python_callable= get_date_in_format
        )

        create_costratedetails_list=rail.SetVariableOperator(
            task_id='create_costratedetails_list',
            append=False,
            name='costrate_details',
            value=[]
        )

        get_user_cost_rate_schedule=rail.RepliconServiceOperator(
            task_id='get_user_cost_rate_schedule',
            endpoint="/services/ResourceService1.svc/GetUserCostRateSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        create_variable_initialrate=rail.SetVariableOperator(
            task_id='create_variable_initialrate',
            append=False,
            name='initialrate',
            value=null
        )

        insert_to_costrate_details_list=rail.SetVariableOperator(
            task_id='insert_to_costrate_details_list',
            append=True,
            name='{{ result("create_costratedetails_list").name }}',
            value={
                "hourlyRate": {
                    "amount": "{{ dag_run.conf.hourlyrate }}",
                    "currency": {
                        "uri": "{{ result('get_currency_number_tobe_added') }}",
                        "name": "null",
                        "symbol": "null"
                    }
                },
                "effectiveDate": {
                    "year": "{{ result('get_date').year }}",
                    "month": "{{ result('get_date').month }}",
                    "day": "{{ result('get_date').day }}"
                }
            }
        )

        foreach_user_cost_rate_schedule=rail.ForEachOperator(
            task_id='foreach_user_cost_rate_schedule',
            items=lambda: rail.result('get_user_cost_rate_schedule'),
            start_task = 'if_effectivedate_not_present',
            end_task = 'foreach_user_cost_rate_schedule_end'
        )

        if_effectivedate_not_present=rail.IfOperator(
            task_id='if_effectivedate_not_present',
            test="{{ result('foreach_user_cost_rate_schedule').effectiveDate | is_falsy }}",
            yes_task="if_uri_present",
            no_task="if_effectivedate_present",
        )

        if_uri_present=rail.IfOperator(
            task_id='if_uri_present',
            test='''{{ result('foreach_user_cost_rate_schedule').uri | is_truthy }}''',
            yes_task="update_initialrate_variable",
            no_task="if_effectivedate_present",
        )

        update_initialrate_variable=rail.SetVariableOperator(
            task_id='update_initialrate_variable',
            append=False,
            name='{{ result("create_variable_initialrate").name }}',
            value={
                "amount": "{{ result('foreach_user_cost_rate_schedule').hourlyRate.amount }}",
                "currency": {
                "uri": "{{ result('foreach_user_cost_rate_schedule').hourlyRate.currency.uri }}",
                "name": "null",
                "symbol": "null"
                }
            }
        )

        if_effectivedate_present=rail.IfOperator(
            task_id='if_effectivedate_present',
            test='''{{ result('foreach_user_cost_rate_schedule').effectiveDate | is_truthy }}''',
            yes_task="log_effectivedate_replicon",
            no_task="foreach_user_cost_rate_schedule_end",
        )

        log_effectivedate_replicon=rail.PythonOperator(
            task_id='log_effectivedate_replicon',
            python_callable= lambda:  str(rail.result('foreach_user_cost_rate_schedule')['effectiveDate']['year']) + '-' +
                                str(rail.result('foreach_user_cost_rate_schedule')['effectiveDate']['month']) + '-' +
                                str(rail.result('foreach_user_cost_rate_schedule')['effectiveDate']['day'])
        )

        if_effective_date_not_equal_effectivedate_replicon=rail.IfOperator(
            task_id='if_effective_date_not_equal_effectivedate_replicon',
            test=lambda dag_run: datetime.strptime(dag_run.conf['effectivedate'],'%Y-%m-%d') != datetime.strptime(
                    rail.result('log_effectivedate_replicon'),'%Y-%m-%d'),
            yes_task="insert_to_costratedetails_list",
            no_task="foreach_user_cost_rate_schedule_end",
        )

        insert_to_costratedetails_list=rail.SetVariableOperator(
            task_id='insert_to_costratedetails_list',
            append=True,
            name='{{ result("create_costratedetails_list").name }}',
            value={
                "hourlyRate": {
                    "amount": "{{ result('foreach_user_cost_rate_schedule').hourlyRate.amount }}",
                    "currency": {
                        "uri": "{{ result('foreach_user_cost_rate_schedule').hourlyRate.currency.uri }}",
                        "name": "{{ result('foreach_user_cost_rate_schedule').hourlyRate.currency.name }}",
                        "symbol": "{{ result('foreach_user_cost_rate_schedule').hourlyRate.currency.symbol }}"
                    }
                },
                "effectiveDate": {
                    "year": "{{ result('foreach_user_cost_rate_schedule').effectiveDate.year }}",
                    "month": "{{ result('foreach_user_cost_rate_schedule').effectiveDate.month }}",
                    "day": "{{ result('foreach_user_cost_rate_schedule').effectiveDate.day }}"
                }
            }
        )

        foreach_user_cost_rate_schedule_end=rail.EmptyOperator(
            task_id='foreach_user_cost_rate_schedule_end',
        )

        log_costratedetails_list_value=rail.PythonOperator(
            task_id='log_costratedetails_list_value',
            python_callable= lambda: rail.get_dag_run_var('costrate_details')
        )

        put_user_cost_rate_schedule=rail.RepliconServiceOperator(
            task_id='put_user_cost_rate_schedule',
            endpoint="/services/ResourceService1.svc/PutUserCostRateSchedule",
            data=lambda dag_run:{
                "userUri": dag_run.conf['useruri'],
                "schedule": {
                    "initialHourlyRate": rail.get_dag_run_var('initialrate') ,
                    "scheduleEntries": rail.result('log_costratedetails_list_value')
                }
            }
        )

        log_costrate_successfully_updated=rail.WriteLogOperator(
            task_id='log_costrate_successfully_updated',
            log="{{  dag_run.conf.loglookuptable  }}",
            message="Cost rate successfully updated|{{dag_run_ecid()}}",
            severity="Success",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "employeeid": "{{dag_run.conf.empid}}",
                "hourlycost": "{{dag_run.conf.hourlyrate}}",
                "effectivedate": "{{dag_run.conf.effectivedate}}",
                "status": "Success",
                "details": "Cost rate successfully updated|{{dag_run_ecid()}}"
            }
        )

        mark_status_variable_success=rail.SetVariableOperator(
            task_id='mark_status_variable_success',
            append=False,
            name='{{ result("create_status_variable").name }}',
            value='Success'
        )

        catch_and_log_error=rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.loglookuptable }}",
            trigger_rule='one_failed',
            message="Error while updating the cost rate|{{dag_run_ecid()}}",
            severity="Error",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "employeeid": "{{dag_run.conf.empid}}",
                "hourlycost": "{{dag_run.conf.hourlyrate}}",
                "effectivedate": "{{dag_run.conf.effectivedate}}",
                "status": "Error",
                "details": "Error while updating the cost rate|{{dag_run_ecid()}}"
            }
        )

        mark_status_variable_failed=rail.SetVariableOperator(
            task_id='mark_status_variable_failed',
            append=False,
            name='{{ result("create_status_variable").name }}',
            value='Failed'
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> create_status_variable
        create_status_variable >> if_effectivedate_invalid
        if_effectivedate_invalid >> rail.Label('Yes')  >> log_invalid_date_fromat >> update_status_variable >> catch_and_log_error
        if_effectivedate_invalid >> rail.Label('No') >> get_enabled_currencies >> get_currency_number_tobe_added >> if_currency_number_not_found
        if_currency_number_not_found >> rail.Label('Yes')  >> log_incorrect_currency >> update_variable_status >> catch_and_log_error
        if_currency_number_not_found >> rail.Label('No') >> get_user_details >> log_current_value_of_annual_hours >> if_annual_hours_present_and_unequal_current
        if_annual_hours_present_and_unequal_current >> rail.Label('Yes') >> get_enabled_custom_fields >> log_annual_hours_customfield_uri
        log_annual_hours_customfield_uri >> if_annual_hours_customfield_uri_present
        if_annual_hours_customfield_uri_present >> rail.Label('Yes')  >> update_annual_hours_numeric_value >> get_date
        if_annual_hours_customfield_uri_present >> rail.Label('No') >> get_date
        if_annual_hours_present_and_unequal_current >> rail.Label('No') >> get_date >> create_costratedetails_list >> get_user_cost_rate_schedule
        get_user_cost_rate_schedule >> create_variable_initialrate >> insert_to_costrate_details_list
        insert_to_costrate_details_list >> foreach_user_cost_rate_schedule >> if_effectivedate_not_present
        if_effectivedate_not_present >> rail.Label('Yes')  >> if_uri_present
        if_uri_present >> rail.Label('Yes')  >> update_initialrate_variable >> if_effectivedate_present
        if_uri_present >> rail.Label('No') >> if_effectivedate_present
        if_effectivedate_not_present >> rail.Label('No') >> if_effectivedate_present
        if_effectivedate_present >> rail.Label('Yes')  >> log_effectivedate_replicon >> if_effective_date_not_equal_effectivedate_replicon
        if_effective_date_not_equal_effectivedate_replicon >> rail.Label('Yes')  >> insert_to_costratedetails_list >> foreach_user_cost_rate_schedule_end
        if_effective_date_not_equal_effectivedate_replicon >> rail.Label('No') >> foreach_user_cost_rate_schedule_end
        if_effectivedate_present >> rail.Label('No') >> foreach_user_cost_rate_schedule_end
        foreach_user_cost_rate_schedule >> foreach_user_cost_rate_schedule_end >> log_costratedetails_list_value >> put_user_cost_rate_schedule
        put_user_cost_rate_schedule >> log_costrate_successfully_updated >> mark_status_variable_success
        mark_status_variable_success >> catch_and_log_error >> mark_status_variable_failed >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
