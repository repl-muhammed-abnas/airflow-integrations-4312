
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'hawaiigas_user_import_activity_and_pay_rate_assignments_{config.instance}',
        description=f'HawaiiGas User Import Activity and Pay rate assignments {config.instance}',
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
            no_task='query_list_5'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_list_5',
            end_task='catch_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        query_list_5=rail.QueryCollectionOperator(
            task_id='query_list_5',
            query='''SELECT  activity_and_payrates_input.recordtype, activity_and_payrates_input.employee, activity_and_payrates_input.paycode,
                activity_and_payrates_input.rate, activity_and_payrates_input.status FROM  activity_and_payrates_input WHERE
                activity_and_payrates_input.employee="{{ dag_run.conf.employee }}" AND  activity_and_payrates_input.status="Active"''',
        )

        get_today_date_object=rail.PythonOperator(
            task_id='get_today_date_object',
            python_callable= lambda: {
                'day': datetime.now().day,
                'month': datetime.now().month,
                'year': datetime.now().year
            }
        )

        get_allcurrencies_11=rail.RepliconServiceOperator(
            task_id='get_allcurrencies_11',
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies",
        )

        get_enabled_activities_12=rail.RepliconServiceOperator(
            task_id='get_enabled_activities_12',
            endpoint="/services/ActivityService1.svc/GetEnabledActivities",
        )

        log_currency_urifor_u_s_d_13=rail.PythonOperator(
            task_id='log_currency_urifor_u_s_d_13',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_allcurrencies_11'),'displayText', "USD$",'uri','')
        )

        def get_user_and_employeetype_uri(response,dag_run):
            usersfound = response['rows']
            matching_user = list(filter(lambda user: user['cells'][0]['textValue'] == dag_run.conf['employee'] ,usersfound))
            full_name = matching_user[0]['cells'][1]['textValue'] if matching_user else ''
            return {
                'useruri': matching_user[0]['cells'][0]['uri'] if matching_user else '',
                'employeetypeuri': matching_user[0]['cells'][2]['uri'] if matching_user else '',
                'name': (full_name.rsplit(',', maxsplit=1)[-1].strip() + " " + full_name.rsplit(',', maxsplit=1)[0].strip()) if matching_user else ''
            }

        search_users_14=rail.RepliconServiceOperator(
            task_id='search_users_14',
            endpoint="/services/UserListService1.svc/GetData",
            data={
              "page": "1",
              "pagesize": "1000",
              "columnUris": [
                "urn:replicon:user-list-column:login-name",
                "urn:replicon:user-list-column:user-name",
                "urn:replicon:user-list-column:employee-type"
              ],
              "sort": [],
              "filterExpression": {
                "leftExpression": {
                  "leftExpression": null,
                  "operatorUri": null,
                  "rightExpression": null,
                  "value": null,
                  "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
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
                    "text": "{{ dag_run.conf.employee }}",
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
            data_handler=get_user_and_employeetype_uri
        )

        if_log_checkiftheuserexists_15_present_17=rail.IfOperator(
            task_id='if_log_checkiftheuserexists_15_present_17',
            test='''{{ result('search_users_14').useruri | is_truthy }}''',
            yes_task="getpayrateschedule_19",
            no_task="hawaii_gas_activity_and_pay_rate_logs_prod_add_entry_48",
        )

        getpayrateschedule_19=rail.RepliconServiceOperator(
            task_id='getpayrateschedule_19',
            endpoint="/services/PayRateService1.svc/GetUserPayRateSchedule",
            data={
                "userUri": "{{ result('search_users_14').useruri }}",
                "filterDimensions": []
            }
        )

        def get_existing_payrate_schedule_list():
            existing_schedule = rail.result('getpayrateschedule_19')
            new_schedule = []
            for schedule in existing_schedule:
                for dimension in schedule['dimensions']:
                    schedule_effectivedate = str(schedule['effectiveDate']['day']) + "/" + str(schedule['effectiveDate']['month']) + "/" + str(
                    schedule['effectiveDate']['year']) if schedule['effectiveDate'] else null
                    if dimension and dimension['activity'] and dimension['activity']['displayText'] and schedule_effectivedate and (
                        datetime.strptime(schedule_effectivedate,'%d/%m/%Y') < (datetime.today() + timedelta(days=1))):
                        new_schedule.append({
                            "activityname": dimension['activity']['name'],
                            "activityuri": dimension['activity']['uri'],
                            "effectivedate": schedule_effectivedate,
                            "amount": schedule['payRate']['amount'],
                            "daydiff": (datetime.strptime(schedule_effectivedate,"%d/%m/%Y") - datetime.today()).days if schedule['effectiveDate'] else -10000
                        })
            return new_schedule

        create_existing_payrate_schedule_list=rail.PythonOperator(
            task_id='create_existing_payrate_schedule_list',
            python_callable=get_existing_payrate_schedule_list
        )

        foreach_query_list_5_24=rail.ForEachOperator(
            task_id='foreach_query_list_5_24',
            items="{{ result('query_list_5') }}",
            start_task = 'log_getlatestassignedpayrateforgivenactivity_25',
            end_task = 'foreach_query_list_5_24_end'
        )

        def get_latest_payrate_for_activity(activity):
            daydifference = -10000
            latest_payrate = 0
            all_payrate_schedules = rail.result('create_existing_payrate_schedule_list')
            for payrate in all_payrate_schedules:
                if payrate['activityname'] == activity and payrate['daydiff'] > daydifference:
                    latest_payrate = payrate['amount']
                    daydifference = payrate['daydiff']
            return latest_payrate

        log_getlatestassignedpayrateforgivenactivity_25=rail.PythonOperator(
            task_id='log_getlatestassignedpayrateforgivenactivity_25',
            python_callable= lambda: get_latest_payrate_for_activity(rail.result('foreach_query_list_5_24')['paycode']) if rail.result(
                'create_existing_payrate_schedule_list') else 0
        )

        if_foreach_query_list_5_24_rate_present_dataforeachforeach_query_list_5_24rate_26=rail.IfOperator(
            task_id='if_foreach_query_list_5_24_rate_present_dataforeachforeach_query_list_5_24rate_26',
            test=lambda: rail.result('foreach_query_list_5_24')['rate'] and float(rail.result(
                'log_getlatestassignedpayrateforgivenactivity_25')!= round(float(rail.result('foreach_query_list_5_24')['rate']),2)),
            yes_task="log_activityuri_27",
            no_task="hawaii_gas_activity_and_pay_rate_logs_prod_add_entry_34",
        )

        log_activityuri_27=rail.PythonOperator(
            task_id='log_activityuri_27',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_enabled_activities_12'),'displayText',(rail.result(
                'foreach_query_list_5_24')['paycode']).strip(),'uri') if rail.result('get_enabled_activities_12') else null
        )

        if_log_activityuri_27_present_28=rail.IfOperator(
            task_id='if_log_activityuri_27_present_28',
            test='''{{ result('log_activityuri_27') | is_truthy }}''',
            yes_task="update_pay_rate_29",
            no_task="hawaii_gas_activity_and_pay_rate_logs_prod_add_entry_32",
        )

        update_pay_rate_29=rail.RepliconServiceOperator(
            task_id='update_pay_rate_29',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('search_users_14').useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "payRatesModifications": {
                    "scheduleEntriesToPut": [
                        {
                        "dimensions": [
                            {
                            "activity": {
                                "uri": "{{ result('log_activityuri_27') }}",
                                "name": null
                            },
                            "extensionFieldValue": null
                            }
                        ],
                        "payRate": {
                            "amount": "{{ result('foreach_query_list_5_24').rate }}",
                            "currency": {
                            "uri": "{{ result('log_currency_urifor_u_s_d_13') }}",
                            "name": null,
                            "symbol": null
                            }
                        },
                        "effectiveDate": {
                            "year": "{{ result('get_today_date_object').year }}",
                            "month": "{{ result('get_today_date_object').month }}",
                            "day": "{{ result('get_today_date_object').day }}"
                        }
                        }
                    ]
                    },
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null
                }
            }
        )

        hawaii_gas_activity_and_pay_rate_logs_prod_add_entry_30=rail.WriteLogOperator(
            task_id='hawaii_gas_activity_and_pay_rate_logs_prod_add_entry_30',
            log="{{ dag_run.conf.activitypayratelogs }}",
            message="na",
            severity="Success",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "activityname": "{{ result('foreach_query_list_5_24').paycode }}",
                "payrateamount": "{{ result('foreach_query_list_5_24').rate }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ result('foreach_query_list_5_24').employee }}",
                "username": "{{ result('search_users_14').name }}",
                "status": "Success",
                "reason": ""
            }
        )

        hawaii_gas_activity_and_pay_rate_logs_prod_add_entry_32=rail.WriteLogOperator(
            task_id='hawaii_gas_activity_and_pay_rate_logs_prod_add_entry_32',
            log="{{ dag_run.conf.activitypayratelogs }}",
            message="na",
            severity="Ignored",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "activityname": "{{ result('foreach_query_list_5_24').paycode }}",
                "payrateamount": "{{ result('foreach_query_list_5_24').rate }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employee }}",
                "username": "{{ result('search_users_14').name }}",
                "status": "Ignored",
                #pylint: disable = line-too-long
                "reason": "Activity and Payrate not updated since the Activity with the name - {{result('foreach_query_list_5_24').paycode}} is not present in Replicon"
            }
        )

        hawaii_gas_activity_and_pay_rate_logs_prod_add_entry_34=rail.WriteLogOperator(
            task_id='hawaii_gas_activity_and_pay_rate_logs_prod_add_entry_34',
            log="{{ dag_run.conf.activitypayratelogs }}",
            message="na",
            severity="Ignored",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "activityname": "{{ result('foreach_query_list_5_24').paycode }}",
                "payrateamount": "{{ result('foreach_query_list_5_24').rate }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employee }}",
                "username": "{{ result('search_users_14').name }}",
                "status": "Ignored",
                #pylint: disable = line-too-long
                "reason": "Payrate not updated since there is no change in the payrate currently assigned for the activity - {{result('foreach_query_list_5_24').paycode}}"
            }
        )

        foreach_query_list_5_24_end=rail.EmptyOperator(
            task_id='foreach_query_list_5_24_end',
        )

        if_log_getemployeetypeuri_16_present_35=rail.IfOperator(
            task_id='if_log_getemployeetypeuri_16_present_35',
            test='''{{ result('search_users_14').employeetypeuri | is_truthy }}''',
            yes_task="get_employee_type_details_36",
            no_task="create_customfield_values_list",
        )

        get_employee_type_details_36=rail.RepliconServiceOperator(
            task_id='get_employee_type_details_36',
            endpoint="/services/EmployeeTypeService1.svc/GetEmployeeTypeDetails",
            data={
                "employeeTypeUri": "{{ result('search_users_14').employeetypeuri }}"
            }
        )

        create_customfield_values_list=rail.PythonOperator(
            task_id='create_customfield_values_list',
            python_callable=lambda: [{
                'name': customfield['customField']['name'],
                'value': customfield['text']
            }for customfield in rail.result('get_employee_type_details_36')['customFields']]
        )

        def create_activityuris_list():
            activities = rail.load_all_records(rail.result('query_list_5'))
            activityuris = [(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_enabled_activities_12'),'displayText',activity['paycode'].strip(),'uri','') if rail.result(
                'get_enabled_activities_12') else null ) for activity in activities if not(rail.find_first_by_attr_and_get_attr(rail.result(
                'create_customfield_values_list'),'value',activity['paycode'],'value','') if rail.result('create_customfield_values_list') else null)]
            return [uri for uri in activityuris if uri != '']

        get_activityuris_list=rail.PythonOperator(
            task_id='get_activityuris_list',
            python_callable=create_activityuris_list
        )

        if_log_final_activitiestobeassigned_44_present_45=rail.IfOperator(
            task_id='if_log_final_activitiestobeassigned_44_present_45',
            test='''{{ result('get_activityuris_list') | is_truthy }}''',
            yes_task="put_activity_assignments_for_user_46",
            no_task="catch_log_error",
        )

        put_activity_assignments_for_user_46=rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_46',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda:{
                "userUri": rail.result('search_users_14')['useruri'],
                "activityUris": rail.result('get_activityuris_list')
            }
        )

        hawaii_gas_activity_and_pay_rate_logs_prod_add_entry_48=rail.WriteLogOperator(
            task_id='hawaii_gas_activity_and_pay_rate_logs_prod_add_entry_48',
            log="{{ dag_run.conf.activitypayratelogs }}",
            message="na",
            severity="Ignored",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "activityname": "",
                "payrateamount": "",
                "childjobid": "{{ dag_run_ecid() }}",
                "employeeid": "{{ dag_run.conf.employee }}",
                "username": "{{ result('search_users_14').name }}",
                "status": "Ignored",
                "reason": "Activity and Payrate not updated since the user with login name - {{dag_run.conf.employee}} is not present in Replicon"
            }
        )

        catch_log_error=rail.WriteLogOperator(
            task_id='catch_log_error',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.activitypayratelogs }}",
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['callerjobid'],
                "activityname": rail.result('foreach_query_list_5_24')['paycode'] if rail.result('foreach_query_list_5_24') else '',
                "payrateamount": rail.result('foreach_query_list_5_24')['rate'] if rail.result('foreach_query_list_5_24') else '',
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "employeeid": dag_run.conf['employee'],
                "username": rail.result('search_users_14')['name'],
                "status": "Error",
                "reason": rail.render_template("{{get_error_message()}}")
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_log_error
        can_run_batch_task >> rail.Label('No') >> query_list_5 >> get_today_date_object >> get_allcurrencies_11 >> get_enabled_activities_12
        get_enabled_activities_12 >> log_currency_urifor_u_s_d_13 >> search_users_14 >> if_log_checkiftheuserexists_15_present_17
        if_log_checkiftheuserexists_15_present_17 >> rail.Label(
            'Yes') >> getpayrateschedule_19 >> create_existing_payrate_schedule_list >> foreach_query_list_5_24
        foreach_query_list_5_24 >> log_getlatestassignedpayrateforgivenactivity_25
        log_getlatestassignedpayrateforgivenactivity_25 >> if_foreach_query_list_5_24_rate_present_dataforeachforeach_query_list_5_24rate_26
        if_foreach_query_list_5_24_rate_present_dataforeachforeach_query_list_5_24rate_26 >> rail.Label(
            'Yes') >> log_activityuri_27 >> if_log_activityuri_27_present_28
        if_log_activityuri_27_present_28 >> rail.Label(
            'Yes') >> update_pay_rate_29 >> hawaii_gas_activity_and_pay_rate_logs_prod_add_entry_30 >> foreach_query_list_5_24_end
        if_log_activityuri_27_present_28 >> rail.Label('No') >> hawaii_gas_activity_and_pay_rate_logs_prod_add_entry_32 >> foreach_query_list_5_24_end
        if_foreach_query_list_5_24_rate_present_dataforeachforeach_query_list_5_24rate_26 >> rail.Label(
            'No') >> hawaii_gas_activity_and_pay_rate_logs_prod_add_entry_34 >> foreach_query_list_5_24_end
        foreach_query_list_5_24 >> foreach_query_list_5_24_end >> if_log_getemployeetypeuri_16_present_35
        if_log_getemployeetypeuri_16_present_35 >> rail.Label('Yes') >> get_employee_type_details_36 >> create_customfield_values_list
        if_log_getemployeetypeuri_16_present_35 >> rail.Label(
            'No') >> create_customfield_values_list >> get_activityuris_list >> if_log_final_activitiestobeassigned_44_present_45
        if_log_final_activitiestobeassigned_44_present_45 >> rail.Label('Yes')  >> put_activity_assignments_for_user_46 >> catch_log_error
        if_log_final_activitiestobeassigned_44_present_45 >> rail.Label('No') >> catch_log_error
        if_log_checkiftheuserexists_15_present_17 >> rail.Label(
            'No') >> hawaii_gas_activity_and_pay_rate_logs_prod_add_entry_48 >> catch_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
