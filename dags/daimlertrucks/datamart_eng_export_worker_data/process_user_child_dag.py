
from datetime import datetime, timedelta
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'daimlertrucks_datamart_eng_export_worker_data_process_user_child_{config.instance}',
        description=f'DTNA Process user child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='final_result',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        declare_list_24 = rail.SetVariableOperator(
            task_id='declare_list_24',
            append=False,
            name='Worker Data File',
            value=[]
        )

        query_list_28 = rail.QueryCollectionOperator(
            task_id='query_list_28',
            query="""SELECT * FROM  input_data_file WHERE  LoginName = "{{ dag_run.conf.LoginName }}" """,
        )

        load_all_query_data_28 = rail.PythonOperator(
            task_id='load_all_query_data_28',
            python_callable=lambda: rail.load_all_records(
                rail.result('query_list_28'))
        )

        get_user_details_30 = rail.RepliconServiceOperator(
            task_id='get_user_details_30',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "loginName": "{{ dag_run.conf.LoginName }}",
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda data: data[0]['userDetails']
        )

        def get_replicon_date(datetime_obj):
            return {
                "day": datetime_obj.day, "month": datetime_obj.month, "year": datetime_obj.year
            }

        log_33 = rail.PythonOperator(
            task_id='log_33',
            python_callable=lambda:  rail.result('get_user_details_30')[
                'employmentDateRange'].get('startDate') or get_replicon_date(datetime.utcnow())
        )

        if_log_33_less_than_todayto_time_34 = rail.IfOperator(
            task_id='if_log_33_less_than_todayto_time_34',
            test=lambda: datetime(**rail.result('log_33')) < datetime(**get_replicon_date(datetime.utcnow(
            ))) or datetime(**rail.result('log_33')) == datetime(**get_replicon_date(datetime.utcnow())),
            yes_task="get_cost_center_schedule_for_user_36",
            no_task="foreach_query_list_28_138_end",
        )

        get_cost_center_schedule_for_user_36 = rail.RepliconServiceOperator(
            task_id='get_cost_center_schedule_for_user_36',
            endpoint="/services/CostCenterService1.svc/GetCostCenterScheduleForUser",
            data={
                "userUri": "{{ result('get_user_details_30').uri}}"
            }
        )

        if_costcenter_displaytext_present_39 = rail.IfOperator(
            task_id='if_costcenter_displaytext_present_39',
            test='''{{ result('get_cost_center_schedule_for_user_36') | is_truthy and result('get_cost_center_schedule_for_user_36')[0].costCenter.displayText | is_truthy }}''',
            yes_task="foreach_response_41",
            no_task="get_current_cost_center_data_for_user_49",
        )

        foreach_response_41 = rail.ForEachOperator(
            task_id='foreach_response_41',
            items="{{ result('get_cost_center_schedule_for_user_36') | to_json }}",
            start_task='log_effective_date_42',
            end_task='foreach_response_41_end'
        )

        log_effective_date_42 = rail.PythonOperator(
            task_id='log_effective_date_42',
            python_callable=lambda: rail.result(
                'foreach_response_41')['effectiveDate'] or get_replicon_date(datetime.utcnow(
                ))
        )

        if_effectivedate_day_present_43 = rail.IfOperator(
            task_id='if_effectivedate_day_present_43',
            test='''{{ result('foreach_response_41').effectiveDate | is_truthy }}''',
            yes_task="if_to_time_equals_to_todayto_time_44",
            no_task="if_effectivedate_day_blank_46",
        )

        if_to_time_equals_to_todayto_time_44 = rail.IfOperator(
            task_id='if_to_time_equals_to_todayto_time_44',
            test=lambda: datetime(**rail.result('log_effective_date_42')) < datetime(**get_replicon_date(datetime.utcnow(
            ))) or datetime(**rail.result('log_effective_date_42')) == datetime(**get_replicon_date(datetime.utcnow())),
            yes_task="dtna_datamart_cost_center_table_eng_user_add_entry_45",
            no_task="if_effectivedate_day_blank_46",
        )

        dtna_datamart_cost_center_table_eng_user_add_entry_45 = rail.WriteLogOperator(
            task_id='dtna_datamart_cost_center_table_eng_user_add_entry_45',
            log="{{ result('create_log') }}",
            message="na",
            severity="Success",
            properties=lambda: {
                "login_name": rail.get_dag_run_conf()['LoginName'],
                "index": rail.result('foreach_response_41', 'index'),
                "cost_center_name": rail.result('foreach_response_41')['costCenter']['displayText'],
                "effective_date": datetime(**rail.result('foreach_response_41')['effectiveDate']).strftime('%m/%d/%Y'),
            }
        )

        if_effectivedate_day_blank_46 = rail.IfOperator(
            task_id='if_effectivedate_day_blank_46',
            test='''{{ result('foreach_response_41').effectiveDate | is_falsy }}''',
            yes_task="dtna_datamart_cost_center_table_eng_user_add_entry_47",
            no_task="foreach_response_41_end",
        )

        dtna_datamart_cost_center_table_eng_user_add_entry_47 = rail.WriteLogOperator(
            task_id='dtna_datamart_cost_center_table_eng_user_add_entry_47',
            log="{{ result('create_log') }}",
            message="na",
            severity="Success",
            properties=lambda: {
                "login_name": rail.get_dag_run_conf()['LoginName'],
                "index": rail.result('foreach_response_41', 'index'),
                "cost_center_name": rail.result('foreach_response_41')['costCenter']['displayText'],
                "effective_date": datetime(**rail.result('log_effective_date_42')['effectiveDate']).strftime('%m/%d/%Y'),
            }
        )

        foreach_response_41_end = rail.EmptyOperator(
            task_id='foreach_response_41_end',
        )

        get_current_cost_center_data_for_user_49 = rail.RepliconServiceOperator(
            task_id='get_current_cost_center_data_for_user_49',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:user-list-column:cost-center"
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
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ result('get_user_details_30').uri }}",
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
            }
        )

        if_first_textvalue_present_52 = rail.IfOperator(
            task_id='if_first_textvalue_present_52',
            test='''{{ result('get_current_cost_center_data_for_user_49').rows | is_truthy }}''',
            yes_task="log_get_current_active_cost_centerfortheuser_53",
            no_task="foreach_query_list_28_138",
        )

        log_get_current_active_cost_centerfortheuser_53 = rail.PythonOperator(
            task_id='log_get_current_active_cost_centerfortheuser_53',
            python_callable=lambda:  rail.result('get_current_cost_center_data_for_user_49')[
                'rows'][0]['cells'][0]['cellCollection'][-1]['textValue']
        )

        log_54 = rail.PythonOperator(
            task_id='log_54',
            python_callable=lambda:  rail.result('get_current_cost_center_data_for_user_49')[
                'rows'][0]['cells'][0]['cellCollection'][-1]['uri']
        )

        get_current_cost_center_data_56 = rail.RepliconServiceOperator(
            task_id='get_current_cost_center_data_56',
            endpoint="/services/CostCenterService1.svc/GetCostCenterDetails",
            data={
                "costCenterUri": "{{ result('log_54') }}"
            }
        )

        if_parent_displaytext_equals_to_dtnaeng_59 = rail.IfOperator(
            task_id='if_parent_displaytext_equals_to_dtnaeng_59',
            test='''{{ result('get_current_cost_center_data_56').parent.displayText == 'DTNA ENG' }}''',
            yes_task="log_checkifthecurrentcostcenterispresetmultipletimes_60",
            no_task="foreach_query_list_28_138",
        )

        log_checkifthecurrentcostcenterispresetmultipletimes_60 = rail.PythonOperator(
            task_id='log_checkifthecurrentcostcenterispresetmultipletimes_60',
            python_callable=lambda:  list(filter(lambda x: x['CostCenter'] == rail.result(
                'log_get_current_active_cost_centerfortheuser_53'), rail.load_all_records(rail.result('query_list_28'))))
        )

        parse_json_61 = rail.PythonOperator(
            task_id='parse_json_61',
            python_callable=lambda: rail.result(
                'log_checkifthecurrentcostcenterispresetmultipletimes_60')
        )

        log_length_62 = rail.PythonOperator(
            task_id='log_length_62',
            python_callable=lambda:  len(rail.result('parse_json_61'))
        )

        if_log_length_62_greater_than_1_63 = rail.IfOperator(
            task_id='if_log_length_62_greater_than_1_63',
            test='''{{ result('log_length_62') > 1 }}''',
            yes_task="foreach_document_64",
            no_task="if_log_length_62_less_than_2_67",
        )

        foreach_document_64 = rail.ForEachOperator(
            task_id='foreach_document_64',
            items="{{ result('parse_json_61') | to_json }}",
            start_task='if_foreach_document_64_costcentereffectivedate_blank_65',
            end_task='foreach_document_64_end'
        )

        if_foreach_document_64_costcentereffectivedate_blank_65 = rail.IfOperator(
            task_id='if_foreach_document_64_costcentereffectivedate_blank_65',
            test='''{{ result('foreach_document_64').CostCenterEffectiveDate | is_falsy }}''',
            yes_task="insert_to_list_66",
            no_task="foreach_document_64_end",
        )

        insert_to_list_66 = rail.SetVariableOperator(
            task_id='insert_to_list_66',
            append=True,
            name='{{ result("declare_list_24").name }}',
            value={
                "repliconworkerid": "{{ result('foreach_document_64').RepliconWorkerID }}",
                "hiringmanagerid": "{{ result('foreach_document_64').HiringManagerID }}",
                "costcenter": "{{ result('foreach_document_64').CostCenter }}",
                "costcentereffectivedate": "{{ result('foreach_document_64').CostCenterEffectiveDate }}",
                "activedate": "{{ result('foreach_document_64').ActiveDate }}",
                "terminationdate": "{{ result('foreach_document_64').TerminationDate }}",
                "status": "Active",
                "loginname": "{{ result('foreach_document_64').LoginName }}",
                "clientworkerid": "{{ result('foreach_document_64').ClientWorkerID }}",
                "workertype": "{{ result('foreach_document_64').WorkerType }}",
                "firstname": "{{ result('foreach_document_64').WorkerFirstName }}",
                "lastname": "{{ result('foreach_document_64').WorkerLastName }}",
                "email": "{{ result('foreach_document_64').email }}",
                "approverid": "{{ result('get_user_details_30').supervisor.user.loginName if result('get_user_details_30').supervisor | is_truthy else '' }}",
                "initialseng": "{{ result('foreach_document_64').InitialsENG }}",
                "managereng": "{{ result('foreach_document_64').ManagerENG }}"
            }
        )

        foreach_document_64_end = rail.EmptyOperator(
            task_id='foreach_document_64_end',
        )

        if_log_length_62_less_than_2_67 = rail.IfOperator(
            task_id='if_log_length_62_less_than_2_67',
            test='''{{ result('log_length_62') < 2 }}''',
            yes_task="insert_to_list_68",
            no_task="log_checkifthecurrentcostcenterispresetmultipletimes_69",
        )

        insert_to_list_68 = rail.SetVariableOperator(
            task_id='insert_to_list_68',
            append=True,
            name='{{ result("declare_list_24").name }}',
            value={
                "repliconworkerid": "{{ result('parse_json_61')[0].RepliconWorkerID }}",
                "hiringmanagerid": "{{ result('parse_json_61')[0].HiringManagerID }}",
                "costcenter": "{{ result('parse_json_61')[0].CostCenter }}",
                "costcentereffectivedate": "{{ result('parse_json_61')[0].CostCenterEffectiveDate }}",
                "activedate": "{{ result('parse_json_61')[0].ActiveDate }}",
                "terminationdate": "{{ result('parse_json_61')[0].TerminationDate }}",
                "status":  "{{  'Inactive' if result('parse_json_61')[0].TerminationDate | is_truthy else 'Active' }}",
                "loginname": "{{ result('parse_json_61')[0].LoginName }}",
                "clientworkerid": "{{ result('parse_json_61')[0].ClientWorkerID }}",
                "workertype": "{{ result('parse_json_61')[0].WorkerType }}",
                "firstname": "{{ result('parse_json_61')[0].WorkerFirstName }}",
                "lastname": "{{ result('parse_json_61')[0].WorkerLastName }}",
                "email": "{{ result('parse_json_61')[0].email }}",
                "approverid": "{{ result('get_user_details_30').supervisor.user.loginName if result('get_user_details_30').supervisor | is_truthy else '' }}",
                "initialseng": "{{ result('parse_json_61')[0].InitialsENG }}",
                "managereng": "{{ result('parse_json_61')[0].ManagerENG }}"
            }
        )

        log_checkifthecurrentcostcenterispresetmultipletimes_69 = rail.PythonOperator(
            task_id='log_checkifthecurrentcostcenterispresetmultipletimes_69',
            python_callable=lambda:  list(
                map(lambda x: x['CostCenterEffectiveDate'], rail.result('parse_json_61')))
        )

        log_checkifthecurrentcostcenterispresetmultipletimes_70 = rail.PythonOperator(
            task_id='log_checkifthecurrentcostcenterispresetmultipletimes_70',
            python_callable=lambda:   list(
                map(lambda x: x['CostCenter'], rail.result('parse_json_61')))
        )

        if_log_length_62_greater_than_1_71 = rail.IfOperator(
            task_id='if_log_length_62_greater_than_1_71',
            test='''{{ result('log_length_62') > 1 }}''',
            yes_task="foreach_create_list_72_73",
            no_task="foreach_query_list_28_138",
        )

        foreach_create_list_72_73 = rail.ForEachOperator(
            task_id='foreach_create_list_72_73',
            items="{{ result('parse_json_61') | to_json }}",
            start_task='log_today_date_74',
            end_task='foreach_create_list_72_73_end'
        )

        log_today_date_74 = rail.PythonOperator(
            task_id='log_today_date_74',
            python_callable=lambda:  get_replicon_date(datetime.utcnow())
        )

        log_80 = rail.PythonOperator(
            task_id='log_80',
            python_callable=lambda: rail.result('log_checkifthecurrentcostcenterispresetmultipletimes_69')[
                rail.result('foreach_create_list_72_73', 'index')]
        )

        if_log_80_present_81 = rail.IfOperator(
            task_id='if_log_80_present_81',
            test='''{{ result('log_80') | is_truthy }}''',
            yes_task="log_86",
            no_task="foreach_create_list_72_73_end",
        )

        log_86 = rail.PythonOperator(
            task_id='log_86',
            python_callable=lambda:  rail.parse_date(
                rail.result('log_80'), "%m/%d/%Y")
        )

        log_findthedaydiff_87 = rail.PythonOperator(
            task_id='log_findthedaydiff_87',
            python_callable=lambda: int(
                (datetime(**rail.result('log_today_date_74')) - datetime(**rail.result('log_86'))).days)
        )

        log_88 = rail.PythonOperator(
            task_id='log_88',
            python_callable=lambda: rail.result('log_checkifthecurrentcostcenterispresetmultipletimes_70')[
                rail.result('foreach_create_list_72_73', 'index')]
        )

        accumulate_list_items_89 = rail.SetVariableOperator(
            task_id='accumulate_list_items_89',
            name='Day diff finder',
            append=True,
            value={
                "loginname": "{{ result('parse_json_61')[0].LoginName }}",
                "costcenter": "{{ result('log_88') }}",
                "costcentereffectivedate": "{{ result('log_80') }}",
                "daydiff": "{{ result('log_findthedaydiff_87') }}",
            }
        )

        foreach_create_list_72_73_end = rail.EmptyOperator(
            task_id='foreach_create_list_72_73_end',
        )

        log_findthesmallestdaydiff_90 = rail.PythonOperator(
            task_id='log_findthesmallestdaydiff_90',
            python_callable=lambda:  min(list(map(lambda x: int(x['daydiff']), filter(lambda x: x['loginname'] == rail.result(
                'parse_json_61')[0]['LoginName'], rail.get_dag_run_var('Day diff finder')))))
        )

        log_gettherequiredeffectivedate_91 = rail.PythonOperator(
            task_id='log_gettherequiredeffectivedate_91',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var(
                'Day diff finder'), 'daydiff', str(rail.result('log_findthesmallestdaydiff_90')), 'costcentereffectivedate')
        )

        if_log_gettherequiredeffectivedate_91_present_2_92 = rail.IfOperator(
            task_id='if_log_gettherequiredeffectivedate_91_present_2_92',
            test='''{{ result('log_gettherequiredeffectivedate_91') | is_truthy }}''',
            yes_task="insert_to_list_93",
            no_task="foreach_accumulate_list_items_89_94_end",
        )

        insert_to_list_93 = rail.SetVariableOperator(
            task_id='insert_to_list_93',
            append=True,
            name='{{ result("declare_list_24").name }}',
            value={
                "repliconworkerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_91'),'RepliconWorkerID') }}",
                "hiringmanagerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate',result('log_gettherequiredeffectivedate_91'),'HiringManagerID') }}",
                "costcenter": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate',result('log_gettherequiredeffectivedate_91'),'CostCenter') }}",
                "costcentereffectivedate": "{{ result('log_gettherequiredeffectivedate_91') }}",
                "activedate": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate',result('log_gettherequiredeffectivedate_91'),'ActiveDate') }}",
                "terminationdate": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate',result('log_gettherequiredeffectivedate_91'),'TerminationDate') }}",
                "status": "{{ 'Inactive' if result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate',result('log_gettherequiredeffectivedate_91'),'TerminationDate') | is_truthy else 'Active' }}",
                "loginname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate',result('log_gettherequiredeffectivedate_91'),'LoginName') }}",
                "clientworkerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate',result('log_gettherequiredeffectivedate_91'),'ClientWorkerID') }}",
                "workertype": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate',result('log_gettherequiredeffectivedate_91'),'WorkerType') }}",
                "firstname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate',result('log_gettherequiredeffectivedate_91'),'WorkerFirstName') }}",
                "lastname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate',result('log_gettherequiredeffectivedate_91'),'WorkerLastName') }}",
                "email": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate',result('log_gettherequiredeffectivedate_91'),'email') }}",
                "approverid": "{{ result('get_user_details_30').supervisor.user.loginName if result('get_user_details_30').supervisor | is_truthy else '' }}",
                "initialseng": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate',result('log_gettherequiredeffectivedate_91'),'InitialsENG') }}",
                "managereng": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate',result('log_gettherequiredeffectivedate_91'),'ManagerENG') }}"
            }
        )

        foreach_accumulate_list_items_89_94 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_89_94',
            items="{{ result('accumulate_list_items_89').value | to_json }}",
            start_task='if_log_findthesmallestdaydiff_90_not_equals_to_dataforeach12252e04daydiff_95',
            end_task='foreach_accumulate_list_items_89_94_end'
        )

        if_log_findthesmallestdaydiff_90_not_equals_to_dataforeach12252e04daydiff_95 = rail.IfOperator(
            task_id='if_log_findthesmallestdaydiff_90_not_equals_to_dataforeach12252e04daydiff_95',
            test='''{{ result('log_findthesmallestdaydiff_90') != result('foreach_accumulate_list_items_89_94').daydiff  and result('log_gettherequiredeffectivedate_91') != result('foreach_accumulate_list_items_89_94').costcentereffectivedate  and result('foreach_accumulate_list_items_89_94').loginname == result('parse_json_61')[0].LoginName }}''',
            yes_task="log_gettherequiredeffectivedate_99",
            no_task="foreach_accumulate_list_items_89_94_end",
        )

        log_gettherequiredeffectivedate_99 = rail.PythonOperator(
            task_id='log_gettherequiredeffectivedate_99',
            python_callable=lambda:  rail.result('foreach_accumulate_list_items_89_94')[
                'costcentereffectivedate']
        )

        def get_terminated_date():
            logs = list(map(lambda x: x['properties'], rail.load_all_records(
                rail.result('create_log'))))
            item = next(iter(filter(lambda x: x.get("login_name") == rail.result('foreach_accumulate_list_items_89_94')['loginname'] and x["cost_center_name"] == rail.result(
                'foreach_accumulate_list_items_89_94')['costcenter'] and x["effective_date"] == rail.result('log_gettherequiredeffectivedate_99'), logs)), None)
            if item:
                next_item = next(iter(filter(lambda x: x.get("login_name") == rail.result('foreach_accumulate_list_items_89_94')[
                                 'loginname'] and str(x.get("index")) == str(logs.index(item)+1), logs)), None)
                if next_item and next_item['effective_date']:
                    return (datetime(**rail.parse_date(next_item['effective_date'], '%m/%d/%Y'))-timedelta(days=1)).strftime('%m/%d/%Y')
            return ''

        log_103 = rail.PythonOperator(
            task_id='log_103',
            python_callable=get_terminated_date
        )

        log_104 = rail.PythonOperator(
            task_id='log_104',
            python_callable=lambda:  list(filter(lambda x: x['Login_Name'] == rail.result('load_all_query_data_28')[
                                          0]['LoginName'] and x['Manager___ENG'] != rail.result('load_all_query_data_28')[0]['ManagerENG'], rail.load_all_records(rail.get_dag_run_conf()['report_manager_eng_collection'])))
        )

        parse_json_105 = rail.PythonOperator(
            task_id='parse_json_105',
            python_callable=lambda: rail.result('log_104')
        )

        log_106 = rail.PythonOperator(
            task_id='log_106',
            python_callable=lambda:  len(rail.result('parse_json_105'))
        )

        if_log_106_less_than_1_107 = rail.IfOperator(
            task_id='if_log_106_less_than_1_107',
            test='''{{ result('log_106') < 1 }}''',
            yes_task="insert_to_list_108",
            no_task="if_log_106_equals_to_1_109",
        )

        insert_to_list_108 = rail.SetVariableOperator(
            task_id='insert_to_list_108',
            append=True,
            name='{{ result("declare_list_24").name }}',
            value={
                "repliconworkerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'RepliconWorkerID') }}",
                "hiringmanagerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'HiringManagerID') }}",
                "costcenter": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'CostCenter') }}",
                "costcentereffectivedate": "{{ result('log_gettherequiredeffectivedate_99') }}",
                "activedate": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'ActiveDate') }}",
                "terminationdate": "{{ result('log_103') }}",
                "status": "Inactive",
                "loginname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'LoginName') }}",
                "clientworkerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'ClientWorkerID') }}",
                "workertype": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'WorkerType') }}",
                "firstname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'WorkerFirstName') }}",
                "lastname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'WorkerLastName') }}",
                "email": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'email') }}",
                "approverid": "{{  result('get_user_details_30').supervisor.user.loginName if result('get_user_details_30').supervisor | is_truthy else '' }}",
                "initialseng": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'InitialsENG') }}",
                "managereng": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'ManagerENG') }}"
            }
        )

        if_log_106_equals_to_1_109 = rail.IfOperator(
            task_id='if_log_106_equals_to_1_109',
            test='''{{ result('log_106') == 1 }}''',
            yes_task="log_110",
            no_task="if_log_106_greater_than_1_112",
        )

        log_110 = rail.PythonOperator(
            task_id='log_110',
            python_callable=lambda:  rail.result('parse_json_105')[
                0]['Manager___ENG']
        )

        insert_to_list_111 = rail.SetVariableOperator(
            task_id='insert_to_list_111',
            append=True,
            name='{{ result("declare_list_24").name }}',
            value={
                "repliconworkerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'RepliconWorkerID') }}",
                "hiringmanagerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'HiringManagerID') }}",
                "costcenter": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'CostCenter') }}",
                "costcentereffectivedate": "{{ result('log_gettherequiredeffectivedate_99') }}",
                "activedate": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'ActiveDate') }}",
                "terminationdate": "{{ result('log_103') }}",
                "status": "Inactive",
                "loginname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'LoginName') }}",
                "clientworkerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'ClientWorkerID') }}",
                "workertype": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'WorkerType') }}",
                "firstname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'WorkerFirstName') }}",
                "lastname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'WorkerLastName') }}",
                "email": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'email') }}",
                "approverid": "{{  result('get_user_details_30').supervisor.user.loginName if result('get_user_details_30').supervisor | is_truthy else '' }}",
                "initialseng": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'InitialsENG') }}",
                "managereng": "{{ result('log_110') }}"
            }
        )

        if_log_106_greater_than_1_112 = rail.IfOperator(
            task_id='if_log_106_greater_than_1_112',
            test='''{{ result('log_106') > 1 }}''',
            yes_task="log_113",
            no_task="foreach_accumulate_list_items_89_94_end",
        )

        log_113 = rail.PythonOperator(
            task_id='log_113',
            python_callable=lambda:  "".join(list(map(lambda x: x['Manager___ENG'],
                                                      filter(lambda x: x['Manager___ENG'] and x['Login_Name'] == rail.result('parse_json_61')[
                                                             0]['LoginName'] and x['Manager___ENG_Effective_Date'] == rail.result('log_gettherequiredeffectivedate_99'),
                                                      rail.result('parse_json_105')))))
        )

        if_log_113_blank_114 = rail.IfOperator(
            task_id='if_log_113_blank_114',
            test='''{{ result('log_113') | is_falsy }}''',
            yes_task="log_115",
            no_task="if_log_113_present_136",
        )

        log_115 = rail.PythonOperator(
            task_id='log_115',
            python_callable=lambda:  "".join(list(map(lambda x: x['HiringManagerID'],
                                                      filter(lambda x: x['HiringManagerID'] and x['LoginName'] == rail.result('parse_json_61')[
                                                             0]['LoginName'] and not bool(x['ActiveDate']),
                                                      rail.result('parse_json_61')))))
        )

        if_log_115_present_116 = rail.IfOperator(
            task_id='if_log_115_present_116',
            test='''{{ result('log_115') | is_truthy }}''',
            yes_task="insert_to_list_117",
            no_task="if_log_115_blank_118",
        )

        insert_to_list_117 = rail.SetVariableOperator(
            task_id='insert_to_list_117',
            append=True,
            name='{{ result("declare_list_24").name }}',
            value={
                "repliconworkerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'RepliconWorkerID') }}",
                "hiringmanagerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'HiringManagerID') }}",
                "costcenter": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'CostCenter') }}",
                "costcentereffectivedate": "{{ result('log_gettherequiredeffectivedate_99') }}",
                "activedate": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'ActiveDate') }}",
                "terminationdate": "{{ result('log_103') }}",
                "status": "Inactive",
                "loginname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'LoginName') }}",
                "clientworkerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'ClientWorkerID') }}",
                "workertype": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'WorkerType') }}",
                "firstname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'WorkerFirstName') }}",
                "lastname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'WorkerLastName') }}",
                "email": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'email') }}",
                "approverid": "{{  result('get_user_details_30').supervisor.user.loginName if result('get_user_details_30').supervisor | is_truthy else '' }}",
                "initialseng": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'InitialsENG') }}",
                "managereng": "{{ result('log_115') }}"
            }
        )

        if_log_115_blank_118 = rail.IfOperator(
            task_id='if_log_115_blank_118',
            test='''{{ result('log_115') | is_falsy }}''',
            yes_task="foreach_document_119",
            no_task="if_log_113_present_136",
        )

        foreach_document_119 = rail.ForEachOperator(
            task_id='foreach_document_119',
            items="{{ result('parse_json_105') | to_json }}",
            start_task='if_foreach_document_119_column_2_present_120',
            end_task='foreach_document_119_end'
        )

        if_foreach_document_119_column_2_present_120 = rail.IfOperator(
            task_id='if_foreach_document_119_column_2_present_120',
            test='''{{ result('foreach_document_119')['Manager___ENG'] | is_truthy  and result('log_gettherequiredeffectivedate_99') | is_truthy }}''',
            yes_task="log_124",
            no_task="foreach_document_119_end",
        )

        log_124 = rail.PythonOperator(
            task_id='log_124',
            python_callable=lambda:  rail.result(
                'log_gettherequiredeffectivedate_99')
        )

        log_129 = rail.PythonOperator(
            task_id='log_129',
            python_callable=lambda:  rail.result('foreach_document_119')[
                'Manager___ENG_Effective_Date']
        )

        log_findthedaydiff_131 = rail.PythonOperator(
            task_id='log_findthedaydiff_131',
            python_callable=lambda: (datetime(**rail.parse_date(rail.result(
                'log_124'), '%m/%d/%Y')) - datetime(**rail.parse_date(rail.result('log_129'), '%m/%d/%Y'))).days
        )

        accumulate_list_items_132 = rail.SetVariableOperator(
            task_id='accumulate_list_items_132',
            name='Day diff finder',
            append=True,
            value={
                "loginname": "{{ result('foreach_document_119')['Login_Name'] }}",
                "mangereng": "{{ result('foreach_document_119')['Manager___ENG'] }}",
                "effectivedate": "{{ result('foreach_document_119')['Manager___ENG_Effective_Date'] }}",
                "costcenter": "{{ result('foreach_accumulate_list_items_89_94').costcenter }}",
                "costcentereffectivedate": "{{ result('foreach_accumulate_list_items_89_94').costcentereffectivedate }}",
                "daydiff": "{{ result('log_findthedaydiff_131') }}"
            }
        )

        foreach_document_119_end = rail.EmptyOperator(
            task_id='foreach_document_119_end',
        )

        log_findthesmallestdaydiff_133 = rail.PythonOperator(
            task_id='log_findthesmallestdaydiff_133',
            python_callable=lambda:  str(min(map(lambda x: int(x['daydiff']), filter(lambda x: x['loginname'] == rail.result(
                'parse_json_61')[0]['LoginName'], rail.result('accumulate_list_items_132')['value']))))

        )

        log_findthemanagerengforthesmallestdaydiff_134 = rail.PythonOperator(
            task_id='log_findthemanagerengforthesmallestdaydiff_134',
            python_callable=lambda:  "".join(map(lambda x: x['mangereng'], filter(lambda x: x.get('mangereng') and x['daydiff'] == rail.result('log_findthesmallestdaydiff_133') and x['loginname'] == rail.result(
                'parse_json_61')[0]['LoginName'], rail.result('accumulate_list_items_132')['value'])))

        )

        insert_to_list_135 = rail.SetVariableOperator(
            task_id='insert_to_list_135',
            append=True,
            name='{{ result("declare_list_24").name }}',
            value={
                "repliconworkerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'RepliconWorkerID') }}",
                "hiringmanagerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'HiringManagerID') }}",
                "costcenter": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'CostCenter') }}",
                "costcentereffectivedate": "{{ result('log_gettherequiredeffectivedate_99') }}",
                "activedate": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'ActiveDate') }}",
                "terminationdate": "{{ result('log_103') }}",
                "status": "Inactive",
                "loginname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'LoginName') }}",
                "clientworkerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'ClientWorkerID') }}",
                "workertype": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'WorkerType') }}",
                "firstname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'WorkerFirstName') }}",
                "lastname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'WorkerLastName') }}",
                "email": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'email') }}",
                "approverid": "{{  result('get_user_details_30').supervisor.user.loginName if result('get_user_details_30').supervisor | is_truthy else '' }}",
                "initialseng": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'InitialsENG') }}",
                "managereng": "{{ result('log_findthemanagerengforthesmallestdaydiff_134') | sn }}"
            }
        )

        if_log_113_present_136 = rail.IfOperator(
            task_id='if_log_113_present_136',
            test='''{{ result('log_113') | is_truthy }}''',
            yes_task="insert_to_list_137",
            no_task="foreach_accumulate_list_items_89_94_end",
        )

        insert_to_list_137 = rail.SetVariableOperator(
            task_id='insert_to_list_137',
            append=True,
            name='{{ result("declare_list_24").name }}',
            value={
                "repliconworkerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'RepliconWorkerID') }}",
                "hiringmanagerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'HiringManagerID') }}",
                "costcenter": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'CostCenter') }}",
                "costcentereffectivedate": "{{ result('log_gettherequiredeffectivedate_99') }}",
                "activedate": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'ActiveDate') }}",
                "terminationdate": "{{ result('log_103') }}",
                "status": "Inactive",
                "loginname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'LoginName') }}",
                "clientworkerid": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'ClientWorkerID') }}",
                "workertype": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'WorkerType') }}",
                "firstname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'WorkerFirstName') }}",
                "lastname": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'WorkerLastName') }}",
                "email": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'email') }}",
                "approverid": "{{  result('get_user_details_30').supervisor.user.loginName if result('get_user_details_30').supervisor | is_truthy else '' }}",
                "initialseng": "{{ result('parse_json_61') | find_first_by_attr_and_get_attr('CostCenterEffectiveDate', result('log_gettherequiredeffectivedate_99'),'InitialsENG') }}",
                "managereng": "{{ result('log_113') }}"
            }
        )

        foreach_accumulate_list_items_89_94_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_89_94_end',
        )

        foreach_query_list_28_138 = rail.ForEachOperator(
            task_id='foreach_query_list_28_138',
            items="{{ result('query_list_28') }}",
            start_task='if_log_2_not_equals_to_dataforeachforeach_1costcenter_139',
            end_task='foreach_query_list_28_138_end'
        )

        if_log_2_not_equals_to_dataforeachforeach_1costcenter_139 = rail.IfOperator(
            task_id='if_log_2_not_equals_to_dataforeachforeach_1costcenter_139',
            test='''{{ result('log_get_current_active_cost_centerfortheuser_53') != result('foreach_query_list_28_138').CostCenter }}''',
            yes_task="log_143",
            no_task="foreach_query_list_28_138_end",
        )

        log_143 = rail.PythonOperator(
            task_id='log_143',
            python_callable=lambda: rail.result('foreach_query_list_28_138')[
                'CostCenterEffectiveDate']
        )

        def get_terminated_date2():
            logs = list(map(lambda x: x['properties'], rail.load_all_records(
                rail.result('create_log'))))
            item = next(iter(filter(lambda x: x.get("login_name") == rail.result('foreach_query_list_28_138')['LoginName'] and x["cost_center_name"] == rail.result(
                'foreach_query_list_28_138')['CostCenter'] and x["effective_date"] == rail.result('log_143'), logs)), None)
            if item:
                next_item = next(iter(filter(lambda x: x.get("login_name") == rail.result('foreach_query_list_28_138')[
                                 'LoginName'] and str(x.get("index", 0)) == str(logs.index(item)+1), logs)), None)
                if next_item and next_item['effective_date']:
                    return (datetime(**rail.parse_date(next_item['effective_date'], '%m/%d/%Y'))-timedelta(days=1)).strftime('%m/%d/%Y')
            return ''

        log_147 = rail.PythonOperator(
            task_id='log_147',
            python_callable=get_terminated_date2

        )

        log_148 = rail.PythonOperator(
            task_id='log_148',
            python_callable=lambda:  list(filter(lambda x: x['Login_Name'] == rail.result('foreach_query_list_28_138')[
                                          'LoginName'] and x['Manager___ENG'] != rail.result('foreach_query_list_28_138')['ManagerENG'], rail.load_all_records(rail.get_dag_run_conf()['report_manager_eng_collection'])))

        )

        parse_json_149 = rail.PythonOperator(
            task_id='parse_json_149',
            python_callable=lambda: rail.result('log_148')
        )

        log_150 = rail.PythonOperator(
            task_id='log_150',
            python_callable=lambda:  len(rail.result('parse_json_149'))
        )

        if_log_8_less_than_1_151 = rail.IfOperator(
            task_id='if_log_8_less_than_1_151',
            test='''{{ result('log_150') < 1 }}''',
            yes_task="log_152",
            no_task="if_log_8_equals_to_1_154",
        )

        log_152 = rail.PythonOperator(
            task_id='log_152',
            python_callable=lambda:  rail.result(
                'foreach_query_list_28_138')['ManagerENG']
        )

        insert_to_list_153 = rail.SetVariableOperator(
            task_id='insert_to_list_153',
            append=True,
            name='{{ result("declare_list_24").name }}',
            value={
                "repliconworkerid": "{{ result('foreach_query_list_28_138').RepliconWorkerID }}",
                "hiringmanagerid": "{{ result('foreach_query_list_28_138').HiringManagerID }}",
                "costcenter": "{{ result('foreach_query_list_28_138').CostCenter }}",
                "costcentereffectivedate": "{{ result('foreach_query_list_28_138').CostCenterEffectiveDate }}",
                "activedate": "{{ result('foreach_query_list_28_138').ActiveDate }}",
                "terminationdate": "{{ result('log_147') }}",
                "status": "Inactive",
                "loginname": "{{ result('foreach_query_list_28_138').LoginName }}",
                "clientworkerid": "{{ result('foreach_query_list_28_138').ClientWorkerID }}",
                "workertype": "{{ result('foreach_query_list_28_138').WorkerType }}",
                "firstname": "{{ result('foreach_query_list_28_138').WorkerFirstName }}",
                "lastname": "{{ result('foreach_query_list_28_138').WorkerLastName }}",
                "email": "{{ result('foreach_query_list_28_138').email }}",
                "approverid": "{{  result('get_user_details_30').supervisor.user.loginName if result('get_user_details_30').supervisor | is_truthy else '' }}",
                "initialseng": "{{ result('foreach_query_list_28_138').InitialsENG }}",
                "managereng": "{{ result('foreach_query_list_28_138').ManagerENG }}"
            }
        )

        if_log_8_equals_to_1_154 = rail.IfOperator(
            task_id='if_log_8_equals_to_1_154',
            test='''{{ result('log_150') == 1 }}''',
            yes_task="log_155",
            no_task="if_log_8_greater_than_1_157",
        )

        log_155 = rail.PythonOperator(
            task_id='log_155',
            python_callable=lambda:  rail.result('parse_json_149')[
                0]['Manager___ENG']
        )

        insert_to_list_156 = rail.SetVariableOperator(
            task_id='insert_to_list_156',
            append=True,
            name='{{ result("declare_list_24").name }}',
            value={
                "repliconworkerid": "{{ result('foreach_query_list_28_138').RepliconWorkerID }}",
                "hiringmanagerid": "{{ result('foreach_query_list_28_138').HiringManagerID }}",
                "costcenter": "{{ result('foreach_query_list_28_138').CostCenter }}",
                "costcentereffectivedate": "{{ result('foreach_query_list_28_138').CostCenterEffectiveDate }}",
                "activedate": "{{ result('foreach_query_list_28_138').ActiveDate }}",
                "terminationdate": "{{ result('log_147') }}",
                "status": "Inactive",
                "loginname": "{{ result('foreach_query_list_28_138').LoginName }}",
                "clientworkerid": "{{ result('foreach_query_list_28_138').ClientWorkerID }}",
                "workertype": "{{ result('foreach_query_list_28_138').WorkerType }}",
                "firstname": "{{ result('foreach_query_list_28_138').WorkerFirstName }}",
                "lastname": "{{ result('foreach_query_list_28_138').WorkerLastName }}",
                "email": "{{ result('foreach_query_list_28_138').email }}",
                "approverid": "{{  result('get_user_details_30').supervisor.user.loginName if result('get_user_details_30').supervisor | is_truthy else '' }}",
                "initialseng": "{{ result('foreach_query_list_28_138').InitialsENG }}",
                "managereng": "{{ result('log_155') }}"
            }
        )

        if_log_8_greater_than_1_157 = rail.IfOperator(
            task_id='if_log_8_greater_than_1_157',
            test='''{{ result('log_150') > 1 }}''',
            yes_task="log_158",
            no_task="foreach_query_list_28_138_end",
        )

        log_158 = rail.PythonOperator(
            task_id='log_158',
            python_callable=lambda:  "".join(map(lambda x: x['Manager___ENG'], filter(lambda x: x['Manager___ENG'] and x['Login_Name'] == rail.result('foreach_query_list_28_138')[
                                             'LoginName'] and x['Manager___ENG_Effective_Date'] == rail.result('foreach_query_list_28_138')['CostCenterEffectiveDate'], rail.result('parse_json_149'))))

        )

        if_log_13_blank_159 = rail.IfOperator(
            task_id='if_log_13_blank_159',
            test='''{{ result('log_158') | is_falsy }}''',
            yes_task="log_160",
            no_task="if_log_13_present_181",
        )

        log_160 = rail.PythonOperator(
            task_id='log_160',
            python_callable=lambda:  "".join(map(lambda x: x['Manager___ENG'], filter(lambda x: x['Manager___ENG'] and x['Login_Name'] == rail.result(
                'foreach_query_list_28_138')['LoginName'] and not x['Manager___ENG_Effective_Date'], rail.result('parse_json_149'))))
        )

        if_log_16_present_161 = rail.IfOperator(
            task_id='if_log_16_present_161',
            test='''{{ result('log_160') | is_truthy }}''',
            yes_task="insert_to_list_162",
            no_task="if_log_16_blank_163",
        )

        insert_to_list_162 = rail.SetVariableOperator(
            task_id='insert_to_list_162',
            append=True,
            name='{{ result("declare_list_24").name }}',
            value={
                "repliconworkerid": "{{ result('foreach_query_list_28_138').RepliconWorkerID }}",
                "hiringmanagerid": "{{ result('foreach_query_list_28_138').HiringManagerID }}",
                "costcenter": "{{ result('foreach_query_list_28_138').CostCenter }}",
                "costcentereffectivedate": "{{ result('foreach_query_list_28_138').CostCenterEffectiveDate }}",
                "activedate": "{{ result('foreach_query_list_28_138').ActiveDate }}",
                "terminationdate": "{{ result('log_147') }}",
                "status": "Inactive",
                "loginname": "{{ result('foreach_query_list_28_138').LoginName }}",
                "clientworkerid": "{{ result('foreach_query_list_28_138').ClientWorkerID }}",
                "workertype": "{{ result('foreach_query_list_28_138').WorkerType }}",
                "firstname": "{{ result('foreach_query_list_28_138').WorkerFirstName }}",
                "lastname": "{{ result('foreach_query_list_28_138').WorkerLastName }}",
                "email": "{{ result('foreach_query_list_28_138').email }}",
                "approverid": "{{  result('get_user_details_30').supervisor.user.loginName if result('get_user_details_30').supervisor | is_truthy else '' }}",
                "initialseng": "{{ result('foreach_query_list_28_138').InitialsENG }}",
                "managereng": "{{ result('log_160') }}"
            }
        )

        if_log_16_blank_163 = rail.IfOperator(
            task_id='if_log_16_blank_163',
            test='''{{ result('log_160') | is_falsy }}''',
            yes_task="foreach_document_164",
            no_task="if_log_13_present_181",
        )

        foreach_document_164 = rail.ForEachOperator(
            task_id='foreach_document_164',
            items="{{ result('parse_json_149') | to_json }}",
            start_task='if_foreach_document_164_column_2_present_165',
            end_task='foreach_document_164_end'
        )

        if_foreach_document_164_column_2_present_165 = rail.IfOperator(
            task_id='if_foreach_document_164_column_2_present_165',
            test='''{{ result('foreach_document_164')['Manager___ENG_Effective_Date'] | is_truthy  and result('foreach_query_list_28_138').CostCenterEffectiveDate | is_truthy }}''',
            yes_task="log_169",
            no_task="foreach_document_164_end",
        )

        log_169 = rail.PythonOperator(
            task_id='log_169',
            python_callable=lambda: rail.result('foreach_query_list_28_138')[
                'CostCenterEffectiveDate']
        )

        log_174 = rail.PythonOperator(
            task_id='log_174',
            python_callable=lambda: rail.result('foreach_document_164')[
                'Manager___ENG_Effective_Date']
        )

        log_findthedaydiff_176 = rail.PythonOperator(
            task_id='log_findthedaydiff_176',
            python_callable=lambda:   (datetime(**rail.parse_date(rail.result(
                'log_169'), '%m/%d/%Y')) - datetime(**rail.parse_date(rail.result('log_174'), '%m/%d/%Y'))).days

        )

        accumulate_list_items_177 = rail.SetVariableOperator(
            task_id='accumulate_list_items_177',
            name='Day diff finder',
            append=True,
            value={
                "loginname": "{{ result('foreach_document_164')['Login_Name'] }}",
                "mangereng": "{{ result('foreach_document_164')['Manager___ENG'] }}",
                "effectivedate": "{{ result('foreach_document_164')['Manager___ENG_Effective_Date'] }}",
                "costcenter": "{{ result('foreach_query_list_28_138').CostCenter }}",
                "costcentereffectivedate": "{{ result('foreach_query_list_28_138').CostCenterEffectiveDate }}",
                "daydiff": "{{ result('log_findthedaydiff_176') }}"
            }
        )

        foreach_document_164_end = rail.EmptyOperator(
            task_id='foreach_document_164_end',
        )

        log_findthesmallestdaydiff_178 = rail.PythonOperator(
            task_id='log_findthesmallestdaydiff_178',
            python_callable=lambda:  str(min(map(lambda x: int(x['daydiff']), filter(lambda x: x['loginname'] == rail.result(
                'foreach_query_list_28_138')['LoginName'], rail.result('accumulate_list_items_177')['value']))))

        )

        log_findthemanagerengforthesmallestdaydiff_179 = rail.PythonOperator(
            task_id='log_findthemanagerengforthesmallestdaydiff_179',
            python_callable=lambda:  "".join(map(lambda x: x['mangereng'], filter(lambda x: x.get('mangereng') and x['daydiff'] == rail.result('log_findthesmallestdaydiff_178') and x['loginname'] == rail.result(
                'foreach_query_list_28_138')['LoginName'], rail.result('accumulate_list_items_177')['value'])))

        )

        insert_to_list_180 = rail.SetVariableOperator(
            task_id='insert_to_list_180',
            append=True,
            name='{{ result("declare_list_24").name }}',
            value={
                "repliconworkerid": "{{ result('foreach_query_list_28_138').RepliconWorkerID }}",
                "hiringmanagerid": "{{ result('foreach_query_list_28_138').HiringManagerID }}",
                "costcenter": "{{ result('foreach_query_list_28_138').CostCenter }}",
                "costcentereffectivedate": "{{ result('foreach_query_list_28_138').CostCenterEffectiveDate }}",
                "activedate": "{{ result('foreach_query_list_28_138').ActiveDate }}",
                "terminationdate": "{{ result('log_147') }}",
                "status": "Inactive",
                "loginname": "{{ result('foreach_query_list_28_138').LoginName }}",
                "clientworkerid": "{{ result('foreach_query_list_28_138').ClientWorkerID }}",
                "workertype": "{{ result('foreach_query_list_28_138').WorkerType }}",
                "firstname": "{{ result('foreach_query_list_28_138').WorkerFirstName }}",
                "lastname": "{{ result('foreach_query_list_28_138').WorkerLastName }}",
                "email": "{{ result('foreach_query_list_28_138').email }}",
                "approverid": "{{  result('get_user_details_30').supervisor.user.loginName if result('get_user_details_30').supervisor | is_truthy else '' }}",
                "initialseng": "{{ result('foreach_query_list_28_138').InitialsENG }}",
                "managereng": "{{ result('log_findthemanagerengforthesmallestdaydiff_179') }}"
            }
        )

        if_log_13_present_181 = rail.IfOperator(
            task_id='if_log_13_present_181',
            test='''{{ result('log_158') | is_truthy }}''',
            yes_task="insert_to_list_182",
            no_task="foreach_query_list_28_138_end",
        )

        insert_to_list_182 = rail.SetVariableOperator(
            task_id='insert_to_list_182',
            append=True,
            name='{{ result("declare_list_24").name }}',
            value={
                "repliconworkerid": "{{ result('foreach_query_list_28_138').RepliconWorkerID }}",
                "hiringmanagerid": "{{ result('foreach_query_list_28_138').HiringManagerID }}",
                "costcenter": "{{ result('foreach_query_list_28_138').CostCenter }}",
                "costcentereffectivedate": "{{ result('foreach_query_list_28_138').CostCenterEffectiveDate }}",
                "activedate": "{{ result('foreach_query_list_28_138').ActiveDate }}",
                "terminationdate": "{{ result('log_147') }}",
                "status": "Inactive",
                "loginname": "{{ result('foreach_query_list_28_138').LoginName }}",
                "clientworkerid": "{{ result('foreach_query_list_28_138').ClientWorkerID }}",
                "workertype": "{{ result('foreach_query_list_28_138').WorkerType }}",
                "firstname": "{{ result('foreach_query_list_28_138').WorkerFirstName }}",
                "lastname": "{{ result('foreach_query_list_28_138').WorkerLastName }}",
                "email": "{{ result('foreach_query_list_28_138').email }}",
                "approverid": "{{  result('get_user_details_30').supervisor.user.loginName if result('get_user_details_30').supervisor | is_truthy else '' }}",
                "initialseng": "{{ result('foreach_query_list_28_138').InitialsENG }}",
                "managereng": "{{ result('log_158') }}"
            }
        )

        foreach_query_list_28_138_end = rail.EmptyOperator(
            task_id='foreach_query_list_28_138_end',
        )

        final_result = rail.PythonOperator(
            task_id='final_result',
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('declare_list_24')['name'])
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        batch_task >> create_log
        batch_task >> final_result
        create_log >> declare_list_24 >> query_list_28 >> load_all_query_data_28 >> get_user_details_30 >> log_33 >> if_log_33_less_than_todayto_time_34
        if_log_33_less_than_todayto_time_34 >> rail.Label(
            'Yes') >> get_cost_center_schedule_for_user_36 >> if_costcenter_displaytext_present_39
        if_costcenter_displaytext_present_39 >> rail.Label(
            'Yes') >> foreach_response_41 >> log_effective_date_42 >> if_effectivedate_day_present_43
        if_effectivedate_day_present_43 >> rail.Label(
            'Yes') >> if_to_time_equals_to_todayto_time_44
        if_to_time_equals_to_todayto_time_44 >> rail.Label(
            'Yes') >> dtna_datamart_cost_center_table_eng_user_add_entry_45 >> if_effectivedate_day_blank_46
        if_to_time_equals_to_todayto_time_44 >> rail.Label(
            'No') >> if_effectivedate_day_blank_46
        if_effectivedate_day_present_43 >> rail.Label(
            'No') >> if_effectivedate_day_blank_46
        if_effectivedate_day_blank_46 >> rail.Label(
            'Yes') >> dtna_datamart_cost_center_table_eng_user_add_entry_47 >> foreach_response_41_end
        if_effectivedate_day_blank_46 >> rail.Label(
            'No') >> foreach_response_41_end
        foreach_response_41 >> foreach_response_41_end >> get_current_cost_center_data_for_user_49
        if_costcenter_displaytext_present_39 >> rail.Label(
            'No') >> get_current_cost_center_data_for_user_49 >> if_first_textvalue_present_52
        if_first_textvalue_present_52 >> rail.Label(
            'Yes') >> log_get_current_active_cost_centerfortheuser_53 >> log_54 >> get_current_cost_center_data_56 >> if_parent_displaytext_equals_to_dtnaeng_59
        if_parent_displaytext_equals_to_dtnaeng_59 >> rail.Label(
            'Yes') >> log_checkifthecurrentcostcenterispresetmultipletimes_60 >> parse_json_61 >> log_length_62 >> if_log_length_62_greater_than_1_63
        if_log_length_62_greater_than_1_63 >> rail.Label(
            'Yes') >> foreach_document_64 >> if_foreach_document_64_costcentereffectivedate_blank_65
        if_foreach_document_64_costcentereffectivedate_blank_65 >> rail.Label(
            'Yes') >> insert_to_list_66 >> foreach_document_64_end >> if_log_length_62_less_than_2_67
        if_foreach_document_64_costcentereffectivedate_blank_65 >> rail.Label(
            'No') >> foreach_document_64_end
        foreach_document_64 >> foreach_document_64_end >> if_log_length_62_less_than_2_67
        if_log_length_62_greater_than_1_63 >> rail.Label(
            'No') >> if_log_length_62_less_than_2_67
        if_log_length_62_less_than_2_67 >> rail.Label(
            'Yes') >> insert_to_list_68 >> log_checkifthecurrentcostcenterispresetmultipletimes_69
        if_log_length_62_less_than_2_67 >> rail.Label(
            'No') >> log_checkifthecurrentcostcenterispresetmultipletimes_69 >> log_checkifthecurrentcostcenterispresetmultipletimes_70 >> if_log_length_62_greater_than_1_71
        if_log_length_62_greater_than_1_71 >> rail.Label(
            'Yes') >> foreach_create_list_72_73 >> log_today_date_74 >> log_80 >> if_log_80_present_81
        if_log_80_present_81 >> rail.Label(
            'Yes') >> log_86 >> log_findthedaydiff_87 >> log_88 >> accumulate_list_items_89 >> foreach_create_list_72_73_end >> log_findthesmallestdaydiff_90
        if_log_80_present_81 >> rail.Label(
            'No') >> foreach_create_list_72_73_end
        foreach_create_list_72_73 >> foreach_create_list_72_73_end >> log_findthesmallestdaydiff_90 >> log_gettherequiredeffectivedate_91 >> if_log_gettherequiredeffectivedate_91_present_2_92
        if_log_gettherequiredeffectivedate_91_present_2_92 >> rail.Label(
            'Yes') >> insert_to_list_93 >> foreach_accumulate_list_items_89_94 >> if_log_findthesmallestdaydiff_90_not_equals_to_dataforeach12252e04daydiff_95
        if_log_findthesmallestdaydiff_90_not_equals_to_dataforeach12252e04daydiff_95 >> rail.Label(
            'Yes') >> log_gettherequiredeffectivedate_99 >> log_103 >> log_104 >> parse_json_105 >> log_106 >> if_log_106_less_than_1_107
        if_log_106_less_than_1_107 >> rail.Label(
            'Yes') >> insert_to_list_108 >> if_log_106_equals_to_1_109
        if_log_106_less_than_1_107 >> rail.Label(
            'No') >> if_log_106_equals_to_1_109
        if_log_106_equals_to_1_109 >> rail.Label(
            'Yes') >> log_110 >> insert_to_list_111 >> if_log_106_greater_than_1_112
        if_log_106_equals_to_1_109 >> rail.Label(
            'No') >> if_log_106_greater_than_1_112
        if_log_106_greater_than_1_112 >> rail.Label(
            'Yes') >> log_113 >> if_log_113_blank_114
        if_log_113_blank_114 >> rail.Label(
            'Yes') >> log_115 >> if_log_115_present_116
        if_log_115_present_116 >> rail.Label(
            'Yes') >> insert_to_list_117 >> if_log_113_present_136
        if_log_115_present_116 >> rail.Label('No') >> if_log_115_blank_118
        if_log_115_blank_118 >> rail.Label(
            'Yes') >> foreach_document_119 >> if_foreach_document_119_column_2_present_120
        if_foreach_document_119_column_2_present_120 >> rail.Label(
            'Yes') >> log_124 >> log_129 >> log_findthedaydiff_131 >> accumulate_list_items_132 >> foreach_document_119_end >> log_findthesmallestdaydiff_133
        if_foreach_document_119_column_2_present_120 >> rail.Label(
            'No') >> foreach_document_119_end
        foreach_document_119 >> foreach_document_119_end >> log_findthesmallestdaydiff_133 >> log_findthemanagerengforthesmallestdaydiff_134 >> insert_to_list_135 >> if_log_113_present_136
        if_log_115_blank_118 >> rail.Label('No') >> if_log_113_present_136
        if_log_113_blank_114 >> rail.Label('No') >> if_log_113_present_136
        if_log_113_present_136 >> rail.Label(
            'Yes') >> insert_to_list_137 >> foreach_accumulate_list_items_89_94_end >> foreach_query_list_28_138
        if_log_113_present_136 >> rail.Label(
            'No') >> foreach_accumulate_list_items_89_94_end >> foreach_query_list_28_138
        if_log_106_greater_than_1_112 >> rail.Label(
            'No') >> foreach_accumulate_list_items_89_94_end >> foreach_query_list_28_138
        if_log_findthesmallestdaydiff_90_not_equals_to_dataforeach12252e04daydiff_95 >> rail.Label(
            'No') >> foreach_accumulate_list_items_89_94_end
        foreach_accumulate_list_items_89_94 >> foreach_accumulate_list_items_89_94_end >> foreach_query_list_28_138
        if_log_gettherequiredeffectivedate_91_present_2_92 >> rail.Label(
            'No') >> foreach_accumulate_list_items_89_94_end >> foreach_query_list_28_138
        if_log_length_62_greater_than_1_71 >> rail.Label(
            'No') >> foreach_query_list_28_138
        if_parent_displaytext_equals_to_dtnaeng_59 >> rail.Label(
            'No') >> foreach_query_list_28_138
        if_first_textvalue_present_52 >> rail.Label(
            'No') >> foreach_query_list_28_138 >> if_log_2_not_equals_to_dataforeachforeach_1costcenter_139
        if_log_2_not_equals_to_dataforeachforeach_1costcenter_139 >> rail.Label(
            'Yes') >> log_143 >> log_147 >> log_148 >> parse_json_149 >> log_150 >> if_log_8_less_than_1_151
        if_log_8_less_than_1_151 >> rail.Label(
            'Yes') >> log_152 >> insert_to_list_153 >> if_log_8_equals_to_1_154
        if_log_8_less_than_1_151 >> rail.Label(
            'No') >> if_log_8_equals_to_1_154
        if_log_8_equals_to_1_154 >> rail.Label(
            'Yes') >> log_155 >> insert_to_list_156 >> if_log_8_greater_than_1_157
        if_log_8_equals_to_1_154 >> rail.Label(
            'No') >> if_log_8_greater_than_1_157
        if_log_8_greater_than_1_157 >> rail.Label(
            'Yes') >> log_158 >> if_log_13_blank_159
        if_log_13_blank_159 >> rail.Label(
            'Yes') >> log_160 >> if_log_16_present_161
        if_log_16_present_161 >> rail.Label(
            'Yes') >> insert_to_list_162 >> if_log_13_present_181
        if_log_16_present_161 >> rail.Label('No') >> if_log_16_blank_163
        if_log_16_blank_163 >> rail.Label(
            'Yes') >> foreach_document_164 >> if_foreach_document_164_column_2_present_165
        if_foreach_document_164_column_2_present_165 >> rail.Label(
            'Yes') >> log_169 >> log_174 >> log_findthedaydiff_176 >> accumulate_list_items_177 >> foreach_document_164_end >> log_findthesmallestdaydiff_178
        if_foreach_document_164_column_2_present_165 >> rail.Label(
            'No') >> foreach_document_164_end
        foreach_document_164 >> foreach_document_164_end >> log_findthesmallestdaydiff_178 >> log_findthemanagerengforthesmallestdaydiff_179 >> insert_to_list_180 >> if_log_13_present_181
        if_log_16_blank_163 >> rail.Label('No') >> if_log_13_present_181
        if_log_13_blank_159 >> rail.Label('No') >> if_log_13_present_181
        if_log_13_present_181 >> rail.Label(
            'Yes') >> insert_to_list_182 >> foreach_query_list_28_138_end
        if_log_13_present_181 >> rail.Label(
            'No') >> foreach_query_list_28_138_end
        if_log_8_greater_than_1_157 >> rail.Label(
            'No') >> foreach_query_list_28_138_end
        if_log_2_not_equals_to_dataforeachforeach_1costcenter_139 >> rail.Label(
            'No') >> foreach_query_list_28_138_end
        if_log_33_less_than_todayto_time_34 >> rail.Label(
            'No') >> foreach_query_list_28_138_end
        foreach_query_list_28_138 >> foreach_query_list_28_138_end >> final_result

        final_result >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
