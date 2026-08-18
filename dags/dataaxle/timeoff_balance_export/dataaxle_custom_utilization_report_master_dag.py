
from datetime import timedelta, datetime
from airflow.models import Variable
import rail
from dataaxle.timeoff_balance_export.tasks.run_report_with_custom_filters import run_report_with_custom_filters
null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dataaxle_timeoff_balance_export_custom_utilization_report_master_{config.instance}',
        description=f'DataAxle_Custom Utilization Report Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        webhook_conf=[rail.WebhookConf(
            bearer_token_var=f'dataaxle_custom_utilization_report_webook_{config.instance}_secret')],
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
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_status_variable=rail.SetVariableOperator(
            task_id='create_status_variable',
            append=False,
            name='status',
            value=None
        )

        get_user_details=rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/UserService1.svc/GetUserDetails",
            data=lambda dag_run: {
                "userUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:user:" + dag_run.conf['webhook']['data']['requestorid']
            }
        )

        if_emailaddress_not_present=rail.IfOperator(
            task_id='if_emailaddress_not_present',
            test='''{{result('get_user_details').emailAddress | is_falsy }}''',
            yes_task="update_status_variable",
            no_task="create_run_details",
        )

        update_status_variable=rail.SetVariableOperator(
            task_id='update_status_variable',
            append=False,
            name='{{ result("create_status_variable").name }}',
            value="Emailid not available in requester's profile"
        )

        create_run_details=rail.PythonOperator(
            task_id='create_run_details',
            python_callable= lambda dag_run: {
                "runtime": datetime.now().strftime("%m%d%YT%H%M%S"),
                "emailtime": datetime.now().strftime("%m/%d/%YT%H:%M:%S"),
                "useruri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:user:" + dag_run.conf['webhook']['data']['requestorid'],
                "startdate": ( 'nil' if dag_run.conf['webhook']['data']['daterange'].split('-')[0] == 'null'
                            else ( datetime.strptime(dag_run.conf['webhook']['data']['daterange'].split('-')[0],'%m%d%Y').strftime('%m/%d/%Y') ) )
                            if dag_run.conf['webhook']['data']['daterange'] else null,
                "enddate": ( 'nil' if dag_run.conf['webhook']['data']['daterange'].split('-')[-1] == 'null'
                            else ( datetime.strptime(dag_run.conf['webhook']['data']['daterange'].split('-')[-1],'%m%d%Y').strftime('%m/%d/%Y') ) )
                            if dag_run.conf['webhook']['data']['daterange'] else null,
                "emailidforlogs": dag_run.conf['webhook']['data']['emailaddress'] + ',' + rail.result('get_user_details')['emailAddress'] if
                                    dag_run.conf['webhook']['data']['emailaddress'] else rail.result('get_user_details')['emailAddress']
            }
        )

        if_daterange_not_present=rail.IfOperator(
            task_id='if_daterange_not_present',
            test=lambda dag_run: bool( not(dag_run.conf['webhook']['data']['daterange']) or 'null' in dag_run.conf['webhook']['data']['daterange']),
            yes_task="send_mail_incorrect_no_date_range",
            no_task="get_client_hrs_report_details",
        )

        send_mail_incorrect_no_date_range=rail.EmailOperator(
            task_id='send_mail_incorrect_no_date_range',
            to="{{result('create_run_details').emailidforlogs}}",
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Custom Utilization Report - Skipped - {{ result('create_run_details').emailtime }}''',
            html_content= '''templates/incorrect_no_daterange.html''',
        )

        update_statusvariable=rail.SetVariableOperator(
            task_id='update_statusvariable',
            append=False,
            name='{{ result("create_status_variable").name }}',
            value="Incorrect/No date range selected"
        )

        get_client_hrs_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_client_hrs_report_details',
            report_name=config.client_hrs_report,
        )

        if_column_configuration_does_not_match=rail.IfOperator(
            task_id='if_column_configuration_does_not_match',
            test=lambda: bool( ','.join([column['column']['displayText'] for column in rail.result(
                            'get_client_hrs_report_details')['columnConfiguration']]) != config.client_hrs_report_column_config),
            yes_task="fail_job_with_column_order_mismatch",
            no_task="get_timesheet_and_timeoff_hrs_report_details",
        )

        fail_job_with_column_order_mismatch=rail.FailOperator(
            task_id='fail_job_with_column_order_mismatch',
            #pylint: disable = line-too-long
            message='''Column order mismatch in ***Client Hrs Report - Base Report. Required column order is "jobtitle,User Name,useruri,Parent Client,Parent Client,Hrs,Billable Hrs,Non-Billable Hrs"'''
        )

        get_timesheet_and_timeoff_hrs_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_timesheet_and_timeoff_hrs_report_details',
            report_name=config.timesheet_timeoff_hrs_report,
        )

        if_column_config_does_not_match=rail.IfOperator(
            task_id='if_column_config_does_not_match',
            test=lambda: bool( ','.join([column['column']['displayText'] for column in rail.result(
                            'get_timesheet_and_timeoff_hrs_report_details')['columnConfiguration']]) != config.timesheet_timeoff_hrs_report_column_config),
            yes_task="fail_job_with_column_order_error",
            no_task="get_user_schedule_hrs_report_details",
        )

        fail_job_with_column_order_error=rail.FailOperator(
            task_id='fail_job_with_column_order_error',
            #pylint: disable = line-too-long
            message='''Column order mismatch in ***Timesheet and Timeoff Hrs Report - Base Report. Required column order is "useruri,User Name,Scheduled Hrs"'''
        )

        get_user_schedule_hrs_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_user_schedule_hrs_report_details',
            report_name=config.user_schedule_hrs_report,
        )

        if_column_config_doesnt_match=rail.IfOperator(
            task_id='if_column_config_doesnt_match',
            test=lambda: bool( ','.join([column['column']['displayText'] for column in rail.result(
                            'get_user_schedule_hrs_report_details')['columnConfiguration']]) != config.user_schedule_hrs_report_column_config),
            yes_task="fail_dag_with_column_order_mismatch",
            no_task="clienthrs_report_group_start",
        )

        fail_dag_with_column_order_mismatch=rail.FailOperator(
            task_id='fail_dag_with_column_order_mismatch',
            #pylint: disable = line-too-long
            message='''Column order mismatch in ***Timesheet and Timeoff Hrs Report - Base Report. Required column order is "useruri,Total Hrs,Time Off Hrs,Hours Worked"'''
        )

        clienthrs_report_group_start = rail.EmptyOperator(
            task_id = 'clienthrs_report_group_start'
        )

        run_clienthrs_report,run_custom_report_clienthrs = run_report_with_custom_filters(
            'clienthrs','get_client_hrs_report_details','DateRangeFilter','clienthrs_report_group_end')

        if_error_in_clienthrs_report=rail.IfOperator(
            task_id='if_error_present_clienthrs',
            test="{{(result('run_custom_report_clienthrs.get_report_result')| load_json_artifact).reportGenerationResults[0].error | is_truthy}}",
            yes_task="fail_dag_with_error_clienthrs",
            no_task="clienthrs_report_group_end",
        )

        fail_dag_with_error_clienthrs=rail.FailOperator(
            task_id=f'fail_dag_with_error_clienthrs',
            message='''{{(result('run_custom_report_clienthrs.get_report_result')| load_json_artifact).reportGenerationResults[0].error }}'''
        )

        clienthrs_report_group_end = rail.EmptyOperator(
            task_id = 'clienthrs_report_group_end'
        )

        timesheettimeoff_report_group_start = rail.EmptyOperator(
            task_id = 'timesheettimeoff_report_group_start'
        )

        run_timesheettimeoff_report,run_custom_report_timesheettimeoff = run_report_with_custom_filters(
            'timesheettimeoff','get_timesheet_and_timeoff_hrs_report_details','EntryDateFilter','timesheettimeoff_report_group_end')

        if_error_in_timesheettimeoff_report=rail.IfOperator(
            task_id='if_error_present_timesheettimeoff',
            test="{{(result('run_custom_report_timesheettimeoff.get_report_result')| load_json_artifact).reportGenerationResults[0].error | is_truthy}}",
            yes_task="fail_dag_with_error_timesheettimeoff",
            no_task="timesheettimeoff_report_group_end",
        )

        fail_dag_with_error_timesheettimeoff=rail.FailOperator(
            task_id=f'fail_dag_with_error_timesheettimeoff',
            message='''{{(result('run_custom_report_timesheettimeoff.get_report_result')| load_json_artifact).reportGenerationResults[0].error }}'''
        )

        timesheettimeoff_report_group_end = rail.EmptyOperator(
            task_id = 'timesheettimeoff_report_group_end'
        )

        clear_entries_from_list=rail.SetVariableOperator(
            task_id='clear_entries_from_list',
            append=False,
            name='reporteefilter_for_payrolldata',
            value=[]
        )

        add_to_reporteefilterforpayrolldata_list_third=rail.SetVariableOperator(
            task_id='add_to_reporteefilterforpayrolldata_list_third',
            append=True,
            name='{{ result("clear_entries_from_list").name }}',
            value=lambda: {
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_user_schedule_hrs_report_details')['filterConfiguration']['enabledFilters'],'displayText','EntryDateFilter','uri',''),
                "value": null
            }
        )

        add_to_reporteefilterforpayroll_data_list_third=rail.SetVariableOperator(
            task_id='add_to_reporteefilterforpayroll_data_list_third',
            append=True,
            name='{{ result("clear_entries_from_list").name }}',
            value=lambda:{
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_user_schedule_hrs_report_details')['filterConfiguration']['enabledFilters'],'displayText','EntryDateFilter','uri',''),
                "value": rail.result('create_run_details')['startdate']
            }
        )

        add_to_reporteefilterfor_payroll_data_list_third=rail.SetVariableOperator(
            task_id='add_to_reporteefilterfor_payroll_data_list_third',
            append=True,
            name='{{ result("clear_entries_from_list").name }}',
            value=lambda:{
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_user_schedule_hrs_report_details')['filterConfiguration']['enabledFilters'],'displayText','EntryDateFilter','uri',''),
                "value": rail.result('create_run_details')['enddate']
            }
        )

        log_report_filter_for_third_report=rail.PythonOperator(
            task_id='log_report_filter_for_third_report',
            python_callable= lambda: rail.get_dag_run_var('reporteefilter_for_payrolldata')
        )

        run_custom_user_schedule_hrs_report = rail.run_report2(
            group_id='run_custom_user_schedule_hrs_report',
            report_params=lambda:{
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_user_schedule_hrs_report_details')['uri'],
                        "filterValues": rail.result('log_report_filter_for_third_report'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact'
        )

        if_error_in_report_result=rail.IfOperator(
            task_id='if_error_in_report_result',
            test='''{{(result('run_custom_user_schedule_hrs_report.get_report_result')| load_json_artifact).reportGenerationResults[0].error | is_truthy}}''',
            yes_task="fail_dag_error_present",
            no_task="load_csv_schedule_data_report",
        )

        fail_dag_error_present=rail.FailOperator(
            task_id='fail_dag_error_present',
            message='''{{(result('run_custom_user_schedule_hrs_report.get_report_result')| load_json_artifact).reportGenerationResults[0].error}}'''
        )

        load_csv_schedule_data_report=rail.LoadCSVFileOperator(
            task_id="load_csv_schedule_data_report",
            document="{{(result('run_custom_user_schedule_hrs_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload }}",
        )

        create_collection_scheduledatareport = rail.CreateCollectionOperator(
            task_id='create_collection_scheduledatareport',
            source = "{{ result('load_csv_schedule_data_report') }}",
            name = "scheduledatareport",
            columns = {
                'useruri':'useruri',
                'User Name':'username',
                'Scheduled Hrs':'scheduledhrs',
                'Entry Date':'entrydate'
            }
        )

        query_records_with_schdeuled_hrs=rail.QueryCollectionOperator(
            task_id='query_records_with_schdeuled_hrs',
            query="""SELECT * FROM  scheduledatareport WHERE  CAST(scheduledatareport.scheduledhrs AS INTEGER) > 0""",
        )

        load_csv_client_hrs_report=rail.LoadCSVFileOperator(
            task_id="load_csv_client_hrs_report",
            document="{{(result('run_custom_report_clienthrs.get_report_result')| load_json_artifact).reportGenerationResults[0].payload }}",
        )

        create_collection_clienthrsreport = rail.CreateCollectionOperator(
            task_id='create_collection_clienthrsreport',
            source = "{{ result('load_csv_client_hrs_report') }}",
            name = "clienthrsreport",
            columns = {
                'jobtitle':'jobtitle',
                'User Name':'username',
                'useruri':'useruri',
                'Parent Client':'parentclient',
                'Hrs':'projecthours',
                'Billable Hrs':'billablehours',
                'Non-Billable Hrs':'nonbillablehours'
            }
        )

        query_all_data_in_clienthrsreport=rail.QueryCollectionOperator(
            task_id='query_all_data_in_clienthrsreport',
            query="""SELECT * FROM  clienthrsreport""",
        )

        load_csv_timesheet_timeoff_hrs_report=rail.LoadCSVFileOperator(
            task_id="load_csv_timesheet_timeoff_hrs_report",
            document="{{(result('run_custom_report_timesheettimeoff.get_report_result')| load_json_artifact).reportGenerationResults[0].payload }}",
        )

        create_collection_timeoffreport = rail.CreateCollectionOperator(
            task_id='create_collection_timeoffreport',
            source = "{{ result('load_csv_timesheet_timeoff_hrs_report') }}",
            name = "timeoffreport",
            columns = {
                'useruri':'useruri',
                'User Name':'username',
                'jobtitle':'jobtitle',
                'Total Hrs':'totalhours',
                'Time Off Hrs':'timeoffhours',
                'Hours Worked':'hoursworked'
            }
        )

        query_all_data_in_timeoffreport=rail.QueryCollectionOperator(
            task_id='query_all_data_in_timeoffreport',
            query="""SELECT * FROM  timeoffreport""",
        )

        query_unique_users_from_timeoffreport=rail.QueryCollectionOperator(
            task_id='query_unique_users_from_timeoffreport',
            query="""SELECT DISTINCT timeoffreport.useruri FROM  timeoffreport""",
        )

        create_predata_list=rail.SetVariableOperator(
            task_id='create_predata_list',
            append=False,
            name='predata',
            value=[]
        )

        create_data_list=rail.SetVariableOperator(
            task_id='create_data_list',
            append=False,
            name='data',
            value=[]
        )

        if_unique_users_present=rail.IfOperator(
            task_id='if_unique_users_present',
            test='''{{ result('query_unique_users_from_timeoffreport','length') > 0 }}''',
            yes_task="add_utilization_data_per_user_to_predatalist",
            no_task="query_unique_roles_from_client_report",
        )

        def get_occurence_count(scheduled_hrs_data,uri):
            count = 0
            sum_hrs = 0
            for data in scheduled_hrs_data:
                if data['useruri'] == uri:
                    count+=1
                    sum_hrs+=float(data['scheduledhrs'])
            return{
                'count': count,
                'sum': sum_hrs
            }

        def hrs_sum_for_type_and_matching_parentclient(client_hrs_data,uri,type_of_data):
            sum_hrs = 0
            for data in client_hrs_data:
                if data['useruri'] == uri and data['parentclient'] == 'DAX Corporate':
                    sum_hrs+=float(data[type_of_data])
            return 0 if not(sum_hrs) or sum_hrs == 'Infinity%' or sum_hrs == 'NaN%' else sum_hrs

        def hrs_sum_for_type_and_not_matching_parentclient(client_hrs_data,uri,type_of_data):
            sum_hrs = 0
            for data in client_hrs_data:
                if data['useruri'] == uri and data['parentclient'] != 'DAX Corporate':
                    sum_hrs+=float(data[type_of_data])
            return 0 if not(sum_hrs) or sum_hrs == 'Infinity%' or sum_hrs == 'NaN%' else sum_hrs

        def get_utilization_data(dag_run):
            finaldata = []
            unique_users_timeoff_report = rail.load_all_records(rail.result('query_unique_users_from_timeoffreport'))#79
            client_hrs_report_data = rail.load_all_records(rail.result('query_all_data_in_clienthrsreport'))#76
            timeoff_report_data = rail.load_all_records(rail.result('query_all_data_in_timeoffreport'))#78
            scheduled_hrs_report_data = rail.load_all_records(rail.result('query_records_with_schdeuled_hrs'))#74
            for user in unique_users_timeoff_report:
                role = rail.find_first_by_attr_and_get_attr(timeoff_report_data,'useruri',user['useruri'],'jobtitle','')
                username = rail.find_first_by_attr_and_get_attr(timeoff_report_data,'useruri',user['useruri'],'username','')
                timeperiod = (datetime.strptime(dag_run.conf['webhook']['data']['daterange'].split('-')[0],'%m%d%Y').strftime('%m/%d/%Y')) + '-' + (
                    datetime.strptime(dag_run.conf['webhook']['data']['daterange'].split('-')[-1],'%m%d%Y').strftime('%m/%d/%Y'))
                count_sum = get_occurence_count(scheduled_hrs_report_data,user['useruri']) if rail.find_first_by_attr_and_get_attr(
                    scheduled_hrs_report_data,'useruri',user['useruri'],'useruri','') else {}
                businessdays = count_sum['count'] if count_sum else 0
                businesshours = count_sum['sum'] if count_sum else 0
                draft_timeoffhrs = rail.find_first_by_attr_and_get_attr(timeoff_report_data,'useruri',user['useruri'],'timeoffhours','')
                timeoffhrs = float(( 0 if draft_timeoffhrs == 'Infinity%' else ( 0 if draft_timeoffhrs == 'NaN%' else draft_timeoffhrs ))
                                if draft_timeoffhrs else 0)
                availablehours = float( float( count_sum['sum'] if rail.find_first_by_attr_and_get_attr(
                    scheduled_hrs_report_data,'useruri',user['useruri'],'scheduledhrs','') else 0) - float(timeoffhrs))
                draft_hoursreported = rail.find_first_by_attr_and_get_attr(timeoff_report_data,'useruri',user['useruri'],'totalhours','')
                hoursreported = float(( 0 if draft_hoursreported == 'Infinity%' else ( 0 if draft_hoursreported == 'NaN%' else draft_hoursreported))
                                if draft_hoursreported else 0)
                draft_hoursworked = rail.find_first_by_attr_and_get_attr(timeoff_report_data,'useruri',user['useruri'],'hoursworked','')
                hoursworked = float(( 0 if draft_hoursworked == 'Infinity%' else ( 0 if draft_hoursworked == 'NaN%' else draft_hoursworked))
                                if draft_hoursworked else 0)
                clienthours = hrs_sum_for_type_and_not_matching_parentclient(client_hrs_report_data,user['useruri'],'projecthours')
                cbhours = hrs_sum_for_type_and_not_matching_parentclient(client_hrs_report_data,user['useruri'],'billablehours')
                cnbhours = hrs_sum_for_type_and_not_matching_parentclient(client_hrs_report_data,user['useruri'],'nonbillablehours')
                corporatehours = hrs_sum_for_type_and_matching_parentclient(client_hrs_report_data,user['useruri'],'projecthours')
                draft_hoursworkedut = (float(hoursworked) / availablehours) if availablehours else 0
                hoursworkedut = (0 if draft_hoursworkedut == 'Infinity%' else ( 0 if draft_hoursworkedut == 'NaN%' else draft_hoursworkedut))*100
                draft_clienthoursut = (float(clienthours) / availablehours) if availablehours else 0
                clienthoursut = (0 if draft_clienthoursut == 'Infinity%' else ( 0 if draft_clienthoursut == 'NaN%' else draft_clienthoursut))*100
                draft_cbhoursut = (float(cbhours) / availablehours) if availablehours else 0
                cbhoursut = (0 if draft_cbhoursut == 'Infinity%' else ( 0 if draft_cbhoursut == 'NaN%' else draft_cbhoursut))*100
                draft_cnbhoursut = (float(cnbhours) / availablehours) if availablehours else 0
                cnbhoursut = (0 if draft_cnbhoursut == 'Infinity%' else ( 0 if draft_cnbhoursut == 'NaN%' else draft_cnbhoursut))*100
                draft_corporatehoursut = (float(corporatehours) / availablehours) if availablehours else 0
                corporatehoursut = (0 if draft_corporatehoursut == 'Infinity%' else (0 if draft_corporatehoursut == 'NaN%' else draft_corporatehoursut))*100
                finaldata.append({
                    'role': role,
                    'username': username,
                    'timeperiod': timeperiod,
                    'businessdays': businessdays,
                    'businesshours': businesshours,
                    'timeoffhrs': timeoffhrs,
                    'availablehours': availablehours,
                    'hoursreported': hoursreported,
                    'hoursworked': hoursworked,
                    'clienthours': clienthours,
                    'cbhours': cbhours,
                    'cnbhours': cnbhours,
                    'corporatehours': corporatehours,
                    'hoursworkedut': hoursworkedut,
                    'clienthoursut': clienthoursut,
                    'cbhoursut': cbhoursut,
                    'cnbhoursut': cnbhoursut,
                    'corporatehoursut': corporatehoursut
                })
            return finaldata

        add_utilization_data_per_user_to_predatalist=rail.SetVariableOperator(
            task_id='add_utilization_data_per_user_to_predatalist',
            append=True,
            name='{{ result("create_predata_list").name }}',
            value=lambda dag_run:{
                'data': get_utilization_data(dag_run)
            }
        )

        def get_finaldata():
            finalist = rail.get_dag_run_var('predata')[0]['data']
            return [{
                'role': data['role'],
                'username': data['username'],
                'timeperiod': data['timeperiod'],
                'businessdays': data['businessdays'],
                'businesshours': data['businesshours'],
                'timeoffhrs': data['timeoffhrs'],
                'availablehours': data['availablehours'],
                'hoursreported': data['hoursreported'],
                'hoursworked': data['hoursworked'],
                'clienthours': data['clienthours'],
                'cbhours': data['cbhours'],
                'cnbhours': data['cnbhours'],
                'corporatehours': data['corporatehours'],
                'hoursworkedut': ( ( ( 0 if data['hoursworkedut'] == 'Infinity' else ( 0 if data['hoursworkedut'] == 'NaN' else data['hoursworkedut'] ))
                                    if data['hoursworkedut'] else 0 ) if float(data['availablehours']) > 0 else 0) if float(data['hoursworked']) > 0 else 0,
                'clienthoursut': ( ( ( 0 if data['clienthoursut'] == 'Infinity' else ( 0 if data['clienthoursut'] == 'NaN' else data['clienthoursut'] ))
                                    if data['clienthoursut'] else 0 ) if float(data['availablehours']) > 0 else 0) if float(data['clienthours']) > 0 else 0,
                'cbhoursut': ( ( ( 0 if data['cbhoursut'] == 'Infinity' else ( 0 if data['cbhoursut'] == 'NaN' else data['cbhoursut'] ))
                                    if data['cbhoursut'] else 0 ) if float(data['availablehours']) > 0 else 0) if float(data['cbhours']) > 0 else 0,
                'cnbhoursut': ( ( ( 0 if data['cnbhoursut'] == 'Infinity' else ( 0 if data['cnbhoursut'] == 'NaN' else data['cnbhoursut'] ))
                                    if data['cnbhoursut'] else 0 ) if float(data['availablehours']) > 0 else 0) if float(data['cnbhours']) > 0 else 0,
                'corporatehoursut': ((( 0 if data['corporatehoursut'] == 'Infinity' else (0 if data['corporatehoursut'] == 'NaN' else data['corporatehoursut']))
                                    if data['corporatehoursut'] else 0) if float(data['availablehours']) > 0 else 0) if float(data['corporatehours']) > 0 else 0
            } for data in finalist]

        insert_to_data_list=rail.SetVariableOperator(
            task_id='insert_to_data_list',
            append=True,
            name='{{ result("create_data_list").name }}',
            value=get_finaldata
        )

        query_unique_roles_from_client_report=rail.QueryCollectionOperator(
            task_id='query_unique_roles_from_client_report',
            query="""SELECT DISTINCT  clienthrsreport.jobtitle FROM  clienthrsreport WHERE
                    NULLIF(jobtitle,'') IS NOT NULL AND  clienthrsreport.jobtitle !='No Job Title' """,
        )

        if_unique_roles_present=rail.IfOperator(
            task_id='if_unique_roles_present',
            test='''{{ result('query_unique_roles_from_client_report','length') > 0 }}''',
            yes_task="insert_additional_header",
            no_task="join_all_data_for_finaldatalist",
        )

        insert_additional_header=rail.SetVariableOperator(
            task_id='insert_additional_header',
            append=True,
            name='additionalheader',
            value=[{
                'role': null if sequence == 1 else ( 'Summary' if sequence == 2 else 'Role'),
                'username': null if sequence == 1 else ( null if sequence == 2 else 'Average total Client hours utilization'),
                'timeperiod': null if sequence == 1 else ( null if sequence == 2 else 'Average total Corporate hours utilization'),
                'businessdays': null,
                'businesshours': null,
                'timeoffhrs': null,
                'availablehours': null,
                'hoursreported': null,
                'hoursworked': null,
                'clienthours': null,
                'cbhours': null,
                'cnbhours': null,
                'corporatehours': null,
                'hoursworkedut': null,
                'clienthoursut': null,
                'cbhoursut': null,
                'cnbhoursut': null,
                'corporatehoursut': null
            } for sequence in [1,2,3]]
        )

        def get_typehrs_sum_perrole(utilization_final_data,role,type_of):
            sum_hrs = 0
            username_list = []
            for data in utilization_final_data:
                if data['role'] == role:
                    sum_hrs+=float(data[type_of])
                    username_list.append(data['username'])
            return str(sum_hrs / len(list(set(username_list))))

        def insert_utilizationdataperrole():
            unique_roles_data = rail.load_all_records(rail.result('query_unique_roles_from_client_report'))
            entries_in_data = rail.get_dag_run_var('data')[0]
            return[{
                'role': data['jobtitle'],
                'username': get_typehrs_sum_perrole(entries_in_data,data['jobtitle'],'clienthoursut') + "%",
                'timeperiod': get_typehrs_sum_perrole(entries_in_data,data['jobtitle'],'corporatehoursut') + "%",
                'businessdays': null,
                'businesshours': null,
                'timeoffhrs': null,
                'availablehours': null,
                'hoursreported': null,
                'hoursworked': null,
                'clienthours': null,
                'cbhours': null,
                'cnbhours': null,
                'corporatehours': null,
                'hoursworkedut': null,
                'clienthoursut': null,
                'cbhoursut': null,
                'cnbhoursut': null,
                'corporatehoursut': null
            } for data in unique_roles_data]

        insert_utilizationdata_per_role=rail.SetVariableOperator(
            task_id='insert_utilizationdata_per_role',
            append=True,
            name='utilizationdataperrole',
            value=insert_utilizationdataperrole
        )

        join_all_data_for_finaldatalist = rail.PythonOperator(
            task_id = 'join_all_data_for_finaldatalist',
            python_callable=lambda: (rail.get_dag_run_var('data')[0] if rail.get_dag_run_var('data') else []) +
                                    (rail.get_dag_run_var('additionalheader')[0] if rail.get_dag_run_var('additionalheader') else []) +
                                    (rail.get_dag_run_var('utilizationdataperrole')[0] if rail.get_dag_run_var('utilizationdataperrole') else [])
        )

        if_no_data_in_final_data_list=rail.IfOperator(
            task_id='if_no_data_in_final_data_list',
            test=lambda: not bool(rail.result('join_all_data_for_finaldatalist')),
            yes_task="send_mail_no_data_to_extract_forthe_givenperiod",
            no_task="compose_final_csv_to_be_sent",
        )

        send_mail_no_data_to_extract_forthe_givenperiod=rail.EmailOperator(
            task_id='send_mail_no_data_to_extract_forthe_givenperiod',
            to="{{result('create_run_details').emailidforlogs}}",
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key()}} | Custom Utilization Report - Skipped - {{ result('create_run_details').emailtime }} ''',
            html_content= '''templates/no_data_to_extract.html''',
        )

        status_update=rail.SetVariableOperator(
            task_id='status_update',
            append=False,
            name='{{ result("create_status_variable").name }}',
            value="No data to extract for the given date range"
        )

        compose_final_csv_to_be_sent=rail.WriteCSVFileOperator(
            task_id='compose_final_csv_to_be_sent',
            source=lambda: rail.result('join_all_data_for_finaldatalist'),
            header=['Role',
                    'Username',
                    'Report Time Period',
                    'Total Business days',
                    'Total Business hours',
                    'Company Holidays + PTO',
                    'Total Available Hours',
                    'Total hours reported',
                    'Total hours worked',
                    'Total Client Hours',
                    'Client Billable Hours',
                    'Client Non-Billable Hours',
                    'Total Corporate Hours',
                    'Hours worked utilization  %',
                    'Total Client Hours utilization  %',
                    'Client Billable Hours utilization  %',
                    'Client Non- Billable utilization %',
                    'Total Corporate Hours utilization  %'],
            row=lambda item:[
                item['role'],
                '0.00%' if item['username'] == 'Infinity%' or item['username'] == 'NaN%' else item['username'],
                '0.00%' if item['timeperiod'] == 'Infinity%' or item['timeperiod'] == 'NaN%' else item['timeperiod'],
                item['businessdays'],
                item['businesshours'],
                item['timeoffhrs'],
                item['availablehours'],
                item['hoursreported'],
                item['hoursworked'],
                item['clienthours'],
                item['cbhours'],
                item['cnbhours'],
                item['corporatehours'],
                ('0.00%' if (item['hoursworkedut'] == 'Infinity%' or item['hoursworkedut'] == 'NaN%') else str(item['hoursworkedut']) + '%')
                if (item['hoursworkedut'] or item['hoursworkedut'] == 0) else null,
                ('0.00%' if (item['clienthoursut'] == 'Infinity%' or item['clienthoursut'] == 'NaN%') else str(item['clienthoursut']) + '%')
                if (item['clienthoursut'] or item['clienthoursut'] == 0) else null,
                ('0.00%' if (item['cbhoursut'] == 'Infinity%' or item['cbhoursut'] == 'NaN%') else str(item['cbhoursut']) + '%')
                if (item['cbhoursut'] or item['cbhoursut'] == 0) else null,
                ('0.00%' if (item['cnbhoursut'] == 'Infinity%' or item['cnbhoursut'] == 'NaN%') else str(item['cnbhoursut']) + '%')
                if (item['cnbhoursut'] or item['cnbhoursut'] == 0) else null,
                ('0.00%' if (item['corporatehoursut'] == 'Infinity%' or item['corporatehoursut'] == 'NaN%') else str(item['corporatehoursut']) + '%')
                if (item['corporatehoursut'] or item['corporatehoursut'] == 0) else null,
            ],
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_final_csv_to_be_sent')}}",
            output_file_name='utilizationreport_{{ result("create_run_details").runtime }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        set_status_success=rail.SetVariableOperator(
            task_id='set_status_success',
            append=False,
            name='{{ result("create_status_variable").name }}',
            value="Successfully exported | utilizationreport_{{ result('create_run_details').runtime }}.csv"
        )

        send_mail_with_extract_file=rail.EmailOperator(
            task_id='send_mail_with_extract_file',
            to="{{result('create_run_details').emailidforlogs}}",
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key()}} | Custom Utilization Report - Processed - {{ result('create_run_details').emailtime }} ''',
            html_content= '''templates/success_mail.html''',
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> create_status_variable
        create_status_variable >> get_user_details >> if_emailaddress_not_present
        if_emailaddress_not_present >> rail.Label('Yes') >> update_status_variable >> finish
        if_emailaddress_not_present >> rail.Label('No') >> create_run_details >> if_daterange_not_present
        if_daterange_not_present >> rail.Label('Yes') >> send_mail_incorrect_no_date_range >> update_statusvariable >> finish
        if_daterange_not_present >> rail.Label('No') >> get_client_hrs_report_details >> if_column_configuration_does_not_match
        if_column_configuration_does_not_match >> rail.Label('Yes') >> fail_job_with_column_order_mismatch >> finish
        if_column_configuration_does_not_match >> rail.Label('No') >> get_timesheet_and_timeoff_hrs_report_details >> if_column_config_does_not_match
        if_column_config_does_not_match >> rail.Label('Yes') >> fail_job_with_column_order_error >> finish
        if_column_config_does_not_match >> rail.Label('No') >> get_user_schedule_hrs_report_details >> if_column_config_doesnt_match
        if_column_config_doesnt_match >> rail.Label('Yes') >> fail_dag_with_column_order_mismatch >> finish
        if_column_config_doesnt_match >> rail.Label('No') >> clienthrs_report_group_start >> run_clienthrs_report
        run_custom_report_clienthrs >> if_error_in_clienthrs_report
        if_error_in_clienthrs_report >> rail.Label('Yes') >> fail_dag_with_error_clienthrs
        if_error_in_clienthrs_report >> rail.Label('No') >> clienthrs_report_group_end >> timesheettimeoff_report_group_start >>  run_timesheettimeoff_report
        run_custom_report_timesheettimeoff >> if_error_in_timesheettimeoff_report
        if_error_in_timesheettimeoff_report >> rail.Label('Yes') >> fail_dag_with_error_timesheettimeoff
        if_error_in_timesheettimeoff_report >> rail.Label(
            'No') >> timesheettimeoff_report_group_end >> clear_entries_from_list >> add_to_reporteefilterforpayrolldata_list_third
        add_to_reporteefilterforpayrolldata_list_third >> add_to_reporteefilterforpayroll_data_list_third >> add_to_reporteefilterfor_payroll_data_list_third
        add_to_reporteefilterfor_payroll_data_list_third >> log_report_filter_for_third_report >> run_custom_user_schedule_hrs_report
        run_custom_user_schedule_hrs_report >> if_error_in_report_result
        if_error_in_report_result >> rail.Label('Yes') >> fail_dag_error_present >> finish
        if_error_in_report_result >> rail.Label('No') >> load_csv_schedule_data_report >> create_collection_scheduledatareport
        create_collection_scheduledatareport >> query_records_with_schdeuled_hrs >> load_csv_client_hrs_report >> create_collection_clienthrsreport
        create_collection_clienthrsreport >> query_all_data_in_clienthrsreport >> load_csv_timesheet_timeoff_hrs_report >> create_collection_timeoffreport
        create_collection_timeoffreport >> query_all_data_in_timeoffreport >> query_unique_users_from_timeoffreport >> create_predata_list
        create_predata_list >> create_data_list >> if_unique_users_present
        if_unique_users_present >> rail.Label(
            'Yes') >> add_utilization_data_per_user_to_predatalist >> insert_to_data_list >> query_unique_roles_from_client_report
        if_unique_users_present >> rail.Label('No') >> query_unique_roles_from_client_report >> if_unique_roles_present
        if_unique_roles_present >> rail.Label('Yes') >> insert_additional_header >> insert_utilizationdata_per_role >> join_all_data_for_finaldatalist
        if_unique_roles_present >> rail.Label('No') >> join_all_data_for_finaldatalist >> if_no_data_in_final_data_list
        if_no_data_in_final_data_list >> rail.Label('Yes') >> send_mail_no_data_to_extract_forthe_givenperiod >> status_update >> finish
        if_no_data_in_final_data_list >> rail.Label('No') >> compose_final_csv_to_be_sent >> generate_download_link
        generate_download_link >> set_status_success >> send_mail_with_extract_file >> finish >> log_to_sumo
        fail_dag_with_error_timesheettimeoff >> finish
        fail_dag_with_error_clienthrs >> finish
    return dag

rail.for_each_instance(create_dag)
