
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'terraconconsultants_report_export_custom_supervisor_report_child_{config.instance}',
        description=f'Terracon_Custom Supervisor Report child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_user_first_name'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_user_first_name',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_user_first_name=rail.PythonOperator(
            task_id='log_user_first_name',
            python_callable= lambda dag_run: ((dag_run.conf['username'].split('|'))[0]).strip()
        )

        log_time_now=rail.PythonOperator(
            task_id='log_time_now',
            python_callable= lambda:  datetime.now().strftime("%m/%d/%YT%H:%M:%S")
        )

        if_error_in_timesheetdatareport_result=rail.IfOperator(
            task_id='if_error_in_timesheetdatareport_result',
            test='''{{ (dag_run.conf.timesheetdatabasereportresult | load_json_artifact).reportGenerationResults[0].error | is_truthy }}''',
            yes_task="fail_job_with_error",
            no_task="if_report_payload_has_no_data",
        )

        fail_job_with_error=rail.FailOperator(
            task_id='fail_job_with_error',
            message='''{{ (dag_run.conf.timesheetdatabasereportresult | load_json_artifact).reportGenerationResults[0].error }}'''
        )

        if_report_payload_has_no_data=rail.IfOperator(
            task_id='if_report_payload_has_no_data',
            test='''{{ (dag_run.conf.timesheetdatabasereportresult | load_json_artifact).reportGenerationResults[0].payload | matches('No Data') }}''',
            yes_task="send_mail_no_datato_extract_forthe_givenperiod",
            no_task="if_error_in_paycodedatareport_result",
        )

        send_mail_no_datato_extract_forthe_givenperiod=rail.EmailOperator(
            task_id='send_mail_no_datato_extract_forthe_givenperiod',
            to='{{dag_run.conf.emailid}}',
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }}| Supervisor summary report extract - {{ result('log_time_now') }} ''',
            html_content= '''templates/no_data_to_extract_mail.html''',
        )

        if_error_in_paycodedatareport_result=rail.IfOperator(
            task_id='if_error_in_paycodedatareport_result',
            test='''{{ (dag_run.conf.payrollbasereportresult | load_json_artifact).reportGenerationResults[0].error | is_truthy }}''',
            yes_task="fail_dag_with_error",
            no_task="parse_csv_timesheet_data",
        )

        fail_dag_with_error=rail.FailOperator(
            task_id='fail_dag_with_error',
            message='''{{ (dag_run.conf.payrollbasereportresult | load_json_artifact).reportGenerationResults[0].error  }}'''
        )

        parse_csv_timesheet_data=rail.LoadCSVFileOperator(
            task_id='parse_csv_timesheet_data',
            document='{{(dag_run.conf.timesheetdatabasereportresult | load_json_artifact).reportGenerationResults[0].payload}}'
        )

        compose_csv_timesheetdata=rail.WriteCSVFileOperator(
            task_id='compose_csv_timesheetdata',
            source="{{ result('parse_csv_timesheet_data') }}",
            header=['User Supervisor Name',
                    'User Name',
                    'Timesheet Period',
                    'Approval Status',
                    'Total Hours',
                    'Total Time Off Hours',
                    'useruri',
                    'Company Miles',
                    'Personal Miles'],
            row=lambda item:[
                item['User Supervisor Name (Current)'],
                item['User Name'],
                item['Timesheet Period'],
                item['Approval Status'],
                item['Total Hours (In Period)'].replace(',','') if item['Total Hours (In Period)'] else 0,
                item['Total Time Off Hours (In Period)'].replace(',','') if item['Total Time Off Hours (In Period)'] else 0,
                item['useruri'],
                item['Company Miles'].replace(',','') if item['Company Miles'] else 0,
                item['Personal Miles'].replace(',','') if item['Personal Miles'] else 0,
            ],
        )

        create_collection_timesheetdata = rail.CreateCollectionOperator(
            task_id='create_collection_timesheetdata',
            source = "{{ result('compose_csv_timesheetdata') }}",
            name = "timesheetdata",
            columns = {
                'User Supervisor Name': 'usersupervisorname',
                'User Name': 'username',
                'Timesheet Period': 'timesheetperiod',
                'Approval Status': 'approvalstatus',
                'Total Hours': 'projecthrs',
                'Total Time Off Hours': 'timeoffhrs',
                'useruri':'useruri',
                'Company Miles': 'companymiles',
                'Personal Miles': 'personlamiles'
            }
        )

        query_distinct_data_from_timesheetdata=rail.QueryCollectionOperator(
            task_id='query_distinct_data_from_timesheetdata',
            query="""SELECT DISTINCT  timesheetdata.usersupervisorname,  timesheetdata.username,  timesheetdata.timesheetperiod,  timesheetdata.approvalstatus,
                    timesheetdata.projecthrs,  timesheetdata.timeoffhrs,  timesheetdata.useruri FROM  timesheetdata WHERE NULLIF(useruri,'') IS NOT NULL""",
        )

        query_data_with_companymiles_or_personalmiles=rail.QueryCollectionOperator(
            task_id='query_data_with_companymiles_or_personalmiles',
            query="""SELECT * FROM  timesheetdata WHERE  NULLIF(companymiles,'') IS NOT NULL OR  NULLIF(personlamiles,'') IS NOT NULL """,
        )

        parse_csv_paycode_data=rail.LoadCSVFileOperator(
            task_id='parse_csv_paycode_data',
            document='{{(dag_run.conf.payrollbasereportresult | load_json_artifact).reportGenerationResults[0].payload}}'
        )

        compose_csv_paycodedata=rail.WriteCSVFileOperator(
            task_id='compose_csv_paycodedata',
            source="{{ result('parse_csv_paycode_data') }}",
            header=['User Name',
                    'Timesheet Period',
                    'useruri',
                    'Budgeted Chargeability %',
                    'Chargeability %',
                    'Miles'],
            row=lambda item:[
                item['User Name'],
                item['Timesheet Period'],
                item['useruri'],
                item['Budgeted Chargeability %'].replace(',','') if item['Budgeted Chargeability %'] else 0,
                item['Chargeability %'].replace(',','') if item['Chargeability %'] else 0,
                item['Miles'].replace(',','') if item['Miles'] else 0,
            ],
        )

        create_collection_paycodedata = rail.CreateCollectionOperator(
            task_id='create_collection_paycodedata',
            source = "{{ result('compose_csv_paycodedata') }}",
            name = "paycodedata",
            columns = {
                'User Name':'username', 
                'Timesheet Period':'timesheetperiod', 
                'useruri':'useruri', 
                'Budgeted Chargeability %':'budgetedchargeability', 
                'Chargeability %':'chargeability', 
                'Miles':'miles'
            }
        )

        query_all_paycode_data=rail.QueryCollectionOperator(
            task_id='query_all_paycode_data',
            query="""SELECT * FROM  paycodedata""",
        )

        load_paycode_and_miles_data = rail.PythonOperator(
            task_id = 'load_paycode_and_miles_data',
            python_callable=lambda: {
                'paycodedata': rail.load_all_records(rail.result('query_all_paycode_data')),
                'milesdata': rail.load_all_records(rail.result('query_data_with_companymiles_or_personalmiles'))
            }
        )

        def get_required_keys_sum(data,key,useruri,timesheetperiod):
            total = 0
            for d in data:
                if d['useruri'] == useruri and d['timesheetperiod'] == timesheetperiod:
                    total+=float(d[key])
            return total

        compose_final_csv_to_be_sent=rail.WriteCSVFileOperator(
            task_id='compose_final_csv_to_be_sent',
            source="{{ result('query_distinct_data_from_timesheetdata') }}",
            header=['Supervisor Name',
                    'Employee Name',
                    'Timesheet Period',
                    'Timesheet Status',
                    'Hours',
                    'Paid Leave (include all time off types)',
                    'Budget Chrg %',
                    'Actual Chrg %',
                    'Company Miles',
                    'Personal Miles'],
            row=lambda item: [
                item['usersupervisorname'],
                item['username'],
                item['timesheetperiod'],
                item['approvalstatus'],
                float(item['projecthrs']) - float(item['timeoffhrs']),
                item['timeoffhrs'],
                get_required_keys_sum(rail.result(
                    'load_paycode_and_miles_data')['paycodedata'],'budgetedchargeability',item['useruri'],item['timesheetperiod']),
                get_required_keys_sum(rail.result('load_paycode_and_miles_data')['paycodedata'],'chargeability',item['useruri'],item['timesheetperiod']),
                float(get_required_keys_sum(rail.result('load_paycode_and_miles_data')['milesdata'],'companymiles',item['useruri'],item['timesheetperiod'])),
                float(get_required_keys_sum(rail.result('load_paycode_and_miles_data')['milesdata'],'personlamiles',item['useruri'],item['timesheetperiod']))
            ],
        )

        log_file_name=rail.PythonOperator(
            task_id='log_file_name',
            python_callable= lambda dag_run:  "Supervisor summary report_" + dag_run.conf['userid'] + "_" + datetime.now().strftime('%H%M%S') + ".csv"
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_final_csv_to_be_sent')}}",
            output_file_name="{{result('log_file_name')}}",
            expires_in_seconds=7*24*60*60,
        )

        send_success_mail=rail.EmailOperator(
            task_id='send_success_mail',
            to="{{dag_run.conf.emailid}}",
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }}| Supervisor summary report extract - {{ result('log_time_now') }}''',
            html_content= '''/templates/success_mail.html''',
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> log_user_first_name
        log_user_first_name >> log_time_now >> if_error_in_timesheetdatareport_result
        if_error_in_timesheetdatareport_result >> rail.Label('Yes')  >> fail_job_with_error >> finish
        if_error_in_timesheetdatareport_result >> rail.Label('No') >> if_report_payload_has_no_data
        if_report_payload_has_no_data >> rail.Label('Yes')  >> send_mail_no_datato_extract_forthe_givenperiod >> finish
        if_report_payload_has_no_data >> rail.Label('No') >> if_error_in_paycodedatareport_result
        if_error_in_paycodedatareport_result >> rail.Label('Yes') >> fail_dag_with_error >> finish
        if_error_in_paycodedatareport_result >> rail.Label('No') >> parse_csv_timesheet_data >> compose_csv_timesheetdata >> create_collection_timesheetdata
        create_collection_timesheetdata >> query_distinct_data_from_timesheetdata >> query_data_with_companymiles_or_personalmiles >> parse_csv_paycode_data
        parse_csv_paycode_data >> compose_csv_paycodedata >> create_collection_paycodedata >> query_all_paycode_data >> load_paycode_and_miles_data
        load_paycode_and_miles_data >> compose_final_csv_to_be_sent >> log_file_name >> generate_download_link >> send_success_mail >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
