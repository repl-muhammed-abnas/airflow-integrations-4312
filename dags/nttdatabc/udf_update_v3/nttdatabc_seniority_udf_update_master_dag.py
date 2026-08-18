
from datetime import timedelta, datetime
from pendulum import datetime as dt
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nttdatabc_seniority_udf_update_master_{config.instance}_v3',
        description=f'NTTDATABC Seniority UDF Update Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2023, 1, 1, tz=config.timezone),
        schedule_interval=config.schedule_interval_master,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_time_tobe_checked'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_time_tobe_checked',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_time_tobe_checked=rail.PythonOperator(
            task_id='get_time_tobe_checked',
            python_callable= lambda: (datetime.now()).strftime("%m/%d/%Y")
        )

        def get_datesplitted():
            date = datetime.strptime(rail.result('get_time_tobe_checked'),'%m/%d/%Y')
            return {
                'day': date.day,
                'month': date.month,
                'year': date.year
            }

        get_date_splitted=rail.PythonOperator(
            task_id='get_date_splitted',
            python_callable= get_datesplitted
        )

        get_uri_employee_approved_timesheets=rail.RepliconReportDetailsOperator(
            task_id='get_uri_employee_approved_timesheets',
            report_name=config.employee_approved_timesheets
        )

        get_uri_employee_pay_code_report=rail.RepliconReportDetailsOperator(
            task_id='get_uri_employee_pay_code_report',
            report_name=config.employee_pay_code_report
        )

        if_for_two_reports_uri_not_present=rail.IfOperator(
            task_id='if_for_two_reports_uri_not_present',
            test=lambda: not bool(rail.result('get_uri_employee_approved_timesheets')['uri'] and rail.result('get_uri_employee_pay_code_report')['uri']),
            yes_task="fail_with_report_not_present",
            no_task="if_column_order_doesnt_match",
        )

        def get_report_not_present_message():
            return "" if rail.result(
                'get_uri_employee_approved_timesheets')['uri'] else "**Employee Approved Timesheets is not present" + ("" if rail.result(
                'get_uri_employee_pay_code_report')['uri'] else ",**Employee Pay Code Report** is not present")

        fail_with_report_not_present=rail.FailOperator(
            task_id='fail_with_report_not_present',
            message=get_report_not_present_message
        )

        def get_column_order_and_compare(report,column_order):
            column_order_from_report = ','.join([ item['column']['displayText']
                                        for item in report['columnConfiguration']])
            return column_order_from_report != column_order

        if_column_order_doesnt_match=rail.IfOperator(
            task_id='if_column_order_doesnt_match',
            test=lambda: get_column_order_and_compare(rail.result('get_uri_employee_approved_timesheets'),config.co_employee_approved_timesheets),
            yes_task="fail_with_column_order_error",
            no_task="get_approval_status_and_approval_date_filter_uri",
        )

        fail_with_column_order_error=rail.FailOperator(
            task_id='fail_with_column_order_error',
            message='''Column order is not as pre-defined: **Employee Approved Timesheets'''
        )

        def get_filter_uris():
            return {
                'approvalstatus': rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_uri_employee_approved_timesheets')['filterConfiguration']['enabledFilters'],
                                    'displayText','ApprovalStatusFilter','uri',''),
                'approvaldate': rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_uri_employee_approved_timesheets')['filterConfiguration']['enabledFilters'],
                                    'displayText','ApprovalDateFilter','uri','')
            }

        get_approval_status_and_approval_date_filter_uri=rail.PythonOperator(
            task_id='get_approval_status_and_approval_date_filter_uri',
            python_callable=get_filter_uris
        )

        run_report_employee_approved_timesheets = rail.run_report2(
            group_id='run_report_employee_approved_timesheets',
            report_params= lambda dag_run: {
                "reportParameters": [
                    {
                    "reportUri": rail.result('get_uri_employee_approved_timesheets')['uri'],
                    "filterValues": [
                                        {
                                        "reportFilterUri": rail.result('get_approval_status_and_approval_date_filter_uri')['approvalstatus'],
                                        "value": "2"
                                        },
                                        {
                                        "reportFilterUri": rail.result('get_approval_status_and_approval_date_filter_uri')['approvaldate'],
                                        "value": null
                                        },
                                        {
                                        "reportFilterUri": rail.result('get_approval_status_and_approval_date_filter_uri')['approvaldate'],
                                        "value": dag_run.conf['start_date'] if 'start_date' in dag_run.conf else rail.result('get_time_tobe_checked')
                                        },
                                        {
                                        "reportFilterUri": rail.result('get_approval_status_and_approval_date_filter_uri')['approvaldate'],
                                        "value": dag_run.conf['end_date'] if 'end_date' in dag_run.conf else rail.result('get_time_tobe_checked')
                                        }
                                    ],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        if_payload_contains_no_data=rail.IfOperator(
            task_id='if_payload_contains_no_data',
            test='''{{ result('run_report_employee_approved_timesheets.get_report_result').reportGenerationResults[0].payload | matches('No Data')}}''',
            yes_task="finish",
            no_task="if_error_present",
        )

        if_error_present=rail.IfOperator(
            task_id='if_error_present',
            test='''{{ result('run_report_employee_approved_timesheets.get_report_result').reportGenerationResults[0].error | is_truthy }}''',
            yes_task="fail_error_in_report_result",
            no_task="if_columnorder_doesntmatch",
        )

        fail_error_in_report_result=rail.FailOperator(
            task_id='fail_error_in_report_result',
            message='''{{ result('run_report_employee_approved_timesheets.get_report_result').reportGenerationResults[0].error }}'''
        )

        if_columnorder_doesntmatch=rail.IfOperator(
            task_id='if_columnorder_doesntmatch',
            test= lambda: get_column_order_and_compare(rail.result('get_uri_employee_pay_code_report'),config.co_employee_pay_code),
            yes_task="failwith_columnorder_error",
            no_task="get_filter_uri_approval_date",
        )

        failwith_columnorder_error=rail.FailOperator(
            task_id='failwith_columnorder_error',
            message='''Column order is not as pre-defined: **Employee pay code report**'''
        )

        get_filter_uri_approval_date=rail.PythonOperator(
            task_id='get_filter_uri_approval_date',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(
                                        rail.result('get_uri_employee_pay_code_report')['filterConfiguration']['enabledFilters'],
                                        'displayText','ApprovalDateFilter','uri','')
        )

        run_report_employee_pay_code = rail.run_report2(
            group_id='run_report_employee_pay_code',
            report_params= lambda dag_run: {
                "reportParameters": [
                    {
                    "reportUri": rail.result('get_uri_employee_pay_code_report')['uri'],
                    "filterValues": [
                                        {
                                        "reportFilterUri": rail.result('get_filter_uri_approval_date'),
                                        "value": null
                                        },
                                        {
                                        "reportFilterUri": rail.result('get_filter_uri_approval_date'),
                                        "value": dag_run.conf['start_date'] if 'start_date' in dag_run.conf else rail.result('get_time_tobe_checked')
                                        },
                                        {
                                        "reportFilterUri": rail.result('get_filter_uri_approval_date'),
                                        "value": dag_run.conf['end_date'] if 'end_date' in dag_run.conf else rail.result('get_time_tobe_checked')
                                        }
                                    ],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        if_error_is_present=rail.IfOperator(
            task_id='if_error_is_present',
            test='''{{ result('run_report_employee_pay_code.get_report_result').reportGenerationResults[0].error | is_truthy }}''',
            yes_task="fail_with_report_error",
            no_task="get_all_custom_fields",
        )

        fail_with_report_error=rail.FailOperator(
            task_id='fail_with_report_error',
            message='''{{ result('run_report_employee_pay_code.get_report_result').reportGenerationResults[0].error }}'''
        )

        get_all_custom_fields=rail.RepliconServiceOperator(
            task_id='get_all_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            }
        )

        load_csv_data_employee_approved_timesheets=rail.LoadCSVFileOperator(
            task_id="load_csv_data_employee_approved_timesheets",
            document="{{result('run_report_employee_approved_timesheets.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_collection_employee_approved_timesheets = rail.CreateCollectionOperator(
            task_id='create_collection_employee_approved_timesheets',
            source = "{{ result('load_csv_data_employee_approved_timesheets') }}",
            name = "inputdatatimesheetapproved",
            columns = {
                'Timesheet Period':'timesheetperiod', 
                'User Name':'username', 
                'Login Name':'loginname', 
                'Activty Name':'activity', 
                'Hours Worked':'hoursworked', 
                'Timeoff Type':'timeofftype', 
                'Time Off Hrs':'timeoffhours', 
                'Total Hours (In Period)':'totalhoursinperiod', 
                'Approval Status':'approvalstatus', 
                'timesheeturi':'timesheeturi', 
                'useruri':'useruri',
                'Employee Type':'employeetype'
            }
        )

        load_csv_data_employee_pay_code=rail.LoadCSVFileOperator(
            task_id="load_csv_data_employee_pay_code",
            document="{{result('run_report_employee_pay_code.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_collection_employee_pay_code = rail.CreateCollectionOperator(
            task_id='create_collection_employee_pay_code',
            source = "{{ result('load_csv_data_employee_pay_code') }}",
            name = "inputdatapaycodehours",
            columns = {
                'Timesheet Period':'timesheetperiod', 
                'User Name':'username', 
                'Login Name':'loginname', 
                'Pay Code Name':'paycodename', 
                'Pay Code Code':'paycodecode', 
                'Approval Status':'approvalstatus', 
                'Pay Code Hours':'paycodehours', 
                'timesheet uri':'timesheeturi', 
                'useruri':'useruri'
            }
        )

        query_distinct_timesheets=rail.QueryCollectionOperator(
            task_id='query_distinct_timesheets',
            query="""SELECT DISTINCT  inputdatatimesheetapproved.timesheeturi, inputdatatimesheetapproved.employeetype, inputdatatimesheetapproved.loginname FROM  inputdatatimesheetapproved""",
        )

        get_seniority_udf_uri=rail.PythonOperator(
            task_id='get_seniority_udf_uri',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'),'displayText','Seniority','uri','')
        )

        process_each_distinct_timesheet = rail.TriggerDagRunForEachItemOperator(
            task_id = 'process_each_distinct_timesheet',
            retries = 0,
            items="{{ result('query_distinct_timesheets') }}",
            trigger_dag_id=f'nttdatabc_seniority_udf_update_process_distinct_timesheets_{config.instance}_v3',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "timesheeturi": item['timesheeturi'],
                "employeetype": item['employeetype'],
                "loginname": item['loginname'],
                'udfuri': rail.result('get_seniority_udf_uri'),
                'masterdagid': rail.render_template("{{dag_run_ecid()}}")
            }
        )

        if_there_are_distinct_timesheet_uris=rail.IfOperator(
            task_id='if_there_are_distinct_timesheet_uris',
            test="{{ result('query_distinct_timesheets','length') > 0 }}",
            yes_task="wait_for_child_dags",
            no_task="finish",
        )

        wait_for_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_distinct_timesheet") }}'
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id = 'gather_logs',
            dag_runs = "{{result('process_each_distinct_timesheet')}}",
            dagrun_task_id= 'create_logs_lookuptable',
            flatten=True
        )
        def load_records(log_artifact):
            try:
                logs = rail.load_all_records(log_artifact)
                return logs
            except:  # pylint: disable=bare-except
                return []

        def do_format_logs():
            log_records = []
            log_artifacts = rail.result('gather_logs')
            if log_artifacts:
                for log in log_artifacts:
                    each_log_records = load_records(log)
                    if each_log_records:
                        log_records.extend(each_log_records)
            return list(map(lambda x: {
                **dict(x['properties'].items()),
                **{
                    'jobid': x['ecid']
                }},log_records))

        format_logs = rail.PythonOperator(
            task_id = 'format_logs',
            python_callable= do_format_logs
        )

        if_entry_present=rail.IfOperator(
            task_id='if_entry_present',
            test='''{{ result('format_logs') | is_truthy }}''',
            yes_task="compose_csv_logs",
            no_task="finish",
        )

        compose_csv_logs=rail.WriteCSVFileOperator(
            task_id='compose_csv_logs',
            source=lambda: rail.result('format_logs'),
            header=['loginname',
                    'timesheetperiod',
                    'Totalhours',
                    'EmployeeType',
                    'finalvalue',
                    'Approvalstatus',
                    'status',
                    'details',
                    'jobid'],
            row= [
                    "{{ item.loginname }}",
                    "{{ item.timesheetperiod }}",
                    "{{ item.totalhours }}",
                    "{{ item.employeetype }}",
                    "{{ item.finalvalue }}",
                    "{{ item.approvalstatus }}",
                    "{{ item.status }}",
                    "{{ item.details }}",
                    "{{ item.jobid }}|{{ item.childjob }}"
                ],
        )

        upload_logs_to_sftp =rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content="{{ result('compose_csv_logs') }}",
            remote_filepath=config.upload_filepath + 'SeniorityUDFimport_' +
                                "{{result('get_date_splitted').day}}" + "{{result('get_date_splitted').month}}" +
                                "{{result('get_date_splitted').year}}" + '.csv',
        )

        is_entry_with_error_present=rail.IfOperator(
            task_id='is_entry_with_error_present',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(rail.result('format_logs'),'status','Error','status','')),
            yes_task="send_error_mail",
            no_task="finish",
        )

        send_error_mail=rail.EmailOperator(
            task_id='send_error_mail',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            subject="{{get_company_key()}}" + "| Seniority UDF import - Completed with errors {{current_time()}}",
            html_content='templates/error_mail.html',
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_time_tobe_checked
        get_time_tobe_checked >> get_date_splitted >> get_uri_employee_approved_timesheets
        get_uri_employee_approved_timesheets >> get_uri_employee_pay_code_report >> if_for_two_reports_uri_not_present
        if_for_two_reports_uri_not_present >> rail.Label('Yes')  >> fail_with_report_not_present >> finish
        if_for_two_reports_uri_not_present >> rail.Label('No') >> if_column_order_doesnt_match
        if_column_order_doesnt_match >> rail.Label('Yes')  >> fail_with_column_order_error >> finish
        if_column_order_doesnt_match >> rail.Label(
            'No') >> get_approval_status_and_approval_date_filter_uri >> run_report_employee_approved_timesheets >> if_payload_contains_no_data
        if_payload_contains_no_data >> rail.Label('Yes')  >> finish
        if_payload_contains_no_data >> rail.Label('No') >> if_error_present
        if_error_present >> rail.Label('Yes')  >> fail_error_in_report_result >> finish
        if_error_present >> rail.Label('No') >> if_columnorder_doesntmatch
        if_columnorder_doesntmatch >> rail.Label('Yes')  >> failwith_columnorder_error >> finish
        if_columnorder_doesntmatch >> rail.Label('No') >> get_filter_uri_approval_date >> run_report_employee_pay_code >> if_error_is_present
        if_error_is_present >> rail.Label('Yes')  >> fail_with_report_error >> finish
        if_error_is_present >> rail.Label(
            'No') >> get_all_custom_fields >> load_csv_data_employee_approved_timesheets >> create_collection_employee_approved_timesheets
        create_collection_employee_approved_timesheets >> load_csv_data_employee_pay_code >> create_collection_employee_pay_code >> query_distinct_timesheets
        query_distinct_timesheets >> get_seniority_udf_uri >> process_each_distinct_timesheet >> if_there_are_distinct_timesheet_uris
        if_there_are_distinct_timesheet_uris >> rail.Label('Yes')  >> wait_for_child_dags >> gather_logs >> format_logs >> if_entry_present
        if_entry_present >> rail.Label('Yes')  >> compose_csv_logs >> upload_logs_to_sftp >> is_entry_with_error_present
        is_entry_with_error_present >> rail.Label('Yes')  >> send_error_mail >> finish
        is_entry_with_error_present >> rail.Label('No') >> finish
        if_entry_present >> rail.Label('No') >> finish
        if_there_are_distinct_timesheet_uris >> rail.Label('No') >> finish >> dagrun_log_to_sumo

    return dag

rail.for_each_instance(create_dag)
