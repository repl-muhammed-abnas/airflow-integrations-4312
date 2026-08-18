
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'crjg3_normalized_report_export_custom_actuals_vs_normalized_report_child_{config.instance}',
        description=f'CRJ_Custom Actuals vs Normalized Report_child{config.instance}',
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
            no_task='log_user_first_name_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_user_first_name_4',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_user_first_name_4=rail.PythonOperator(
            task_id='log_user_first_name_4',
            python_callable= lambda dag_run:  ((dag_run.conf['username'].split("|"))[0]).strip()
        )

        log_time_now_5=rail.PythonOperator(
            task_id='log_time_now_5',
            python_callable= lambda:  datetime.now().strftime("%m/%d/%YT%H:%M:%S")
        )

        if_error_in_report_result=rail.IfOperator(
            task_id='if_error_in_report_result',
            test='''{{ (dag_run.conf.reportresult | load_json_artifact).reportGenerationResults[0].error | is_truthy }}''',
            yes_task="fail_with_error",
            no_task="if_first_payload_contains_nodata_11",
        )

        fail_with_error=rail.FailOperator(
            task_id='fail_with_error',
            message='''{{ (dag_run.conf.reportresult | load_json_artifact).reportGenerationResults[0].error }}'''
        )

        if_first_payload_contains_nodata_11=rail.IfOperator(
            task_id='if_first_payload_contains_nodata_11',
            test='''{{ (dag_run.conf.reportresult | load_json_artifact).reportGenerationResults[0].payload | matches('No Data') }}''',
            yes_task="send_mail_no_datato_extractforthegivenperiod_12",
            no_task="load_csv_from_report_data",
        )

        send_mail_no_datato_extractforthegivenperiod_12=rail.EmailOperator(
            task_id='send_mail_no_datato_extractforthegivenperiod_12',
            to="{{dag_run.conf.emailid}}",
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} |  Custom actuals vs normalized report extract  - {{ result('log_time_now_5') }} ''',
            html_content= '''templates/no_data_to_extract_mail.html''',
        )

        load_csv_from_report_data=rail.LoadCSVFileOperator(
            task_id="load_csv_from_report_data",
            document="{{(dag_run.conf.reportresult | load_json_artifact).reportGenerationResults[0].payload}}",
        )

        create_collection_rawnormalizedhourreport = rail.CreateCollectionOperator(
            task_id='create_collection_rawnormalizedhourreport',
            source = "{{ result('load_csv_from_report_data') }}",
            name = "rawnormalizedhourreport",
           # fixme update this map from actual csv header for key name
            columns = {
                'User Name':'username', 
                'Project Name':'projectname', 
                'Project Code':'projectcode', 
                'Timesheet Period':'timesheetperiod', 
                'Entry Date':'entrydate', 
                'Hours Worked':'hoursworked', 
                'Week (Entry Date)':'weekentrydate', 
                'Normalization Required?':'normalizationrequired', 
                'Contract Type':'reimbursementtype', 
                'Login Name':'loginname', 
                'Employee Category':'employeecategory', 
                'Time Type Name (Full Path)':'taskname', 
                'Time Type Code':'taskcode'
            }
        )

        write_csv_for_normalizeddata=rail.WriteCSVFileOperator(
            task_id='write_csv_for_normalizeddata',
            source="{{ result('load_csv_from_report_data') }}",
            header=['Username',
                    'Projectname',
                    'Projectcode',
                    'Timesheetperiod',
                    'Entrydate',
                    'Hoursworked',
                    'Weekentrydate',
                    'Normalizationrequired',
                    'Reimbursementtype',
                    'Loginname',
                    'Employeecategory',
                    'Taskname',
                    'Taskcode'],
            row=lambda item: [
                item['User Name'],
                item['Project Name'],
                item['Project Code'],
                item['Timesheet Period'],
                datetime.strptime(item['Entry Date'],'%b %d, %Y').strftime('%Y-%m-%d'),
                float(item['Hours Worked']),
                datetime.strptime(item['Entry Date'],'%b %d, %Y').isocalendar().week + 1 if
                datetime.strptime(item['Entry Date'],'%b %d, %Y').weekday() == 6 else datetime.strptime(item['Entry Date'],'%b %d, %Y').isocalendar().week,
                item['Normalization Required?'],
                item['Contract Type'],
                item['Login Name'],
                item['Employee Category'],
                item['Time Type Name (Full Path)'],
                item['Time Type Code']
            ],
        )

        declare_finaldataperperweek_list=rail.SetVariableOperator(
            task_id='declare_finaldataperperweek_list',
            append=False,
            name='finaldataperperweek',
            value=[]
        )

        create_collection_normalizedhourreport = rail.CreateCollectionOperator(
            task_id='create_collection_normalizedhourreport',
            source = "{{ result('write_csv_for_normalizeddata') }}",
            name = "normalizedhourreport",
            columns = {
                'Username':'username', 
                'Projectname':'projectname', 
                'Projectcode':'projectcode', 
                'Timesheetperiod':'timesheetperiod', 
                'Entrydate':'entrydate', 
                'Hoursworked':'hoursworked', 
                'Weekentrydate':'weekentrydate', 
                'Normalizationrequired':'normalizationrequired', 
                'Reimbursementtype':'reimbursementtype', 
                'Loginname':'loginname', 
                'Employeecategory':'employeecategory', 
                'Taskname':'taskname', 
                'Taskcode':'taskcode'
            }
        )

        query_list_distinct_weeksinexport_21=rail.QueryCollectionOperator(
            task_id='query_list_distinct_weeksinexport_21',
            query="""SELECT DISTINCT  normalizedhourreport.weekentrydate,  normalizedhourreport.loginname,
                normalizedhourreport.employeecategory FROM normalizedhourreport""",
        )

        foreach_query_list_distinct_weeksinexport_21_22=rail.ForEachOperator(
            task_id='foreach_query_list_distinct_weeksinexport_21_22',
            items=lambda: rail.load_all_records(rail.result('query_list_distinct_weeksinexport_21')),
            start_task = 'query_list_datarelatedtodistinctweeks_23',
            end_task = 'foreach_query_list_distinct_weeksinexport_21_22_end'
        )

        query_list_datarelatedtodistinctweeks_23=rail.QueryCollectionOperator(
            task_id='query_list_datarelatedtodistinctweeks_23',
            query="""SELECT  normalizedhourreport.username,  normalizedhourreport.projectname,  normalizedhourreport.projectcode,
                normalizedhourreport.timesheetperiod,  normalizedhourreport.hoursworked,  normalizedhourreport.reimbursementtype,
                normalizedhourreport.normalizationrequired,  normalizedhourreport.taskname FROM  normalizedhourreport WHERE
                normalizedhourreport.weekentrydate='{{ result('foreach_query_list_distinct_weeksinexport_21_22').weekentrydate }}' AND
                normalizedhourreport.loginname='{{ result('foreach_query_list_distinct_weeksinexport_21_22').loginname }}'""",
        )

        if_foreach_query_list_distinct_weeksinexport_21_22_employeecategory_equals_to_nonexempt_24=rail.IfOperator(
            task_id='if_foreach_query_list_distinct_weeksinexport_21_22_employeecategory_equals_to_nonexempt_24',
            test='''{{ result('foreach_query_list_distinct_weeksinexport_21_22').employeecategory == 'Non-Exempt' }}''',
            yes_task="query_list_datarelatedtodistinctweeks_25",
            no_task="query_list_totalhoursfordistinctweeks_29",
        )

        query_list_datarelatedtodistinctweeks_25=rail.QueryCollectionOperator(
            task_id='query_list_datarelatedtodistinctweeks_25',
            query="""SELECT DISTINCT  normalizedhourreport.username,  normalizedhourreport.projectname,  normalizedhourreport.projectcode,
                normalizedhourreport.timesheetperiod,  normalizedhourreport.reimbursementtype,  normalizedhourreport.taskname FROM  normalizedhourreport WHERE
                normalizedhourreport.weekentrydate='{{ result('foreach_query_list_distinct_weeksinexport_21_22').weekentrydate }}' AND
                normalizedhourreport.loginname='{{ result('foreach_query_list_distinct_weeksinexport_21_22').loginname }}'""",
        )

        foreach_query_list_datarelatedtodistinctweeks_25_26=rail.ForEachOperator(
            task_id='foreach_query_list_datarelatedtodistinctweeks_25_26',
            items=lambda: rail.load_all_records(rail.result('query_list_datarelatedtodistinctweeks_25')),
            start_task = 'insert_to_list_27',
            end_task = 'foreach_query_list_datarelatedtodistinctweeks_25_26_end'
        )

        def get_hours_sum(all_data,user):
            full_data = rail.load_all_records(all_data)
            sum_of_hours = 0
            for data in full_data:
                if (data['username'] == user['username'] and data['projectname'] == user['projectname'] and data['projectcode'] == user['projectcode']):
                    if (data['timesheetperiod'] == user['timesheetperiod'] and data['reimbursementtype'] == user['reimbursementtype'] and
                        data['taskname'] == user['taskname']):
                        sum_of_hours+=float(data['hoursworked'])
            return sum_of_hours

        insert_to_list_27=rail.SetVariableOperator(
            task_id='insert_to_list_27',
            append=True,
            name='{{ result("declare_finaldataperperweek_list").name }}',
            value=lambda: {
                "week": rail.result('foreach_query_list_distinct_weeksinexport_21_22')['weekentrydate'],
                "timesheetperiod": rail.result('foreach_query_list_datarelatedtodistinctweeks_25_26')['timesheetperiod'],
                "reimbursementtype": rail.result('foreach_query_list_datarelatedtodistinctweeks_25_26')['reimbursementtype'],
                "name": rail.result('foreach_query_list_datarelatedtodistinctweeks_25_26')['username'],
                "projectcode": rail.result('foreach_query_list_datarelatedtodistinctweeks_25_26')['projectcode'],
                "projectname": rail.result('foreach_query_list_datarelatedtodistinctweeks_25_26')['projectname'],
                "hours": get_hours_sum(rail.result(
                    'query_list_datarelatedtodistinctweeks_23'),rail.result('foreach_query_list_datarelatedtodistinctweeks_25_26')),
                "actualhours": get_hours_sum(rail.result(
                    'query_list_datarelatedtodistinctweeks_23'),rail.result('foreach_query_list_datarelatedtodistinctweeks_25_26')),
                "taskname": rail.result('foreach_query_list_datarelatedtodistinctweeks_25_26')['taskname']
            }
        )

        foreach_query_list_datarelatedtodistinctweeks_25_26_end=rail.EmptyOperator(
            task_id='foreach_query_list_datarelatedtodistinctweeks_25_26_end',
        )

        query_list_totalhoursfordistinctweeks_29=rail.QueryCollectionOperator(
            task_id='query_list_totalhoursfordistinctweeks_29',
            query="""SELECT SUM( normalizedhourreport.hoursworked) as totalhoursinweek FROM  normalizedhourreport WHERE
                normalizedhourreport.weekentrydate='{{ result('foreach_query_list_distinct_weeksinexport_21_22').weekentrydate }}' AND
                normalizedhourreport.loginname='{{ result('foreach_query_list_distinct_weeksinexport_21_22').loginname }}'""",
        )

        def get_float_hours(query_result):
            query_hours = rail.load_all_records(query_result)
            hours = query_hours[0]['totalhoursinweek'] if query_hours else 0
            return float(hours) if hours else 0

        get_totalhoursfordistinctweeks_29 = rail.PythonOperator(
            task_id = 'get_totalhoursfordistinctweeks_29',
            python_callable=lambda: get_float_hours(rail.result('query_list_totalhoursfordistinctweeks_29'))
        )

        if_totalhoursinweek_greater_than_40=rail.IfOperator(
            task_id='if_totalhoursinweek_greater_than_40',
            test=lambda: rail.result('get_totalhoursfordistinctweeks_29') > 40,
            yes_task="log_checkifnormalizationrequiredprojectisthere_31",
            no_task="query_list_datarelatedtodistinctweeks_67",
        )

        log_checkifnormalizationrequiredprojectisthere_31=rail.PythonOperator(
            task_id='log_checkifnormalizationrequiredprojectisthere_31',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.load_all_records(rail.result(
                'query_list_datarelatedtodistinctweeks_23')),'normalizationrequired','Yes','normalizationrequired','')
        )

        if_log_checkifnormalizationrequiredprojectisthere_31_present_32=rail.IfOperator(
            task_id='if_log_checkifnormalizationrequiredprojectisthere_31_present_32',
            test='''{{ result('log_checkifnormalizationrequiredprojectisthere_31') | is_truthy }}''',
            yes_task="query_list_total_hoursfordistinctweekswith_normalization_33",
            no_task="query_list_total_datafordistinctweekwith_excludedtime_types_57",
        )

        query_list_total_hoursfordistinctweekswith_normalization_33=rail.QueryCollectionOperator(
            task_id='query_list_total_hoursfordistinctweekswith_normalization_33',
            query="""SELECT SUM( normalizedhourreport.hoursworked) as totalhoursinweek FROM  normalizedhourreport WHERE
                normalizedhourreport.weekentrydate='{{ result('foreach_query_list_distinct_weeksinexport_21_22').weekentrydate }}' AND
                normalizedhourreport.normalizationrequired='Yes' AND
                normalizedhourreport.loginname='{{ result('foreach_query_list_distinct_weeksinexport_21_22').loginname }}' AND
                normalizedhourreport.taskcode!='02 Floating Holiday' AND  normalizedhourreport.taskcode!='04 Sick' AND
                normalizedhourreport.taskcode!='7H Holiday' AND  normalizedhourreport.taskcode!='03 Vacation'""",
        )

        get_total_hoursfordistinctweekswith_normalization_33 = rail.PythonOperator(
            task_id = 'get_total_hoursfordistinctweekswith_normalization_33',
            python_callable=lambda: get_float_hours(rail.result('query_list_total_hoursfordistinctweekswith_normalization_33'))
        )

        query_list_total_datafordistinctweekwith_excludedtime_types_34=rail.QueryCollectionOperator(
            task_id='query_list_total_datafordistinctweekwith_excludedtime_types_34',
            query="""SELECT SUM( normalizedhourreport.hoursworked) as totalhoursinweek FROM  normalizedhourreport WHERE
                normalizedhourreport.weekentrydate='{{ result('foreach_query_list_distinct_weeksinexport_21_22').weekentrydate }}' AND
                normalizedhourreport.loginname='{{ result('foreach_query_list_distinct_weeksinexport_21_22').loginname }}' AND
                ( normalizedhourreport.taskcode='02 Floating Holiday' OR  normalizedhourreport.taskcode='04 Sick' OR
                normalizedhourreport.taskcode='7H Holiday' OR  normalizedhourreport.taskcode='03 Vacation')""",
        )

        get_total_datafordistinctweekwith_excludedtime_types_34 = rail.PythonOperator(
            task_id = 'get_total_datafordistinctweekwith_excludedtime_types_34',
            python_callable=lambda: get_float_hours(rail.result('query_list_total_datafordistinctweekwith_excludedtime_types_34'))
        )

        query_list_datarelatedtodistinctweekwith_excludedtime_types_35=rail.QueryCollectionOperator(
            task_id='query_list_datarelatedtodistinctweekwith_excludedtime_types_35',
            query="""SELECT  normalizedhourreport.username,  normalizedhourreport.projectname,  normalizedhourreport.projectcode,
                normalizedhourreport.timesheetperiod,  normalizedhourreport.hoursworked,  normalizedhourreport.reimbursementtype,
                normalizedhourreport.taskname  FROM  normalizedhourreport WHERE
                normalizedhourreport.weekentrydate='{{ result('foreach_query_list_distinct_weeksinexport_21_22').weekentrydate }}' AND
                normalizedhourreport.loginname='{{ result('foreach_query_list_distinct_weeksinexport_21_22').loginname }}' AND
                ( normalizedhourreport.taskcode='02 Floating Holiday' OR  normalizedhourreport.taskcode='04 Sick' OR
                normalizedhourreport.taskcode='7H Holiday' OR  normalizedhourreport.taskcode='03 Vacation')""",
        )

        foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36=rail.ForEachOperator(
            task_id='foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36',
            items=lambda: rail.load_all_records(rail.result('query_list_datarelatedtodistinctweekwith_excludedtime_types_35')),
            start_task = 'insert_to_list_37',
            end_task = 'foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36_end'
        )

        insert_to_list_37=rail.SetVariableOperator(
            task_id='insert_to_list_37',
            append=True,
            name='{{ result("declare_finaldataperperweek_list").name }}',
            value=lambda:{
                "week": rail.result('foreach_query_list_distinct_weeksinexport_21_22')['weekentrydate'],
                "timesheetperiod": rail.result('foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36')['timesheetperiod'],
                "reimbursementtype": rail.result('foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36')['reimbursementtype'],
                "name": rail.result('foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36')['username'],
                "projectcode": rail.result('foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36')['projectcode'],
                "projectname": rail.result('foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36')['projectname'],
                "hours": float(rail.result('foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36')['hoursworked']),
                "actualhours": float(rail.result('foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36')['hoursworked']),
                "taskname": rail.result('foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36')['taskname']
            }
        )

        foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36_end=rail.EmptyOperator(
            task_id='foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36_end',
        )

        query_list_datarelatedtodistinctweekswith_normalization_38=rail.QueryCollectionOperator(
            task_id='query_list_datarelatedtodistinctweekswith_normalization_38',
            # fixme use NULLIF(col_name,'') for IS NULL or IS NOT NULL where clause
            query="""SELECT  normalizedhourreport.username,  normalizedhourreport.projectname,  normalizedhourreport.projectcode,
                normalizedhourreport.timesheetperiod,  normalizedhourreport.hoursworked,  normalizedhourreport.reimbursementtype,
                normalizedhourreport.taskname FROM  normalizedhourreport WHERE
                normalizedhourreport.weekentrydate='{{ result('foreach_query_list_distinct_weeksinexport_21_22').weekentrydate }}' AND
                normalizedhourreport.normalizationrequired='Yes' AND
                normalizedhourreport.loginname='{{ result('foreach_query_list_distinct_weeksinexport_21_22').loginname }}' AND
                normalizedhourreport.taskcode!='02 Floating Holiday' AND  normalizedhourreport.taskcode!='04 Sick' AND
                normalizedhourreport.taskcode!='7H Holiday' AND  normalizedhourreport.taskcode!='03 Vacation'""",
        )

        def get_exhaustive_normalization():
            totalhoursinweek_with_normalization = rail.result('get_total_hoursfordistinctweekswith_normalization_33')
            totalhoursinweek = rail.result('get_totalhoursfordistinctweeks_29')
            return totalhoursinweek_with_normalization - (totalhoursinweek - 40) if (totalhoursinweek_with_normalization - (totalhoursinweek - 40)) > 0 else 0

        log_exhaustive_normalization_39=rail.PythonOperator(
            task_id='log_exhaustive_normalization_39',
            python_callable= get_exhaustive_normalization
        )

        query_list_final_datarelatedtodistinctweekswith_normalization_40=rail.QueryCollectionOperator(
            task_id='query_list_final_datarelatedtodistinctweekswith_normalization_40',
            query="""SELECT DISTINCT  normalizedhourreport.username,  normalizedhourreport.projectname,  normalizedhourreport.projectcode,
                normalizedhourreport.timesheetperiod,  normalizedhourreport.reimbursementtype,  normalizedhourreport.taskname FROM
                normalizedhourreport WHERE  normalizedhourreport.weekentrydate='{{ result('foreach_query_list_distinct_weeksinexport_21_22').weekentrydate }}' AND
                normalizedhourreport.normalizationrequired='Yes' AND
                normalizedhourreport.loginname='{{ result('foreach_query_list_distinct_weeksinexport_21_22').loginname }}' AND
                normalizedhourreport.taskcode!='02 Floating Holiday' AND  normalizedhourreport.taskcode!='04 Sick' AND
                normalizedhourreport.taskcode!='7H Holiday' AND  normalizedhourreport.taskcode!='03 Vacation'""",
        )

        def get_exhaustive_normalization_per_project():
            totalhoursinweek_with_normalization = rail.result('get_total_hoursfordistinctweekswith_normalization_33')
            totalhoursinweek = rail.result('get_totalhoursfordistinctweeks_29')
            list_size = rail.result('query_list_final_datarelatedtodistinctweekswith_normalization_40','length')
            return float(rail.result('log_exhaustive_normalization_39')) / float(list_size) if (
                totalhoursinweek_with_normalization - (totalhoursinweek - 40)) > 0 else 0

        log_exhaustive_normalization_per_project_41=rail.PythonOperator(
            task_id='log_exhaustive_normalization_per_project_41',
            python_callable= get_exhaustive_normalization_per_project
        )

        query_list_totaldatafordistinctweekswithout_normalization_42=rail.QueryCollectionOperator(
            task_id='query_list_totaldatafordistinctweekswithout_normalization_42',
            query="""SELECT SUM( normalizedhourreport.hoursworked) as totalhoursinweek FROM  normalizedhourreport WHERE
                normalizedhourreport.weekentrydate='{{ result('foreach_query_list_distinct_weeksinexport_21_22').weekentrydate }}' AND
                normalizedhourreport.normalizationrequired='No' AND
                normalizedhourreport.loginname='{{ result('foreach_query_list_distinct_weeksinexport_21_22').loginname }}' AND
                normalizedhourreport.taskcode!='02 Floating Holiday' AND  normalizedhourreport.taskcode!='04 Sick' AND
                normalizedhourreport.taskcode!='7H Holiday' AND  normalizedhourreport.taskcode!='03 Vacation'""",
        )

        get_totaldatafordistinctweekswithout_normalization_42 = rail.PythonOperator(
            task_id = 'get_totaldatafordistinctweekswithout_normalization_42',
            python_callable=lambda: get_float_hours(rail.result('query_list_totaldatafordistinctweekswithout_normalization_42'))
        )

        query_list_datarelatedtodistinctweekswithout_normalization_43=rail.QueryCollectionOperator(
            task_id='query_list_datarelatedtodistinctweekswithout_normalization_43',
            query="""SELECT  normalizedhourreport.username,  normalizedhourreport.projectname,  normalizedhourreport.projectcode,
                normalizedhourreport.timesheetperiod,  normalizedhourreport.hoursworked,  normalizedhourreport.reimbursementtype,
                normalizedhourreport.taskname FROM  normalizedhourreport WHERE
                normalizedhourreport.weekentrydate='{{ result('foreach_query_list_distinct_weeksinexport_21_22').weekentrydate }}' AND
                normalizedhourreport.normalizationrequired='No' AND
                normalizedhourreport.loginname='{{ result('foreach_query_list_distinct_weeksinexport_21_22').loginname }}' AND
                normalizedhourreport.taskcode!='02 Floating Holiday' AND  normalizedhourreport.taskcode!='04 Sick' AND
                normalizedhourreport.taskcode!='7H Holiday' AND  normalizedhourreport.taskcode!='03 Vacation'""",
        )

        foreach_final_datarelatedtodistinctweekswith_normalization_40_44=rail.ForEachOperator(
            task_id='foreach_final_datarelatedtodistinctweekswith_normalization_40_44',
            items=lambda: rail.load_all_records(rail.result('query_list_final_datarelatedtodistinctweekswith_normalization_40')),
            start_task = 'insert_to_list_45',
            end_task = 'foreach_final_datarelatedtodistinctweekswith_normalization_40_44_end'
        )

        insert_to_list_45=rail.SetVariableOperator(
            task_id='insert_to_list_45',
            append=True,
            name='{{ result("declare_finaldataperperweek_list").name }}',
            value=lambda: {
                "week": rail.result('foreach_query_list_distinct_weeksinexport_21_22')['weekentrydate'],
                "timesheetperiod": rail.result('foreach_final_datarelatedtodistinctweekswith_normalization_40_44')['timesheetperiod'],
                "reimbursementtype": rail.result('foreach_final_datarelatedtodistinctweekswith_normalization_40_44')['reimbursementtype'],
                "name": rail.result('foreach_final_datarelatedtodistinctweekswith_normalization_40_44')['username'],
                "projectcode": rail.result('foreach_final_datarelatedtodistinctweekswith_normalization_40_44')['projectcode'],
                "projectname": rail.result('foreach_final_datarelatedtodistinctweekswith_normalization_40_44')['projectname'],
                "hours": float(rail.result('log_exhaustive_normalization_per_project_41')),
                "actualhours": get_hours_sum(rail.result(
                    'query_list_datarelatedtodistinctweekswith_normalization_38'),rail.result(
                    'foreach_final_datarelatedtodistinctweekswith_normalization_40_44')),
                "taskname": rail.result('foreach_final_datarelatedtodistinctweekswith_normalization_40_44')['taskname']
            }
        )

        foreach_final_datarelatedtodistinctweekswith_normalization_40_44_end=rail.EmptyOperator(
            task_id='foreach_final_datarelatedtodistinctweekswith_normalization_40_44_end',
        )

        def check_criteria_of_hours():
            totalhoursinweek_withoutnormalization = rail.result('get_totaldatafordistinctweekswithout_normalization_42')
            totalhoursfor_excluded_timetypes = rail.result('get_total_datafordistinctweekwith_excludedtime_types_34')
            exhaustive_normalization = float(rail.result('log_exhaustive_normalization_39'))
            return (totalhoursinweek_withoutnormalization + totalhoursfor_excluded_timetypes + exhaustive_normalization) > 40

        if_to_f_greater_than_40_46=rail.IfOperator(
            task_id='if_to_f_greater_than_40_46',
            test=check_criteria_of_hours,
            yes_task="log_normal_normalization_47",
            no_task="query_list_final_datarelatedtodistinctweekswithout_normalization_53",
        )

        log_normal_normalization_47=rail.PythonOperator(
            task_id='log_normal_normalization_47',
            python_callable= lambda: rail.result('get_totaldatafordistinctweekswithout_normalization_42') +
                                float(rail.result('log_exhaustive_normalization_39')) -
                                ( 40 - rail.result('get_total_datafordistinctweekwith_excludedtime_types_34'))
        )

        log_normal_normalizationperproject_48=rail.PythonOperator(
            task_id='log_normal_normalizationperproject_48',
            python_callable= lambda: (40 - rail.result('get_total_datafordistinctweekwith_excludedtime_types_34')) / ( rail.result(
                                'get_totaldatafordistinctweekswithout_normalization_42') + float(rail.result('log_exhaustive_normalization_39')))
        )

        query_list_final_datarelatedtodistinctweekswithout_normalization_49=rail.QueryCollectionOperator(
            task_id='query_list_final_datarelatedtodistinctweekswithout_normalization_49',
            query="""SELECT DISTINCT  normalizedhourreport.username,  normalizedhourreport.projectname,  normalizedhourreport.projectcode,
                normalizedhourreport.timesheetperiod,  normalizedhourreport.reimbursementtype,  normalizedhourreport.taskname FROM
                normalizedhourreport WHERE
                normalizedhourreport.weekentrydate='{{ result('foreach_query_list_distinct_weeksinexport_21_22').weekentrydate }}' AND
                normalizedhourreport.normalizationrequired='No' AND
                normalizedhourreport.loginname='{{ result('foreach_query_list_distinct_weeksinexport_21_22').loginname }}' AND
                normalizedhourreport.taskcode!='02 Floating Holiday' AND  normalizedhourreport.taskcode!='04 Sick' AND
                normalizedhourreport.taskcode!='7H Holiday' AND  normalizedhourreport.taskcode!='03 Vacation'""",
        )

        foreach_final_datarelatedtodistinctweekswithout_normalization_49_50=rail.ForEachOperator(
            task_id='foreach_final_datarelatedtodistinctweekswithout_normalization_49_50',
            items=lambda: rail.load_all_records(rail.result('query_list_final_datarelatedtodistinctweekswithout_normalization_49')),
            start_task = 'insert_to_list_51',
            end_task = 'foreach_final_datarelatedtodistinctweekswithout_normalization_49_50_end'
        )

        insert_to_list_51=rail.SetVariableOperator(
            task_id='insert_to_list_51',
            append=True,
            name='{{ result("declare_finaldataperperweek_list").name }}',
            value=lambda:{
                "week": rail.result('foreach_query_list_distinct_weeksinexport_21_22')['weekentrydate'],
                "timesheetperiod": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_49_50')['timesheetperiod'],
                "reimbursementtype": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_49_50')['reimbursementtype'],
                "name": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_49_50')['username'],
                "projectcode": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_49_50')['projectcode'],
                "projectname": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_49_50')['projectname'],
                "hours": float(get_hours_sum(rail.result('query_list_datarelatedtodistinctweekswithout_normalization_43'),rail.result(
                    'foreach_final_datarelatedtodistinctweekswithout_normalization_49_50'))) * float(rail.result(
                        'log_normal_normalizationperproject_48')),
                "actualhours": float(get_hours_sum(rail.result('query_list_datarelatedtodistinctweekswithout_normalization_43'),rail.result(
                    'foreach_final_datarelatedtodistinctweekswithout_normalization_49_50'))),
                "taskname": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_49_50')['taskname']
            }
        )

        foreach_final_datarelatedtodistinctweekswithout_normalization_49_50_end=rail.EmptyOperator(
            task_id='foreach_final_datarelatedtodistinctweekswithout_normalization_49_50_end',
        )

        query_list_final_datarelatedtodistinctweekswithout_normalization_53=rail.QueryCollectionOperator(
            task_id='query_list_final_datarelatedtodistinctweekswithout_normalization_53',
            query="""SELECT DISTINCT  normalizedhourreport.username,  normalizedhourreport.projectname,  normalizedhourreport.projectcode,
                normalizedhourreport.timesheetperiod,  normalizedhourreport.reimbursementtype,  normalizedhourreport.taskname FROM  normalizedhourreport WHERE
                normalizedhourreport.weekentrydate='{{ result('foreach_query_list_distinct_weeksinexport_21_22').weekentrydate }}' AND
                normalizedhourreport.normalizationrequired='No' AND
                normalizedhourreport.loginname='{{ result('foreach_query_list_distinct_weeksinexport_21_22').loginname }}' AND
                normalizedhourreport.taskcode!="02 Floating Holiday" AND  normalizedhourreport.taskcode!="04 Sick" AND
                normalizedhourreport.taskcode!="7H Holiday" AND  normalizedhourreport.taskcode!="03 Vacation" """,
        )

        foreach_final_datarelatedtodistinctweekswithout_normalization_53_54=rail.ForEachOperator(
            task_id='foreach_final_datarelatedtodistinctweekswithout_normalization_53_54',
            items=lambda: rail.load_all_records(rail.result('query_list_final_datarelatedtodistinctweekswithout_normalization_53')),
            start_task = 'insert_to_list_55',
            end_task = 'foreach_final_datarelatedtodistinctweekswithout_normalization_53_54_end'
        )

        insert_to_list_55=rail.SetVariableOperator(
            task_id='insert_to_list_55',
            append=True,
            name='{{ result("declare_finaldataperperweek_list").name }}',
            value=lambda:{
                "week": rail.result('foreach_query_list_distinct_weeksinexport_21_22')['weekentrydate'],
                "timesheetperiod": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_53_54')['timesheetperiod'],
                "reimbursementtype": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_53_54')['reimbursementtype'],
                "name": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_53_54')['username'],
                "projectcode": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_53_54')['projectcode'],
                "projectname": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_53_54')['projectname'],
                "hours": get_hours_sum(rail.result('query_list_datarelatedtodistinctweekswithout_normalization_43'),rail.result(
                    'foreach_final_datarelatedtodistinctweekswithout_normalization_53_54')),
                "actualhours": get_hours_sum(rail.result('query_list_datarelatedtodistinctweekswithout_normalization_43'),rail.result(
                    'foreach_final_datarelatedtodistinctweekswithout_normalization_53_54')),
                "taskname": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_53_54')['taskname']
            }
        )

        foreach_final_datarelatedtodistinctweekswithout_normalization_53_54_end=rail.EmptyOperator(
            task_id='foreach_final_datarelatedtodistinctweekswithout_normalization_53_54_end',
        )

        query_list_total_datafordistinctweekwith_excludedtime_types_57=rail.QueryCollectionOperator(
            task_id='query_list_total_datafordistinctweekwith_excludedtime_types_57',
            query="""SELECT SUM( normalizedhourreport.hoursworked) as totalhoursinweek FROM  normalizedhourreport WHERE
                normalizedhourreport.weekentrydate='{{ result('foreach_query_list_distinct_weeksinexport_21_22').weekentrydate }}' AND
                normalizedhourreport.loginname='{{ result('foreach_query_list_distinct_weeksinexport_21_22').loginname }}' AND
                ( normalizedhourreport.taskcode='02 Floating Holiday' OR  normalizedhourreport.taskcode='04 Sick' OR
                normalizedhourreport.taskcode='7H Holiday' OR  normalizedhourreport.taskcode='03 Vacation')""",
        )

        get_total_datafordistinctweekwith_excludedtime_types_57 = rail.PythonOperator(
            task_id = 'get_total_datafordistinctweekwith_excludedtime_types_57',
            python_callable=lambda: get_float_hours(rail.result('query_list_total_datafordistinctweekwith_excludedtime_types_57'))
        )

        query_list_datarelatedtodistinctweekwith_excludedtime_types_58=rail.QueryCollectionOperator(
            task_id='query_list_datarelatedtodistinctweekwith_excludedtime_types_58',
            query="""SELECT  normalizedhourreport.username,  normalizedhourreport.projectname,  normalizedhourreport.projectcode,
                normalizedhourreport.timesheetperiod,  normalizedhourreport.hoursworked,  normalizedhourreport.reimbursementtype ,
                normalizedhourreport.taskname FROM  normalizedhourreport WHERE
                normalizedhourreport.weekentrydate='{{ result('foreach_query_list_distinct_weeksinexport_21_22').weekentrydate }}' AND
                normalizedhourreport.loginname='{{ result('foreach_query_list_distinct_weeksinexport_21_22').loginname }}' AND
                ( normalizedhourreport.taskcode='02 Floating Holiday' OR  normalizedhourreport.taskcode='04 Sick' OR
                normalizedhourreport.taskcode='7H Holiday' OR  normalizedhourreport.taskcode='03 Vacation')""",
        )

        foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59=rail.ForEachOperator(
            task_id='foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59',
            items=lambda: rail.load_all_records(rail.result('query_list_datarelatedtodistinctweekwith_excludedtime_types_58')),
            start_task = 'insert_to_list_60',
            end_task = 'foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59_end'
        )

        insert_to_list_60=rail.SetVariableOperator(
            task_id='insert_to_list_60',
            append=True,
            name='{{ result("declare_finaldataperperweek_list").name }}',
            value=lambda:{
                "week": rail.result('foreach_query_list_distinct_weeksinexport_21_22')['weekentrydate'],
                "timesheetperiod": rail.result('foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59')['timesheetperiod'],
                "reimbursementtype": rail.result('foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59')['reimbursementtype'],
                "name": rail.result('foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59')['username'],
                "projectcode": rail.result('foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59')['projectcode'],
                "projectname": rail.result('foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59')['projectname'],
                "hours": float(rail.result('foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59')['hoursworked']),
                "actualhours": float(rail.result('foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59')['hoursworked']),
                "taskname": rail.result('foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59')['taskname']
            }
        )

        foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59_end=rail.EmptyOperator(
            task_id='foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59_end',
        )

        log_normal_normalization_61=rail.PythonOperator(
            task_id='log_normal_normalization_61',
            python_callable= lambda: rail.result('get_totalhoursfordistinctweeks_29') - 40
        )

        query_list_final_datarelatedtodistinctweekswithout_normalization_62=rail.QueryCollectionOperator(
            task_id='query_list_final_datarelatedtodistinctweekswithout_normalization_62',
            query="""SELECT DISTINCT  normalizedhourreport.username,  normalizedhourreport.projectname,  normalizedhourreport.projectcode,
                normalizedhourreport.timesheetperiod,  normalizedhourreport.reimbursementtype,  normalizedhourreport.taskname FROM
                normalizedhourreport WHERE
                normalizedhourreport.weekentrydate='{{ result('foreach_query_list_distinct_weeksinexport_21_22').weekentrydate }}' AND
                normalizedhourreport.loginname='{{ result('foreach_query_list_distinct_weeksinexport_21_22').loginname }}' AND
                normalizedhourreport.taskcode!='02 Floating Holiday' AND  normalizedhourreport.taskcode!='04 Sick' AND
                normalizedhourreport.taskcode!='7H Holiday' AND  normalizedhourreport.taskcode!='03 Vacation'""",
        )

        def get_normal_normalization_per_project():
            totalhoursinweek_for_distinctweek_with_excluded_timetypes = rail.result('get_total_datafordistinctweekwith_excludedtime_types_57')
            totalhours_for_distinctweeks = rail.result('get_totalhoursfordistinctweeks_29')
            return ((40 - totalhoursinweek_for_distinctweek_with_excluded_timetypes) / (
                totalhours_for_distinctweeks - totalhoursinweek_for_distinctweek_with_excluded_timetypes)) if (
                totalhours_for_distinctweeks - totalhoursinweek_for_distinctweek_with_excluded_timetypes) > 0 else 0

        log_normal_normalizationperproject_63=rail.PythonOperator(
            task_id='log_normal_normalizationperproject_63',
            python_callable=get_normal_normalization_per_project
        )

        foreach_final_datarelatedtodistinctweekswithout_normalization_62_64=rail.ForEachOperator(
            task_id='foreach_final_datarelatedtodistinctweekswithout_normalization_62_64',
            items=lambda: rail.load_all_records(rail.result('query_list_final_datarelatedtodistinctweekswithout_normalization_62')),
            start_task = 'insert_to_list_65',
            end_task = 'foreach_final_datarelatedtodistinctweekswithout_normalization_62_64_end'
        )

        insert_to_list_65=rail.SetVariableOperator(
            task_id='insert_to_list_65',
            append=True,
            name='{{ result("declare_finaldataperperweek_list").name }}',
            value=lambda:{
                "week": rail.result('foreach_query_list_distinct_weeksinexport_21_22')['weekentrydate'],
                "timesheetperiod": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_62_64')['timesheetperiod'],
                "reimbursementtype": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_62_64')['reimbursementtype'],
                "name": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_62_64')['username'],
                "projectcode": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_62_64')['projectcode'],
                "projectname": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_62_64')['projectname'],
                "hours": float(get_hours_sum(rail.result('query_list_datarelatedtodistinctweeks_23'),rail.result(
                    'foreach_final_datarelatedtodistinctweekswithout_normalization_62_64'))) * float(rail.result(
                    'log_normal_normalizationperproject_63')),
                "actualhours": float(get_hours_sum(rail.result('query_list_datarelatedtodistinctweeks_23'),rail.result(
                    'foreach_final_datarelatedtodistinctweekswithout_normalization_62_64'))),
                "taskname": rail.result('foreach_final_datarelatedtodistinctweekswithout_normalization_62_64')['taskname']
            }
        )

        foreach_final_datarelatedtodistinctweekswithout_normalization_62_64_end=rail.EmptyOperator(
            task_id='foreach_final_datarelatedtodistinctweekswithout_normalization_62_64_end',
        )

        query_list_datarelatedtodistinctweeks_67=rail.QueryCollectionOperator(
            task_id='query_list_datarelatedtodistinctweeks_67',
            query="""SELECT DISTINCT  normalizedhourreport.username,  normalizedhourreport.projectname,  normalizedhourreport.projectcode,
                normalizedhourreport.timesheetperiod,  normalizedhourreport.reimbursementtype,  normalizedhourreport.taskname FROM  normalizedhourreport WHERE
                normalizedhourreport.weekentrydate='{{ result('foreach_query_list_distinct_weeksinexport_21_22').weekentrydate }}' AND
                normalizedhourreport.loginname='{{ result('foreach_query_list_distinct_weeksinexport_21_22').loginname }}'""",
        )

        foreach_query_list_datarelatedtodistinctweeks_67_68=rail.ForEachOperator(
            task_id='foreach_query_list_datarelatedtodistinctweeks_67_68',
            items=lambda: rail.load_all_records(rail.result('query_list_datarelatedtodistinctweeks_67')),
            start_task = 'insert_to_list_69',
            end_task = 'foreach_query_list_datarelatedtodistinctweeks_67_68_end'
        )

        insert_to_list_69=rail.SetVariableOperator(
            task_id='insert_to_list_69',
            append=True,
            name='{{ result("declare_finaldataperperweek_list").name }}',
            value=lambda: {
                "week": rail.result('foreach_query_list_distinct_weeksinexport_21_22')['weekentrydate'],
                "timesheetperiod": rail.result('foreach_query_list_datarelatedtodistinctweeks_67_68')['timesheetperiod'],
                "reimbursementtype": rail.result('foreach_query_list_datarelatedtodistinctweeks_67_68')['reimbursementtype'],
                "name": rail.result('foreach_query_list_datarelatedtodistinctweeks_67_68')['username'],
                "projectcode": rail.result('foreach_query_list_datarelatedtodistinctweeks_67_68')['projectcode'],
                "projectname": rail.result('foreach_query_list_datarelatedtodistinctweeks_67_68')['projectname'],
                "hours": get_hours_sum(rail.result('query_list_datarelatedtodistinctweeks_23'),rail.result(
                    'foreach_query_list_datarelatedtodistinctweeks_67_68')),
                "actualhours": get_hours_sum(rail.result('query_list_datarelatedtodistinctweeks_23'),rail.result(
                    'foreach_query_list_datarelatedtodistinctweeks_67_68')),
                "taskname": rail.result('foreach_query_list_datarelatedtodistinctweeks_67_68')['taskname']
            }
        )

        foreach_query_list_datarelatedtodistinctweeks_67_68_end=rail.EmptyOperator(
            task_id='foreach_query_list_datarelatedtodistinctweeks_67_68_end',
        )

        foreach_query_list_distinct_weeksinexport_21_22_end=rail.EmptyOperator(
            task_id='foreach_query_list_distinct_weeksinexport_21_22_end',
        )

        declare_finaldata_list=rail.SetVariableOperator(
            task_id='declare_finaldata_list',
            append=False,
            name='finaldata',
            value=[]
        )

        log_final_data_per_perweek = rail.PythonOperator(
            task_id = 'log_final_data_per_perweek',
            python_callable= lambda: rail.get_dag_run_var('finaldataperperweek')
        )

        query_list_final_datato_export_distinct_usersand_timesheet_periods_71=rail.QueryCollectionOperator(
            task_id='query_list_final_datato_export_distinct_usersand_timesheet_periods_71',
            query="""SELECT DISTINCT  normalizedhourreport.username,  normalizedhourreport.timesheetperiod FROM  normalizedhourreport""",
        )

        foreach_final_datato_export_distinct_usersand_timesheet_periods_71_72=rail.ForEachOperator(
            task_id='foreach_final_datato_export_distinct_usersand_timesheet_periods_71_72',
            items=lambda: rail.load_all_records(rail.result('query_list_final_datato_export_distinct_usersand_timesheet_periods_71')),
            start_task = 'query_list_final_datato_export_73',
            end_task = 'foreach_final_datato_export_distinct_usersand_timesheet_periods_71_72_end'
        )

        query_list_final_datato_export_73=rail.QueryCollectionOperator(
            task_id='query_list_final_datato_export_73',
            #pylint: disable = line-too-long
            query="""SELECT DISTINCT  normalizedhourreport.username,  normalizedhourreport.projectname,  normalizedhourreport.projectcode,  normalizedhourreport.timesheetperiod,  normalizedhourreport.reimbursementtype,  normalizedhourreport.taskname FROM  normalizedhourreport WHERE  normalizedhourreport.username='{{ result('foreach_final_datato_export_distinct_usersand_timesheet_periods_71_72').username }}' AND  normalizedhourreport.timesheetperiod='{{ result('foreach_final_datato_export_distinct_usersand_timesheet_periods_71_72').timesheetperiod }}'""",
        )

        declare_variable_actualhours=rail.SetVariableOperator(
            task_id='declare_variable_actualhours',
            append=False,
            name='actualhours',
            value=0
        )

        declare_variable_allocatedhours=rail.SetVariableOperator(
            task_id='declare_variable_allocatedhours',
            append=False,
            name='allocatedhours',
            value=0
        )

        foreach_query_list_final_datato_export_73_76=rail.ForEachOperator(
            task_id='foreach_query_list_final_datato_export_73_76',
            items=lambda: rail.load_all_records(rail.result('query_list_final_datato_export_73')),
            start_task = 'insert_to_list_77',
            end_task = 'foreach_query_list_final_datato_export_73_76_end'
        )

        def get_hours_sum_from_finaldataperperweek(user,hourtype):
            finaldata_per_per_week = rail.get_dag_run_var('finaldataperperweek')
            sum_of_hours = 0.0
            for data in finaldata_per_per_week:
                if data['name'] == user['username'] and data['projectname'] == user['projectname'] and data['projectcode'] == user['projectcode']:
                    if (data['timesheetperiod'] == user['timesheetperiod'] and data['reimbursementtype'] == user['reimbursementtype'] and
                        data['taskname'] == user['taskname']):
                        sum_of_hours+=float(data[hourtype])
            return sum_of_hours

        insert_to_list_77=rail.SetVariableOperator(
            task_id='insert_to_list_77',
            append=True,
            name='{{ result("declare_finaldata_list").name }}',
            value=lambda: {
                "timesheetperiod": rail.result('foreach_query_list_final_datato_export_73_76')['timesheetperiod'],
                "reimbursementtype": rail.result('foreach_query_list_final_datato_export_73_76')['reimbursementtype'],
                "name": rail.result('foreach_query_list_final_datato_export_73_76')['username'],
                "projectcode": rail.result('foreach_query_list_final_datato_export_73_76')['projectcode'],
                "projectname": rail.result('foreach_query_list_final_datato_export_73_76')['projectname'],
                "hours": get_hours_sum_from_finaldataperperweek(rail.result('foreach_query_list_final_datato_export_73_76'),'hours'),
                "actualhours": get_hours_sum_from_finaldataperperweek(rail.result('foreach_query_list_final_datato_export_73_76'),'actualhours'),
                "taskname": rail.result('foreach_query_list_final_datato_export_73_76')['taskname']
            }
        )

        update_variable_actualhours=rail.SetVariableOperator(
            task_id='update_variable_actualhours',
            append=False,
            name='{{ result("declare_variable_actualhours").name }}',
            value=lambda: float(rail.get_dag_run_var('actualhours')) + get_hours_sum_from_finaldataperperweek(rail.result(
                'foreach_query_list_final_datato_export_73_76'),'actualhours')
        )

        update_variable_allocatedhours=rail.SetVariableOperator(
            task_id='update_variable_allocatedhours',
            append=False,
            name='{{ result("declare_variable_allocatedhours").name }}',
            value=lambda: float(rail.get_dag_run_var('allocatedhours')) + get_hours_sum_from_finaldataperperweek(rail.result(
                'foreach_query_list_final_datato_export_73_76'),'hours')
        )

        foreach_query_list_final_datato_export_73_76_end=rail.EmptyOperator(
            task_id='foreach_query_list_final_datato_export_73_76_end',
        )

        insert_to_finaldata_list=rail.SetVariableOperator(
            task_id='insert_to_finaldata_list',
            append=True,
            name='{{ result("declare_finaldata_list").name }}',
            value=lambda:{
                "timesheetperiod": "Total",
                "reimbursementtype": '',
                "name": '',
                "projectcode": '',
                "projectname": '',
                "hours": rail.get_dag_run_var('allocatedhours'),
                "actualhours": rail.get_dag_run_var('actualhours'),
                "taskname": ''
            }
        )

        foreach_final_datato_export_distinct_usersand_timesheet_periods_71_72_end=rail.EmptyOperator(
            task_id='foreach_final_datato_export_distinct_usersand_timesheet_periods_71_72_end',
        )

        create_csv_lines_final_extract_data_81=rail.WriteCSVFileOperator(
            task_id='create_csv_lines_final_extract_data_81',
            source=lambda: rail.get_dag_run_var('finaldata'),
            header=['Timesheet Period',
                    'Reimbursement Type',
                    'Name',
                    'Cost Center',
                    'Description',
                    'Time Type',
                    'Actual Hrs',
                    'Allocated Hrs'],
            row= [
                "{{ item.timesheetperiod }}",
                "{{ item.reimbursementtype }}",
                "{{ item.name }}",
                "{{ item.projectcode }}",
                "{{ item.projectname }}",
                "{{ item.taskname }}",
                "{{ item.actualhours }}",
                "{{ item.hours }}"
            ],
        )

        log_file_name_82=rail.PythonOperator(
            task_id='log_file_name_82',
            python_callable= lambda dag_run:  "Custom CJI - Actuals vs Normalized Report_" + dag_run.conf['requesterid'] + "_" +
                                datetime.now().strftime("%d%m%Y%H%M%S") + ".csv"
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines_final_extract_data_81')}}",
            output_file_name="{{ result('log_file_name_82') }}",
            expires_in_seconds=7*24*60*60,
        )

        send_mail_with_cshare_send_file_extracttouser_84=rail.EmailOperator(
            task_id='send_mail_with_cshare_send_file_extracttouser_84',
            to="{{dag_run.conf.emailid}}",
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Custom actuals vs normalized report extract - {{ result('log_time_now_5') }} ''',
            html_content= '''templates/success_mail_with_export.html''',
            params=None,
        )

        finish=rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> log_user_first_name_4
        log_user_first_name_4 >> log_time_now_5 >> if_error_in_report_result
        if_error_in_report_result >> rail.Label('Yes')  >> fail_with_error >> finish
        if_error_in_report_result >> rail.Label('No') >> if_first_payload_contains_nodata_11
        if_first_payload_contains_nodata_11 >> rail.Label('Yes') >> send_mail_no_datato_extractforthegivenperiod_12 >> finish
        if_first_payload_contains_nodata_11 >> rail.Label('No') >> load_csv_from_report_data >> create_collection_rawnormalizedhourreport
        create_collection_rawnormalizedhourreport >> write_csv_for_normalizeddata >> declare_finaldataperperweek_list >> create_collection_normalizedhourreport
        create_collection_normalizedhourreport >> query_list_distinct_weeksinexport_21 >> foreach_query_list_distinct_weeksinexport_21_22
        foreach_query_list_distinct_weeksinexport_21_22 >> query_list_datarelatedtodistinctweeks_23
        query_list_datarelatedtodistinctweeks_23 >> if_foreach_query_list_distinct_weeksinexport_21_22_employeecategory_equals_to_nonexempt_24
        if_foreach_query_list_distinct_weeksinexport_21_22_employeecategory_equals_to_nonexempt_24 >> rail.Label(
            'Yes')  >> query_list_datarelatedtodistinctweeks_25 >> foreach_query_list_datarelatedtodistinctweeks_25_26 >> insert_to_list_27
        insert_to_list_27 >> foreach_query_list_datarelatedtodistinctweeks_25_26_end
        foreach_query_list_datarelatedtodistinctweeks_25_26 >> foreach_query_list_datarelatedtodistinctweeks_25_26_end
        foreach_query_list_datarelatedtodistinctweeks_25_26_end >> foreach_query_list_distinct_weeksinexport_21_22_end
        if_foreach_query_list_distinct_weeksinexport_21_22_employeecategory_equals_to_nonexempt_24 >> rail.Label(
            'No') >> query_list_totalhoursfordistinctweeks_29 >> get_totalhoursfordistinctweeks_29 >> if_totalhoursinweek_greater_than_40
        if_totalhoursinweek_greater_than_40 >> rail.Label(
            'Yes') >> log_checkifnormalizationrequiredprojectisthere_31 >> if_log_checkifnormalizationrequiredprojectisthere_31_present_32
        if_log_checkifnormalizationrequiredprojectisthere_31_present_32 >> rail.Label('Yes')  >> query_list_total_hoursfordistinctweekswith_normalization_33
        query_list_total_hoursfordistinctweekswith_normalization_33 >> get_total_hoursfordistinctweekswith_normalization_33
        get_total_hoursfordistinctweekswith_normalization_33 >> query_list_total_datafordistinctweekwith_excludedtime_types_34
        query_list_total_datafordistinctweekwith_excludedtime_types_34 >> get_total_datafordistinctweekwith_excludedtime_types_34
        get_total_datafordistinctweekwith_excludedtime_types_34 >> query_list_datarelatedtodistinctweekwith_excludedtime_types_35
        query_list_datarelatedtodistinctweekwith_excludedtime_types_35 >> foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36
        foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36 >> insert_to_list_37
        insert_to_list_37 >> foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36_end
        foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36 >> foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36_end
        foreach_datarelatedtodistinctweekwith_excludedtime_types_35_36_end >> query_list_datarelatedtodistinctweekswith_normalization_38
        query_list_datarelatedtodistinctweekswith_normalization_38 >> log_exhaustive_normalization_39
        log_exhaustive_normalization_39 >> query_list_final_datarelatedtodistinctweekswith_normalization_40 >> log_exhaustive_normalization_per_project_41
        log_exhaustive_normalization_per_project_41 >> query_list_totaldatafordistinctweekswithout_normalization_42
        query_list_totaldatafordistinctweekswithout_normalization_42 >> get_totaldatafordistinctweekswithout_normalization_42
        get_totaldatafordistinctweekswithout_normalization_42 >> query_list_datarelatedtodistinctweekswithout_normalization_43
        query_list_datarelatedtodistinctweekswithout_normalization_43 >> foreach_final_datarelatedtodistinctweekswith_normalization_40_44
        foreach_final_datarelatedtodistinctweekswith_normalization_40_44 >> insert_to_list_45
        insert_to_list_45 >> foreach_final_datarelatedtodistinctweekswith_normalization_40_44_end
        foreach_final_datarelatedtodistinctweekswith_normalization_40_44 >> foreach_final_datarelatedtodistinctweekswith_normalization_40_44_end
        foreach_final_datarelatedtodistinctweekswith_normalization_40_44_end >> if_to_f_greater_than_40_46
        if_to_f_greater_than_40_46 >> rail.Label('Yes') >> log_normal_normalization_47 >> log_normal_normalizationperproject_48
        log_normal_normalizationperproject_48 >> query_list_final_datarelatedtodistinctweekswithout_normalization_49
        query_list_final_datarelatedtodistinctweekswithout_normalization_49 >> foreach_final_datarelatedtodistinctweekswithout_normalization_49_50
        foreach_final_datarelatedtodistinctweekswithout_normalization_49_50 >> insert_to_list_51
        insert_to_list_51 >> foreach_final_datarelatedtodistinctweekswithout_normalization_49_50_end
        foreach_final_datarelatedtodistinctweekswithout_normalization_49_50 >> foreach_final_datarelatedtodistinctweekswithout_normalization_49_50_end
        foreach_final_datarelatedtodistinctweekswithout_normalization_49_50_end >> foreach_query_list_distinct_weeksinexport_21_22_end
        if_to_f_greater_than_40_46 >> rail.Label('No') >> query_list_final_datarelatedtodistinctweekswithout_normalization_53
        query_list_final_datarelatedtodistinctweekswithout_normalization_53 >> foreach_final_datarelatedtodistinctweekswithout_normalization_53_54
        foreach_final_datarelatedtodistinctweekswithout_normalization_53_54 >> insert_to_list_55
        insert_to_list_55 >> foreach_final_datarelatedtodistinctweekswithout_normalization_53_54_end
        foreach_final_datarelatedtodistinctweekswithout_normalization_53_54 >> foreach_final_datarelatedtodistinctweekswithout_normalization_53_54_end
        foreach_final_datarelatedtodistinctweekswithout_normalization_53_54_end >> foreach_query_list_distinct_weeksinexport_21_22_end
        if_log_checkifnormalizationrequiredprojectisthere_31_present_32 >> rail.Label('No') >> query_list_total_datafordistinctweekwith_excludedtime_types_57
        query_list_total_datafordistinctweekwith_excludedtime_types_57 >> get_total_datafordistinctweekwith_excludedtime_types_57
        get_total_datafordistinctweekwith_excludedtime_types_57 >> query_list_datarelatedtodistinctweekwith_excludedtime_types_58
        query_list_datarelatedtodistinctweekwith_excludedtime_types_58 >> foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59
        foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59 >> insert_to_list_60
        insert_to_list_60 >> foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59_end
        foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59 >> foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59_end
        foreach_datarelatedtodistinctweekwith_excludedtime_types_58_59_end >> log_normal_normalization_61
        log_normal_normalization_61 >> query_list_final_datarelatedtodistinctweekswithout_normalization_62 >> log_normal_normalizationperproject_63
        log_normal_normalizationperproject_63 >> foreach_final_datarelatedtodistinctweekswithout_normalization_62_64 >> insert_to_list_65
        insert_to_list_65 >> foreach_final_datarelatedtodistinctweekswithout_normalization_62_64_end
        foreach_final_datarelatedtodistinctweekswithout_normalization_62_64 >> foreach_final_datarelatedtodistinctweekswithout_normalization_62_64_end
        foreach_final_datarelatedtodistinctweekswithout_normalization_62_64_end >> foreach_query_list_distinct_weeksinexport_21_22_end
        if_totalhoursinweek_greater_than_40 >> rail.Label('No') >> query_list_datarelatedtodistinctweeks_67
        query_list_datarelatedtodistinctweeks_67 >> foreach_query_list_datarelatedtodistinctweeks_67_68 >> insert_to_list_69
        insert_to_list_69 >> foreach_query_list_datarelatedtodistinctweeks_67_68_end
        foreach_query_list_datarelatedtodistinctweeks_67_68 >> foreach_query_list_datarelatedtodistinctweeks_67_68_end
        foreach_query_list_datarelatedtodistinctweeks_67_68_end >> foreach_query_list_distinct_weeksinexport_21_22_end
        foreach_query_list_distinct_weeksinexport_21_22 >> foreach_query_list_distinct_weeksinexport_21_22_end >> declare_finaldata_list
        declare_finaldata_list >> log_final_data_per_perweek >> query_list_final_datato_export_distinct_usersand_timesheet_periods_71
        query_list_final_datato_export_distinct_usersand_timesheet_periods_71 >> foreach_final_datato_export_distinct_usersand_timesheet_periods_71_72
        foreach_final_datato_export_distinct_usersand_timesheet_periods_71_72 >> query_list_final_datato_export_73 >> declare_variable_actualhours
        declare_variable_actualhours >> declare_variable_allocatedhours >> foreach_query_list_final_datato_export_73_76 >> insert_to_list_77
        insert_to_list_77 >> update_variable_actualhours >> update_variable_allocatedhours >> foreach_query_list_final_datato_export_73_76_end
        foreach_query_list_final_datato_export_73_76 >> foreach_query_list_final_datato_export_73_76_end >> insert_to_finaldata_list
        insert_to_finaldata_list >> foreach_final_datato_export_distinct_usersand_timesheet_periods_71_72_end
        foreach_final_datato_export_distinct_usersand_timesheet_periods_71_72 >> foreach_final_datato_export_distinct_usersand_timesheet_periods_71_72_end
        foreach_final_datato_export_distinct_usersand_timesheet_periods_71_72_end >> create_csv_lines_final_extract_data_81 >> log_file_name_82
        log_file_name_82 >> generate_download_link >> send_mail_with_cshare_send_file_extracttouser_84 >> finish

    return dag

rail.for_each_instance(create_dag)
