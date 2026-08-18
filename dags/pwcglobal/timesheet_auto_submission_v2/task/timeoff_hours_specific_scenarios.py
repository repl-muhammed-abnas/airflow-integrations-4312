import datetime
import json
import rail


def get_specific_scenarios(config):
    with rail.TaskGroup(group_id='specific_scenarios', prefix_group_id=False) as specifics_cenarios:

        get_report2_details = rail.RepliconReportDetailsOperator(
            task_id='get_report2_details',
            report_name=config.timesheet_report_name,
        )

        run_report2_group_entry, run_report2_group_exit = rail.run_report(
            group_id='run_report2',
            wait_timeout=config.run_report_wait_timeout,
            retries=0,
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report2_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report2_has_data = rail.IfOperator(
            task_id="report2_has_data",
            test="{{ result('run_report2.get_report_result','has_data')}}",
            yes_task='load_report2_data',
            no_task='finish',
        )

        load_report2_data = rail.LoadCSVFileOperator(
            task_id='load_report2_data',
            document="{{ result('run_report2.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_loa_timeoff_users_report1_collection = rail.CreateCollectionOperator(
            task_id='create_loa_timeoff_users_report1_collection',
            name='loa_timeoff_users_report1',
            source="{{ result('load_report2_data') }}",
        )

        get_report3_details = rail.RepliconReportDetailsOperator(
            task_id='get_report3_details',
            report_name=config.timesheet_with_timeoffhours_report_name,
        )

        run_report3_group_entry, run_report3_group_exit = rail.run_report(
            group_id='run_report3',
            wait_timeout=config.run_report_wait_timeout,
            retries=0,
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report3_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report3_has_data = rail.IfOperator(
            task_id="report3_has_data",
            test="{{ result('run_report3.get_report_result','has_data')}}",
            yes_task='load_report3_data',
            no_task='finish',
        )

        load_report3_data = rail.LoadCSVFileOperator(
            task_id='load_report3_data',
            document="{{ result('run_report3.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_loa_timeoff_users_report3_collection = rail.CreateCollectionOperator(
            task_id='create_loa_timeoff_users_report3_collection',
            name='loa_timeoff_users_report2',
            source="{{ result('load_report3_data') }}",
        )

        query_timesheets_projecthrs = rail.QueryCollectionOperator(
            task_id='query_timesheets_projecthrs',
            query='''SELECT * FROM loa_timeoff_users_report1
                     WHERE (Project_Hrs__In_Period_=Scheduled_Hrs__In_Period_ OR
                            CAST(Project_Hrs__In_Period_ as decimal) > CAST(Scheduled_Hrs__In_Period_ as decimal)) AND
                            CAST(daydiff as decimal) < 1 AND
                            (timesheeturi IS NOT NULL OR timesheeturi != "") AND
                            (
                            Validation_Message="" OR Validation_Message IS NULL OR Validation_Message = "Null" OR
                            Validation_Message=?)
                    ''',
            query_params=[config.validation_message]
        )

        query_union1 = rail.QueryCollectionOperator(
            task_id='query_union1',
            query='''SELECT DISTINCT timesheeturi, Timesheet_Start_Date, Timesheet_End_Date
                        FROM loa_timeoff_users_report2
                        WHERE timesheeturi IN (
                            SELECT DISTINCT timesheeturi FROM loa_timeoff_users_report1) AND
                            (Project_Hours__In_Period_=Scheduled_Hrs__In_Period_ OR
                            CAST(Project_Hours__In_Period_ as decimal) > CAST(Scheduled_Hrs__In_Period_ as decimal)) AND
                            CAST(daydiff as decimal) < 1 AND Project_Name IS NULL '''
        )

        query_union2 = rail.QueryCollectionOperator(
            task_id='query_union2',
            query='''SELECT DISTINCT timesheeturi as timesheeturi, Timesheet_Start_Date, Timesheet_End_Date
                        FROM query_timesheets_projecthrs
                        WHERE timesheeturi IN (
                            SELECT DISTINCT timesheeturi FROM loa_timeoff_users_report2)'''
        )

        query_merge = rail.QueryCollectionOperator(
            task_id='query_merge',
            query='''SELECT * FROM  query_union1 WHERE timesheeturi != '' AND timesheeturi IS NOT NULL
                     UNION
                     SELECT * FROM  query_union2 WHERE timesheeturi != '' AND timesheeturi IS NOT NULL
                     '''
        )

        query_merge_has_data = rail.IfOperator(
            task_id="query_merge_has_data",
            test="{{ result('query_merge','length') > 0}}",
            yes_task='get_report4_details',
            no_task='finish',
        )

        get_report4_details = rail.RepliconReportDetailsOperator(
            task_id='get_report4_details',
            report_name=config.timesheet_with_project_actuals_report_name,
        )

        def get_report_date(date):
            if not date:
                return None
            # date format in mm/dd/yyyy
            return f'{date.month}/{date.day}/{date.year}'

        def do_get_report4_filter():
            filters = []
            query_merge_records = rail.load_all_records(
                rail.result('query_merge'))
            entry_date_filter_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                'get_report4_details')['filterConfiguration']['enabledFilters'], 'displayText', 'EntryDateFilter', 'uri')
            filters.append({
                'reportFilterUri': entry_date_filter_uri,
                'value': None
            })
            filters.append({
                'reportFilterUri': entry_date_filter_uri,
                'value': get_report_date(min(list(map(lambda x: datetime.datetime.strptime(x['Timesheet_Start_Date'], '%d/%m/%Y'),
                                                      query_merge_records))))
            })
            filters.append({
                'reportFilterUri': entry_date_filter_uri,
                'value': get_report_date(max(list(map(lambda x: datetime.datetime.strptime(x['Timesheet_End_Date'], '%d/%m/%Y'),
                                                      query_merge_records))))
            })

            return json.dumps({
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report4_details')['uri'],
                        "filterValues": filters,
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            })
        get_report4_filter = rail.PythonOperator(
            task_id='get_report4_filter',
            python_callable=do_get_report4_filter
        )
        run_report4_group_entry, run_report4_group_exit = rail.run_report(
            group_id='run_report4',
            wait_timeout=config.run_report_wait_timeout,
            retries=0,
            report_params="{{result('get_report4_filter')}}",
            replicon_conn_id=config.replicon_conn_id,
        )

        report4_has_data = rail.IfOperator(
            task_id="report4_has_data",
            test="{{ result('run_report4.get_report_result','has_data')}}",
            yes_task='load_report4_data',
            no_task='finish',
        )

        load_report4_data = rail.LoadCSVFileOperator(
            task_id='load_report4_data',
            document="{{ result('run_report4.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_loa_timeoff_users_report4_collection = rail.CreateCollectionOperator(
            task_id='create_loa_timeoff_users_report4_collection',
            name='loa_timeoff_users_report3',
            source="{{ result('load_report4_data') }}",
        )

        query_validateddata2 = rail.QueryCollectionOperator(
            task_id='query_validateddata2',
            name='validateddata2',
            query='''SELECT * FROM loa_timeoff_users_report3
                    WHERE timesheeturi IN
                    (SELECT DISTINCT timesheeturi FROM query_merge)
                    '''
        )

        query_statistical = rail.QueryCollectionOperator(
            task_id='query_statistical',
            name='validatedstaticalprojects',
            query='''SELECT * FROM validateddata2
                    WHERE Project_type ='Statistical'
                    '''
        )

        query_nonstatistical = rail.QueryCollectionOperator(
            task_id='query_nonstatistical',
            name='validatednonstaticalprojects',
            query='''SELECT * FROM validateddata2
                    WHERE Project_type ='NonStatistical'
                    '''
        )

        query_final_data = rail.QueryCollectionOperator(
            task_id='query_final_data',
            query='''SELECT DISTINCT SUBSTR(Timesheet_Period,1,10) as Timesheet_Start_Date,
                            SUBSTR(Timesheet_Period,13) as Timesheet_End_Date, User_Name, useruri, timesheeturi
                        FROM  validatedstaticalprojects
                        WHERE timesheeturi  NOT IN
                        (SELECT DISTINCT timesheeturi FROM validatednonstaticalprojects)
                    '''
        )

        process_timesheet2 = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timesheet2',
            retries=0,
            items=lambda: rail.result('query_final_data'),
            batch_size=50,
            execution_timeout=datetime.timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'pwcglobal_timesheet_auto_submission_timeoff_hours_v4_child_{config.instance}',
        )

        wait_for_process_timesheet2 = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timesheet2',
            dag_runs='{{ result("process_timesheet2") }}',
            execution_timeout=datetime.timedelta(
                days=config.execution_timeout_days),
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_report2_details >> run_report2_group_entry >> run_report2_group_exit >> report2_has_data
        report2_has_data >> rail.Label('Yes') >> load_report2_data
        report2_has_data >> rail.Label('No') >> finish
        load_report2_data >> create_loa_timeoff_users_report1_collection >> get_report3_details >> \
            run_report3_group_entry >> run_report3_group_exit >> report3_has_data
        report3_has_data >> rail.Label('Yes') >> load_report3_data
        report3_has_data >> rail.Label('No') >> finish
        load_report3_data >> create_loa_timeoff_users_report3_collection >> query_timesheets_projecthrs >> \
            query_union1 >> query_union2 >> query_merge >> query_merge_has_data
        query_merge_has_data >> rail.Label('Yes') >> get_report4_details
        query_merge_has_data >> rail.Label('No') >> finish
        get_report4_details >> get_report4_filter >> \
            run_report4_group_entry >> run_report4_group_exit >> report4_has_data
        report4_has_data >> rail.Label('Yes') >> load_report4_data
        report4_has_data >> rail.Label('No') >> finish
        load_report4_data >> create_loa_timeoff_users_report4_collection >> query_validateddata2 >> \
            query_statistical >> query_nonstatistical >> query_final_data >> process_timesheet2 >> \
            wait_for_process_timesheet2 >> finish

        return specifics_cenarios
