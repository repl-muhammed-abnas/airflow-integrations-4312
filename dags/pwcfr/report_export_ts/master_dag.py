from datetime import timedelta, datetime
import calendar
import pytz
from dateutil.relativedelta import relativedelta
from pendulum import datetime as dt
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'pwcfr_report_export_ts_master_{config.instance}',
        description=f'Pwcfr_report_export_ts {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=dt(2023, 5, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        log_rundate = rail.PythonOperator(
            task_id='log_rundate',
            python_callable=lambda:  datetime.now(pytz.timezone(
                "Europe/Paris")).strftime("%d%m%Y")
        )

        log_currentfiscalyear = rail.PythonOperator(
            task_id='log_currentfiscalyear',
            python_callable=lambda: int(datetime.now(pytz.timezone("Europe/Paris")).strftime("%Y")) - 1 if int(datetime.now(
                pytz.timezone("Europe/Paris")).strftime("%m")) <= 6 else int(datetime.now(pytz.timezone("Europe/Paris")).strftime("%Y"))
        )

        def get_month():
            month = int(datetime.now(pytz.timezone(
                "Europe/Paris")).strftime("%m"))
            current_month = 12 if month == 6 else (11 if month == 5 else
                                                   (10 if month == 4 else
                                                    (9 if month == 3 else
                                                     (8 if month == 2 else
                                                      (7 if month == 1 else
                                                       (6 if month == 12 else
                                                        (5 if month == 11 else
                                                         (4 if month == 10 else
                                                          (3 if month == 9 else
                                                           (2 if month == 8 else
                                                            (1 if month == 7 else 0)))))))))))
            return current_month

        log_numberofmonthstobeprocessed = rail.PythonOperator(
            task_id='log_numberofmonthstobeprocessed',
            python_callable=get_month
        )

        create_list = rail.SetVariableOperator(
            task_id='create_list',
            append=False,
            name=None,
            value=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        run_report_entry, run_report_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{result('get_report_details').uri}}"
                    }
                ]
            }
        )

        log_modified_timesheetperiod_filter = rail.PythonOperator(
            task_id='log_modified_timesheetperiod_filter',
            python_callable=lambda: rail.smartjoin_by_delim(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'TimesheetPeriodFilter', 'uri', null), "")
        )

        declare_variable = rail.SetVariableOperator(
            task_id='declare_variable',
            append=False,
            name='processchildupdate',
            value=[]
        )

        foreach_item_in_list_do = rail.ForEachOperator(
            task_id='foreach_item_in_list_do',
            items=lambda: rail.result('create_list')['value'],
            start_task='if_log_numberofmonthstobeprocessed_less_than_12',
            end_task='foreach_item_in_list_do_end'
        )

        if_log_numberofmonthstobeprocessed_less_than_12 = rail.IfOperator(
            task_id='if_log_numberofmonthstobeprocessed_less_than_12',
            test=lambda: rail.result('log_numberofmonthstobeprocessed') < 12,
            yes_task="if_seq_no_is_less_than_log_numberofmonthstobeprocessed",
            no_task="no_of_months_to_process"
        )

        if_seq_no_is_less_than_log_numberofmonthstobeprocessed = rail.IfOperator(
            task_id='if_seq_no_is_less_than_log_numberofmonthstobeprocessed',
            test="{{result('foreach_item_in_list_do') < (result('log_numberofmonthstobeprocessed') + 1)}}",
            yes_task="no_of_months_to_process",
            no_task="if_islast_true"
        )

        if_islast_true = rail.IfOperator(
            task_id='if_islast_true',
            test=lambda: rail.result('foreach_item_in_list_do') == 12,
            yes_task='months_to_process',
            no_task='months_to_get_processed'
        )

        months_to_process = rail.PythonOperator(
            task_id='months_to_process',
            python_callable=lambda: (datetime.now(pytz.timezone("Europe/Paris")).replace(
                day=1) + relativedelta(months=12 - rail.result('log_numberofmonthstobeprocessed'))).strftime('%Y-%m-%d')
        )

        months_to_get_processed = rail.PythonOperator(
            task_id='months_to_get_processed',
            python_callable=lambda: (datetime.now(pytz.timezone("Europe/Paris")).replace(
                day=1) + relativedelta(months=12 - rail.result('foreach_item_in_list_do'))).strftime('%Y-%m-%d')
        )

        no_of_months_to_process = rail.PythonOperator(
            task_id='no_of_months_to_process',
            python_callable=lambda: ((datetime.now(pytz.timezone("Europe/Paris")).replace(
                day=1) - relativedelta(months=(rail.result('log_numberofmonthstobeprocessed') - rail.result('foreach_item_in_list_do'))))).strftime('%Y-%m-%d')
        )

        process_child = rail.TriggerDagRunOperator(
            task_id='process_child',
            retries=0,
            trigger_dag_id=f'pwcfr_report_export_ts_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "reporturi": rail.result('get_report_details')['uri'],
                "filteruri": rail.result('log_modified_timesheetperiod_filter'),
                "startdate": datetime.strptime(rail.result('no_of_months_to_process'), '%Y-%m-%d').strftime('%Y/%m/%d'),
                "enddate": datetime.strptime(rail.result('no_of_months_to_process'), "%Y-%m-%d").replace(day=calendar.monthrange(datetime.strptime(rail.result('no_of_months_to_process'), "%Y-%m-%d").year, datetime.strptime(rail.result('no_of_months_to_process'), "%Y-%m-%d").month)[1]).strftime('%m/%d/%Y'),
                "month": datetime.strptime(rail.result('no_of_months_to_process'), "%Y-%m-%d").replace(day=calendar.monthrange(datetime.strptime(rail.result('no_of_months_to_process'), "%Y-%m-%d").year, datetime.strptime(rail.result('no_of_months_to_process'), "%Y-%m-%d").month)[1]).strftime('%b'),
                "filepath": config.input_filepath
            }
        )

        process_each_child = rail.TriggerDagRunOperator(
            task_id='process_each_child',
            retries=0,
            trigger_dag_id=f'pwcfr_report_export_ts_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "reporturi": rail.result('get_report_details')['uri'],
                "filteruri": rail.result('log_modified_timesheetperiod_filter'),
                "startdate": datetime.strptime(rail.result('months_to_process'), '%Y-%m-%d').strftime('%Y/%m/%d') if rail.result('months_to_process') else (datetime.strptime(rail.result('months_to_get_processed'), '%Y-%m-%d')).strftime('%Y/%m/%d'),
                "enddate": datetime.strptime(rail.result('months_to_process'), "%Y-%m-%d").replace(day=calendar.monthrange(datetime.strptime(rail.result('months_to_process'), "%Y-%m-%d").year, datetime.strptime(rail.result('months_to_process'), "%Y-%m-%d").month)[1]).strftime('%m/%d/%Y') if rail.result('months_to_process') else
                datetime.strptime(rail.result('months_to_get_processed'), "%Y-%m-%d").replace(day=calendar.monthrange(datetime.strptime(rail.result(
                    'months_to_get_processed'), "%Y-%m-%d").year, datetime.strptime(rail.result('months_to_get_processed'), "%Y-%m-%d").month)[1]).strftime('%m/%d/%Y'),
                "month": datetime.strptime(rail.result('months_to_process'), "%Y-%m-%d").replace(day=calendar.monthrange(datetime.strptime(rail.result('months_to_process'), "%Y-%m-%d").year, datetime.strptime(rail.result('months_to_process'), "%Y-%m-%d").month)[1]).strftime('%b') if rail.result('months_to_process') else datetime.strptime(rail.result('months_to_get_processed'), "%Y-%m-%d").replace(day=calendar.monthrange(datetime.strptime(rail.result('months_to_get_processed'), "%Y-%m-%d").year, datetime.strptime(rail.result('months_to_get_processed'), "%Y-%m-%d").month)[1]).strftime('%b'),
                "filepath": config.input_filepath
            }
        )

        foreach_item_in_list_do_end = rail.EmptyOperator(
            task_id='foreach_item_in_list_do_end'
        )

        insert_to_process_child = rail.SetVariableOperator(
            task_id='insert_to_process_each_child',
            append=True,
            name='{{ result("declare_variable").name }}',
            value='{{result("process_child")}}'
        )

        insert_to_process_each_child = rail.SetVariableOperator(
            task_id='insert_to_process_deal_child',
            append=True,
            name='{{ result("declare_variable").name }}',
            value='{{result("process_each_child")}}'
        )

        get_dag_run_id = rail.PythonOperator(
            task_id='get_dag_run_id',
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('declare_variable')['name'])
        )

        wait_for_completion_of_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_of_child',
            execution_timeout=timedelta(days=14),
            dag_runs="{{ result('get_dag_run_id') | to_json}}"
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_rundate >> log_currentfiscalyear >> log_numberofmonthstobeprocessed >> create_list
        create_list >> get_report_details >> run_report_entry
        run_report_exit >> log_modified_timesheetperiod_filter >> declare_variable >> foreach_item_in_list_do
        foreach_item_in_list_do >> if_log_numberofmonthstobeprocessed_less_than_12 >> rail.Label(
            'Yes') >> if_seq_no_is_less_than_log_numberofmonthstobeprocessed
        if_seq_no_is_less_than_log_numberofmonthstobeprocessed >> rail.Label(
            'Yes') >> no_of_months_to_process >> process_child >> insert_to_process_child >> foreach_item_in_list_do_end
        foreach_item_in_list_do_end >> get_dag_run_id >> wait_for_completion_of_child >> finish

        if_seq_no_is_less_than_log_numberofmonthstobeprocessed >> rail.Label(
            'No') >> if_islast_true
        if_islast_true >> rail.Label(
            'Yes') >> months_to_process >> process_each_child >> insert_to_process_each_child >> foreach_item_in_list_do_end
        if_islast_true >> rail.Label(
            'No') >> months_to_get_processed >> process_each_child >> insert_to_process_each_child >> foreach_item_in_list_do_end

        if_log_numberofmonthstobeprocessed_less_than_12 >> rail.Label(
            'No') >> no_of_months_to_process >> process_child
        foreach_item_in_list_do >> foreach_item_in_list_do_end

        return dag


rail.for_each_instance(create_dag)
