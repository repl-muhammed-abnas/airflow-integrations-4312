import json
from datetime import timedelta
import pendulum
from functools import lru_cache
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.mci_time_sync_master,
        description=f'MCIUSA_Replicon to Paycome_Timesync V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        get_conf_payload = rail.PythonOperator(
            task_id='get_conf_payload',
            python_callable=lambda: json.dumps(rail.get_dag_run_conf())
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.timeoff_report_name,
        )

        def get_previous_day():
            current_utc_time = pendulum.now('UTC')
            previous_day = current_utc_time - timedelta(days=1)
            # return "01/08/2022"
            return previous_day.strftime('%m/%d/%Y')

        @lru_cache(maxsize=8)
        def get_dagrun_conf():
            return rail.get_dag_run_conf()

        def get_start_date():
            if get_dagrun_conf():
                return get_dagrun_conf().get('start_date', get_previous_day())
            return get_previous_day()
        
        def get_end_date():
            if get_dagrun_conf():
                return get_dagrun_conf().get('end_date', get_previous_day())
            return get_previous_day()

        def get_specific_filter_uri(report_details, filter_name):
            filterList = report_details["filterConfiguration"]["enabledFilters"]
            return rail.find_first_by_attr_and_get_attr(filterList, 'displayText', filter_name, 'uri')

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details')['uri'],
                        "filterValues": [
                            {
                                "reportFilterUri": get_specific_filter_uri(rail.result('get_report_details'), "ApprovalDateFilter"),
                                "value": null
                            },
                            {
                                "reportFilterUri": get_specific_filter_uri(rail.result('get_report_details'), "ApprovalDateFilter"),
                                "value": get_start_date()
                            },
                            {
                                "reportFilterUri": get_specific_filter_uri(rail.result('get_report_details'), "ApprovalDateFilter"),
                                "value": get_end_date()
                            }
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='payload_column_validation',
            no_task='can_fail_dag'
        )

        payload_column_validation = rail.IfOperator(
            task_id='payload_column_validation',
            # pylint: disable=consider-using-f-string
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_report_columns,
            yes_task="load_report_data_csv",
            no_task="fail_dagrun_column_not_match",
        )

        fail_dagrun_column_not_match = rail.FailOperator(
            task_id="fail_dagrun_column_not_match",
            message='column order has been changed in the base report'
        )

        load_report_data_csv = rail.LoadCSVFileOperator(
            task_id='load_report_data_csv',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )


        create_time_data_collection = rail.CreateCollectionOperator(
            task_id='create_time_data_collection',
            name='timesyncdata',
            source="{{ result('load_report_data_csv') }}",
            columns={
                'User Name': 'username',
                'Employee Type (Current) (Full Path)': 'employeetypefullpath',
                'Employee ID': 'employeeid',
                'Entry Date': 'entrydate',
                'Time In': 'timein',
                'Time Out': 'timeout',
                'Total Hrs': 'totalhrs',
                'Timesheet Period': 'timesheetperiod',
                'Timesheet Start Date': 'timesheetstartdate',
                'Timesheet End Date': 'timesheetenddate',
                'Time Off Hrs': 'timeoffhrs',
                'Time Off Type': 'timeofftype'
            }
        )

        query_time_data = rail.QueryCollectionOperator(
            task_id='query_time_data',
            # pylint: disable=line-too-long
            query="""SELECT DISTINCT employeeid, timesheetperiod FROM timesyncdata WHERE ( totalhrs > '0.00' OR timeoffhrs>'0.00') AND employeeid IS NOT NULL AND timeofftype IS NOT NULL"""
        )

        query_report_time_has_data = rail.IfOperator(
            task_id="query_report_time_has_data",
            test="{{ result('query_time_data','length') > 0 }}",
            yes_task='variable_trigger_dag_ids',
            no_task='can_fail_dag'
        )

        variable_trigger_dag_ids = rail.SetVariableOperator(
            task_id='variable_trigger_dag_ids',
            name='put_time_entry_process',
            value=[]
        )

        foreach_query_time_data = rail.ForEachOperator(
            task_id='foreach_query_time_data',
            items="{{ result('query_time_data') | load_all_records() | to_json }}",
            start_task='query_timesync_data',
            end_task='foreach_query_time_data_end'
        )

        query_timesync_data = rail.QueryCollectionOperator(
            task_id='query_timesync_data',
            query="""SELECT * FROM timesyncdata WHERE (employeeid = '{{ result('foreach_query_time_data').employeeid }}' AND timesheetperiod = '{{ result('foreach_query_time_data').timesheetperiod }}' AND (totalhrs > '0.00' OR timeoffhrs >'0.00')) AND timeofftype IS NOT NULL""",
        )

        def get_time_entry_data():
            time_entry_data = []
            time_sync_data = rail.load_all_records(rail.result('query_timesync_data'))
            for sync_data in time_sync_data:
                timeofftype = sync_data['timeofftype']
                paycode = rail.find_first_by_attr_and_get_attr(
                    config.TIMEOFF_PAYCODE_MAPPER, 'timeoff', timeofftype, 'code', 'R'
                ) if timeofftype else 'R'
                time_entry_data.append({
                    "employeeid": sync_data['employeeid'],
                    "entrydate": sync_data['entrydate'],
                    "intime": sync_data['timein'],
                    "hours": sync_data['timeoffhrs'] if sync_data['totalhrs'] == '0.00' else sync_data['totalhrs'],
                    "outtime": sync_data['timeout'],
                    "paycode": paycode,
                    "username": sync_data['username'],
                    "employeetype": (sync_data['employeetypefullpath']).split()[0],
                })
            return {
                "time_sync_user_data": time_entry_data,
                "timesheetperiod": rail.result('foreach_query_time_data')['timesheetperiod'],
                "timesheetstartdate": time_sync_data[0]['timesheetstartdate'] if time_sync_data else "",
                "timesheetenddate": time_sync_data[0]['timesheetenddate'] if time_sync_data else "",
                "conf": rail.result('get_conf_payload')
            }

        trigger_puttimeentry_in_paycom_child = rail.TriggerDagRunOperator(
            task_id='trigger_puttimeentry_in_paycom_child',
            trigger_dag_id=config.mci_time_sync_puttimeentry_in_paycom_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: get_time_entry_data()
        )

        insert_put_time_entry_dag_runs = rail.SetVariableOperator(
            task_id='insert_put_time_entry_dag_runs',
            append=True,
            name='{{ result("variable_trigger_dag_ids").name }}',
            value='{{result("trigger_puttimeentry_in_paycom_child")}}'
        )

        foreach_query_time_data_end = rail.EmptyOperator(
            task_id='foreach_query_time_data_end',
        )

        get_variable_trigger_dag_ids = rail.GetVariableOperator(
            task_id='get_variable_trigger_dag_ids',
            name='{{ result("variable_trigger_dag_ids").name }}'
        )

        wait_for_variable_trigger_dag_ids = rail.WaitForDagRunsSensor(
            task_id='wait_for_variable_trigger_dag_ids',
            dag_runs='{{ result("get_variable_trigger_dag_ids").value | to_json }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_time_sync_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_time_sync_logs',
            dag_runs="{{ result('get_variable_trigger_dag_ids').value | to_json }}",
            dagrun_task_id='process_time_sync_log',
            execution_timeout=timedelta(
                hours=config.gather_time_sync_logs_timeout_hours),
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation,
            conf=lambda: {
                'entry_logs': rail.result('gather_time_sync_logs') if rail.result('gather_time_sync_logs') else [],
                'otherlogs': [],
                'filename': f"""TimeExport {rail.render_template("{{current_time_in_specified_tz(fmt='%m%d%Y-%H%M%S') | replace(':', '-')}}")}.csv"""
            }
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun_with_error",
        )

        fail_dagrun_with_error = rail.FailOperator(
            task_id="fail_dagrun_with_error",
            message='{{ get_error_message() }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        get_conf_payload >> get_report_details >> run_report_group_entry
        run_report_group_exit >> report_has_data

        report_has_data >> rail.Label('No') >> can_fail_dag
        report_has_data >> rail.Label('Yes') >> payload_column_validation
        payload_column_validation >> rail.Label(
            'Yes') >> load_report_data_csv >> create_time_data_collection >> query_time_data >> query_report_time_has_data
        query_report_time_has_data >> rail.Label(
            'Yes') >> variable_trigger_dag_ids
        query_report_time_has_data >> rail.Label(
            'No') >> can_fail_dag
        payload_column_validation >> rail.Label('No') >> fail_dagrun_column_not_match
        variable_trigger_dag_ids >> foreach_query_time_data >> query_timesync_data >> \
        trigger_puttimeentry_in_paycom_child >> insert_put_time_entry_dag_runs >> foreach_query_time_data_end
        foreach_query_time_data >> foreach_query_time_data_end >> \
        get_variable_trigger_dag_ids >> wait_for_variable_trigger_dag_ids >> gather_time_sync_logs >> process_log_generation >> \
        can_fail_dag >> rail.Label(
            'Yes') >> fail_dagrun_with_error >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
