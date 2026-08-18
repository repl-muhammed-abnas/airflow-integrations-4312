import json
from datetime import timedelta
from airflow.models import Variable
import rail

null = None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.mci_timeoff_sync_master,
        description=f'MCIUSA_Replicon to Paycome_TimeOffSync V2.0 {config.instance}',
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

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='payload_column_validation',
            no_task='log_to_sumo'
        )


        expected_report_columns = 'User Name,Employee ID,Time Off Type,Time Off Date,Time Off Hrs,Approval Date'
        payload_column_validation = rail.IfOperator(
            task_id='payload_column_validation',
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | \
                starts_with('%s') }}" % expected_report_columns,
            yes_task="load_report_data",
            no_task="fail_with_error",
        )

        fail_with_error = rail.FailOperator(
            task_id='fail_with_error',
            message='''Report colomun format is changed'''
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_user_collection = rail.CreateCollectionOperator(
            task_id='create_user_collection',
            name='timeoffdata',
            source="{{ result('load_report_data') }}",
            columns={
                'User Name': 'username',
                'Employee ID': 'employeeid',
                'Time Off Type': 'timeofftype',
                'Time Off Date': 'timeoffdate',
                'Time Off Hrs': 'timeoffhrs',
                'Approval Date': 'approvaldate'
            }
        )

        query_timeoff_data = rail.QueryCollectionOperator(
            task_id='query_timeoff_data',
            # pylint: disable=line-too-long
            query="""SELECT DISTINCT employeeid FROM timeoffdata WHERE employeeid IS NOT NULL AND timeoffhrs > '0.00'"""
        )

        query_report_timeoff_has_data = rail.IfOperator(
            task_id="query_report_timeoff_has_data",
            test="{{ result('query_timeoff_data','length') > 0 }}",
            yes_task='declare_list_update_dag_runs',
            no_task='log_to_sumo'
        )

        declare_list_update_dag_runs = rail.SetVariableOperator(
            task_id='declare_list_update_dag_runs',
            name='user_process_update_dag_runs_753',
            value=[]
        )

        foreach_query_list_timeoff_data = rail.ForEachOperator(
            task_id='foreach_query_list_timeoff_data',
            items="{{ result('query_timeoff_data') }}",
            start_task='query_all_data_of_employeeid',
            end_task='foreach_query_list_timeoff_data_end'
        )

        query_all_data_of_employeeid = rail.QueryCollectionOperator(
            task_id='query_all_data_of_employeeid',
            # pylint: disable=line-too-long
            query="""SELECT * FROM  timeoffdata WHERE employeeid = '{{ result('foreach_query_list_timeoff_data').employeeid }}' AND  timeoffhrs > '0.00'""",
        )

        def get_timeoff_data_to_process():
            timeoff_data = []
            timeoffs_query_data = rail.load_all_records(
                rail.result('query_all_data_of_employeeid'))
            for timeoff in timeoffs_query_data:
                paycoed_from_mapper = rail.find_first_by_attr_and_get_attr(config.TIMEOFF_PAYCODE_MAPPER,
                    'timeoff', timeoff['timeofftype'], 'code', '')
                timeoff_data.append({
                    "username": timeoff['username'],
                    "employeeid": timeoff['employeeid'],
                    "timeoffhours": timeoff['timeoffhrs'],
                    "entrydate": timeoff['timeoffdate'],
                    "paycode": paycoed_from_mapper
                })

            return {
                "usertimeoffdata": timeoff_data,
                "conf": rail.result('get_conf_payload')
            }

        trigger_puttimeoffentry_in_paycom_child = rail.TriggerDagRunOperator(
            task_id='trigger_puttimeoffentry_in_paycom_child',
            trigger_dag_id=config.mci_timeoff_sync_puttimeentry_in_paycom_child,
            execution_timeout=timedelta(days=14),
            conf=get_timeoff_data_to_process
        )

        insert_to_update_dag_runs_var = rail.SetVariableOperator(
            task_id='insert_to_update_dag_runs_var',
            append=True,
            name='{{ result("declare_list_update_dag_runs").name }}',
            # pylint: disable=line-too-long
            value='{{result("trigger_puttimeoffentry_in_paycom_child")}}'
        )

        foreach_query_list_timeoff_data_end = rail.EmptyOperator(
            task_id='foreach_query_list_timeoff_data_end',
        )

        wait_for_trigger_puttimeoffentry_in_paycom_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_puttimeoffentry_in_paycom_child',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_update_dag_runs_var").value | to_json }}'
        )
        
        gather_time_sync_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_time_sync_logs',
            dag_runs="{{ result('insert_to_update_dag_runs_var').value | to_json }}",
            dagrun_task_id='process_timeoff_sync_log',
            execution_timeout=timedelta(
                hours=config.gather_timeoff_sync_logs_timeout_hours),
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
                'filename': f"""TimeoffExport {rail.render_template("{{current_time_in_specified_tz(fmt='%m%d%Y-%H%M%S') | replace(':', '-')}}")}.csv"""
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        get_conf_payload >> get_report_details >> run_report_group_entry
        run_report_group_exit >> report_has_data >> rail.Label(
            "No") >> log_to_sumo
        report_has_data >> rail.Label("Yes") >> payload_column_validation
        payload_column_validation >> rail.Label(
            'Yes') >> load_report_data >> create_user_collection >> query_timeoff_data >> query_report_timeoff_has_data
        query_report_timeoff_has_data >> rail.Label(
            'Yes') >> declare_list_update_dag_runs
        query_report_timeoff_has_data >> rail.Label(
            'No') >> log_to_sumo
        payload_column_validation >> rail.Label(
            'No') >> fail_with_error
        declare_list_update_dag_runs >> \
            foreach_query_list_timeoff_data >> \
            query_all_data_of_employeeid >> trigger_puttimeoffentry_in_paycom_child >> \
            insert_to_update_dag_runs_var >> \
            foreach_query_list_timeoff_data_end
        foreach_query_list_timeoff_data >> foreach_query_list_timeoff_data_end >> \
            wait_for_trigger_puttimeoffentry_in_paycom_child >> gather_time_sync_logs >> process_log_generation >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
