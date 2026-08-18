
from datetime import timedelta
from pendulum import datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'xencorprod_timeoffbalancealert_xencor_send_timeoffbalance_email_and_push_notification_master_{config.instance}',
        description=f'Xencor send timeoffbalance email and push notification - Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 5, 1, tz=config.pacific_timezone),
        schedule_interval=config.schedule_interval_daily,
        catchup=False,
        max_active_runs=1,
        default_args={
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_report_details_7'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_report_details_7',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_report_details_7 = rail.RepliconReportDetailsOperator(
            task_id='get_report_details_7',
            report_name=config.timeoff_report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_report_details_7').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data_7 = rail.IfOperator(
            task_id="report_has_data_7",
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='if_generate_report_34_payload_starts_with_nodata_7',
            no_task='log_to_sumo'
        )

        if_generate_report_34_payload_starts_with_nodata_7 = rail.IfOperator(
            task_id='if_generate_report_34_payload_starts_with_nodata_7',
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('No Data') }}",
            yes_task="stop_15",
            no_task="if_generate_report_34_payload_not_starts_with_column_7",
        )

        stop_15 = rail.FailOperator(
            task_id='stop_15',
            message='''Base report column order doesn't match'''
        )

        if_generate_report_34_payload_not_starts_with_column_7 = rail.IfOperator(
            task_id='if_generate_report_34_payload_not_starts_with_column_7',
            # pylint: disable=line-too-long
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('User Name,Time Off Type,Units,Time Off Balance,User Supervisor Name (Current),UserUri,SupervisorUri,TimeOffTypeUri,User Email,User Supervisor Email address')}}",
            yes_task="load_report_data_7",
            no_task="stop_13",
        )

        stop_13 = rail.EmptyOperator(
            task_id='stop_13',

        )

        load_report_data_7 = rail.LoadCSVFileOperator(
            task_id='load_report_data_7',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_user_collection_7 = rail.CreateCollectionOperator(
            task_id='create_user_collection_7',
            name='timeoffreportdata',
            source="{{ result('load_report_data_7') }}",
            columns={
                'User Name': 'username',
                'Time Off Type': 'timeofftype',
                'Units': 'units',
                'Time Off Balance': 'timeoffbalance',
                'User Supervisor Name (Current)': 'usersupervisorname',
                'UserUri': 'useruri',
                'SupervisorUri': 'supervisoruri',
                'TimeOffTypeUri': 'timeofftypeuri',
                'User Email': 'useremail',
                'User Supervisor Email address': 'supervisoremail'
            }
        )

        query_list_userlist_17 = rail.QueryCollectionOperator(
            task_id='query_list_userlist_17',
            query="""SELECT DISTINCT timeoffreportdata.useruri FROM  timeoffreportdata""",
        )

        declare_list_dag_runs_17 = rail.SetVariableOperator(
            task_id='declare_list_dag_runs_17',
            name='user_process_dag_runs',
            value=[]
        )

        foreach_query_list_userlist_17_18 = rail.ForEachOperator(
            task_id='foreach_query_list_userlist_17_18',
            items="{{ result('query_list_userlist_17') }}",
            start_task='query_list_timeoffreportdataforeachuser_19',
            end_task='foreach_query_list_userlist_17_18_end'
        )

        query_list_timeoffreportdataforeachuser_19 = rail.QueryCollectionOperator(
            task_id='query_list_timeoffreportdataforeachuser_19',
            query="""SELECT * FROM  timeoffreportdata WHERE  timeoffreportdata.useruri='{{ result('foreach_query_list_userlist_17_18').useruri }}'""",
        )

        def get_data_from_document(document):
            with rail.lib.readers.get_data_reader(document) as reader:
                return list(reader)

        def get_first_record(task_name):
            userrecords = []
            to_balance_reocrds = get_data_from_document(rail.result(task_name))
            for balance_record in to_balance_reocrds:
                userrecords.append({
                    "username": balance_record['username'],
                    "timeofftype": balance_record['timeofftype'],
                    "units": balance_record['units'],
                    "timeoffbalance": balance_record['timeoffbalance'],
                    "usersupervisorname": balance_record['usersupervisorname'],
                    "useruri": balance_record['useruri'],
                    "supervisoruri": balance_record['supervisoruri'],
                    "timeofftypeuri": balance_record['timeofftypeuri'],
                    "useremail": balance_record['useremail'],
                    "supervisoremail": balance_record['supervisoremail']
                })

            return userrecords

        tobalance_record_20 = rail.PythonOperator(
            task_id='tobalance_record_20',
            python_callable=lambda: get_first_record(
                'query_list_timeoffreportdataforeachuser_19')
        )

        trigger_dag_run_live_xencor_send_timeoffbalance_email_and_push_notification_processbyuser_childasync_20 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_xencor_send_timeoffbalance_email_and_push_notification_processbyuser_childasync_20',
            retries=0,
            items=[-1],
            trigger_dag_id=f'xencor_send_timeoffbalance_email_and_push_notification_processbyuser_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda: {
                "userrecords": get_first_record('query_list_timeoffreportdataforeachuser_19')
            }
        )

        insert_to_user_dag_run_list_20 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_20',
            append=True,
            name='{{ result("declare_list_dag_runs_17").name }}',
            value='{{(result("trigger_dag_run_live_xencor_send_timeoffbalance_email_and_push_notification_processbyuser_childasync_20"))[0]}}'
        )

        foreach_query_list_userlist_17_18_end = rail.EmptyOperator(
            task_id='foreach_query_list_userlist_17_18_end',
        )

        wait_for_completion_trigger_dag_run_live_xencor_send_timeoffbalance_email_and_push_notification_processbyuser_childasync_20 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_xencor_send_timeoffbalance_email_and_push_notification_processbyuser_childasync_20',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list_20").value | to_json }}'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_report_details_7 >> run_report_group_entry
        run_report_group_exit >> report_has_data_7
        report_has_data_7 >> rail.Label('No') >> log_to_sumo
        report_has_data_7 >> rail.Label(
            'Yes') >> if_generate_report_34_payload_starts_with_nodata_7
        if_generate_report_34_payload_starts_with_nodata_7 >> rail.Label(
            'Yes') >> stop_15 >> log_to_sumo
        if_generate_report_34_payload_starts_with_nodata_7 >> rail.Label(
            'No') >> if_generate_report_34_payload_not_starts_with_column_7
        if_generate_report_34_payload_not_starts_with_column_7 >> rail.Label(
            'No') >> stop_13 >> log_to_sumo
        if_generate_report_34_payload_not_starts_with_column_7 >> rail.Label('Yes') >> load_report_data_7 >> \
            create_user_collection_7 >> query_list_userlist_17 >> declare_list_dag_runs_17 >> \
            foreach_query_list_userlist_17_18 >> query_list_timeoffreportdataforeachuser_19 >> tobalance_record_20 >> \
            trigger_dag_run_live_xencor_send_timeoffbalance_email_and_push_notification_processbyuser_childasync_20 >> \
            insert_to_user_dag_run_list_20 >> foreach_query_list_userlist_17_18_end
        foreach_query_list_userlist_17_18 >> foreach_query_list_userlist_17_18_end >> \
            wait_for_completion_trigger_dag_run_live_xencor_send_timeoffbalance_email_and_push_notification_processbyuser_childasync_20 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
