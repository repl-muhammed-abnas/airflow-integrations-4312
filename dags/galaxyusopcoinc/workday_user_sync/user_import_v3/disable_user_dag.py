from datetime import timedelta
import rail

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.disable_user_dag_id,
        description=f'VialtoPartners_Disable User V1.0 - SFTP {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.disable_schedule,
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name=config.user_report_name
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_user_report_details').uri}}",
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
            yes_task='load_user_csv_data',
            no_task='finish'
        )

        load_user_csv_data = rail.LoadCSVFileOperator(
            task_id="load_user_csv_data",
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_reportuser_data_collection = rail.CreateCollectionOperator(
            task_id="create_reportuser_data_collection",
            name="reportuserdata",
            source="{{ result('load_user_csv_data') }}",
            columns={
                'Employee ID': 'employee_id',
                'Login Name': 'login_name',
                'User Email': 'user_email',
                'User Name': 'user_name',
                'User Uri': 'user_uri',
                'User Status': 'user_status',
                'User Start Date': 'user_start_date',
                'User End Date': 'user_end_date',
                'disableuser': 'disableuser'

            }
        )

        query_users = rail.QueryCollectionOperator(
            task_id='query_users',
            query='''SELECT DISTINCT user_uri, user_end_date FROM reportuserdata
                    WHERE disableuser="Yes"'''
        )

        has_users = rail.IfOperator(
            task_id='has_users',
            test='{{ result("query_users", "length") > 0 }}',
            yes_task='trigger_disable_user_child',
            no_task='finish'
        )

        trigger_disable_user_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_disable_user_child',
            retries=0,
            items=lambda: rail.result('query_users'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.disable_user_child_dag_id,
            conf=lambda item: {
                "user_uri" :item['user_uri'],
                "user_end_date": item['user_end_date'],
                "caller":"disable_user"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        get_user_report_details >> run_report_group_entry >> run_report_group_exit >> report_has_data
        report_has_data >> rail.Label(
            "No") >> finish
        report_has_data >> rail.Label(
            "Yes") >> load_user_csv_data >> create_reportuser_data_collection >> query_users
        query_users >> has_users >> rail.Label(
            "Yes") >> trigger_disable_user_child
        has_users >> rail.Label(
            "No") >> finish

    return dag


rail.for_each_instance(create_dag)
