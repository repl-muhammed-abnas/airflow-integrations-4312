from datetime import timedelta
import rail


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_custom_notification_shiftassignment_master_{config.instance}',
        description=f'DXC Custom notification for shiftassignment - India V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_active_runs,
    ) as dag:

        custom_notification_shiftassignment = rail.RepliconReportDetailsOperator(
            task_id='custom_notification_shiftassignment',
            report_name=config.basereport_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('custom_notification_shiftassignment').uri}}",
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
            yes_task='report_has_expected_columns',
            no_task='finish'

        )

        expected_report_columns = "User Name,Login Name,supervisorname,supervisoremail,scheduletype,useruri,supervisoruri,startdatetocheck,enddatetocheck"

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % expected_report_columns,
            no_task='fail_invalid_report_columns',
            yes_task='load_report_data',
        )

        fail_invalid_report_columns = rail.FailOperator(
            task_id="fail_invalid_report_columns",
            message="Base report columns do not match",
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_customnotification_shiftassignment_collection = rail.CreateCollectionOperator(
            task_id='create_customnotification_shiftassignment_collection',
            name='shiftassignment_basereportdata',
            source="{{ result('load_report_data') }}",
        )

        query_basereportdata = rail.QueryCollectionOperator(
            task_id='query_basereportdata',
            name='distinctsupervisors',
            query="""SELECT DISTINCT supervisoruri,supervisorname,supervisoremail
                        FROM shiftassignment_basereportdata
                        WHERE scheduletype='Shift Schedule' 
                        AND supervisoremail IS NOT NULL"""
        )

        process_each_supervisor = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_supervisor',
            retries=0,
            items=lambda: rail.result('query_basereportdata'),
            trigger_dag_id=f'dxctechnology_custom_notification_shiftassignment_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_hours),
            conf=lambda item: {
                'supervisoruri': item['supervisoruri'],
                'supervisorname': item['supervisorname'],
                'supervisoremail': item['supervisoremail']
            }
        )

        wait_process_each_supervisor = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_supervisor',
            dag_runs='{{ result("process_each_supervisor") }}',
            execution_timeout=timedelta(days=config.execution_timeout_hours),
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        custom_notification_shiftassignment >> run_report_group_entry >> run_report_group_exit >> report_has_data
        report_has_data >> rail.Label("No") >> finish
        report_has_data >> rail.Label("Yes") >> report_has_expected_columns
        report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_report_columns >> finish
        report_has_expected_columns >> rail.Label("Yes") >> \
            load_report_data >> create_customnotification_shiftassignment_collection >> \
            query_basereportdata >> process_each_supervisor >> wait_process_each_supervisor

    return dag


rail.for_each_instance(create_dag)
