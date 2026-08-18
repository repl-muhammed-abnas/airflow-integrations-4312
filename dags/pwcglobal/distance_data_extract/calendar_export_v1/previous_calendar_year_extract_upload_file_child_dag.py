import rail
from pwcglobal.distance_data_extract.calendar_export_v1 import request_payload

def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'pwcglobal_process_previous_calendar_year_extract_upload_file_child_dag_{config.instance}_v1',
        description=f'Process previous calendar year extract upload file {config.instance} V1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        load_report = rail.run_report(
            group_id='load_report',
            report_params=request_payload.get_run_report_payload
        )

        has_data = rail.IfOperator(
            task_id  = "has_data",
            test = '{{"No Data" not in result("load_report.get_report_result").reportGenerationResults[0].payload}}',
            yes_task= 'report_has_expected_columns',
            no_task= 'finish_export'
        )

        finish_export = rail.EmptyOperator(
           task_id= 'finish_export'
        )

        expected_report_columns = config.column_order
        # pylint: disable=line-too-long
        report_has_expected_columns = rail.IfOperator(
            task_id = "report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            test="{{ result('load_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % expected_report_columns,
            no_task='fail_invalid_report_colums',
            yes_task='report_payload_to_csv',
        )

        fail_invalid_report_colums = rail.FailOperator(
            task_id = "fail_invalid_report_colums",
            message="Base report column does not match"
        )

        report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id = "report_payload_to_csv",
            document= '{{result("load_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        load_report >> has_data
        has_data >> rail.Label("No") >> finish_export

        has_data >> rail.Label("Yes") >> report_has_expected_columns
        report_has_expected_columns >> rail.Label("Yes") >> report_payload_to_csv

        report_has_expected_columns >> rail.Label("No") >> fail_invalid_report_colums

    return dag

rail.for_each_instance(create_child_dag)
