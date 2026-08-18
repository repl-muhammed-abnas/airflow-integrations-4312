import rail
from pwcglobal.distance_data_extract.fiscal_export_v2 import request_payload


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.upload_file_child_dag_id,
        description=f'PwC - Process previous Fiscal year extract upload file',
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
            report_params=request_payload.get_run_report_payload,
            target='artifact',
        )

        has_data = rail.IfOperator(
            task_id="has_data",
            test="{{result('load_report.get_report_result','has_data')}}",
            yes_task='report_has_expected_columns',
            no_task='finish_export'
        )

        finish_export = rail.EmptyOperator(
            task_id='finish_export'
        )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            # pylint: disable=consider-using-f-string
            test="{{ (result('load_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | starts_with('%s') }}" % config.column_order,
            no_task='fail_invalid_report_colums',
            yes_task='report_payload_to_csv',
        )

        fail_invalid_report_colums = rail.FailOperator(
            task_id="fail_invalid_report_colums",
            message="Base report column does not match"
        )

        report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="report_payload_to_csv",
            document="{{(result('load_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload}}"
        )

        load_report >> has_data
        has_data >> rail.Label("No") >> finish_export

        has_data >> rail.Label("Yes") >> report_has_expected_columns
        report_has_expected_columns >> rail.Label(
            "Yes") >> report_payload_to_csv
        report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_report_colums

    return dag


rail.for_each_instance(create_child_dag)
