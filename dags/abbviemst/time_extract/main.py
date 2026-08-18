import rail
from pendulum import datetime as dt
from datetime import timedelta
from rail.lib.ecid import get_dagrun_ecid
from abbviemst.time_extract.utils import python_callable

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'AbbvieMST Time Extract Sync Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.master_schedule_interval,
        start_date=dt(2023, 1, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        # Check if today is the correct day to run based on Workato schedule
        # Skip this check for trial instance
        should_check_schedule = config.instance != "trial"

        if should_check_schedule:
            check_should_run = rail.IfOperator(
                task_id='check_should_run',
                test=python_callable.should_run_based_on_workato_schedule,
                yes_task='logging_details',
                no_task='skip_run'
            )

            skip_run = rail.EmptyOperator(
                task_id='skip_run'
            )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=python_callable.get_logging_details
        )

        get_all_scripts = rail.RepliconServiceOperator(
            task_id='get_all_scripts',
            endpoint='/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts'
        )

        get_all_columns = rail.RepliconServiceOperator(
            task_id="get_all_columns",
            endpoint="/services/TimeDataExportService1.svc/GetAllColumns"
        )

        get_task_columns_parse_json_7 = rail.PythonOperator(
            task_id='get_task_columns_parse_json_7',
            python_callable=python_callable.get_required_column_uris
        )

        if_required_column_uris_present = rail.IfOperator(
            task_id='if_required_column_uris_present',
            test="{{ result('get_task_columns_parse_json_7') | is_truthy }}",
            yes_task='get_file_format_uri_10',
            no_task='log_required_column_not_present'
        )

        log_required_column_not_present = rail.WriteLogOperator(
            task_id='log_required_column_not_present',
            message="Required column not present",
            severity="Error",
            properties=lambda dag_run: {
                "Status": "Error",
                "Details": "Required column not present",
                "childjobid": get_dagrun_ecid(dag_run)
            }
        )

        get_file_format_uri_10 = rail.PythonOperator(
            task_id='get_file_format_uri_10',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts'), "displayText", "Custom Time export", "uri", " ")
        )

        if_file_format_uri_present_11 = rail.IfOperator(
            task_id='if_file_format_uri_present_11',
            test=lambda: rail.result('get_file_format_uri_10'),
            yes_task='trigger_time_extract_new_delta_child',
            no_task='log_required_fileformat_not_present'
        )

        trigger_time_extract_new_delta_child = rail.TriggerDagRunOperator(
            task_id='trigger_time_extract_new_delta_child',
            trigger_dag_id=config.time_extract_delta_child_dagid,
            conf=lambda: {
                'fileFormatScriptUri': rail.result('get_file_format_uri_10'),
                'startDate': rail.result('logging_details')['start_date'],
                'endDate': rail.result('logging_details')['end_date'],
                # Get URIs from parsed task columns
                'protocolcode_columnuri': rail.result('get_task_columns_parse_json_7').get('protocol_code_uri'),
                'compassrootcode_columnuri': rail.result('get_task_columns_parse_json_7').get('compass_root_product_code_uri'),
                'beneficiarycode_columnuri': rail.result('get_task_columns_parse_json_7').get('beneficiary_code_uri'),
                'compass_dropdown': rail.result('get_task_columns_parse_json_7').get('compass_uri'),
                'export_name': rail.result('logging_details')['export_name'],
                'file_name': rail.result('logging_details')['file_name']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        log_required_fileformat_not_present = rail.WriteLogOperator(
            task_id='log_required_fileformat_not_present',
            message="Required file format is not available",
            severity="Error",
            properties=lambda dag_run: {
                "Status": "Error",
                "Details": "Required file format is not available",
                "childjobid": get_dagrun_ecid(dag_run)
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        fail_dag = rail.FailOperator(
            task_id = "fail_dag",
            message="Required data not present"
        )

        # Task dependencies
        if should_check_schedule:
            check_should_run >> rail.Label("Yes") >> logging_details
            check_should_run >> rail.Label("No") >> skip_run

        logging_details >> get_all_scripts >> get_all_columns >> get_task_columns_parse_json_7 >> if_required_column_uris_present

        if_required_column_uris_present >> rail.Label("Yes") >> get_file_format_uri_10 >> if_file_format_uri_present_11
        if_required_column_uris_present >> rail.Label("No") >> log_required_column_not_present >> fail_dag

        if_file_format_uri_present_11 >> rail.Label("Yes") >> trigger_time_extract_new_delta_child >> finish
        if_file_format_uri_present_11 >> rail.Label("No") >> log_required_fileformat_not_present >> fail_dag

    return dag


rail.for_each_instance(create_dag)