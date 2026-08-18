from datetime import timedelta
from pendulum import datetime
import pendulum
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Capgemini Time Export File based Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.schedule_interval),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id_internal,
            'retries': 0
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.exports_data_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='download_file',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor')}}",
        )

        archive_input_file = rail.SFTPMoveFileOperator(
            task_id='archive_input_file',
            new_filename=config.exports_data_archive_filepath + '/{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}',
            existing_filename=config.exports_data_filepath+'/{{ result("new_file_sensor") | file_name }}',
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            delimiter=",",
            document="{{ result('download_file') }}",
        )

        create_export_uri_collection = rail.CreateCollectionOperator(
            task_id='create_export_uri_collection',
            source="{{ result('parse_csv') }}",
            name="time_exports_data",
            columns={
                "Export_name": "exportname",
                "Export_URI": "exporturi",
                "Export_Period": "exportperiod",
                "Export_Createdby": "exportcreatedby",
                "Export_Creationtime": "exportcreationtime",
                "Export_Location": "exportlocation",
                "Data_Present": "datapresent"
            }
        )

        get_time_download_script = rail.RepliconServiceOperator(
            task_id='get_time_download_script',
            endpoint="/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(response.json()['d'],
                    'displayText', config.time_export_file_format, 'uri')
        )

        download_each_export = rail.TriggerDagRunForEachItemOperator(
            task_id='download_each_export',
            items='{{ result("create_export_uri_collection") }}',
            trigger_dag_id=config.time_export_child_dag_id,
            conf=lambda item: {
                "file_format_uri": rail.result("get_time_download_script"),
                "time_export_uri": item["exporturi"],
                "time_export_name": item["exportname"],
                "time_export_period": item["exportperiod"]
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        new_file_sensor >> was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        was_new_file_found >> rail.Label("Yes") >> download_file >> archive_input_file >> parse_csv \
            >> create_export_uri_collection >> get_time_download_script >> download_each_export

    return dag

rail.for_each_instance(create_dag)
