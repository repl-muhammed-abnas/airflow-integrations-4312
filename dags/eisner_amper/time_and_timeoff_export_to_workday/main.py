from datetime import timedelta
from pendulum import datetime
import rail
from eisner_amper.time_and_timeoff_export_to_workday.utils import request_payload, response_filter


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"eisner_amper_time_export_master_{config.instance}",
        description=f"Eisner Amper Time Export Master {config.instance}",
        company_key=config.company_key,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2022, 4, 1, tz=config.timezone),
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs_master
    ) as dag:
        
        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=request_payload.logging_details,
            op_args=[config.timezone]
        )

        get_all_filter_definitions = rail.RepliconServiceOperator(
            task_id="get_all_filter_definitions",
            endpoint="/services/TimeDataExportService1.svc/GetAllFilterDefinitions",
            data=request_payload.getallfilterdefinitions
        )

        get_all_scripts = rail.RepliconServiceOperator(
            task_id="get_all_scripts",
            endpoint="/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts",
            data=[]
        )

        get_all_object_Extensionfield_details = rail.RepliconServiceOperator(
            task_id="get_all_object_Extensionfield_details",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data=request_payload.getallobjectExtensionfielddetails
        )

        get_enabled_employeetype_groups = rail.RepliconServiceOperator(
            task_id="get_enabled_employeetype_groups",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups",
            data=[],
            response_filter=response_filter.getenabledemployeetypegroups
        )

        get_object_extension_tag_definition_details = rail.RepliconServiceOperator(
            task_id="get_object_extension_tag_definition_details",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=request_payload.getobjectextensiontagdefinitiondetails,
            response_filter=response_filter.get_object_tag_definitiondetails
        )

        get_object_extension_tag_definition_detail_project_type = rail.RepliconServiceOperator(
            task_id="get_object_extension_tag_definition_detail_project_type",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=request_payload.getobjectextensiontagdefinitiondetail_project_type,
            response_filter=response_filter.get_object_tag_definitiondetails_project_type
        )

        create_timedata_batch = rail.RepliconServiceOperator(
            task_id="create_timedata_batch",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataExportBatch",
            data=request_payload.get_timedata_batch_data
        )

        execute_timedata_batch, wait_fortimedata_batch = rail.batch_execution(
            'execute_payrun_batch', create_timedata_batch.task_id)

        get_timedata_batch_result = rail.RepliconServiceOperator(
            task_id="get_timedata_batch_result",
            endpoint="/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults",
            data={
                "timeDataExportBatchUri": "{{ result('create_timedata_batch') }}"}
        )

        update_timedata_name = rail.RepliconServiceOperator(
            task_id="update_timedata_name",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data=request_payload.get_update_timedata_name
        )

        mark_timedata_as_complete = rail.RepliconServiceOperator(
            task_id="mark_timedata_as_complete",
            endpoint="/services/TimeDataExportService1.svc/MarkTimeDataExportAsComplete",
            data={"target": {
                "uri": "{{ result('get_timedata_batch_result').timeDataExportUri }}"}}
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed'
        )

        cancel_timedatarun = rail.RepliconServiceOperator(
            task_id="cancel_timedatarun",
            endpoint="/services/TimeDataExportService1.svc/CancelTimeDataExport",
            data={"target": {
                "uri": "{{ result('get_timedata_batch_result').timeDataExportUri }}"}}
        )

        update_timedata_name_cancelled = rail.RepliconServiceOperator(
            task_id="update_timedata_name_cancelled",
            endpoint="/services/TimeDataExportService1.svc/CancelTimeDataExport",
            data=request_payload.get_update_timedata_name_cancelled
        )

        fail_export = rail.FailOperator(
            task_id="fail_export",
            message=" The time export is cancelled"
        )


        process_timedata_export = rail.TriggerDagRunOperator(
            task_id='process_timedata_export',
            retries=0,
            trigger_dag_id=f'eisner_amper_time_export_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.process_timedata_export_conf
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        get_logging_details >> get_all_filter_definitions >> get_all_scripts >> get_all_object_Extensionfield_details >> get_enabled_employeetype_groups\
            >> get_object_extension_tag_definition_details >> get_object_extension_tag_definition_detail_project_type\
            >> create_timedata_batch >> execute_timedata_batch >> wait_fortimedata_batch >> get_timedata_batch_result\
            >> update_timedata_name >> mark_timedata_as_complete >> rail.Label(
                "on_error") >> catch_error >> cancel_timedatarun >> update_timedata_name_cancelled >> fail_export

        mark_timedata_as_complete >> rail.Label("on_success") >> process_timedata_export >> log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag


rail.for_each_instance(create_main_dag)
