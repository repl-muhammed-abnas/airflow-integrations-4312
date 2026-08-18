from datetime import timedelta
from pendulum import datetime
from elevatebiomanagement.time_export.utils import request_payload
from elevatebiomanagement.time_export.utils import response_filter
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'elevatebio_management_time_export_master_{config.instance}',
        description=f'Elevate Bio Management Time Export Master 1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 7, 16, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_master,
        default_args={
            'retries': 0
        }
    ) as dag:
        
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_time_download_scripts'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_time_download_scripts',
            end_task='batch_end',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_time_download_scripts = rail.RepliconServiceOperator(
            task_id='get_all_time_download_scripts',
            endpoint="/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts",
            response_filter=response_filter.get_export_uri
        )

        create_export = rail.RepliconServiceOperator(
            task_id='create_export',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportBatch',
            data=request_payload.get_create_export_payload
        )

        (execute_time_data, wait_for_time_data) = rail.batch_execution(
            group_id='execute_row_counts_batch',
            creation_task_id=create_export.task_id,
            wait_timeout=60*60*5,
        )

        get_export_batch_results = rail.RepliconServiceOperator(
            task_id='get_export_batch_results',
            endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
            data=request_payload.get_export_batch_results_payload
        )

        update_export_name = rail.RepliconServiceOperator(
            task_id="update_export_name",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": '{{ result("get_export_batch_results").timeDataExportUri }}',
                    "name": null
                },
                "name": "TimeDate_Export_{{ ecid() | transform('^.{0,10}')}}"
            }
        )

        mark_as_completed = rail.RepliconServiceOperator(
            task_id="mark_as_completed",
            endpoint="/services/TimeDataExportService1.svc/MarkTimeDataExportAsComplete",
            data=request_payload.get_mark_as_completed_payload,
        )

        create_download_batch = rail.RepliconServiceOperator(
            task_id='create_download_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch',
            data=request_payload.get_time_download_payload
        )

        (execute_download_batch, wait_for_download_batch) = rail.batch_execution(
            group_id='execute_download_batch',
            creation_task_id=create_download_batch.task_id,
            wait_timeout=60*60*5,
        )

        get_download_url = rail.RepliconServiceOperator(
            task_id='get_download_url',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults',
            data=request_payload.get_download_url_payload,
            data_handler=lambda response: response['downloadUrl'],
        )

        download_export = rail.HTTPDownloadFileOperator(
            task_id='download_export',
            url="{{result('get_download_url')}}",
        )

        load_export = rail.LoadCSVFileOperator(
            task_id='load_export',
            document="{{result('download_export')}}"
        )

        create_timeexport_collection = rail.CreateCollectionOperator(
            task_id='create_timeexport_collection',
            name='datatoexport',
            source="{{ result('load_export') }}"
        )

        data_adaptor_output = rail.DumpToOutputOperator(
            task_id='data_adaptor_output',
            source='{{result("load_export")}}',
            raw=True
        )

        has_any_data = rail.IfOperator(
            task_id='has_any_data',
            test="{{ result('create_timeexport_collection', 'length') > 0 }}",
            yes_task="send_result_to_downstream"
        )

        send_result_to_downstream = rail.IfOperator(
            task_id="send_result_to_downstream",
            test=lambda: Variable.get(
                f"{config.downstream_variable}").lower() == 'true',
            yes_task="authentication",
        )

        authentication = rail.SimpleHttpOperator(
            task_id='authentication',
            method='POST',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json; charset=utf-8'
            },
            extra_options={
                'verify': False
            }
        )

        time_export_child = rail.TriggerDagRunForEachItemOperator(
            task_id='time_export_child',
            items=['one_run'],
            trigger_dag_id=f'elevatebio_management_time_export_child_{config.instance}',
            conf=request_payload.get_child_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        catch_dataexport_error = rail.EmptyOperator(
            task_id='catch_dataexport_error',
            trigger_rule='one_failed'
        )

        if_time_export_uri_present = rail.IfOperator(
            task_id='if_time_export_uri_present',
            test='{{ result("get_export_batch_results").timeDataExportUri | is_truthy }}',
            yes_task='get_time_export_status',
            no_task='fail_timeoff_export'
        )

        get_time_export_status = rail.RepliconServiceOperator(
            task_id='get_time_export_status',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataExportDetails',
            data=request_payload.get_time_export_details_payload,
            data_handler=response_filter.get_time_export_status
        )

        if_time_export_status_is_complete = rail.IfOperator(
            task_id='if_time_export_status_is_complete',
            test='{{ result("get_time_export_status") == "Complete" }}',
            yes_task='revert_to_draft',
            no_task='cancel_timeoff_export'
        )

        revert_to_draft = rail.RepliconServiceOperator(
            task_id='revert_to_draft',
            endpoint='/services/TimeDataExportService1.svc/MarkTimeDataExportAsDraft',
            data=request_payload.get_revert_draft_payload
        )

        cancel_timeoff_export = rail.RepliconServiceOperator(
            task_id='cancel_timeoff_export',
            endpoint="/services/TimeDataExportService1.svc/CancelTimeDataExport",
            data=request_payload.get_cancel_timeoff_export_payload
        )

        fail_timeoff_export = rail.FailOperator(
            task_id='fail_timeoff_export',
            message='{{ get_error_message() }}',
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> batch_end
        can_run_batch_task >> rail.Label("No") >> get_all_time_download_scripts
        get_all_time_download_scripts >> create_export >> execute_time_data >> wait_for_time_data \
            >> get_export_batch_results >> update_export_name
        update_export_name >> mark_as_completed >> create_download_batch >> execute_download_batch
        wait_for_download_batch >> get_download_url >> download_export >> load_export \
            >> create_timeexport_collection >> data_adaptor_output >> has_any_data
        has_any_data >> rail.Label(
            "Yes") >> send_result_to_downstream >> rail.Label("Yes") >> authentication >> time_export_child \
                >> rail.Label("On Error") >> catch_dataexport_error >> if_time_export_uri_present
        if_time_export_uri_present >> rail.Label("Yes") >> get_time_export_status >> if_time_export_status_is_complete
        if_time_export_status_is_complete >> rail.Label("Yes") >> revert_to_draft >> cancel_timeoff_export
        if_time_export_status_is_complete >> rail.Label("No") >> cancel_timeoff_export
        if_time_export_uri_present >> rail.Label("No") >> fail_timeoff_export
        cancel_timeoff_export >> fail_timeoff_export >> batch_end

        return dag


rail.for_each_instance(create_dag)
