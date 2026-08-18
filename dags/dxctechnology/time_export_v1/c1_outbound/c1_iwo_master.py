
from datetime import timedelta
from pendulum import datetime
import pendulum
from airflow.models import Variable
from dxctechnology.time_export_v1.c1_outbound.tasks.update_export_status import cancel_time_export
from dxctechnology.time_export_v1.c1_outbound.utils import request_payload, response_filters, custom_methods
import rail

null = None
def create_main_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.c1_iwo_time_export_master_dagid,
        description=f"DXC - C1 IWO Time Export Master - {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2024, 2, 1, tz=config.utc_timezone),
        schedule_interval=config.iwo_schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_dag_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        if_to_skip_scheduled_run = rail.IfOperator(
            task_id='if_to_skip_scheduled_run',
            test=lambda: pendulum.now(config.utc_timezone).day_of_week == config.skip_run_weekday and pendulum.now(config.utc_timezone).hour == config.iwo_skip_run_hour,
            no_task="can_run_batch_task"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_process_starttime'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_process_starttime',
            end_task='batch_end',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_process_starttime = rail.PythonOperator(
            task_id='get_process_starttime',
            python_callable=lambda: pendulum.now(config.utc_timezone).isoformat()
        )

        get_all_gsap_compass_divisions = rail.RepliconServicePageOperator(
            task_id="get_all_gsap_compass_divisions",
            endpoint="/services/DivisionListService1.svc/GetData",
            data=request_payload.get_gsap_compass_divisions_payload,
            page_handler=custom_methods.page_handler,
            all_result_data_handler=response_filters.filter_divisions_data
        )

        get_all_filter_definitions = rail.RepliconServiceOperator(
            task_id="get_all_filter_definitions",
            endpoint="/services/TimeDataExportService1.svc/GetAllFilterDefinitions"
        )

        get_time_download_script = rail.RepliconServiceOperator(
            task_id='get_time_download_script',
            endpoint="/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts"
        )

        get_employeetype_groups = rail.RepliconServiceOperator(
            task_id="get_employeetype_groups",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
            data_handler=response_filters.filter_employee_groups
        )

        get_iwo_indicator_oef_details= rail.RepliconServiceOperator(
            task_id='get_iwo_indicator_oef_details',
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:project"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'name', 'IWO Indicator', 'uri')
        )

        get_oef_drop_down_values = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_iwo_indicator_oef_details') }}"
            }
        )

        get_filter_data_for_c1_iwo_time_export = rail.PythonOperator(
            task_id='get_filter_data_for_c1_iwo_time_export',
            python_callable=custom_methods.filter_data_for_c1_iwo_time_export,
            op_args=[config]
        )

        get_data_for_all_past_time_exports_for_C1 = rail.RepliconServiceOperator(
            task_id='get_data_for_all_past_time_exports_for_C1',
            endpoint='/services/TimeDataExportListService1.svc/GetData',
            data=lambda: request_payload.get_all_past_time_export_data_payload("IWO-C1", rail.result("get_filter_data_for_c1_iwo_time_export")),
            data_handler=response_filters.completed_exports_list
        )

        get_all_object_extension_field_bindings = rail.RepliconServiceOperator(
            task_id='get_all_object_extension_field_bindings',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings',
            data={
                'bindingContextUri': 'urn:replicon:object-type:time-data-export'
            }
        )

        create_timedata_exportbatch = rail.RepliconServiceOperator(
            task_id='create_timedata_exportbatch',
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataExportBatch",
            data=request_payload.create_c1_iwo_time_export_payload
        )

        execute_timedata_batch, wait_for_batch = rail.batch_execution(
            group_id='process_timedata_batch',
            creation_task_id=create_timedata_exportbatch.task_id,
            retries=0
        )

        get_timedataexport_batchresults = rail.RepliconServiceOperator(
            task_id='get_timedataexport_batchresults',
            endpoint="/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults",
            data={
                "timeDataExportBatchUri": "{{result('create_timedata_exportbatch')}}",
            }
        )

        has_batch_error = rail.IfOperator(
            task_id='has_batch_error',
            test="{{ result('get_timedataexport_batchresults').error | is_truthy }}",
            yes_task="fail_batch_error",
            no_task="log_current_export_name",
        )

        fail_batch_error = rail.FailOperator(
            task_id='fail_batch_error',
            message=config.error_template,
        )

        log_current_export_name = rail.PythonOperator(
            task_id='log_current_export_name',
            python_callable=custom_methods.get_current_export_name,
            op_args=["IWO-C1-"]
        )

        update_timedataexport_name = rail.RepliconServiceOperator(
            task_id='update_timedataexport_name',
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{ result('get_timedataexport_batchresults').timeDataExportUri }}",
                    "name": null
                },
                "name": "{{ result('log_current_export_name') }}",
            }
        )

        marktimedataexport_as_complete = rail.RepliconServiceOperator(
            task_id='marktimedataexport_as_complete',
            endpoint="/services/TimeDataExportService1.svc/MarkTimeDataExportAsComplete",
            data={
                "target": {
                    "uri": "{{ result('get_timedataexport_batchresults').timeDataExportUri}}",
                    "name": null
                }
            }
        )

        catch_dataexport_error = rail.EmptyOperator(
            task_id='catch_dataexport_error',
            trigger_rule='one_failed'
        )

        get_export_uri_failed = rail.RepliconServiceOperator(
            task_id='get_export_uri_failed',
            endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
            data={
                "timeDataExportBatchUri": "{{ result('create_timedata_exportbatch') }}"
            },
            data_handler=request_payload.retrieve_export_uri
        )

        mark_export_status_cancel_start, mark_export_status_cancel_end = cancel_time_export()

        update_cancelled_timedataexport_name = rail.RepliconServiceOperator(
            task_id='update_cancelled_timedataexport_name',
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{ result('get_export_uri_failed') }}",
                    "name": null
                },
                "name": 'C1_cancel_{{ dag_run_ecid() }}_IWO',
            }
        )

        fail_time_export = rail.FailOperator(
            task_id='fail_time_export',
            message=config.error_template,
        )

        process_gsap_iwo_time_export = rail.TriggerDagRunOperator(
            task_id='process_gsap_iwo_time_export',
            retries=0,
            trigger_dag_id=config.gsap_process_time_export_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                'downloadurl': null,
                'fileformaturi': rail.result('get_filter_data_for_c1_iwo_time_export')['fileformaturi'],
                'hoursfileformaturi': rail.result('get_filter_data_for_c1_iwo_time_export')['hoursfileformaturi'],
                'timeexporturi': rail.result('get_timedataexport_batchresults')['timeDataExportUri'],
                'twbname': rail.result('log_current_export_name'),
                'postdata': "yes",
                'lasttwbname': rail.result('get_data_for_all_past_time_exports_for_C1')[0]['timeexport'],
                'lasttwburi': rail.result('get_data_for_all_past_time_exports_for_C1')[0]['uri'],
                'payload_identifier_replicon_uniqueid': rail.result('log_current_export_name') + "|GS",
                'oefname': "GSAP_Payload_Processed",
                'lasttwbuniqueindentifier': rail.result('get_data_for_all_past_time_exports_for_C1')[0]['timeexport'] + "|GS",
                'twblist': rail.result('get_data_for_all_past_time_exports_for_C1'),
                'oefuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field_bindings"),
                    "displayText", "GSAP_Payload_Processed", "uri", ""),
                'process_start_time': rail.result('get_process_starttime')
            }
        )

        process_c1_iwo_time_export = rail.TriggerDagRunOperator(
            task_id='process_c1_iwo_time_export',
            retries=0,
            trigger_dag_id=config.c1_process_time_export_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                'downloadurl': null,
                'fileformaturi': rail.result('get_filter_data_for_c1_iwo_time_export')['fileformaturi'],
                'hoursfileformaturi': rail.result('get_filter_data_for_c1_iwo_time_export')['hoursfileformaturi'],
                'timeexporturi': rail.result('get_timedataexport_batchresults')['timeDataExportUri'],
                'twbname': rail.result('log_current_export_name'),
                'postdata': "yes",
                'lasttwbname': rail.result('get_data_for_all_past_time_exports_for_C1')[0]['timeexport'],
                'lasttwburi': rail.result('get_data_for_all_past_time_exports_for_C1')[0]['uri'],
                'payload_identifier_replicon_uniqueid': rail.result('log_current_export_name') + "|C1",
                'oefname': "C1_Payload_Processed",
                'lasttwbuniqueindentifier': rail.result('get_data_for_all_past_time_exports_for_C1')[0]['timeexport'] + "|C1",
                'twblist': rail.result('get_data_for_all_past_time_exports_for_C1'),
                'type': "IWO",
                'process_start_time': rail.result('get_process_starttime')
            }
        )

        process_compass_iwo_time_export = rail.TriggerDagRunOperator(
            task_id='process_compass_iwo_time_export',
            retries=0,
            trigger_dag_id=config.compass_process_time_export_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                'downloadurl': null,
                'fileformaturi': rail.result('get_filter_data_for_c1_iwo_time_export')['fileformaturi'],
                'hoursfileformaturi': rail.result('get_filter_data_for_c1_iwo_time_export')['hoursfileformaturi'],
                'timeexporturi': rail.result('get_timedataexport_batchresults')['timeDataExportUri'],
                'twbname': rail.result('log_current_export_name'),
                'postdata': "yes",
                'lasttwbname': rail.result('get_data_for_all_past_time_exports_for_C1')[0]['timeexport'],
                'lasttwburi': rail.result('get_data_for_all_past_time_exports_for_C1')[0]['uri'],
                'payload_identifier_replicon_uniqueid_PN1': rail.result('log_current_export_name') + ("|PN1"
                    if (config.company_key).lower() == "dxctechnology" else "|NT1"),
                'payload_identifier_replicon_uniqueid_PJ1': rail.result('log_current_export_name') + ("|PJ1"
                    if (config.company_key).lower() == "dxctechnology" else "|NT3"),
                'payload_identifier_replicon_uniqueid_P01': rail.result('log_current_export_name') + ("|P01"
                    if (config.company_key).lower() == "dxctechnology" else "|NT2"),
                'oefname_PN1': "Compass_PN1/NT1_Payload_Processed",
                'oefname_PJ1': "Compass_PJ1/NT3_Payload_Processed",
                'oefname_P01': "Compass_P01/NT2_Payload_Processed",
                'lasttwbuniqueindentifier_PN1': rail.result('get_data_for_all_past_time_exports_for_C1')[0]['timeexport'] + ("|PN1"
                    if (config.company_key).lower() == "dxctechnology" else "|NT1"),
                'lasttwbuniqueindentifier_PJ1': rail.result('get_data_for_all_past_time_exports_for_C1')[0]['timeexport'] + ("|PJ1"
                    if (config.company_key).lower() == "dxctechnology" else "|NT3"),
                'lasttwbuniqueindentifier_P01': rail.result('get_data_for_all_past_time_exports_for_C1')[0]['timeexport'] + ("|P01"
                    if (config.company_key).lower() == "dxctechnology" else "|NT2"),
                "PN1_sent_oef": custom_methods.get_oef_bindings_uri("COMPASS_PN1_sent"),
                "PJ1_sent_oef": custom_methods.get_oef_bindings_uri("COMPASS_PJ1_sent"),
                "P01_sent_oef": custom_methods.get_oef_bindings_uri("COMPASS_P01_sent"),
                'twblist': rail.result('get_data_for_all_past_time_exports_for_C1'),
                'type': "IWO",
                'process_start_time': rail.result('get_process_starttime')
            }
        )

        process_psa_iwo_time_export = rail.TriggerDagRunOperator(
            task_id='process_psa_iwo_time_export',
            retries=0,
            trigger_dag_id=config.psa_outbound_time_export_psa_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                'downloadurl': null,
                'fileformaturi': rail.result('get_filter_data_for_c1_iwo_time_export')['fileformaturi'],
                'hoursfileformaturi': rail.result('get_filter_data_for_c1_iwo_time_export')['hoursfileformaturi'],
                'timeexporturi': rail.result('get_timedataexport_batchresults')['timeDataExportUri'],
                'twbname': rail.result('log_current_export_name'),
                'postdata': "yes",
                'lasttwbname': rail.result('get_data_for_all_past_time_exports_for_C1')[0]['timeexport'],
                'lasttwburi': rail.result('get_data_for_all_past_time_exports_for_C1')[0]['uri'],
                'payload_identifier_replicon_uniqueid': rail.result('log_current_export_name') + "|PSA",
                'oefname': "PSA_Payload_Processed",
                'lasttwbuniqueindentifier': rail.result('get_data_for_all_past_time_exports_for_C1')[0]['timeexport'] + "|PSA",
                'twblist': rail.result('get_data_for_all_past_time_exports_for_C1'),
                'oefuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field_bindings"),
                    "displayText", "PSA_Payload_Processed", "uri", ""),
                'process_start_time': rail.result('get_process_starttime')
            }
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        if_to_skip_scheduled_run >> rail.Label("No") >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> batch_end
        can_run_batch_task >> rail.Label('No') >> get_process_starttime
        get_process_starttime >> get_all_gsap_compass_divisions >> get_all_filter_definitions \
            >> get_time_download_script >> get_employeetype_groups >> get_iwo_indicator_oef_details \
                >> get_oef_drop_down_values >> get_filter_data_for_c1_iwo_time_export \
                    >> get_data_for_all_past_time_exports_for_C1 \
                        >> get_all_object_extension_field_bindings >> create_timedata_exportbatch

        create_timedata_exportbatch >> execute_timedata_batch
        wait_for_batch >> get_timedataexport_batchresults >> has_batch_error

        has_batch_error >> rail.Label("Yes") >> fail_batch_error >> batch_end
        has_batch_error >> rail.Label("No") >> log_current_export_name >> update_timedataexport_name \
            >> marktimedataexport_as_complete >> batch_end

        batch_end >> rail.Label('on error') >> catch_dataexport_error >> get_export_uri_failed >> mark_export_status_cancel_start
        mark_export_status_cancel_end >> update_cancelled_timedataexport_name >> fail_time_export
        batch_end >> rail.Label('on success') >> process_gsap_iwo_time_export \
            >> process_c1_iwo_time_export >> process_compass_iwo_time_export >> process_psa_iwo_time_export

    return dag

rail.for_each_instance(create_main_dag)
