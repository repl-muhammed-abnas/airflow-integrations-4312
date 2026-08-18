from datetime import timedelta
import pendulum
import rail
from dxctechnology.cwf_time_export_v5.utils import python_callable_method
from dxctechnology.cwf_time_export_v5.utils import request_payload


null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_cwf_time_export_c1_compass_master_{config.instance}_v5',
        description=f'DXCTechnology_CWF Time export - Master V5 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        max_active_tasks=config.dag_max_active_tasks,
        start_date=pendulum.datetime(2022, 4, 1, tz=config.utc_timezone),
        schedule_interval=config.compass_master_schedule_interval,
    ) as dag:

        get_all_time_download_scripts = rail.RepliconServiceOperator(
            task_id='get_all_time_download_scripts',
            endpoint="/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts",
        )

        get_all_employee_groups = rail.RepliconServiceOperator(
            task_id='get_all_employee_groups',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
        )

        log_message_fileformat_uri = rail.PythonOperator(
            task_id='log_message_fileformat_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_time_download_scripts'), 'displayText', 'CWFTime - Master', 'uri')
        )

        log_message_contractor_uri = rail.PythonOperator(
            task_id='log_message_contractor_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_employee_groups'), 'displayText', 'Contractor', 'uri')
        )

        log_message_sowcontractor_uri = rail.PythonOperator(
            task_id='log_message_sowcontractor_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_employee_groups'), 'displayText', 'SOW Contractor', 'uri')
        )

        log_message_agencycontractor_uri = rail.PythonOperator(
            task_id='log_message_agencycontractor_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_employee_groups'), 'displayText', 'Agency Contractor', 'uri')
        )

        get_cwf_data = rail.PythonOperator(
            task_id='get_cwf_data',
            python_callable=python_callable_method.get_cwf_data
        )

        get_data_For_all_past_time_exports = rail.RepliconServiceOperator(
            task_id='get_data_For_all_past_time_exports',
            endpoint='/services/TimeDataExportListService1.svc/GetData',
            data=request_payload.get_all_past_time_export_data_payload,
        )

        completed_exports_list = rail.PythonOperator(
            task_id='completed_exports_list',
            python_callable=python_callable_method.completed_exports_list
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
            data={
                "columnUris": [],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export-status"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": null,
                                "uris": [
                                    "urn:replicon:time-data-item-time-data-export-status:none"
                                ],
                                "bool": null,
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "dateTimeUtc": null,
                                "dateTimeUtcRange": null
                            },
                            "filterDefinitionUri": null
                        },
                        "value": null,
                        "filterDefinitionUri": null
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "leftExpression": {
                                "leftExpression": null,
                                "operatorUri": null,
                                "rightExpression": null,
                                "value": null,
                                "filterDefinitionUri": "urn:replicon:time-data-export-filter:employee-type-group"
                            },
                            "operatorUri": "urn:replicon:filter-operator:in",
                            "rightExpression": {
                                "leftExpression": null,
                                "operatorUri": null,
                                "rightExpression": null,
                                "value": {
                                    "uri": null,
                                    "uris": [
                                        "{{ result('log_message_contractor_uri') }}",
                                        "{{ result('log_message_agencycontractor_uri') }}",
                                    ],
                                    "bool": null,
                                    "date": null,
                                    "money": null,
                                    "number": null,
                                    "text": null,
                                    "time": null,
                                    "calendarDayDurationValue": null,
                                    "workdayDurationValue": null,
                                    "dateRange": null,
                                    "dateTimeUtc": null,
                                    "dateTimeUtcRange": null
                                },
                                "filterDefinitionUri": null
                            },
                            "value": null,
                            "filterDefinitionUri": null
                        },
                        "operatorUri": "urn:replicon:filter-operator:and",
                        "rightExpression": {
                            "leftExpression": {
                                "leftExpression": null,
                                "operatorUri": null,
                                "rightExpression": null,
                                "value": null,
                                "filterDefinitionUri": "urn:replicon:time-data-export-filter:approval-status"
                            },
                            "operatorUri": "urn:replicon:filter-operator:in",
                            "rightExpression": {
                                "leftExpression": null,
                                "operatorUri": null,
                                "rightExpression": null,
                                "value": {
                                    "uri": null,
                                    "uris": [
                                        "urn:replicon:approval-status:approved"
                                    ],
                                    "bool": null,
                                    "date": null,
                                    "money": null,
                                    "number": null,
                                    "text": null,
                                    "time": null,
                                    "calendarDayDurationValue": null,
                                    "workdayDurationValue": null,
                                    "dateRange": null,
                                    "dateTimeUtc": null,
                                    "dateTimeUtcRange": null
                                },
                                "filterDefinitionUri": null
                            },
                            "value": null,
                            "filterDefinitionUri": null
                        },
                        "value": null,
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                },
                "fileFormatScriptUri": "{{result('log_message_fileformat_uri')}}",
            }
        )

        execute_timedata_batch = rail.batch_execution(
            group_id='execute_timedata_batch',
            creation_task_id=create_timedata_exportbatch.task_id,
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
            no_task="update_timedataexport_name",
        )

        fail_batch_error = rail.FailOperator(
            task_id='fail_batch_error',
            message=config.error_template,
        )

        def get_current_export_name():
            previous_export_name = int(rail.result("completed_exports_list")[
                                       "Timeexport"].split('-')[-1])+1
            # pylint: disable=consider-using-f-string
            return "REG-CWF-"+"{:09d}".format(previous_export_name)

        current_export_name = rail.PythonOperator(
            task_id="current_export_name",
            python_callable=get_current_export_name
        )

        update_timedataexport_name = rail.RepliconServiceOperator(
            task_id='update_timedataexport_name',
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{ result('get_timedataexport_batchresults').timeDataExportUri }}",
                    "name": null
                },
                "name": '{{result("current_export_name")}}',
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

        cancel_timedataexport = rail.RepliconServiceOperator(
            task_id='cancel_timedataexport',
            endpoint="/services/TimeDataExportService1.svc/CancelTimeDataExport",
            data={
                "target": {
                    "uri": "{{ result('get_timedataexport_batchresults').timeDataExportUri}}",
                    "name": null
                }
            }
        )

        update_cancelled_timedataexport_name = rail.RepliconServiceOperator(
            task_id='update_cancelled_timedataexport_name',
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{ result('get_timedataexport_batchresults').timeDataExportUri }}",
                    "name": null
                },
                "name": 'CWF_cancelled_{{ dag_run_ecid() }}',
            }
        )

        fail_time_export = rail.FailOperator(
            task_id='fail_time_export',
            message=config.error_template,
        )

        process_c1_time_export = rail.TriggerDagRunOperator(
            task_id='process_c1_time_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_cwf_time_export_c1_child_{config.instance}_v5',
            execution_timeout=timedelta(days=14),
            conf=lambda: request_payload.get_c1_time_export_conf(rail.result(
                "current_export_name"), rail.result("get_data_For_all_past_time_exports"))
        )

        process_compass_time_export = rail.TriggerDagRunOperator(
            task_id='process_compass_time_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_cwf_time_export_compass_child_{config.instance}_v5',
            execution_timeout=timedelta(days=14),
            # pylint: disable=line-too-long
            conf=lambda: request_payload.get_compass_time_export_conf(rail.result(
                "current_export_name"), rail.result("get_data_For_all_past_time_exports"), config)
        )

        process_gsap_time_export = rail.TriggerDagRunOperator(
            task_id='process_gsap_time_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_cwf_time_export_gsap_child_{config.instance}_v5',
            execution_timeout=timedelta(days=14),
            # pylint: disable=line-too-long
            conf=lambda: request_payload.get_gsap_time_export_conf(rail.result(
                "current_export_name"), rail.result("get_data_For_all_past_time_exports"))
        )

        process_psa_time_export = rail.TriggerDagRunOperator(
            task_id='process_psa_time_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_cwf_time_export_psa_child_{config.instance}_v5',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.get_psa_time_export_conf(rail.result(
                "current_export_name"), rail.result("get_data_For_all_past_time_exports"))
        )

        process_psa_f142d_time_export = rail.TriggerDagRunOperator(
            task_id='process_psa_f142d_time_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_cwf_time_export_psa_f142d_child_{config.instance}_v5',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.get_psa_time_export_conf(rail.result(
                "current_export_name"), rail.result("get_data_For_all_past_time_exports"))
        )

        get_all_time_download_scripts >> get_all_employee_groups >> log_message_fileformat_uri >> log_message_contractor_uri >> \
            log_message_sowcontractor_uri >> log_message_agencycontractor_uri >> \
            get_cwf_data >> get_data_For_all_past_time_exports >> completed_exports_list >> current_export_name >> \
            get_all_object_extension_field_bindings >> create_timedata_exportbatch >> execute_timedata_batch >> \
            get_timedataexport_batchresults >> has_batch_error
        has_batch_error >> rail.Label(
            'Yes') >> fail_batch_error
        has_batch_error >> rail.Label(
            'No') >> update_timedataexport_name >> marktimedataexport_as_complete
        marktimedataexport_as_complete >> rail.Label(
            'on error') >> catch_dataexport_error >> cancel_timedataexport >> update_cancelled_timedataexport_name >> fail_time_export
        marktimedataexport_as_complete >> rail.Label('on success') >> process_c1_time_export >> \
            process_compass_time_export >> process_gsap_time_export >> process_psa_time_export >> process_psa_f142d_time_export

    return dag


rail.for_each_instance(create_dag)
