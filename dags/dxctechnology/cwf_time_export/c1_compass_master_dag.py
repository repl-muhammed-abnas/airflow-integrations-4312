from datetime import datetime, timedelta
import rail

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/cwf_time_export/config.py

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_cwf_time_export_c1_compass_master_{config.instance}',
        description=f'DXCTechnology_CWF Time export - Master V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        max_active_tasks=config.dag_max_active_tasks,
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

        log_message_requiredfilename = rail.PythonOperator(
            task_id='log_message_requiredfilename',
            python_callable=lambda: f'CWF_Time_Extract_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}'
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

        update_timedataexport_name = rail.RepliconServiceOperator(
            task_id='update_timedataexport_name',
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{ result('get_timedataexport_batchresults').timeDataExportUri }}",
                    "name": null
                },
                "name": "{{ result('log_message_requiredfilename') }}",
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

        fail_time_export = rail.FailOperator(
            task_id='fail_time_export',
            message=config.error_template,
        )

        process_c1_time_export = rail.TriggerDagRunOperator(
            task_id='process_c1_time_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_cwf_time_export_c1_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf={
                'downloadurl': null,
                'fileformaturi': "{{ result('log_message_fileformat_uri') }}",
                'timeexporturi': "{{ result('get_timedataexport_batchresults')['timeDataExportUri'] }}",
                'twbname': "{{ result('log_message_requiredfilename') }}"
            }
        )

        wait_for_process_c1_time_export = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_c1_time_export',
            dag_runs='{{ result("process_c1_time_export") }}',
            execution_timeout=timedelta(days=14),
        )

        process_compass_time_export = rail.TriggerDagRunOperator(
            task_id='process_compass_time_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_cwf_time_export_compass_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf={
                'downloadurl': null,
                'fileformaturi': "{{ result('log_message_fileformat_uri') }}",
                'timeexporturi': "{{ result('get_timedataexport_batchresults')['timeDataExportUri'] }}",
            }
        )

        wait_for_process_compass_time_export = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_compass_time_export',
            dag_runs='{{ result("process_compass_time_export") }}',
            execution_timeout=timedelta(days=14),
        )

        get_all_time_download_scripts >> get_all_employee_groups >> log_message_fileformat_uri >> log_message_contractor_uri >> \
            log_message_sowcontractor_uri >> log_message_agencycontractor_uri >> log_message_requiredfilename >> \
            create_timedata_exportbatch >> execute_timedata_batch >> get_timedataexport_batchresults >> has_batch_error
        has_batch_error >> rail.Label(
            'Yes') >> fail_batch_error
        has_batch_error >> rail.Label(
            'No') >> update_timedataexport_name >> marktimedataexport_as_complete
        marktimedataexport_as_complete >> rail.Label(
            'on error') >> catch_dataexport_error >> cancel_timedataexport >> fail_time_export
        marktimedataexport_as_complete >> rail.Label('on success') >> process_c1_time_export >> wait_for_process_c1_time_export >> \
            process_compass_time_export >> wait_for_process_compass_time_export

    return dag


rail.for_each_instance(create_dag)
