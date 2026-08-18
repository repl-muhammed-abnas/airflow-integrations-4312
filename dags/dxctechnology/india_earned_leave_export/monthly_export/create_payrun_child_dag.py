
from datetime import timedelta
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_india_earned_leave_export_monthly_create_payrun_child_{config.instance}',
        description=f'dxctechnology_india_earned_leave_export_monthly_create_payrun_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        create_pay_run_batch_5 = rail.RepliconServiceOperator(
            task_id='create_pay_run_batch_5',
            endpoint="/services/PayRunService1.svc/CreatePayRunBatch",
            data={
                "columnUris": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:pay-run-filter:entry-date-range"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": null,
                                "uris": [],
                                "bool": null,
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": {
                                    "startDate": {
                                        "year": "{{ dag_run.conf.startdate.year }}",
                                        "month": "{{ dag_run.conf.startdate.month }}",
                                        "day": "{{ dag_run.conf.startdate.day }}",
                                    },
                                    "endDate": {
                                        "year": "{{ dag_run.conf.enddate.year }}",
                                        "month": "{{ dag_run.conf.enddate.month }}",
                                        "day": "{{ dag_run.conf.enddate.day }}",
                                    },
                                    "relativeDateRangeUri": null,
                                    "relativeDateRangeAsOfDate": null
                                },
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
                                "filterDefinitionUri": "urn:replicon:pay-run-filter:payable-time-approval-status"
                            },
                            "operatorUri": "urn:replicon:filter-operator:in",
                            "rightExpression": {
                                "leftExpression": null,
                                "operatorUri": null,
                                "rightExpression": null,
                                "value": {
                                    "uri": null,
                                    "uris": ["urn:replicon:payable-time-approval-status:approved"],
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
                                    "filterDefinitionUri": "urn:replicon:pay-run-filter:division"
                                },
                                "operatorUri": "urn:replicon:filter-operator:in",
                                "rightExpression": {
                                    "leftExpression": null,
                                    "operatorUri": null,
                                    "rightExpression": null,
                                    "value": {
                                        "uri": null,
                                        "uris": ["{{ dag_run.conf.divisionuri }}"],
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
                                    "filterDefinitionUri": "urn:replicon:pay-run-filter:pay-run-status"
                                },
                                "operatorUri": "urn:replicon:filter-operator:in",
                                "rightExpression": {
                                    "leftExpression": null,
                                    "operatorUri": null,
                                    "rightExpression": null,
                                    "value": {
                                        "uri": null,
                                        "uris": [
                                            "urn:replicon:payable-time-pay-run-status:none"
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
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        batch_management_6 = rail.batch_execution(
            group_id='execute_batch_management_6',
            creation_task_id='create_pay_run_batch_5',
        )

        get_create_pay_run_batch_results_10 = rail.RepliconServiceOperator(
            task_id='get_create_pay_run_batch_results_10',
            endpoint="/services/PayRunService1.svc/GetCreatePayRunBatchResults",
            data={
                "payRunBatchUri": "{{ result('create_pay_run_batch_5') }}"
            }
        )

        if_get_create_pay_run_batch_results_10_error_present_11 = rail.IfOperator(
            task_id='if_get_create_pay_run_batch_results_10_error_present_11',
            test='''{{ result('get_create_pay_run_batch_results_10').error | is_truthy }}''',
            yes_task="stop_12",
            no_task="update_pay_run_name_13",
        )

        stop_12 = rail.FailOperator(
            task_id='stop_12',
            message='''{{ result('get_create_pay_run_batch_results_10').error }}'''
        )

        update_pay_run_name_13 = rail.RepliconServiceOperator(
            task_id='update_pay_run_name_13',
            endpoint="/services/PayRunService1.svc/UpdatePayRunName",
            data={
                "target": {
                    "uri": "{{ result('get_create_pay_run_batch_results_10').payRunUri }}",
                    "name": null
                },
                "name": "{{ dag_run.conf.filename }}"
            }
        )

        trigger_dag_run_download_upload_to_s3_async_14 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_download_upload_to_s3_async_14',
            retries=0,
            items=[1],
            trigger_dag_id=f'dxctechnology_india_earned_leave_export_monthly_download_and_upload_to_s3_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf={
                "fileformaturi": "{{ dag_run.conf.fileformaturi }}",
                "startdate": {
                    "year": "{{ dag_run.conf.startdate.year }}",
                    "month": "{{ dag_run.conf.startdate.month }}",
                    "day": "{{ dag_run.conf.startdate.day }}",
                },
                "enddate": {
                    "year": "{{ dag_run.conf.enddate.year }}",
                    "month": "{{ dag_run.conf.enddate.month }}",
                    "day": "{{ dag_run.conf.enddate.day }}",
                },
                "division": "{{ dag_run.conf.division }}",
                "divisionuri": "{{ dag_run.conf.divisionuri }}",
                "timenow": "{{ dag_run.conf.timenow }}",
                "rundateinYYYYMMDDformat": "{{ dag_run.conf.rundateinYYYYMMDDformat }}",
                "runtimeinHHMMSSformat": "{{ dag_run.conf.runtimeinHHMMSSformat }}",
                "filename": "{{ dag_run.conf.filename }}",
                "twburi": "{{ result('get_create_pay_run_batch_results_10').payRunUri }}",
                "log": "{{ dag_run.conf.timenow }}- Process started\nCompany Code : {{ dag_run.conf.division }}"
            }
        )

        wait_for_completion_trigger_dag_run_download_upload_to_s3_async_14 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_download_upload_to_s3_async_14',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_download_upload_to_s3_async_14") }}'
        )

        catch_17 = rail.EmptyOperator(
            task_id='catch_17',
            trigger_rule='one_failed',
        )

        cancel_pay_run_19 = rail.RepliconServiceOperator(
            task_id='cancel_pay_run_19',
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data={
                "target": {
                    "uri": "{{ result('get_create_pay_run_batch_results_10').payRunUri }}",
                    "name": null
                }
            }
        )

        stop_payrun = rail.FailOperator(
            task_id='stop_payrun',
            message="parun batch failed"
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        create_pay_run_batch_5 >> batch_management_6[0] >> batch_management_6[
            1] >> get_create_pay_run_batch_results_10 >> if_get_create_pay_run_batch_results_10_error_present_11
        if_get_create_pay_run_batch_results_10_error_present_11 >> rail.Label(
            'Yes') >> stop_12 >> finish
        if_get_create_pay_run_batch_results_10_error_present_11 >> rail.Label(
            'No') >> update_pay_run_name_13 >> trigger_dag_run_download_upload_to_s3_async_14 >> wait_for_completion_trigger_dag_run_download_upload_to_s3_async_14
        wait_for_completion_trigger_dag_run_download_upload_to_s3_async_14 >> catch_17
        wait_for_completion_trigger_dag_run_download_upload_to_s3_async_14 >> finish
        catch_17 >> cancel_pay_run_19 >> stop_payrun >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
