
from datetime import timedelta
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_india_earned_leave_export_monthly_download_and_upload_to_s3_child_{config.instance}',
        description=f'dxctechnology_india_earned_leave_export_monthly_download_and_upload_to_s3_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        create_payroll_download_batch_5 = rail.RepliconServiceOperator(
            task_id='create_payroll_download_batch_5',
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data={
                "columnUris": [],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:pay-run-filter:pay-run"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": ["{{ dag_run.conf.twburi }}"],
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
                "fileFormatScriptUri": "{{ dag_run.conf.fileformaturi }}"
            }
        )

        batch_management_6 = rail.batch_execution(
            group_id='execute_batch_management_6',
            creation_task_id='create_payroll_download_batch_5',
        )

        get_payroll_download_batch_results_7 = rail.RepliconServiceOperator(
            task_id='get_payroll_download_batch_results_7',
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={
                "payrollDownloadBatchUri": "{{ result('create_payroll_download_batch_5') }}"
            }
        )

        mark_pay_run_as_complete_14 = rail.RepliconServiceOperator(
            task_id='mark_pay_run_as_complete_14',
            endpoint="/services/PayRunService1.svc/CreateMarkPayRunAsCompleteBatch",
            data={
                "target": {
                    "uri": "{{ dag_run.conf.twburi }}",
                    "name": null
                }
            }
        )

        batch_management_15 = rail.batch_execution(
            group_id='execute_batch_management_15',
            creation_task_id='mark_pay_run_as_complete_14',
        )

        catch_21 = rail.EmptyOperator(
            task_id='catch_21',
            trigger_rule='one_failed',
        )

        cancel_pay_run_23 = rail.RepliconServiceOperator(
            task_id='cancel_pay_run_23',
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data={
                "target": {
                    "uri": "{{ dag_run.conf.twburi }}",
                    "name": null
                }
            }
        )

        stop_25 = rail.FailOperator(
            task_id='stop_25',
            message='''pay run batch failed'''
        )

        download_payrun_file_27 = rail.HTTPDownloadFileOperator(
            task_id='download_payrun_file_27',
            url='''{{ result('get_payroll_download_batch_results_7').downloadUrl }}''',
        )

        upload_file_uploadfiletos3_28 = rail.S3UploadFileOperator(
            task_id='upload_file_uploadfiletos3_28',
            source="{{ result('download_payrun_file_27') }}",
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name="Dxctechnology/Payrollexport/INDIAES/Source_{{ dag_run.conf.filename }}.csv",
        )

        trigger_dag_run_upload_to_sftp_async_29 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_upload_to_sftp_async_29',
            retries=0,
            items=[1],
            trigger_dag_id=f'dxctechnology_india_earned_leave_export_monthly_process_and_upload_to_sftp_child_{config.instance}',
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
                "twburi": "{{ dag_run.conf.twburi }}",
                "log": "{{ dag_run.conf.log }}",
                "s3path": "Dxctechnology/Payrollexport/INDIAES/Source_{{ dag_run.conf.filename }}.csv"
            }
        )

        wait_for_completion_trigger_dag_run_upload_to_sftp_async_29 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_upload_to_sftp_async_29',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_upload_to_sftp_async_29") }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        create_payroll_download_batch_5 >> batch_management_6[0] >> batch_management_6[
            1] >> get_payroll_download_batch_results_7 >> mark_pay_run_as_complete_14 >> batch_management_15[0] >> batch_management_15[1]
        batch_management_15[1] >> catch_21 >> cancel_pay_run_23 >> stop_25 >> finish
        batch_management_15[1] >> download_payrun_file_27 >> upload_file_uploadfiletos3_28 >> trigger_dag_run_upload_to_sftp_async_29 >> wait_for_completion_trigger_dag_run_upload_to_sftp_async_29 >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
