
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_india_earned_leave_export_monthly_download_and_validate_child_{config.instance}',
        description=f'dxctechnology_india_earned_leave_export_monthly_download_and_validate_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_payroll_download_batch_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_payroll_download_batch_4',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_payroll_download_batch_4 = rail.RepliconServiceOperator(
            task_id='create_payroll_download_batch_4',
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
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
                                        "uris": ["{{ dag_run.conf.divisionuri}}"],
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
                                        "uris": ["urn:replicon:payable-time-pay-run-status:none"],
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
                },
                "fileFormatScriptUri": "{{ dag_run.conf.fileformaturi }}"
            }
        )

        batch_management_5 = rail.batch_execution(
            group_id='execute_batch_management_5',
            creation_task_id='create_payroll_download_batch_4',
        )

        get_payroll_download_batch_results_6 = rail.RepliconServiceOperator(
            task_id='get_payroll_download_batch_results_6',
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={
                "payrollDownloadBatchUri": "{{ result('create_payroll_download_batch_4') }}"
            }
        )

        download_payrun_file_11 = rail.HTTPDownloadFileOperator(
            task_id='download_payrun_file_11',
            url='''{{ result('get_payroll_download_batch_results_6').downloadUrl }}''',
        )

        load_csv_create_list_from_csv_12 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_12",
            document="{{ result('download_payrun_file_11') }}",
        )

        create_collection_create_list_from_csv_12 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_12',
            source="{{ result('load_csv_create_list_from_csv_12') }}",
            name="payrolldata",
            columns={
                'RECTY': 'RECTY',
                'CLID': 'CLID',
                'INTCA': 'INTCA',
                'ORDNO': 'ORDNO',
                'IOPER': 'IOPER',
                'INFTY': 'INFTY',
                'BEGDA': 'BEGDA',
                'ENDDA': 'ENDDA',
                'OBJPS': 'OBJPS',
                'SPRPS': 'SPRPS',
                'SEQNR': 'SEQNR',
                'EXTRA': 'EXTRA',
                'Pay Code Code': 'paycodecode',
                'STDAZ': 'STDAZ',
                'BEGUZ': 'BEGUZ',
                'ENDUZ': 'ENDUZ',
                'BETRG': 'BETRG',
                'WAERS': 'WAERS',
                'Pay Code Hours': 'PayCodeHours',
                'ZEINH': 'ZEINH',
                'VTKEN': 'VTKEN',
                'BWGRL': 'BWGRL',
                'AUFKZ': 'AUFKZ',
                'ENDOF': 'ENDOF',
                'UFLD1': 'UFLD1',
                'UFLD2': 'UFLD2',
                'UFLD3': 'UFLD3',
                'KEYPR': 'KEYPR',
                'TRFGR': 'TRFGR',
                'TRFST': 'TRFST',
                'PRAKN': 'PRAKN',
                'PRAKZ': 'PRAKZ',
                'OTYPE': 'OTYPE',
                'PLANS': 'PLANS',
                'VERSL': 'VERSL',
                'EXBEL': 'EXBEL',
                'WTART': 'WTART',
                'TDLANGU': 'TDLANGU',
                'TDSUBLA': 'TDSUBLA',
                'TDTYPE': 'TDTYPE'
            }
        )

        if_create_list_from_csv_12_row_count_greater_than_0_13 = rail.IfOperator(
            task_id='if_create_list_from_csv_12_row_count_greater_than_0_13',
            test='''{{ result('create_collection_create_list_from_csv_12','length') > 0 }}''',
            yes_task="trigger_dag_run_create_payrun_async_14",
            no_task="finish",
        )

        trigger_dag_run_create_payrun_async_14 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_create_payrun_async_14',
            retries=0,
            items=[1],
            trigger_dag_id=f'dxctechnology_india_earned_leave_export_monthly_create_payrun_child_{config.instance}',
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
                "filename": "{{ dag_run.conf.filename }}"
            }
        )

        wait_for_completion_trigger_dag_run_create_payrun_async_14 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_create_payrun_async_14',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_create_payrun_async_14") }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> create_payroll_download_batch_4
        create_payroll_download_batch_4 >> batch_management_5[0] >> batch_management_5[
            1] >> get_payroll_download_batch_results_6 >> download_payrun_file_11 >> load_csv_create_list_from_csv_12 >> create_collection_create_list_from_csv_12 >> if_create_list_from_csv_12_row_count_greater_than_0_13
        if_create_list_from_csv_12_row_count_greater_than_0_13 >> rail.Label(
            'Yes') >> trigger_dag_run_create_payrun_async_14 >> wait_for_completion_trigger_dag_run_create_payrun_async_14 >> finish >> log_to_sumo
        if_create_list_from_csv_12_row_count_greater_than_0_13 >> rail.Label(
            'No') >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
