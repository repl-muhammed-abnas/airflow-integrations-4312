
from datetime import datetime
import itertools
from dateutil.relativedelta import relativedelta
import rail
from dxctechnology.india_earned_leave_export.daily_export.dxc_payroll_extract_mapper_india_mapper import dxc_payroll_extract_mapper_india
null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_india_earned_leave_export_daily_export_child_{config.instance}',
        description=f'DXC_India companycode_wise_PayrollData_Export_Child Daily Terminated - V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        create_object_set_4 = rail.RepliconServiceOperator(
            task_id='create_object_set_4',
            endpoint="/services/UserService1.svc/CreateObjectSet",
            data=lambda: {
                "userUris": rail.get_dag_run_conf()['useruri']
            }
        )

        declare_list_logforpayroll_7 = rail.SetVariableOperator(
            task_id='declare_list_logforpayroll_7',
            append=False,
            name='Payrolllog',
            value=[]
        )

        insert_to_list_8 = rail.SetVariableOperator(
            task_id='insert_to_list_8',
            append=True,
            name='{{ result("declare_list_logforpayroll_7").name }}',
            value={
                "log": '''{{dag_run.conf.timenow}} - Process started\nCompany Code: "{{dag_run.conf.division}}'''
            }
        )

        create_payroll_download_batch_10 = rail.RepliconServiceOperator(
            task_id='create_payroll_download_batch_10',
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data=lambda: {
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
                                    "startDate": rail.get_dag_run_conf()['startdate'],
                                    "endDate": rail.get_dag_run_conf()['enddate'],
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
                                        "uris": [
                                            "urn:replicon:payable-time-approval-status:approved"
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
                                        "filterDefinitionUri": "urn:replicon:pay-run-filter:division"
                                    },
                                    "operatorUri": "urn:replicon:filter-operator:in",
                                    "rightExpression": {
                                        "leftExpression": null,
                                        "operatorUri": null,
                                        "rightExpression": null,
                                        "value": {
                                            "uri": null,
                                            "uris": rail.get_dag_run_conf()['divisionuri'],
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
                                        "filterDefinitionUri": "urn:replicon:pay-run-filter:user"
                                    },
                                    "operatorUri": "urn:replicon:filter-operator:in",
                                    "rightExpression": {
                                        "leftExpression": null,
                                        "operatorUri": null,
                                        "rightExpression": null,
                                        "value": {
                                            "uri": null,
                                            "uris": [rail.result('create_object_set_4')],
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
                    "value": null,
                    "filterDefinitionUri": null
                },
                "fileFormatScriptUri": rail.get_dag_run_conf()['fileformaturi']
            }
        )

        execute_payroll_batch = rail.batch_execution(
            group_id='execute_payroll_batch',
            creation_task_id='create_payroll_download_batch_10'
        )

        get_payroll_download_batch_results_13 = rail.RepliconServiceOperator(
            task_id='get_payroll_download_batch_results_13',
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={
                "payrollDownloadBatchUri": "{{ result('create_payroll_download_batch_10') }}"
            }
        )

        if_d_error_present_16 = rail.IfOperator(
            task_id='if_d_error_present_16',
            test='''{{ result('get_payroll_download_batch_results_13').error | is_truthy }}''',
            yes_task="stop_17",
            no_task="read_file_18",
        )

        stop_17 = rail.FailOperator(
            task_id='stop_17',
            message='''{{ result('get_payroll_download_batch_results_13').error }}'''
        )

        read_file_18 = rail.HTTPDownloadFileOperator(
            task_id='read_file_18',
            url='''{{ result('get_payroll_download_batch_results_13').downloadUrl }}''',
        )

        load_csv_create_list_from_csv_19 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_19",
            document="{{result('read_file_18')}}",
        )

        create_collection_create_list_from_csv_19 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_19',
            source="{{ result('load_csv_create_list_from_csv_19') }}",
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

        if_create_list_from_csv_19_row_count_greater_than_0_20 = rail.IfOperator(
            task_id='if_create_list_from_csv_19_row_count_greater_than_0_20',
            test='''{{ result('create_collection_create_list_from_csv_19','length') > 0 }}''',
            yes_task="log_requiredfilename_21",
            no_task="finish",
        )

        log_requiredfilename_21 = rail.PythonOperator(
            task_id='log_requiredfilename_21',
            python_callable=lambda:  "PP3220" + "_" +
            datetime.utcnow().strftime("%Y%m%d%H%M%S") + "_" + "INREPL_REPL01_DUT8G2I" if str(
                config.company_key).lower() == 'dxctechnology' else "PQ3220" + "_" + datetime.utcnow().strftime("%Y%m%d%H%M%S") + "_" + "INREPL_REPL01_DUT8G2I"
        )

        create_pay_run_batch_24 = rail.RepliconServiceOperator(
            task_id='create_pay_run_batch_24',
            endpoint="/services/PayRunService1.svc/CreatePayRunBatch",
            data=lambda: {
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
                                    "startDate": rail.get_dag_run_conf()['startdate'],
                                    "endDate": rail.get_dag_run_conf()['enddate'],
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
                                        "uris": [
                                            "urn:replicon:payable-time-approval-status:approved"
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
                                        "filterDefinitionUri": "urn:replicon:pay-run-filter:division"
                                    },
                                    "operatorUri": "urn:replicon:filter-operator:in",
                                    "rightExpression": {
                                        "leftExpression": null,
                                        "operatorUri": null,
                                        "rightExpression": null,
                                        "value": {
                                            "uri": null,
                                            "uris": rail.get_dag_run_conf()['divisionuri'],
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
                                        "filterDefinitionUri": "urn:replicon:pay-run-filter:user"
                                    },
                                    "operatorUri": "urn:replicon:filter-operator:in",
                                    "rightExpression": {
                                        "leftExpression": null,
                                        "operatorUri": null,
                                        "rightExpression": null,
                                        "value": {
                                            "uri": null,
                                            "uris": [rail.result('create_object_set_4')],
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
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        batch_management_25 = rail.batch_execution(
            group_id='execute_batch_management_25',
            creation_task_id='create_pay_run_batch_24',
        )

        get_create_pay_run_batch_results_29 = rail.RepliconServiceOperator(
            task_id='get_create_pay_run_batch_results_29',
            endpoint="/services/PayRunService1.svc/GetCreatePayRunBatchResults",
            data={
                "payRunBatchUri": "{{ result('create_pay_run_batch_24') }}"
            }
        )

        if_get_create_pay_run_batch_results_29_error_present_30 = rail.IfOperator(
            task_id='if_get_create_pay_run_batch_results_29_error_present_30',
            test='''{{ result('get_create_pay_run_batch_results_29').error | is_truthy }}''',
            yes_task="stop_31",
            no_task="update_pay_run_name_32",
        )

        stop_31 = rail.FailOperator(
            task_id='stop_31',
            message='''{{ result('get_create_pay_run_batch_results_29').error }}'''
        )

        update_pay_run_name_32 = rail.RepliconServiceOperator(
            task_id='update_pay_run_name_32',
            endpoint="/services/PayRunService1.svc/UpdatePayRunName",
            data={
                "target": {
                    "uri": "{{ result('get_create_pay_run_batch_results_29').payRunUri }}",
                    "name": null
                },
                "name": "{{ result('log_requiredfilename_21') }}"
            }
        )

        create_payroll_download_batch_34 = rail.RepliconServiceOperator(
            task_id='create_payroll_download_batch_34',
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
                            "uris": ["{{ result('get_create_pay_run_batch_results_29').payRunUri }}"],
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

        execute_create_payroll_download_batch_34 = rail.batch_execution(
            group_id='excecute_create_payroll_download_batch_34',
            creation_task_id='create_payroll_download_batch_34'
        )

        get_payroll_download_batch_results_37 = rail.RepliconServiceOperator(
            task_id='get_payroll_download_batch_results_37',
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={
                "payrollDownloadBatchUri": "{{ result('create_payroll_download_batch_34') }}"
            }
        )

        mark_pay_run_as_complete_40 = rail.RepliconServiceOperator(
            task_id='mark_pay_run_as_complete_40',
            endpoint="/services/PayRunService1.svc/MarkPayRunAsComplete",
            data={
                "target": {
                    "uri": "{{ result('get_create_pay_run_batch_results_29').payRunUri }}",
                    "name": null
                }
            }
        )

        catch_43 = rail.EmptyOperator(
            task_id='catch_43',
            trigger_rule='one_failed',
        )

        cancel_pay_run_45 = rail.RepliconServiceOperator(
            task_id='cancel_pay_run_45',
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data={
                "target": {
                    "uri": "{{ result('get_create_pay_run_batch_results_29').payRunUri }}",
                    "name": null
                }
            }
        )

        stop_48 = rail.FailOperator(
            task_id='stop_48',
            message='''payroll batch error'''
        )

        read_file_49 = rail.HTTPDownloadFileOperator(
            task_id='read_file_49',
            url='''{{ result('get_payroll_download_batch_results_37').downloadUrl }}''',
        )

        load_csv_create_list_from_csv_50 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_50",
            document="{{result('read_file_49') }}",
        )

        create_collection_create_list_from_csv_50 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_50',
            source="{{ result('load_csv_create_list_from_csv_50') }}",
            name="finalpayrolldata",
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

        get_report_details_51 = rail.RepliconReportDetailsOperator(
            task_id='get_report_details_51',
            report_name='IN ES Termination Balance Report',
        )

        invoke_custom_ruby_code_52 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_52',
            python_callable=lambda: rail.get_dag_run_conf()['userids']
        )

        invoke_custom_ruby_code_53 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_53',
            python_callable=lambda: list(map(lambda x: {
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_51')['filterConfiguration']['enabledFilters'], 'displayText', "UserFilter", 'uri'),
                "value": x
            },   rail.result('invoke_custom_ruby_code_52'))),
        )

        generate_reports_batch_54 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_54',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_51')['uri'],
                        "filterValues": rail.result('invoke_custom_ruby_code_53'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    },
                ]},
        )

        execute_generate_reports_batch_54 = rail.batch_execution(
            group_id='execute_execute_generate_reports_batch_54',
            creation_task_id='generate_reports_batch_54',
        )

        get_report_batch_results_57 = rail.RepliconServiceOperator(
            task_id='get_report_batch_results_57',
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data={
                'reportGenerationBatchUri': "{{ result('generate_reports_batch_54') }}"},
        )

        load_csv_create_list_from_csv_58 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_58",
            document="{{result('get_report_batch_results_57').reportGenerationResults[0].payload }}",
        )

        create_collection_create_list_from_csv_58 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_58',
            source="{{ result('load_csv_create_list_from_csv_58') }}",
            name="terminatedusertimeoffbalance",
            columns={
                'User Name': 'username',
                'Login Name': 'loginname',
                'Employee ID': 'employeeid',
                'useruri': 'useruri',
                'Time Off Type': 'timeofftype',
                'Time Off Type Description': 'timeofftypedescription',
                'Time Off Balance': 'balance',
                'User End Date': 'userenddate'
            }
        )

        query_list_final_datafor_terminated_userforallrequiredpaycodeexcept2301_59 = rail.QueryCollectionOperator(
            task_id='query_list_final_datafor_terminated_userforallrequiredpaycodeexcept2301_59',
            query="""SELECT * FROM  finalpayrolldata WHERE  finalpayrolldata.paycodecode='2504' OR  finalpayrolldata.paycodecode='2511' OR  finalpayrolldata.paycodecode='2512' OR  finalpayrolldata.paycodecode='2513' OR  finalpayrolldata.paycodecode='2514' OR  finalpayrolldata.paycodecode='2515' OR  finalpayrolldata.paycodecode='9720'""",
        )

        declare_list_60 = rail.SetVariableOperator(
            task_id='declare_list_60',
            append=False,
            name='data',
            value=[]
        )

        insert_to_list_61 = rail.SetVariableOperator(
            task_id='insert_to_list_61',
            append=True,
            name='{{ result("declare_list_60").name }}',
            value=lambda: list(map(lambda item: {
                "RECTY": item['RECTY'],
                "CLID": item['CLID'],
                "INTCA": item['INTCA'],
                "ORDNO": item['ORDNO'],
                "IOPER": "DEL" if float(item['PayCodeHours'] or '0') < 0 else "INS",
                "INFTY": item['INFTY'],
                "paycodecode": item['paycodecode'],
                "BEGDA": item['BEGDA'],
                "ENDDA": item['ENDDA'],
                "OBJPS": item['OBJPS'],
                "SPRPS": item['SPRPS'],
                "SEQNR": item['SEQNR'],
                "EXTRA": item['EXTRA'],
                "paycodecode2": item['paycodecode'],
                "STDAZ": item['STDAZ'],
                "BEGUZ": item['BEGUZ'],
                "ENDUZ": item['ENDUZ'],
                "BETRG": item['BETRG'],
                "WAERS": item['WAERS'],
                "PayCodeHours": abs(float(item['PayCodeHours'] or '0')),
                "ZEINH": item['ZEINH'],
                "VTKEN": item['VTKEN'],
                "BWGRL": item['BWGRL'],
                "AUFKZ": item['AUFKZ'],
                "ENDOF": item['ENDOF'],
                "UFLD1": item['UFLD1'],
                "UFLD2": item['UFLD2'],
                "UFLD3": item['UFLD3'],
                "KEYPR": item['KEYPR'],
                "TRFGR": item['TRFGR'],
                "TRFST": item['TRFST'],
                "PRAKN": item['PRAKN'],
                "PRAKZ": item['PRAKZ'],
                "OTYPE": item['OTYPE'],
                "PLANS": item['PLANS'],
                "VERSL": item['VERSL'],
                "EXBEL": item['EXBEL'],
                "WTART": item['WTART'],
                "TDLANGU": item['TDLANGU'],
                "TDSUBLA": item['TDSUBLA'],
                "TDTYPE": item['TDTYPE'],
            }, rail.load_all_records(rail.result('query_list_final_datafor_terminated_userforallrequiredpaycodeexcept2301_59')))),
        )

        query_list_final_datafor_terminated_userfor2301paycode_62 = rail.QueryCollectionOperator(
            task_id='query_list_final_datafor_terminated_userfor2301paycode_62',
            query="""SELECT * FROM  finalpayrolldata WHERE  finalpayrolldata.paycodecode='2301'""",
        )

        insert_to_list_63 = rail.SetVariableOperator(
            task_id='insert_to_list_63',
            append=True,
            name='{{ result("declare_list_60").name }}',
            value=lambda:  list(map(lambda item: {
                "RECTY": item['RECTY'],
                "CLID": item['CLID'],
                "INTCA": item['INTCA'],
                "ORDNO": item['ORDNO'],
                "IOPER": "INS" if (item['PayCodeHours'] and float(item['PayCodeHours'])) > 0 else "DEL",
                "INFTY": item['INFTY'],
                "paycodecode": "2301" if float(item['PayCodeHours']) > 0 else "2302",
                "BEGDA": item['BEGDA'],
                "ENDDA": item['ENDDA'],
                "OBJPS": item['OBJPS'],
                "SPRPS": item['SPRPS'],
                "SEQNR": item['SEQNR'],
                "EXTRA": item['EXTRA'],
                "paycodecode2": "2301" if float(item['PayCodeHours']) > 0 else "2302",
                "STDAZ": item['STDAZ'],
                "BEGUZ": item['BEGUZ'],
                "ENDUZ": item['ENDUZ'],
                "BETRG": item['BETRG'],
                "WAERS": item['WAERS'],
                "PayCodeHours": "1" if abs(float(item['PayCodeHours'] or '0')) == 9 else "0.5",
                "ZEINH": item['ZEINH'],
                "VTKEN": item['VTKEN'],
                "BWGRL": item['BWGRL'],
                "AUFKZ": item['AUFKZ'],
                "ENDOF": item['ENDOF'],
                "UFLD1": item['UFLD1'],
                "UFLD2": item['UFLD2'],
                "UFLD3": item['UFLD3'],
                "KEYPR": item['KEYPR'],
                "TRFGR": item['TRFGR'],
                "TRFST": item['TRFST'],
                "PRAKN": item['PRAKN'],
                "PRAKZ": item['PRAKZ'],
                "OTYPE": item['OTYPE'],
                "PLANS": item['PLANS'],
                "VERSL": item['VERSL'],
                "EXBEL": item['EXBEL'],
                "WTART": item['WTART'],
                "TDLANGU": item['TDLANGU'],
                "TDSUBLA": item['TDSUBLA'],
                "TDTYPE": item['TDTYPE']
            }, rail.load_all_records(rail.result('query_list_final_datafor_terminated_userfor2301paycode_62')))),
        )

        query_list_terminated_user_balance_64 = rail.QueryCollectionOperator(
            task_id='query_list_terminated_user_balance_64',
            query="""SELECT * FROM  terminatedusertimeoffbalance WHERE  terminatedusertimeoffbalance.employeeid IN (SELECT DISTINCT  finalpayrolldata.CLID FROM  finalpayrolldata)""",
        )

        insert_to_list_65 = rail.SetVariableOperator(
            task_id='insert_to_list_65',
            append=True,
            name='{{ result("declare_list_60").name }}',
            value=lambda:  list(map(lambda item: {
                "RECTY": null,
                "CLID": item['employeeid'],
                "INTCA": null,
                "ORDNO": null,
                "IOPER": "INS",
                "INFTY": null,
                "paycodecode": rail.find_first_by_attr_and_get_attr(dxc_payroll_extract_mapper_india, "companycode", item['timeofftype'], "export"),
                "BEGDA": item['userenddate'],
                "ENDDA": item['userenddate'],
                "OBJPS": null,
                "SPRPS": null,
                "SEQNR": null,
                "EXTRA": null,
                "paycodecode2": rail.find_first_by_attr_and_get_attr(dxc_payroll_extract_mapper_india, "companycode", item['timeofftype'], "export"),
                "STDAZ": null,
                "BEGUZ": null,
                "ENDUZ": null,
                "BETRG": null,
                "WAERS": null,
                "PayCodeHours": "30.00" if float(item['balance'] or '0') > 30 else item['balance'],
                "ZEINH": null,
                "VTKEN": null,
                "BWGRL": null,
                "AUFKZ": null,
                "ENDOF": null,
                "UFLD1": null,
                "UFLD2": null,
                "UFLD3": null,
                "KEYPR": null,
                "TRFGR": null,
                "TRFST": null,
                "PRAKN": null,
                "PRAKZ": null,
                "OTYPE": null,
                "PLANS": null,
                "VERSL": null,
                "EXBEL": null,
                "WTART": null,
                "TDLANGU": null,
                "TDSUBLA": null,
                "TDTYPE": null
            }, rail.load_all_records(rail.result('query_list_terminated_user_balance_64'))))
        )

        invoke_custom_ruby_code_66 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_66',
            python_callable=lambda: list(itertools.chain.from_iterable(
                rail.get_dag_run_var(rail.result("declare_list_60")['name'])))
        )

        if_first_cliid_blank_67 = rail.IfOperator(
            task_id='if_first_cliid_blank_67',
            test='''{{ result('invoke_custom_ruby_code_66') | is_falsy or result('invoke_custom_ruby_code_66')[0] | is_falsy or result('invoke_custom_ruby_code_66')[0].CLID | is_falsy }}''',
            yes_task="stop_68",
            no_task="create_list_69",
        )

        stop_68 = rail.EmptyOperator(
            task_id='stop_68',
        )

        create_list_69 = rail.CreateCollectionOperator(
            task_id='create_list_69',
            source="{{ result('invoke_custom_ruby_code_66') | to_json }}",
            name="finaldata",
        )

        query_list_final_datawithoutemployeeid_70 = rail.QueryCollectionOperator(
            task_id='query_list_final_datawithoutemployeeid_70',
            query="""SELECT * FROM  finaldata WHERE  NULLIF(finaldata.CLID,'')  IS NULL OR  finaldata.CLID="" """,
        )

        if_query_list_final_datawithoutemployeeid_70_rows_greater_than_0_71 = rail.IfOperator(
            task_id='if_query_list_final_datawithoutemployeeid_70_rows_greater_than_0_71',
            test='''{{ result('query_list_final_datawithoutemployeeid_70','length') > 0 }}''',
            yes_task="mark_pay_run_as_draft_72",
            no_task="create_csv_lines_75",
        )

        mark_pay_run_as_draft_72 = rail.RepliconServiceOperator(
            task_id='mark_pay_run_as_draft_72',
            endpoint="/services/PayRunService1.svc/MarkPayRunAsDraft",
            data={
                "target": {
                    "uri": "{{ result('get_create_pay_run_batch_results_29').payRunUri }}",
                    "name": null
                }
            }
        )

        cancel_pay_run_73 = rail.RepliconServiceOperator(
            task_id='cancel_pay_run_73',
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data={
                "target": {
                    "uri": "{{ result('get_create_pay_run_batch_results_29').payRunUri }}",
                    "name": null
                }
            }
        )

        stop_74 = rail.FailOperator(
            task_id='stop_74',
            message='''Employee ID not present for some users. Users available to validate in payrun "{{ result('log_requiredfilename_21') }}" '''
        )

        create_csv_lines_75 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_75',
            source="{{ result('invoke_custom_ruby_code_66') | to_json }}",
            header=null,
            delimiter='|',
            row=lambda item: {
                "column_0": "P2010",
                "column_1": item['CLID'],
                "column_2": "IN",
                "column_3": null,
                "column_4": item['IOPER'],
                "column_5": "2010",
                "column_6": item['paycodecode'],
                "column_7": datetime.strptime(item['BEGDA'], '%d %B %Y').strftime("%Y%m%d"),
                "column_8": datetime.strptime(item['ENDDA'], '%d %B %Y').strftime("%Y%m%d"),
                "column_9": null,
                "column_10": null,
                "column_11": null,
                "column_12": null,
                "column_13": item['paycodecode2'],
                "column_14": null,
                "column_15": null,
                "column_16": null,
                "column_17": null,
                "column_18": null,
                "column_19": item['PayCodeHours'],
                "column_20": "010",
                "column_21": null,
                "column_22": null,
                "column_23": null,
                "column_24": null,
                "column_25": null,
                "column_26": null,
                "column_27": null,
                "column_28": null,
                "column_29": null,
                "column_30": null,
                "column_31": null,
                "column_32": null,
                "column_33": null,
                "column_34": null,
                "column_35": null,
                "column_36": null,
                "column_37": null,
                "column_38": null,
                "column_39": null,
                "column_40": null
            }.values(),
        )

        log_total_recordsincludingheaderandfooter_76 = rail.PythonOperator(
            task_id='log_total_recordsincludingheaderandfooter_76',
            python_callable=lambda:  len(
                rail.result('invoke_custom_ruby_code_66')) + 2
        )

        log_formatted_data_77 = rail.PythonOperator(
            task_id='log_formatted_data_77',
            python_callable=lambda:  rail.render_template("HEADR|G2DX|DXC|REPLICON|||{{result('log_requiredfilename_21')}}.SAP|{{ dag_run.conf.rundateinYYYYMMDDformat }}|{{ dag_run.conf.runtimeinHHMMSSformat }}|P|03") + "\r\n" + rail.read_artifact(
                rail.result('create_csv_lines_75')) + rail.render_template("TRAIL|{{result('log_total_recordsincludingheaderandfooter_76')}}")
        )

        log_g_s_u_bwith_78 = rail.PythonOperator(
            task_id='log_g_s_u_bwith_78',
            python_callable=lambda:  rail.result(
                'log_formatted_data_77').replace('|', '|"')
        )

        insert_to_list_79 = rail.SetVariableOperator(
            task_id='insert_to_list_79',
            append=True,
            name='{{ result("declare_list_logforpayroll_7").name }}',
            value=lambda: {
                 "log": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") + " INFO admin No of records exported = " + str(len(rail.result('invoke_custom_ruby_code_66')))
            }
        )

        upload_file_uploadfiletosftp_81 = rail.S3UploadFileOperator(
            task_id='upload_file_uploadfiletosftp_81',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name="Dxctechnology/Payrollexport/INDIAES/{{ result('log_requiredfilename_21') }}.SAP",
            source="{{ result('log_g_s_u_bwith_78') }}"
        )

        encrypt2_a_d_p_public_key_82 = rail.PGPEncryptionOperator(
            task_id='encrypt2_a_d_p_public_key_82',
            source="{{ result('log_g_s_u_bwith_78') }}",
            pgp_conn_id=config.pgp_conn_id,
        )

        upload_uploadfiletosftp_83 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadfiletosftp_83',
            content="{{ result('encrypt2_a_d_p_public_key_82') }}",
            # append = false,
            remote_filepath=config.datafilepath + \
            "/{{ result('log_requiredfilename_21') }}.SAP.pgp"
        )

        insert_to_list_86 = rail.SetVariableOperator(
            task_id='insert_to_list_86',
            append=True,
            name='{{ result("declare_list_logforpayroll_7").name }}',
            value=lambda: {
                "log": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") + " INFO admin Export File_" + rail.render_template("{{ result('log_requiredfilename_21') }}.SAP.pgp") + " created"
            }
        )

        log_processended_87 = rail.PythonOperator(
            task_id='log_processended_87',
            python_callable=lambda:  datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        )

        insert_to_list_88 = rail.SetVariableOperator(
            task_id='insert_to_list_88',
            append=True,
            name='{{ result("declare_list_logforpayroll_7").name }}',
            value=lambda: {
                "log": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") + " - Process ended"
            }
        )

        create_csv_lines_compsepayrolllog_89 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_compsepayrolllog_89',
            source="{{ dag_run_var(result('declare_list_logforpayroll_7').name) | to_json }}",
            header=['Log file'],
            row=lambda item: {
                "column_0": item['log']
            }.values(),
        )

        send_mail_sendemailafterpayrollexport_90 = rail.EmailOperator(
            task_id='send_mail_sendemailafterpayrollexport_90',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }}| Replicon daily payroll export for IN ES terminated users completed - {{ dag_run.conf.timenow }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Replicon daily payroll export for IN ES terminated users is completed successfully on {{ dag_run.conf.timenow }}. Please find the export details for reference. </p>
            <ul>
            <li>Process started: {{ dag_run.conf.timenow }} </li>
            <li>Payroll extract file name: {{ result('log_requiredfilename_21') }} </li>
            <li>File path: {{ params.datafilepath }} </li>
            <li>Company Code: {{ dag_run.conf.division }} </li>
            <li>Number of records in payroll extract: {{ result('invoke_custom_ruby_code_66') | length }} </li>
            <li>Payroll log file name: Log_{{ result('log_requiredfilename_21') }} </li>
            <li>Log file path: {{params.logfilepath}} </li>
            <li>Process ended: {{ result('log_processended_87') }} </li>
            </ul>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params={'datafilepath': config.datafilepath,
                    'logfilepath': config.logfilepath},
        )

        upload_file_uploadfiletosftp_92 = rail.S3UploadFileOperator(
            task_id='upload_file_uploadfiletosftp_92',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name="Dxctechnology/Payrollexport/INDIAES/Log_{{ result('log_requiredfilename_21') }}.txt",
            source="{{ result('create_csv_lines_compsepayrolllog_89') }}"
        )

        upload_upload_payrolllogstosftp_93 = rail.SFTPUploadFileOperator(
            task_id='upload_upload_payrolllogstosftp_36',
            content='''{{ result('create_csv_lines_compsepayrolllog_89') }}''',
            # append = false,
            remote_filepath=config.logfilepath + \
            "/Log_{{ result('log_requiredfilename_21') }}.txt",
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        create_object_set_4 >> declare_list_logforpayroll_7 >> insert_to_list_8 >> create_payroll_download_batch_10 >> execute_payroll_batch[
            0] >> execute_payroll_batch[1] >> get_payroll_download_batch_results_13 >> if_d_error_present_16
        if_d_error_present_16 >> rail.Label('Yes') >> stop_17 >> finish
        if_d_error_present_16 >> rail.Label(
            'No') >> read_file_18 >> load_csv_create_list_from_csv_19 >> create_collection_create_list_from_csv_19 >> if_create_list_from_csv_19_row_count_greater_than_0_20
        if_create_list_from_csv_19_row_count_greater_than_0_20 >> rail.Label(
            'Yes') >> log_requiredfilename_21 >> create_pay_run_batch_24 >> batch_management_25 >> get_create_pay_run_batch_results_29 >> if_get_create_pay_run_batch_results_29_error_present_30
        if_create_list_from_csv_19_row_count_greater_than_0_20 >> rail.Label(
            'No') >> finish
        if_get_create_pay_run_batch_results_29_error_present_30 >> rail.Label(
            'Yes') >> stop_31 >> finish
        if_get_create_pay_run_batch_results_29_error_present_30 >> rail.Label('No') >> update_pay_run_name_32 >> create_payroll_download_batch_34 >> execute_create_payroll_download_batch_34[
            0] >> execute_create_payroll_download_batch_34[1] >> get_payroll_download_batch_results_37 >> mark_pay_run_as_complete_40
        mark_pay_run_as_complete_40 >> catch_43 >> cancel_pay_run_45 >> stop_48 >> finish
        mark_pay_run_as_complete_40 >> read_file_49 >> load_csv_create_list_from_csv_50 >> create_collection_create_list_from_csv_50 >> get_report_details_51 >> invoke_custom_ruby_code_52 >> invoke_custom_ruby_code_53 >> generate_reports_batch_54 >> execute_generate_reports_batch_54[
            0] >> execute_generate_reports_batch_54[1] >> get_report_batch_results_57 >> load_csv_create_list_from_csv_58 >> create_collection_create_list_from_csv_58 >> query_list_final_datafor_terminated_userforallrequiredpaycodeexcept2301_59 >> declare_list_60 >> insert_to_list_61 >> query_list_final_datafor_terminated_userfor2301paycode_62 >> insert_to_list_63 >> query_list_terminated_user_balance_64 >> insert_to_list_65 >> invoke_custom_ruby_code_66 >> if_first_cliid_blank_67
        if_first_cliid_blank_67 >> rail.Label('Yes') >> stop_68 >> finish
        if_first_cliid_blank_67 >> rail.Label(
            'No') >> create_list_69 >> query_list_final_datawithoutemployeeid_70 >> if_query_list_final_datawithoutemployeeid_70_rows_greater_than_0_71
        if_query_list_final_datawithoutemployeeid_70_rows_greater_than_0_71 >> rail.Label(
            'Yes') >> mark_pay_run_as_draft_72 >> cancel_pay_run_73 >> stop_74 >> finish
        if_query_list_final_datawithoutemployeeid_70_rows_greater_than_0_71 >> rail.Label(
            'No') >> create_csv_lines_75 >> log_total_recordsincludingheaderandfooter_76 >> log_formatted_data_77 >> log_g_s_u_bwith_78 >> insert_to_list_79 >> upload_file_uploadfiletosftp_81 >> encrypt2_a_d_p_public_key_82 >> upload_uploadfiletosftp_83 >> insert_to_list_86 >> log_processended_87 >> insert_to_list_88 >> create_csv_lines_compsepayrolllog_89 >> send_mail_sendemailafterpayrollexport_90 >> upload_file_uploadfiletosftp_92 >> upload_upload_payrolllogstosftp_93 >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
