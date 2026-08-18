
from datetime import timedelta, datetime
import rail
from sigroup.payroll_export_japan.mappers.sigroup_valid_paycodenames_mapper import valid_paycodename_japan
null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'sigroup_payroll_export_japan_business_unit_child_{config.instance}',
        description=f'SiGroup - Japan Business Unit Payroll Export_Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        create_log_list=rail.SetVariableOperator(
            task_id='create_log_list',
            append=False,
            name='log',
            value=[]
        )

        log_start_time=rail.PythonOperator(
            task_id='log_start_time',
            python_callable= lambda:  datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        )

        insert_to_log=rail.SetVariableOperator(
            task_id='insert_to_log',
            append=True,
            name='{{ result("create_log_list").name }}',
            value={
              "log": "{{ result('log_start_time') }} - Process started"
            }
        )

        insert_territory_to_log=rail.SetVariableOperator(
            task_id='insert_territory_to_log',
            append=True,
            name='{{ result("create_log_list").name }}',
            value={
              "log": "Territory : {{ dag_run.conf.businessunit }}"
            }
        )

        def get_time_object():
            now = datetime.now()
            return {
                "format1": now.strftime("%Y%m%d%H%M%S"),
                "format2": now.strftime("%m/%d/%YT%H:%M:%S"),
                "format3": now.strftime("%Y%m%d"),
                "format4": now.strftime("%H%M%S")
            }

        get_time_in_required_formats=rail.PythonOperator(
            task_id='get_time_in_required_formats',
            python_callable= get_time_object
        )

        log_required_filename=rail.PythonOperator(
            task_id='log_required_filename',
            python_callable=lambda: "SIGTT" + "-" + "PROD-CPM-" + rail.result('get_time_in_required_formats')['format3'] +
                              "-" + rail.result('get_time_in_required_formats')['format4'] + "-" + "PD21"
        )

        create_payroll_download_batch=rail.RepliconServiceOperator(
            task_id='create_payroll_download_batch',
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data={
              "columnUris": [],
              "sort": [],
              "filterExpression": {
                "leftExpression": {
                  "leftExpression": {
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
                              "year": "{{ dag_run.conf.startdateyear }}",
                              "month": "{{ dag_run.conf.startdatemonth }}",
                              "day": "{{ dag_run.conf.startdateday }}"
                            },
                            "endDate": {
                              "year": "{{ dag_run.conf.enddateyear }}",
                              "month": "{{ dag_run.conf.enddatemonth }}",
                              "day": "{{ dag_run.conf.enddateday }}"
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
                  "operatorUri": "urn:replicon:filter-operator:and",
                  "rightExpression": {
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
                    "filterDefinitionUri": "urn:replicon:pay-run-filter:division"
                  },
                  "operatorUri": "urn:replicon:filter-operator:in",
                  "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                      "uri": null,
                      "uris": [
                        "{{ dag_run.conf.businessunituri }}"
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
              "fileFormatScriptUri": "{{ dag_run.conf.fileformaturi }}"
            }
        )

        execute_payroll_download_batch, wait_for_payroll_download_batch = rail.batch_execution(
            'execute_payroll_download_batch', create_payroll_download_batch.task_id)

        get_payroll_download_batch_result = rail.RepliconServiceOperator(
            task_id="get_payroll_download_batch_result",
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={
                "payrollDownloadBatchUri": "{{ result('create_payroll_download_batch') }}"
            }
        )

        if_error_in_batch_result=rail.IfOperator(
            task_id='if_error_in_batch_result',
            test='''{{ result('get_payroll_download_batch_result').error | is_truthy }}''',
            yes_task="fail_as_error_in_batch_result",
            no_task="download_payload_file_from_url",
        )

        fail_as_error_in_batch_result=rail.FailOperator(
            task_id='fail_as_error_in_batch_result',
            message='''{{ result('get_payroll_download_batch_result').error }}'''
        )

        download_payload_file_from_url=rail.HTTPDownloadFileOperator(
            task_id='download_payload_file_from_url',
            url='''{{ result('get_payroll_download_batch_result').downloadUrl }}''',
        )

        load_csv_from_file=rail.LoadCSVFileOperator(
            task_id="load_csv_from_file",
            document="{{result('download_payload_file_from_url')}}",
        )

        create_collection_payrolldata = rail.CreateCollectionOperator(
            task_id='create_collection_payrolldata',
            source = "{{ result('load_csv_from_file') }}",
            name = "payrolldata",
            columns={
              "Employee ID": "employeeid",
              "Entry Date": "entrydate",
              "CLOUDPAY_PAYCODE": "cloudpaypaycode",
              "Pay Code Code": "paycodecode",
              "Pay Code Hours": "paycodehours",
              "Pay Code Pay": "paycodepay"
            }
        )

        if_collection_has_data=rail.IfOperator(
            task_id='if_collection_has_data',
            test='''{{ result('create_collection_payrolldata','length') > 0 }}''',
            yes_task="create_pay_run_batch",
            no_task="finish",
        )

        create_pay_run_batch=rail.RepliconServiceOperator(
            task_id='create_pay_run_batch',
            endpoint="/services/PayRunService1.svc/CreatePayRunBatch",
            data={
              "filterExpression": {
                "leftExpression": {
                  "leftExpression": {
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
                              "year": "{{ dag_run.conf.startdateyear }}",
                              "month": "{{ dag_run.conf.startdatemonth }}",
                              "day": "{{ dag_run.conf.startdateday }}"
                            },
                            "endDate": {
                              "year": "{{ dag_run.conf.enddateyear }}",
                              "month": "{{ dag_run.conf.enddatemonth }}",
                              "day": "{{ dag_run.conf.enddateday }}"
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
                  "operatorUri": "urn:replicon:filter-operator:and",
                  "rightExpression": {
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
                    "filterDefinitionUri": "urn:replicon:pay-run-filter:division"
                  },
                  "operatorUri": "urn:replicon:filter-operator:in",
                  "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                      "uri": null,
                      "uris": [
                        "{{ dag_run.conf.businessunituri }}"
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
              "columnUris": [],
              "unitOfWorkId": "{{ current_time() }}"
            }
        )

        execute_payrun_batch, wait_forpayrun_batch = rail.batch_execution(
            'execute_payrun_batch', create_pay_run_batch.task_id)

        get_create_pay_run_batch_results=rail.RepliconServiceOperator(
            task_id='get_create_pay_run_batch_results',
            endpoint="/services/PayRunService1.svc/GetCreatePayRunBatchResults",
            data={
              "payRunBatchUri": "{{ result('create_pay_run_batch') }}"
            }
        )

        if_error_in_payrun_batch=rail.IfOperator(
            task_id='if_error_in_payrun_batch',
            test='''{{ result('get_create_pay_run_batch_results').error | is_truthy }}''',
            yes_task="fail_error_in_pay_run_batch",
            no_task="update_pay_run_name",
        )

        fail_error_in_pay_run_batch=rail.FailOperator(
            task_id='fail_error_in_pay_run_batch',
            message='''{{ result('get_create_pay_run_batch_results').error }}'''
        )

        update_pay_run_name=rail.RepliconServiceOperator(
            task_id='update_pay_run_name',
            endpoint="/services/PayRunService1.svc/UpdatePayRunName",
            data={
              "target": {
                "uri": "{{ result('get_create_pay_run_batch_results').payRunUri }}",
                "name": null
              },
              "name": "Japan_{{ result('log_required_filename') }}"
            }
        )

        createpayroll_downloadbatch=rail.RepliconServiceOperator(
            task_id='createpayroll_downloadbatch',
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
                    "uris": ["{{ result('get_create_pay_run_batch_results').payRunUri }}"],
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

        executepayroll_downloadbatch, wait_for_payroll_downloadbatch = rail.batch_execution(
            'executepayroll_downloadbatch', createpayroll_downloadbatch.task_id)

        get_payrolldownload_batchresults=rail.RepliconServiceOperator(
            task_id='get_payrolldownload_batchresults',
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={
              "payrollDownloadBatchUri": "{{ result('createpayroll_downloadbatch') }}"
            }
        )

        mark_pay_run_as_complete=rail.RepliconServiceOperator(
            task_id='mark_pay_run_as_complete',
            endpoint="/services/PayRunService1.svc/MarkPayRunAsComplete",
            data={
              "target": {
                "uri": "{{ result('get_create_pay_run_batch_results').payRunUri }}",
                "name": null
              }
            }
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed'
        )

        cancel_pay_run=rail.RepliconServiceOperator(
            task_id='cancel_pay_run',
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data={
              "target": {
                "uri": "{{ result('get_create_pay_run_batch_results').payRunUri }}",
                "name": null
              }
            }
        )

        read_file=rail.HTTPDownloadFileOperator(
            task_id='read_file',
            url='''{{ result('get_payrolldownload_batchresults').downloadUrl }}''',
        )

        load_csv_from_read_file=rail.LoadCSVFileOperator(
            task_id="load_csv_from_read_file",
            document="{{result('read_file')}}",
        )

        write_csv_for_payrolldata_with_required_dateformat = rail.WriteCSVFileOperator(
            task_id = 'write_csv_for_payrolldata_with_required_dateformat',
            source="{{result('load_csv_from_read_file')}}",
            header=[
                "Employee ID",
                "Entry Date",
                "CLOUDPAY_PAYCODE",
                "Pay Code Code",
                "Pay Code Hours",
                "Pay Code Pay",
                "Pay Code Name"
            ],
            row=lambda item:[
                item['Employee ID'],
                (datetime.strptime(item['Entry Date'],'%Y/%m/%d')).strftime('%Y-%m-%d') if item['Entry Date'] else '',
                item['CLOUDPAY_PAYCODE'],
                item['Pay Code Code'],
                item['Pay Code Hours'],
                item['Pay Code Pay'],
                item['Pay Code Name']
            ]

        )

        create_collection_finalpayrolldata = rail.CreateCollectionOperator(
            task_id='create_collection_finalpayrolldata',
            source = "{{ result('write_csv_for_payrolldata_with_required_dateformat') }}",
            name = "finalpayrolldata",
            columns = {
              "Employee ID": "employeeid",
              "Entry Date": "entrydate",
              "CLOUDPAY_PAYCODE": "cloudpaypaycode",
              "Pay Code Code": "paycodecode",
              "Pay Code Hours": "paycodehours",
              "Pay Code Pay": "paycodepay",
              "Pay Code Name": "paycodename"
            }
        )

        def get_all_required_pacodes(mapper):
            return "'"+"','".join(mapper)+"'"

        get_valid_paycode_names = rail.PythonOperator(
            task_id = 'get_valid_paycode_names',
            python_callable=lambda: get_all_required_pacodes(valid_paycodename_japan)
        )

        query_list_final_data=rail.QueryCollectionOperator(
            task_id='query_list_final_data',
            name='finaldata',
            query="""SELECT * FROM  finalpayrolldata WHERE
              finalpayrolldata.paycodename IN ({{result('get_valid_paycode_names')}}) ORDER BY  finalpayrolldata.employeeid ASC""",
        )

        get_user_details_report=rail.RepliconReportDetailsOperator(
            task_id='get_user_details_report',
            report_name=config.user_details_report,
        )

        get_division_filter_uri=rail.PythonOperator(
            task_id='get_division_filter_uri',
            python_callable= lambda dag_run: {
              "divisionuri": rail.find_first_by_attr_and_get_attr(
                              rail.result('get_user_details_report')['filterConfiguration']['enabledFilters'],'displayText', 'DivisionFilter','uri',''),
              "guidid": (dag_run.conf['businessunituri']).split(":")[-1]
            }
        )

        run_user_details_report = rail.run_report2(
            group_id='run_user_details_report',
            report_params={
                "reportParameters": [
                    {
                    "reportUri": "{{result('get_user_details_report').uri}}",
                    "filterValues": [
                        {
                            "reportFilterUri": "{{result('get_division_filter_uri').divisionuri}}",
                            "value": "{{result('get_division_filter_uri').guidid}}"
                        }
                    ],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact'
        )

        if_payload_has_data=rail.IfOperator(
            task_id='if_payload_has_data',
            test="{{ result('run_user_details_report.get_report_result','has_data')}}",
            yes_task="if_payload_doesnt_match_required_column_order",
            no_task="load_csv_user_details_data",
        )

        if_payload_doesnt_match_required_column_order=rail.IfOperator(
            task_id='if_payload_doesnt_match_required_column_order',
            #pylint: disable = line-too-long
            test='''{{not (result('run_user_details_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload | starts_with('User Name,Employee ID,User End Date,User Status,User URI,CurrentHourlyPayroll,BusinessUnit') }}''',
            yes_task="fail_column_order_does_not_match",
            no_task="load_csv_user_details_data",
        )

        fail_column_order_does_not_match=rail.FailOperator(
            task_id='fail_column_order_does_not_match',
            message='''Base report column order does not match'''
        )

        load_csv_user_details_data=rail.LoadCSVFileOperator(
            task_id="load_csv_user_details_data",
            document="{{(result('run_user_details_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload}}",
        )

        create_collection_report_data = rail.CreateCollectionOperator(
            task_id='create_collection_report_data',
            source = "{{ result('load_csv_user_details_data') }}",
            name = "reportdata",
            columns = {
              'User Name':'username', 
              'Employee ID':'employeeid', 
              'User End Date':'userenddate', 
              'User Status':'status', 
              'User URI':'useruri', 
              'CurrentHourlyPayroll':'currenthourlypayroll', 
              'BusinessUnit':'businessunit'
            }
        )

        query_users_with_enddate=rail.QueryCollectionOperator(
            task_id='query_users_with_enddate',
            query="""SELECT * FROM  reportdata WHERE  reportdata.status="Disabled" AND  NULLIF(userenddate,'') IS NOT NULL  AND
                  reportdata.businessunit='AP Region (Japan)'""",
        )

        query_users_with_hourlypayroll_rate=rail.QueryCollectionOperator(
            task_id='query_users_with_hourlypayroll_rate',
            name='alluserwithhourlypayrollrate',
            query="""SELECT * FROM  reportdata""",
        )

        if_users_with_enddate_present=rail.IfOperator(
            task_id='if_users_with_enddate_present',
            test='''{{ result('query_users_with_enddate','length') > 0 }}''',
            yes_task="compose_csv_disabled_users",
            no_task="create_structureddata_collection",
        )

        compose_csv_disabled_users=rail.WriteCSVFileOperator(
            task_id='compose_csv_disabled_users',
            source="{{ result('query_users_with_enddate') }}",
            header=['username',
                    'employeeid',
                    'userenddate',
                    'userstatus',
                    'useruri',
                    'id',
                    'currenthourlypayroll'],
            row=lambda item: [
                item['username'],
                item['employeeid'],
                datetime.strptime(item['userenddate'],"%b %d, %Y").strftime("%Y-%m-%d"),
                item['status'],
                item['useruri'],
                (item['useruri']).split(":")[-1],
                item['currenthourlypayroll']
            ],
        )

        get_startdate_enddate_in_comparable_format=rail.PythonOperator(
            task_id='get_startdate_enddate_in_comparable_format',
            python_callable=lambda dag_run: {
              "startdate": datetime.strptime(dag_run.conf['startdate'],"%d-%m-%Y").strftime("%Y-%m-%d"),
              "enddate": datetime.strptime(dag_run.conf['enddate'],"%d-%m-%Y").strftime("%Y-%m-%d")
            }
        )

        create_collection_disableduserdata = rail.CreateCollectionOperator(
            task_id='create_collection_disableduserdata',
            source = "{{ result('compose_csv_disabled_users') }}",
            name = "disableduserdata",
            columns = {
              'username':'username', 
              'employeeid':'employeeid', 
              'userenddate':'userenddate', 
              'userstatus':'status', 
              'useruri':'useruri', 
              'id':'id', 
              'currenthourlypayroll':'currenthourlypayroll'
            }
        )

        query_users_to_consider_for_this_period=rail.QueryCollectionOperator(
            task_id='query_users_to_consider_for_this_period',
            query="""SELECT * FROM  disableduserdata WHERE
                  disableduserdata.userenddate > '{{ result('get_startdate_enddate_in_comparable_format').startdate }}' AND
                  disableduserdata.userenddate < '{{ result('get_startdate_enddate_in_comparable_format').enddate }}'""",
        )

        if_query_users_to_consider_has_data=rail.IfOperator(
            task_id='if_query_users_to_consider_has_data',
            test='''{{ result('query_users_to_consider_for_this_period','length') > 0 }}''',
            yes_task="get_timeoff_termination_balance_report_details",
            no_task="create_structureddata_collection",
        )

        get_timeoff_termination_balance_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_timeoff_termination_balance_report_details',
            report_name=config.timeoff_termination_report,
        )

        def get_report_filter_for_termination():
            users = rail.load_all_records(rail.result('query_users_to_consider_for_this_period'))
            filteruri = rail.find_first_by_attr_and_get_attr(
                        rail.result('get_timeoff_termination_balance_report_details')['filterConfiguration']['enabledFilters'],
                        'displayText','UserFilter','uri','')
            return [{
                "reportFilterUri": filteruri,
                "value": user['id']
            } for user in users ]

        create_report_filter_for_termination_list=rail.PythonOperator(
            task_id='create_report_filter_for_termination_list',
            python_callable=get_report_filter_for_termination
        )

        run_timeoff_termination_balance_report = rail.run_report2(
            group_id='run_timeoff_termination_balance_report',
            report_params=lambda:{
                "reportParameters": [
                    {
                    "reportUri": rail.result('get_timeoff_termination_balance_report_details')['uri'],
                    "filterValues": rail.result('create_report_filter_for_termination_list'),
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact'
        )

        if_payload_hasdata=rail.IfOperator(
            task_id='if_payload_hasdata',
            test='''{{result('run_timeoff_termination_balance_report.get_report_result','has_data')}}''',
            yes_task="if_payload_doesnt_match_required_columnorder",
            no_task="load_csv_from_timeoff_report_result",
        )

        if_payload_doesnt_match_required_columnorder=rail.IfOperator(
            task_id='if_payload_doesnt_match_required_columnorder',
            #pylint: disable = line-too-long
            test='''{{not (result('run_timeoff_termination_balance_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload | starts_with('User Name,Time Off Type,Time Off Balance,Time Off Type Description,Employee ID,Units,User End Date,CLOUDPAY_PAYCODE') }}''',
            yes_task="fail_columnorder_does_not_match",
            no_task="load_csv_from_timeoff_report_result",
        )

        fail_columnorder_does_not_match=rail.FailOperator(
            task_id='fail_columnorder_does_not_match',
            message='''Base report column order does not match'''
        )

        load_csv_from_timeoff_report_result=rail.LoadCSVFileOperator(
            task_id="load_csv_from_timeoff_report_result",
            document="{{(result('run_timeoff_termination_balance_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload}}",
        )

        write_csv_for_timeoff_report_data_with_required_dateformat = rail.WriteCSVFileOperator(
            task_id = 'write_csv_for_timeoff_report_data_with_required_dateformat',
            source="{{result('load_csv_from_timeoff_report_result')}}",
            header=[
                "User Name",
                "Time Off Type",
                "Time Off Balance",
                "Time Off Type Description",
                "Employee ID",
                "Units",
                "User End Date",
                "CLOUDPAY_PAYCODE"
            ],
            row=lambda item:[
                item['User Name'],
                item['Time Off Type'],
                item['Time Off Balance'],
                item['Time Off Type Description'],
                item['Employee ID'],
                item['Units'],
                (datetime.strptime(item['User End Date'],'%b %d, %Y')).strftime('%Y-%m-%d') if item['User End Date'] else '',
                item['CLOUDPAY_PAYCODE']
            ]
        )

        create_collection_timeoff_report_data = rail.CreateCollectionOperator(
            task_id='create_collection_timeoff_report_data',
            source = "{{ result('write_csv_for_timeoff_report_data_with_required_dateformat') }}",
            name = "timeoffreportdata",
            columns= {
              "User Name": "username",
              "Time Off Type": "timeofftype",
              "Time Off Balance": "timeoffbalance",
              "Time Off Type Description": "timeofftypedescription",
              "Employee ID": "employeeid",
              "Units": "units",
              "User End Date": "userenddate",
              "CLOUDPAY_PAYCODE": "cloudpaypaycode"
            }
        )

        query_end_user_data=rail.QueryCollectionOperator(
            task_id='query_end_user_data',
            name='enduserdata',
            query="""SELECT * FROM  timeoffreportdata WHERE   timeoffreportdata.timeofftypedescription LIKE 'Export Balance on Termination%'"""
        )

        create_structureddata_collection = rail.QueryCollectionOperator(
            task_id='create_structureddata_collection',
            name = 'structureddata',
            query="""SELECT * FROM finaldata"""
        )

        if_timeoffreportdata_has_records=rail.IfOperator(
            task_id='if_timeoff_reportdata_has_records',
            test='''{{ result('create_collection_timeoff_report_data') | is_truthy and result('create_collection_timeoff_report_data','length') > 0 }}''',
            yes_task="query_distinct_payrollid_with_timeoff_data",
            no_task="query_distinct_payrollid",
        )

        query_distinct_payrollid_with_timeoff_data=rail.QueryCollectionOperator(
            task_id='query_distinct_payrollid_with_timeoff_data',
            query="""SELECT DISTINCT structureddata.cloudpaypaycode FROM  structureddata WHERE NOT  structureddata.cloudpaypaycode= '""' AND
                  NULLIF(cloudpaypaycode,'') IS NOT NULL UNION SELECT DISTINCT  timeoffreportdata.cloudpaypaycode FROM  timeoffreportdata WHERE
                  NOT  timeoffreportdata.cloudpaypaycode= '""' AND  NULLIF(cloudpaypaycode,'') IS NOT NULL AND
                  timeoffreportdata.timeofftypedescription  LIKE 'Export Balance on Termination%'"""
        )

        query_distinct_payrollid=rail.QueryCollectionOperator(
            task_id='query_distinct_payrollid',
            query="""SELECT DISTINCT structureddata.cloudpaypaycode FROM  structureddata WHERE NOT  structureddata.cloudpaypaycode= '""' AND
                  NULLIF(cloudpaypaycode,'') IS NOT NULL """,
        )

        if_distinct_payrollid_with_timeoff_data_present = rail.IfOperator(
            task_id = 'if_distinct_payrollid_with_timeoff_data_present',
            test="{{result('query_distinct_payrollid_with_timeoff_data') | is_truthy and result('query_distinct_payrollid_with_timeoff_data','length') > 0 }}",
            yes_task='trigger_child_per_payroll_id_with_timeoffdata',
            no_task='trigger_child_per_payroll_id'
        )

        trigger_child_per_payroll_id_with_timeoffdata=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_per_payroll_id_with_timeoffdata',
            retries=0,
            items="{{ result('query_distinct_payrollid_with_timeoff_data') }}",
            trigger_dag_id=f'sigroup_payroll_export_japan_per_payroll_id_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
              "businessunit": "{{ dag_run.conf.businessunit }}",
              "businessunituri": "{{ dag_run.conf.businessunituri }}",
              "fileformat": "{{ dag_run.conf.fileformat }}",
              "fileformaturi": "{{ dag_run.conf.fileformaturi }}",
              "companykey": "{{ get_company_key() }}",
              "startdate": "{{ dag_run.conf.startdate }}",
              "startdateday": "{{ dag_run.conf.startdateday }}",
              "startdatemonth": "{{ dag_run.conf.startdatemonth }}",
              "startdateyear": "{{ dag_run.conf.startdateyear }}",
              "enddate": "{{ dag_run.conf.enddate }}",
              "enddateday": "{{ dag_run.conf.enddateday }}",
              "enddatemonth": "{{ dag_run.conf.enddatemonth }}",
              "enddateyear": "{{ dag_run.conf.enddateyear }}",
              "filenamecounter": "{{ dag_run.conf.filenamecounter }}",
              "payrollid": "{{ item.cloudpaypaycode }}",
              "isenduserdatapresent": "{{result('query_end_user_data')}}"
            }
        )

        wait_for_completion_of_trigger_child_per_payroll_id_with_timeoffdata = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_of_trigger_child_per_payroll_id_with_timeoffdata',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_per_payroll_id_with_timeoffdata") }}'
        )

        trigger_child_per_payroll_id=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_per_payroll_id',
            retries=0,
            items="{{ result('query_distinct_payrollid') }}",
            trigger_dag_id=f'sigroup_payroll_export_japan_per_payroll_id_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
              "businessunit": "{{ dag_run.conf.businessunit }}",
              "businessunituri": "{{ dag_run.conf.businessunituri }}",
              "fileformat": "{{ dag_run.conf.fileformat }}",
              "fileformaturi": "{{ dag_run.conf.fileformaturi }}",
              "companykey": "{{ get_company_key() }}",
              "startdate": "{{ dag_run.conf.startdate }}",
              "startdateday": "{{ dag_run.conf.startdateday }}",
              "startdatemonth": "{{ dag_run.conf.startdatemonth }}",
              "startdateyear": "{{ dag_run.conf.startdateyear }}",
              "enddate": "{{ dag_run.conf.enddate }}",
              "enddateday": "{{ dag_run.conf.enddateday }}",
              "enddatemonth": "{{ dag_run.conf.enddatemonth }}",
              "enddateyear": "{{ dag_run.conf.enddateyear }}",
              "filenamecounter": "{{ dag_run.conf.filenamecounter }}",
              "payrollid": "{{ item.cloudpaypaycode }}",
              "isenduserdatapresent": "{{result('query_end_user_data')}}"
            }
        )

        wait_for_completion_of_trigger_child_per_payroll_id = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_of_trigger_child_per_payroll_id',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_per_payroll_id") }}'
        )

        if_query_distinct_payrollid_has_no_data=rail.IfOperator(
            task_id='if_query_distinct_payrollid_has_no_data',
            test='''{{ result('query_distinct_payrollid','length') == 0 and result('query_distinct_payrollid_with_timeoff_data') | is_falsy }}''',
            yes_task="send_mail_no_data_for_business_unit",
            no_task="send_success_mail",
        )

        send_mail_no_data_for_business_unit=rail.EmailOperator(
            task_id='send_mail_no_data_for_business_unit',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            #pylint: disable = line-too-long
            subject='''{{ get_company_key() }}| Payroll extract completed with no data for AP Region (Japan) - {{ result('get_time_in_required_formats').format1 }} ''',
            html_content= '''templates/no_data_for_businessunit_email.html''',
        )

        send_success_mail=rail.EmailOperator(
            task_id='send_success_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            #pylint: disable = line-too-long
            subject='''{{ get_company_key() }}| Payroll extract completed for {{ dag_run.conf.businessunit }} - {{ result('get_time_in_required_formats').format1 }}''',
            html_content= '''templates/success_mail.html''',
            params={
              'file_upload_path': config.log_upload_path
            }
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        create_log_list >> log_start_time >> insert_to_log >> insert_territory_to_log >> get_time_in_required_formats
        get_time_in_required_formats >> log_required_filename >> create_payroll_download_batch >> execute_payroll_download_batch
        execute_payroll_download_batch >> wait_for_payroll_download_batch >> get_payroll_download_batch_result >> if_error_in_batch_result
        if_error_in_batch_result >> rail.Label('Yes')  >> fail_as_error_in_batch_result >> finish
        if_error_in_batch_result >> rail.Label('No') >> download_payload_file_from_url >> load_csv_from_file
        load_csv_from_file >> create_collection_payrolldata >> if_collection_has_data
        if_collection_has_data >> rail.Label('Yes') >> create_pay_run_batch >> execute_payrun_batch
        execute_payrun_batch >> wait_forpayrun_batch >> get_create_pay_run_batch_results >> if_error_in_payrun_batch
        if_error_in_payrun_batch >> rail.Label('Yes')  >> fail_error_in_pay_run_batch >> finish
        if_error_in_payrun_batch >> rail.Label('No') >> update_pay_run_name >> createpayroll_downloadbatch >> executepayroll_downloadbatch
        executepayroll_downloadbatch >> wait_for_payroll_downloadbatch >> get_payrolldownload_batchresults >> mark_pay_run_as_complete
        mark_pay_run_as_complete >> rail.Label(
            'on_success') >> read_file >> load_csv_from_read_file >> write_csv_for_payrolldata_with_required_dateformat >> create_collection_finalpayrolldata
        create_collection_finalpayrolldata >> get_valid_paycode_names >> query_list_final_data >> get_user_details_report
        get_user_details_report >> get_division_filter_uri >> run_user_details_report >> if_payload_has_data
        if_payload_has_data >> rail.Label('Yes')  >> if_payload_doesnt_match_required_column_order
        if_payload_doesnt_match_required_column_order >> rail.Label('Yes')  >> fail_column_order_does_not_match >> finish
        if_payload_doesnt_match_required_column_order >> rail.Label('No') >> load_csv_user_details_data
        if_payload_has_data >> rail.Label('No') >> load_csv_user_details_data >> create_collection_report_data >> query_users_with_enddate
        query_users_with_enddate >> query_users_with_hourlypayroll_rate >> if_users_with_enddate_present
        if_users_with_enddate_present >> rail.Label('Yes')  >> compose_csv_disabled_users >> get_startdate_enddate_in_comparable_format
        get_startdate_enddate_in_comparable_format >> create_collection_disableduserdata >> query_users_to_consider_for_this_period
        query_users_to_consider_for_this_period >> if_query_users_to_consider_has_data
        if_query_users_to_consider_has_data >> rail.Label('Yes')  >> get_timeoff_termination_balance_report_details
        get_timeoff_termination_balance_report_details >> create_report_filter_for_termination_list >> run_timeoff_termination_balance_report
        run_timeoff_termination_balance_report >> if_payload_hasdata
        if_payload_hasdata >> rail.Label('Yes')  >> if_payload_doesnt_match_required_columnorder
        if_payload_doesnt_match_required_columnorder >> rail.Label('Yes')  >> fail_columnorder_does_not_match >> finish
        if_payload_doesnt_match_required_columnorder >> rail.Label('No') >> load_csv_from_timeoff_report_result
        if_payload_hasdata >> rail.Label(
            'No') >> load_csv_from_timeoff_report_result >> write_csv_for_timeoff_report_data_with_required_dateformat >> create_collection_timeoff_report_data
        create_collection_timeoff_report_data >> query_end_user_data >> create_structureddata_collection
        if_query_users_to_consider_has_data >> rail.Label('No') >> create_structureddata_collection
        if_users_with_enddate_present >> rail.Label('No') >> create_structureddata_collection >> if_timeoffreportdata_has_records
        if_timeoffreportdata_has_records >> rail.Label('Yes')  >> query_distinct_payrollid_with_timeoff_data >> query_distinct_payrollid
        if_timeoffreportdata_has_records >> rail.Label('No') >> query_distinct_payrollid >> if_distinct_payrollid_with_timeoff_data_present
        if_distinct_payrollid_with_timeoff_data_present >> rail.Label('Yes') >> trigger_child_per_payroll_id_with_timeoffdata
        trigger_child_per_payroll_id_with_timeoffdata >> wait_for_completion_of_trigger_child_per_payroll_id_with_timeoffdata
        wait_for_completion_of_trigger_child_per_payroll_id_with_timeoffdata >> if_query_distinct_payrollid_has_no_data
        if_distinct_payrollid_with_timeoff_data_present >> rail.Label('No') >> trigger_child_per_payroll_id
        trigger_child_per_payroll_id >> wait_for_completion_of_trigger_child_per_payroll_id >> if_query_distinct_payrollid_has_no_data
        if_query_distinct_payrollid_has_no_data >> rail.Label('Yes')  >> send_mail_no_data_for_business_unit >> finish
        if_query_distinct_payrollid_has_no_data >> rail.Label('No') >> send_success_mail >> finish >> log_to_sumo
        if_collection_has_data >> rail.Label('No')  >> finish
        mark_pay_run_as_complete >> rail.Label('on_error') >> catch_error >> cancel_pay_run >> finish
    return dag

rail.for_each_instance(create_dag)
