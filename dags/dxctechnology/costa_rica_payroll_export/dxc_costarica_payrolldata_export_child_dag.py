
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_costa_rica_payroll_export_child_{config.instance}',
        description=f'DXC_CostaRica_PayrollData_Export_Child - {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        log_start_time=rail.PythonOperator(
            task_id='log_start_time',
            python_callable= lambda:  {
                'timeforpayrunname': datetime.now().strftime('%y%m%d'),
                'starttime': datetime.now().strftime("%m/%d/%YT%H:%M:%S")
            }
        )

        log_required_filename=rail.PythonOperator(
            task_id='log_required_filename',
            python_callable= lambda:  ((datetime.now().replace(day=1)-timedelta(days=4)).replace(day=1)).strftime("%Y%m%d")
        )

        def get_divisions_uri(response):
            return{
                'CRET': rail.find_first_by_attr_and_get_attr(response,'displayText','CRET','uri',''),
                'CRES': rail.find_first_by_attr_and_get_attr(response,'displayText','CRES','uri','')
            }

        get_all_divisions = rail.RepliconServiceOperator(
            task_id = 'get_all_divisions',
            endpoint='/services/DivisionService1.svc/GetAllDivisions',
            data_handler=get_divisions_uri
        )

        def get_employeetype_uri(response):
            return {
              'contractor': rail.find_first_by_attr_and_get_attr(response,'displayText','Contractor','uri','')
            }

        get_all_employee_type_groups = rail.RepliconServiceOperator(
            task_id = 'get_all_employee_type_groups',
            endpoint='/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups',
            data_handler=get_employeetype_uri
        )

        create_payroll_download_batch=rail.RepliconServiceOperator(
            task_id='create_payroll_download_batch',
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data=lambda dag_run:{
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
                          "year": dag_run.conf['startdateyear'],
                          "month": dag_run.conf['startdatemonth'],
                          "day": dag_run.conf['startdateday']
                        },
                        "endDate": {
                          "year": dag_run.conf['enddateyear'],
                          "month": dag_run.conf['enddatemonth'],
                          "day": dag_run.conf['enddateday']
                        },
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                      },
                      "dateTimeUtc": null,
                      "dateTimeUtcRange": null,
                      "numberRange": null
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
                        "dateTimeUtcRange": null,
                        "numberRange": null
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
                        "filterDefinitionUri": "urn:replicon:pay-run-filter:employee-type-group"
                      },
                      "operatorUri": "urn:replicon:filter-operator:not-in",
                      "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                          "uri": null,
                          "uris": [
                            rail.result('get_all_employee_type_groups')['contractor']
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
                          "dateTimeUtcRange": null,
                          "numberRange": null
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
                            "uris": [
                              rail.result('get_all_divisions')['CRET'],
                              rail.result('get_all_divisions')['CRES']
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
                            "dateTimeUtcRange": null,
                            "numberRange": null
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
                            "dateTimeUtcRange": null,
                            "numberRange": null
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
              "fileFormatScriptUri": dag_run.conf['fileformaturi']
            }
        )

        execute_payroll_download_batch, wait_for_payroll_download_batch = rail.batch_execution(
            'execute_payroll_download_batch', create_payroll_download_batch.task_id)

        get_payroll_download_batch_results=rail.RepliconServiceOperator(
            task_id='get_payroll_download_batch_results',
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={
              "payrollDownloadBatchUri": "{{ result('create_payroll_download_batch') }}"
            }
        )

        if_error_in_payroll_download_batch=rail.IfOperator(
            task_id='if_error_in_payroll_download_batch',
            test='''{{ result('get_payroll_download_batch_results').error | is_truthy }}''',
            yes_task="fail_dag_with_error",
            no_task="read_file_from_results_url",
        )

        fail_dag_with_error=rail.FailOperator(
            task_id='fail_dag_with_error',
            message='''{{ result('get_payroll_download_batch_results').error }}'''
        )

        read_file_from_results_url=rail.HTTPDownloadFileOperator(
            task_id='read_file_from_results_url',
            url='''{{ result('get_payroll_download_batch_results').downloadUrl }}''',
        )

        load_csv_from_payroll_download_batch_result=rail.LoadCSVFileOperator(
            task_id="load_csv_from_payroll_download_batch_result",
            document="{{result('read_file_from_results_url')}}",
        )

        create_payrolldata_collection = rail.CreateCollectionOperator(
            task_id='create_payrolldata_collection',
            source = "{{ result('load_csv_from_payroll_download_batch_result') }}",
            name = "payrolldata",
            columns = {
              'EMPID':'EMPID', 
              'Name':'Name', 
              'Wage Code':'Wage_Code', 
              'Hours':'Quantity', 
              'Entry Date':'start_date', 
              'Actual Employee ID':'end_date'
            }
        )

        if_payrolldata_collection_has_data=rail.IfOperator(
            task_id='if_payrolldata_collection_has_data',
            test='''{{ result('create_payrolldata_collection','length') > 0}}''',
            yes_task="create_pay_run_batch",
            no_task="insert_logs",
        )

        create_pay_run_batch=rail.RepliconServiceOperator(
            task_id='create_pay_run_batch',
            endpoint="/services/PayRunService1.svc/CreatePayRunBatch",
            data=lambda dag_run:{
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
                          "year": dag_run.conf['startdateyear'],
                          "month": dag_run.conf['startdatemonth'],
                          "day": dag_run.conf['startdateday']
                        },
                        "endDate": {
                          "year": dag_run.conf['enddateyear'],
                          "month": dag_run.conf['enddatemonth'],
                          "day": dag_run.conf['enddateday']
                        },
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                      },
                      "dateTimeUtc": null,
                      "dateTimeUtcRange": null,
                      "numberRange": null
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
                        "dateTimeUtcRange": null,
                        "numberRange": null
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
                        "filterDefinitionUri": "urn:replicon:pay-run-filter:employee-type-group"
                      },
                      "operatorUri": "urn:replicon:filter-operator:not-in",
                      "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                          "uri": null,
                          "uris": [
                            rail.result('get_all_employee_type_groups')['contractor']
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
                          "dateTimeUtcRange": null,
                          "numberRange": null
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
                            "uris": [
                              rail.result('get_all_divisions')['CRET'],
                              rail.result('get_all_divisions')['CRES']
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
                            "dateTimeUtcRange": null,
                            "numberRange": null
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
                            "dateTimeUtcRange": null,
                            "numberRange": null
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
              "fileFormatScriptUri": dag_run.conf['fileformaturi']
            }
        )

        execute_payrun_batch, wait_for_payrun_batch = rail.batch_execution(
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
            yes_task="fail_job_with_error",
            no_task="update_payrun_name",
        )

        fail_job_with_error=rail.FailOperator(
            task_id='fail_job_with_error',
            message='''{{ result('get_create_pay_run_batch_results').error }}'''
        )

        update_payrun_name=rail.RepliconServiceOperator(
            task_id='update_payrun_name',
            endpoint="/services/PayRunService1.svc/UpdatePayRunName",
            data=lambda:{
              "target": {
                "uri": rail.result('get_create_pay_run_batch_results')['payRunUri'],
                "name": null
              },
              "name": rail.result('log_start_time')['timeforpayrunname'] + "_COSTA_RICA"
            }
        )

        create_payroll_downloadbatch=rail.RepliconServiceOperator(
            task_id='create_payroll_downloadbatch',
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

        execute_payroll_downloadbatch, wait_for_payroll_downloadbatch = rail.batch_execution(
            'execute_payroll_downloadbatch', create_payroll_downloadbatch.task_id)

        get_payroll_downloadbatch_results=rail.RepliconServiceOperator(
            task_id='get_payroll_downloadbatch_results',
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={
              "payrollDownloadBatchUri": "{{ result('create_payroll_downloadbatch') }}"
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

        catch_error=rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
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
            url='''{{ result('get_payroll_downloadbatch_results').downloadUrl }}''',
        )

        load_csv_for_finalpayrolldata=rail.LoadCSVFileOperator(
            task_id="load_csv_for_finalpayrolldata",
            document="{{result('read_file')}}",
        )

        create_collection_finalpayrolldata = rail.CreateCollectionOperator(
            task_id='create_collection_finalpayrolldata',
            source = "{{ result('load_csv_for_finalpayrolldata') }}",
            name = "finalpayrolldata",
            columns = {
              'EMPID':'EMPID', 
              'Name':'Name', 
              'Wage Code':'Wage_Code', 
              'Hours':'Quantity', 
              'Entry Date':'Entry_Date'
            }
        )

        query_finaldata_without_employeeid=rail.QueryCollectionOperator(
            task_id='query_finaldata_without_employeeid',
            query="SELECT * FROM  finalpayrolldata WHERE NULLIF(EMPID,'') IS NULL OR  finalpayrolldata.EMPID=''",
        )

        if_finaldata_without_employeeid_present=rail.IfOperator(
            task_id='if_finaldata_without_employeeid_present',
            test='''{{ result('query_finaldata_without_employeeid','length') > 0 }}''',
            yes_task="mark_pay_run_as_draft",
            no_task="query_data_for_required_wage_codes",
        )

        mark_pay_run_as_draft=rail.RepliconServiceOperator(
            task_id='mark_pay_run_as_draft',
            endpoint="/services/PayRunService1.svc/MarkPayRunAsDraft",
            data={
              "target": {
                "uri": "{{ result('get_create_pay_run_batch_results').payRunUri }}",
                "name": null
              }
            }
        )

        cancel_payrun=rail.RepliconServiceOperator(
            task_id='cancel_payrun',
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data={
              "target": {
                "uri": "{{ result('get_create_pay_run_batch_results').payRunUri }}",
                "name": null
              }
            }
        )

        fail_job_because_of_error=rail.FailOperator(
            task_id='fail_job_because_of_error',
            message="Employee ID not present for some users. Users available to validate in payrun {{result('log_start_time').timeforpayrunname}}_COSTA_RICA"
        )

        query_data_for_required_wage_codes=rail.QueryCollectionOperator(
            task_id='query_data_for_required_wage_codes',
            query="""SELECT * FROM finalpayrolldata WHERE finalpayrolldata.Wage_Code='BE06A' OR finalpayrolldata.Wage_Code='BE06' OR
                  finalpayrolldata.Wage_Code='BE09' OR finalpayrolldata.Wage_Code='BE21' OR finalpayrolldata.Wage_Code='BE02' OR
                  finalpayrolldata.Wage_Code='BE03' OR finalpayrolldata.Wage_Code='BE33' OR finalpayrolldata.Wage_Code='BE07' OR
                  finalpayrolldata.Wage_Code='BE08' OR finalpayrolldata.Wage_Code='BE02-N' OR finalpayrolldata.Wage_Code='BE03-N' OR
                  finalpayrolldata.Wage_Code='BE33-N' OR finalpayrolldata.Wage_Code='BE47' OR finalpayrolldata.Wage_Code='BE48' OR
                  finalpayrolldata.Wage_Code='BE49' OR finalpayrolldata.Wage_Code='DE27' OR finalpayrolldata.Wage_Code='BE04' OR
                  finalpayrolldata.Wage_Code='DE02' OR finalpayrolldata.Wage_Code='BE03a' OR finalpayrolldata.Wage_Code='BE03a-N' OR
                  finalpayrolldata.Wage_Code='BE04'""",
        )

        if_finalpayrolldata_has_records=rail.IfOperator(
            task_id='if_finalpayrolldata_has_records',
            test='''{{ result('query_data_for_required_wage_codes','length') > 0 }}''',
            yes_task="compose_csv_for_aws_s3",
            no_task="upload_csv_logs_to_sftp",
        )

        compose_csv_for_aws_s3=rail.WriteCSVFileOperator(
            task_id='compose_csv_for_aws_s3',
            source="{{ result('query_data_for_required_wage_codes') }}",
            header=['EMPID',
                    'Name',
                    'Wage Code',
                    'Quantity',
                    'Start Date',
                    'End Date'],
            row=lambda item: [
                item['EMPID'],
                ' '.join(item['Name'].split(" ")[:-2]),
                item['Wage_Code'],
                item['Quantity'],
                ((datetime.strptime(item['Entry_Date'],'%m/%d/%Y')).replace(day=1)).strftime('%m/%d/%Y'),
                ((((datetime.strptime(item['Entry_Date'],'%m/%d/%Y')).replace(day=1))+relativedelta(months=1))-timedelta(days=1)).strftime('%m/%d/%Y'),
            ],
        )

        def get_export_logs():
            return [{
                'log': "File Name : " + str(rail.result('log_required_filename'))
            },
            {
                'log': " Exported at " + str(rail.result('log_start_time')['starttime'])
            },
            {
                'log': " No of records exported : " + str(len(rail.load_all_records(rail.result('query_data_for_required_wage_codes'))))
            },
            {
                'log': " Status: Success"
            }]

        add_to_log_list=rail.PythonOperator(
            task_id='add_to_log_list',
            python_callable= get_export_logs
        )

        compose_csv_to_upload_on_sftp=rail.WriteCSVFileOperator(
            task_id='compose_csv_to_upload_on_sftp',
            source=lambda: rail.result('add_to_log_list'),
            header=['Log file'],
            row= [
                "{{ item.log }}"
            ],
        )

        encrypt_file= rail.PGPEncryptionOperator(
            task_id='encrypt_file',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('compose_csv_for_aws_s3') }}",
        )

        if_finalpayrolldata_has_data=rail.IfOperator(
            task_id='if_finalpayrolldata_has_data',
            test='''{{ result('create_collection_finalpayrolldata','length') > 0 }}''',
            yes_task="upload_encrypted_file_to_sftp",
            no_task="upload_csv_logs_to_sftp",
        )

        upload_encrypted_file_to_sftp=rail.SFTPUploadFileOperator(
            task_id='upload_encrypted_file_to_sftp',
            content='''{{result("encrypt_file")}}''',
            remote_filepath= config.upload_filepath + "{{result('log_required_filename')}}.csv.pgp",
        )

        upload_csv_logs_to_sftp=rail.SFTPUploadFileOperator(
            task_id='upload_csv_logs_to_sftp',
            content='''{{ result('compose_csv_to_upload_on_sftp') }}''',
            remote_filepath=config.upload_filepath + "Log_{{ result('log_required_filename') }}.csv",
        )

        send_success_mail_after_export_completion=rail.EmailOperator(
            task_id='send_success_mail_after_export_completion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }}| Replicon payroll export for Costa Rica completed successfully {{ result('log_start_time').starttime }}''',
            html_content= '''templates/success_mail.html''',
            params={
                'file_upload_path': config.upload_filepath,
                'log_upload_path': config.upload_filepath
            },
        )

        def get_logs():
            return[{
                'log': 'No Records Found'
            },{
                'log': " TimeStamp: " + str(rail.result('log_start_time')['starttime'])
            },{
                'log': "Status:Success"
            }]

        insert_logs=rail.PythonOperator(
            task_id='insert_logs',
            python_callable=get_logs
        )

        compose_csv_for_no_records=rail.WriteCSVFileOperator(
            task_id='compose_csv_for_no_records',
            source=lambda: rail.result('insert_logs'),
            header=['Log file'],
            row= [
                "{{ item.log }}"
            ]
        )

        upload_no_records_csv_to_sftp=rail.SFTPUploadFileOperator(
            task_id='upload_no_records_csv_to_sftp',
            content='''{{ result('compose_csv_for_no_records') }}''',
            remote_filepath=config.upload_filepath + '''Log_{{ result('log_required_filename') }}.csv''',
        )

        send_mail_for_no_records=rail.EmailOperator(
            task_id='send_mail_for_no_records',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }}| No Records Found Replicon payroll export for Costa Rica  {{ result('log_start_time').starttime }} ''',
            html_content= '''templates/no_records_mail.html''',
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_start_time >> log_required_filename >> get_all_divisions >> get_all_employee_type_groups
        get_all_employee_type_groups >> create_payroll_download_batch >> execute_payroll_download_batch
        execute_payroll_download_batch >> wait_for_payroll_download_batch >> get_payroll_download_batch_results >> if_error_in_payroll_download_batch
        if_error_in_payroll_download_batch >> rail.Label('Yes')  >> fail_dag_with_error >> finish
        if_error_in_payroll_download_batch >> rail.Label('No') >> read_file_from_results_url >> load_csv_from_payroll_download_batch_result
        load_csv_from_payroll_download_batch_result >> create_payrolldata_collection >> if_payrolldata_collection_has_data
        if_payrolldata_collection_has_data >> rail.Label(
            'Yes')  >> create_pay_run_batch >> execute_payrun_batch >> wait_for_payrun_batch >> get_create_pay_run_batch_results >> if_error_in_payrun_batch
        if_error_in_payrun_batch >> rail.Label('Yes')  >> fail_job_with_error >> finish
        if_error_in_payrun_batch >> rail.Label('No') >> update_payrun_name >> create_payroll_downloadbatch >> execute_payroll_downloadbatch
        execute_payroll_downloadbatch >> wait_for_payroll_downloadbatch >> get_payroll_downloadbatch_results >> mark_pay_run_as_complete
        mark_pay_run_as_complete >> rail.Label('on_success') >> read_file >> load_csv_for_finalpayrolldata >> create_collection_finalpayrolldata
        create_collection_finalpayrolldata >> query_finaldata_without_employeeid >> if_finaldata_without_employeeid_present
        if_finaldata_without_employeeid_present >> rail.Label('Yes')  >> mark_pay_run_as_draft >> cancel_payrun >> fail_job_because_of_error >> finish
        if_finaldata_without_employeeid_present >> rail.Label('No') >> query_data_for_required_wage_codes >> if_finalpayrolldata_has_records
        if_finalpayrolldata_has_records >> rail.Label('Yes') >> compose_csv_for_aws_s3 >> add_to_log_list >> compose_csv_to_upload_on_sftp
        compose_csv_to_upload_on_sftp >> encrypt_file >> if_finalpayrolldata_has_data
        if_finalpayrolldata_has_data >> rail.Label('Yes')  >> upload_encrypted_file_to_sftp >> upload_csv_logs_to_sftp
        if_finalpayrolldata_has_data >> rail.Label('No') >> upload_csv_logs_to_sftp
        if_finalpayrolldata_has_records >> rail.Label('No') >> upload_csv_logs_to_sftp >> send_success_mail_after_export_completion >> finish
        if_payrolldata_collection_has_data >> rail.Label('No') >> insert_logs >> compose_csv_for_no_records >> upload_no_records_csv_to_sftp
        upload_no_records_csv_to_sftp >> send_mail_for_no_records >> finish
        mark_pay_run_as_complete >> rail.Label('on_success') >> catch_error >> cancel_pay_run >> finish
    return dag

rail.for_each_instance(create_dag)
