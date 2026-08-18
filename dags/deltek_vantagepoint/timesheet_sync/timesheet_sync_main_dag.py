from datetime import datetime, timedelta
from airflow.models import Variable
from pendulum import datetime as dt
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_vantagepoint_timesheet_sync_main_{config.instance}',
        description='Syncs the time data from Replicon Time WorkBench to Vantagepoint as timesheets',
        start_date=dt(2025, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'vp_conn_id': config.deltek_vantagepoint_conn_id
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='time_export_download_script'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='time_export_download_script',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        time_export_download_script = rail.RepliconServiceOperator(
            task_id='time_export_download_script',
            endpoint='/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', config.replicon_export_file_format_name, 'uri')
        )

        get_exportname = rail.PythonOperator(
            task_id = 'get_exportname',
            python_callable= lambda: 'Time_Data_Export_Vantagepoint_' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )

        def get_export_request():
            return {
                "columnUris": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": {
                                "filterDefinitionUri": "urn:replicon:time-data-export-filter:entry-date-range"
                            },
                            "operatorUri": "urn:replicon:filter-operator:in",
                            "rightExpression": {
                                "value": {
                                    "dateRange": {
                                        "startDate": rail.get_replicon_date(datetime.now() - timedelta(days=config.lookback_days)),
                                        "endDate": rail.get_replicon_date(datetime.now() + timedelta(days=config.lookahead_days))
                                    }
                                }
                            }
                        },
                        "operatorUri": "urn:replicon:filter-operator:and",
                        "rightExpression": {
                            "leftExpression": {
                                "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export-status"
                            },
                            "operatorUri": "urn:replicon:filter-operator:in",
                            "rightExpression": {
                                "value": {
                                    "uris": [
                                        "urn:replicon:time-data-item-time-data-export-status:" + config.export_filter_export_status
                                    ]
                                }
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "leftExpression": {
                                "filterDefinitionUri": "urn:replicon:time-data-export-filter:timesheet-only-approval-status"
                            },
                            "operatorUri": "urn:replicon:filter-operator:in",
                            "rightExpression": {
                                "value": {
                                    "uris": [
                                        "urn:replicon:approval-status:" + config.export_filter_timesheet_status
                                    ]
                                }
                            }
                        },
                        "operatorUri": "urn:replicon:filter-operator:and",
                        "rightExpression": {
                            "leftExpression": {
                                "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-type"
                            },
                            "operatorUri": "urn:replicon:filter-operator:in",
                            "rightExpression": {
                                "value": {
                                    "uris": [
                                        f"urn:replicon:time-entry-type:{entry_type}"
                                        for entry_type in config.export_filter_time_entry_types.split(',')
                                    ]
                                }
                            }
                        }
                    }
                }
            }

        export_time_data = rail.time_data_export(
            group_id='time_data_export',
            get_export_name="{{result('get_exportname')}}",
            generate_request=get_export_request,
            file_script_uri='result(\'' +
            time_export_download_script.task_id + '\')'
        )

        create_timedata_collection = rail.CreateCollectionOperator(
            task_id='create_timedata_collection',
            source='{{result(\'' + export_time_data[1].task_id + '\')}}',
            name='all_data'
        )

        if_data_present = rail.IfOperator(
            task_id='if_data_present',
            test=lambda: rail.result('create_timedata_collection', 'length') > 0,
            yes_task='query_distinct_companies',
            no_task='log_to_sumo'
        )

        query_distinct_companies = rail.QueryCollectionOperator(
            task_id='query_distinct_companies',
            query=f'SELECT DISTINCT {config.department_name} from all_data',
        )

        download_users_timecategory_values = rail.S3DownloadFileOperator(
            task_id='download_users_timecategory_values',
            key_name=config.s3_upload_filepath + config.company_key + config.timecategory_file_name,
            bucket_name=config.bucket_name,
            aws_conn_id=config.aws_conn_id
        )

        load_timecategory_values_csv = rail.LoadCSVFileOperator(
            task_id = 'load_timecategory_values_csv',
            document = "{{result('download_users_timecategory_values')}}"
        )

        create_timecategories_collection = rail.CreateCollectionOperator(
            task_id = 'create_timecategories_collection',
            name = 'users_timecategory_values',
            source = '{{result("load_timecategory_values_csv")}}'
        )

        process_employees_timedata_foreach_company = rail.TriggerDagRunForEachItemOperator(
            task_id='process_employees_timedata_foreach_company',
            items="{{result('query_distinct_companies')}}",
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_vantagepoint_timesheet_sync_foreach_company_child_{config.instance}',
            conf=lambda item: {
                'company': item[config.department_name],
                'export_time': (rail.result('get_exportname').split('Time_Data_Export_Vantagepoint_'))[-1]
            }
        )

        wait_for_processing_employees_foreach_company = rail.WaitForDagRunsSensor(
            task_id='wait_for_processing_employees_foreach_company',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_employees_timedata_foreach_company") }}'
        )

        search_logs = rail.FilterLogEntriesOperator(
            task_id='search_logs',
            severity='Error/Exception'
        )

        if_logs_present = rail.IfOperator(
            task_id='if_logs_present',
            test=lambda: rail.result('search_logs', 'length') > 0,
            yes_task='write_logs_to_csv',
            no_task='log_to_sumo'
        )

        write_logs_to_csv = rail.WriteCSVFileOperator(
            task_id='write_logs_to_csv',
            source='{{result("search_logs")}}',
            header=["Employee",
                    "Status",
                    "Details",
                    "Vantagepoint Batch",
                    "Ecid"
                    ],
            row=[
                '{{item.properties| attr_or_default("loginname","")}}',
                '{{item.properties| attr_or_default("status","")}}',
                '{{item.properties| attr_or_default("details","")}}',
                '{{item.properties| attr_or_default("batch","")}}',
                '{{item| attr_or_default("ecid","")}}'
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('write_logs_to_csv')}}",
            output_file_name='TimesheetSyncLogs - {{ current_time() }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_email = rail.EmailOperator(
            task_id='send_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Deltek Vantagepoint Timesheet Sync Completed with Errors/Exceptions - {{ current_time() }}''',
            html_content="templates/failure_email.html",
            params=None,
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> time_export_download_script >> get_exportname >> export_time_data[0]
        export_time_data[1] >> create_timedata_collection >> if_data_present
        if_data_present >> rail.Label('No') >> log_to_sumo
        if_data_present >> rail.Label(
            'Yes') >> query_distinct_companies >> download_users_timecategory_values >> load_timecategory_values_csv
        load_timecategory_values_csv >> create_timecategories_collection >> process_employees_timedata_foreach_company
        process_employees_timedata_foreach_company >> wait_for_processing_employees_foreach_company >> search_logs
        search_logs >> if_logs_present
        if_logs_present >> rail.Label(
            'Yes') >> write_logs_to_csv >> generate_download_link >> send_email >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo
        return dag


rail.for_each_instance(create_dag)
