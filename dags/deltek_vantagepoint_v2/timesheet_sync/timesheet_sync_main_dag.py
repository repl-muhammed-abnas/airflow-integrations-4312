from datetime import datetime, timedelta
from airflow.models import Variable
from pendulum import datetime as dt
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.timesheet_sync_main_dag_id,
        description=f'{config.company_key} Syncs the time data from Replicon Time WorkBench to Vantagepoint as timesheets',
        start_date=dt(2025, 1, 1, tz=config.time_zone),
        company_key=config.company_key,
        max_active_runs=config.max_active_runs,
        multi_tenant=True
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
            end_task='should_log_history',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        time_export_download_script = rail.RepliconServiceOperator(
            task_id='time_export_download_script',
            endpoint='/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', config.replicon_export_file_format_name, 'uri'),
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
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
            time_export_download_script.task_id + '\')',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        get_user_oef_definitions = rail.RepliconServiceOperator(
            task_id='get_user_oef_definitions',
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                "bindingContextUri": "urn:replicon:object-type:user"
            }
        )

        def is_budget_labor_code_enabled():
            raw = getattr(config, 'enable_budget_labor_codes_level', False)
            if isinstance(raw, str):
                raw = raw.strip().lower() == 'true'
            return bool(raw) and getattr(config, 'budget_labor_codes_level', '') in ('Task', 'TimesheetFields')

        def build_laborcodelevels(dag_run):
            if is_budget_labor_code_enabled():
                return []
            initial_cs = dag_run.conf.get('initial_custom_settings', {})
            if 'M' in initial_cs and isinstance(initial_cs.get('M'), dict):
                initial_cs = initial_cs['M']
            labor_code_setting = initial_cs.get('laborCodeSetting', {}) or {}
            if labor_code_setting.get('configureLaborCode', False):
                levels = labor_code_setting.get('levels', [])
                if levels:
                    return [str(name).replace(' ', '_') + '__Code_' for name in levels]
            return config.laborcodelevels

        get_dynamic_laborcodelevels = rail.PythonOperator(
            task_id='get_dynamic_laborcodelevels',
            python_callable=build_laborcodelevels
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
            no_task='should_log_history'
        )

        query_distinct_companies = rail.QueryCollectionOperator(
            task_id='query_distinct_companies',
            query=f'SELECT DISTINCT {config.department_name} from all_data',
        )

        download_users_timecategory_values = rail.S3DownloadFileOperator(
            task_id='download_users_timecategory_values',
            key_name=config.s3_upload_filepath + '{{ dag_run.conf.company_key }}' + config.timecategory_file_name,
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
            trigger_dag_id=config.timesheet_per_company_dag_id,
            conf=lambda dag_run, item: {
                'company': item[config.department_name],
                'export_time': (rail.result('get_exportname').split('Time_Data_Export_Vantagepoint_'))[-1],
                'company_key': dag_run.conf['company_key'],
                'vantagepoint_conn_id': dag_run.conf['vantagepoint_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id'],
                'laborcodelevels': rail.result('get_dynamic_laborcodelevels')
            }
        )

        wait_for_processing_employees_foreach_company = rail.WaitForDagRunsSensor(
            task_id='wait_for_processing_employees_foreach_company',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_employees_timedata_foreach_company") }}'
        )

        gather_child_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_dag_errors',
            dag_runs="{{ [result('process_employees_timedata_foreach_company')] }}",
            dagrun_task_id='catch_error',
            flatten=True
        )

        is_time_entry_error = rail.IfOperator(
            task_id='is_time_entry_error',
            test="{{ (get_task_state('gather_child_dag_errors') == 'success' and result('gather_child_dag_errors') | length > 0) }}",
            yes_task='fail_time_entry_error',
            no_task='should_log_history'
        )

        fail_time_entry_error = rail.FailOperator(
            task_id='fail_time_entry_error',
            message="{{ result('gather_child_dag_errors') | map_to_attr('error') | join('\n') }}"
        )

        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ result('if_data_present') == 'query_distinct_companies' }}",
            trigger_rule='all_done',
            yes_task='log_dagrun_details_to_table',
            no_task='delete_this_dagrun'
        )

        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id='log_dagrun_details_to_table',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.hmac_secret
            },
            company_key='{{ dag_run.conf.company_key }}',
            connector_name=config.provider,
            integration_type=config.workflow
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task

        can_run_batch_task >> rail.Label(
            'No') >> time_export_download_script >> get_exportname >> export_time_data[0]
        export_time_data[1] >> get_user_oef_definitions >> get_dynamic_laborcodelevels >> create_timedata_collection >> if_data_present
        if_data_present >> rail.Label('No') >> should_log_history
        if_data_present >> rail.Label(
            'Yes') >> query_distinct_companies >> download_users_timecategory_values
        download_users_timecategory_values >> load_timecategory_values_csv
        load_timecategory_values_csv >> create_timecategories_collection >> process_employees_timedata_foreach_company
        process_employees_timedata_foreach_company >> wait_for_processing_employees_foreach_company >> gather_child_dag_errors >> is_time_entry_error
        is_time_entry_error >> rail.Label('Yes') >> fail_time_entry_error >> should_log_history
        is_time_entry_error >> rail.Label('No') >> should_log_history
        should_log_history >> rail.Label('Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label('No') >> delete_this_dagrun

        batch_task >> should_log_history

        return dag


rail.for_each_instance(create_dag)
