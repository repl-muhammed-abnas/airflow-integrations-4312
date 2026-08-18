from datetime import timedelta
import rail
from galaxyusopcoinc.workday_user_sync.time_off_plan_v1.utils import request_payload
from galaxyusopcoinc.workday_user_sync.time_off_plan_v1.utils import custom_method


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_generic_key_value_mapper_dag_id,
        description=f'Vialto Partners Update User Sync Mapper Child V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.assign_newly_created_timeoff_type_max_active_run
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_all_time_off_type_created_country = rail.QueryCollectionOperator(
            task_id="get_all_time_off_type_created_country",
            query="SELECT * FROM query_create_timeoff WHERE Country = :country",
            query_params={
                "country": "{{dag_run.conf.country}}"
            }
        )

        get_all_created_time_off = rail.PythonOperator(
            task_id="get_all_created_time_off",
            python_callable=lambda: custom_method.get_data_from_document(
                 rail.result("get_all_time_off_type_created_country")),
        )

        get_key_value_country = rail.RepliconServiceOperator(
            task_id='get_key_value_country',
            endpoint='/services/GenericKeyValueStoreService1.svc/GetKeyValue',
            data={
                    "keyNamespace": config.mapper_name,
                    "key": "{{ dag_run.conf.country}}"
            }
        )

        is_country_present = rail.IfOperator(
            task_id="is_country_present",
            test=lambda: bool(rail.result('get_key_value_country')),
            yes_task='put_key_value_country',
            no_task='add_country_to_mapper',
        )

        add_country_to_mapper = rail.RepliconServiceOperator(
            task_id= "add_country_to_mapper",
            endpoint='/services/GenericKeyValueStoreService1.svc/PutKeyValue',
            data=lambda dag_run: request_payload.get_put_key_value_payload_add(dag_run,
                                                                           config)
        )

        put_key_value_country = rail.RepliconServiceOperator(
            task_id='put_key_value_country',
            endpoint='/services/GenericKeyValueStoreService1.svc/PutKeyValue',
            data=lambda dag_run: request_payload.get_put_key_value_payload(dag_run,
                                                                           config)
        )

        get_user_data_based_country_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_data_based_country_report_details',
            report_name=config.extract_report_name,
        )

        # pylint: disable=consider-using-f-string
        filter_uri_expr = "{{ (result('get_user_data_based_country_report_details').filterConfiguration.enabledFilters | " + \
            "filter_by_attr('displayText', 'equals', '%s') | first()).uri }}" % config.report_filter_name

        report_group_entry, report_group_exit = rail.run_report(
            group_id='vialto_partners_users_by_country',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_user_data_based_country_report_details').uri }}",
                        "filterValues": [{"reportFilterUri": filter_uri_expr, "value": "{{ dag_run.conf.country }}"}],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test=lambda: rail.result('vialto_partners_users_by_country.get_report_result')[
                'reportGenerationResults'][0]['payload'] != 'No Data\r\n',
            yes_task='load_report_data',
            no_task='log_no_user_found_for_country',
        )

        log_no_user_found_for_country = rail.WriteLogOperator(
            task_id='log_no_user_found_for_country',
            message='No Active User found for country in Replicon',
            severity='Exception',
            properties={
                'time_off_type_desc': "",
                'time_off_type_name': "",
                'unit_of_time': "",
                'country': "{{ dag_run.conf.country}}",
                'status': 'Exception'
            }
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('vialto_partners_users_by_country.get_report_result').reportGenerationResults[0].payload }}",
        )

        get_all_user_uri_matched_country = rail.PythonOperator(
            task_id="get_all_user_uri_matched_country",
            python_callable=custom_method.active_user,
            op_args=['load_report_data']
        )

        process_each_user_time_off = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_user_time_off',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result('get_all_user_uri_matched_country'),
            trigger_dag_id=config.update_user_timeoff_dag_id,
            conf=request_payload.get_process_each_user_record_conf
        )

        wait_for_process_each_user_time_off = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_user_time_off',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_user_time_off") }}',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'time_off_type_desc': "NA",
                'time_off_type_name': "NA",
                'unit_of_time': "NA",
                'country': "{{ dag_run.conf.country}}",
                'status': 'Error',
                'message': '{{ get_error_message() }}',

            },
        )

        get_all_time_off_type_created_country >> get_all_created_time_off >> get_key_value_country
        get_key_value_country >> is_country_present >> rail.Label('Yes') >> put_key_value_country >> get_user_data_based_country_report_details
        is_country_present >> rail.Label('No') >> add_country_to_mapper >> get_user_data_based_country_report_details
        get_user_data_based_country_report_details >> report_group_entry
        report_group_exit >> report_has_data >> rail.Label(
            "YES") >> load_report_data >> get_all_user_uri_matched_country
        report_has_data >> rail.Label(
            'NO') >> log_no_user_found_for_country
        get_all_user_uri_matched_country >> process_each_user_time_off >> wait_for_process_each_user_time_off >> catch_and_log_errors

        return dag


rail.for_each_instance(create_child_dag)
