from datetime import timedelta
import rail
from bearingpoint.timedata_export_v1.utils import custom_methods, response_filters
from airflow.models import Variable

OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.time_export_post_export_dag_id,
        description="Bearingpoint Time Export post payload to API endpoint",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.post_to_endpoint_max_active_run,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="query_records_to_post"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="query_records_to_post",
            end_task="finish"
        )

        query_records_to_post = rail.QueryCollectionOperator(
            task_id = "query_records_to_post",
            query="""SELECT * FROM timeexport_data
                """
        )
        
        get_all_billing_rates = rail.RepliconServiceOperator(
            task_id="get_all_billing_rates",
            endpoint="/services/BillingRateListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:billing-rate-list-column:name",
                    "urn:replicon:billing-rate-list-column:description",
                    "urn:replicon:billing-rate-list-column:enabled"
                ],
                "sort": []
            },
            data_handler=response_filters.get_billing_rates_filter
        )

        final_export_data = rail.DataAdaptorOperator(
            task_id="final_export_data",
            source="{{result('query_records_to_post')}}",
            columns=[
                'TimeEntryID',
                'ControllingArea',
                'UserCostCenter',
                'BillingRate',
                'EmployeeID',
                'WorkforceID',
                'TimeEntryDate',
                'TaskName',
                'Hours',
                'Comments',
                'UserServiceCenter',
                'WorkLocation',
                'Onsite_Remote',
                'ProjectCode',
                'BillingControlCategory',
                'ProjectCategory',
                'TimeOffTypeName',
                'TimeOffTypeDescription'
            ],
            data=custom_methods.final_export_data_callable
        )

        create_s4hc_json_payload = rail.PythonOperator(
            task_id="create_s4hc_json_payload",
            python_callable=custom_methods.create_s4hc_json_payload_callable,
            op_args=[final_export_data.task_id]
        )

        create_h4s4_json_payload = rail.PythonOperator(
            task_id="create_h4s4_json_payload",
            python_callable=custom_methods.create_h4s4_json_payload_callable,
            op_args=[final_export_data.task_id]
        )

        process_submit_to_s4hc = rail.TriggerDagRunOperator(
            task_id="process_submit_to_s4hc",
            trigger_dag_id=config.time_export_post_to_s4hc_dag_id,
            conf=lambda dag_run: {
                        "data": rail.result("create_s4hc_json_payload"),
                        'process_start_time': dag_run.conf['process_start_time'],
                        'time_export_name': dag_run.conf['time_export_name'],
                        'data_count': rail.result("create_s4hc_json_payload", "length"),
                        'export_file_time_stamp': dag_run.conf['export_file_time_stamp']
                    },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        process_submit_to_h4s4 = rail.TriggerDagRunOperator(
            task_id="process_submit_to_h4s4",
            trigger_dag_id=config.time_export_post_to_h4s4_dag_id,
            conf=lambda dag_run: {
                        "data": rail.result("create_h4s4_json_payload"),
                        'process_start_time': dag_run.conf['process_start_time'],
                        'time_export_name': dag_run.conf['time_export_name'],
                        'data_count': rail.result("create_h4s4_json_payload", "length"),
                        'export_file_time_stamp': dag_run.conf['export_file_time_stamp']
                    },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_child_run_ids = rail.PythonOperator(
            task_id = "gather_child_run_ids",
            python_callable= lambda: [rail.result('process_submit_to_h4s4'), rail.result('process_submit_to_s4hc')]
        )

        wait_submit_to_sap = rail.WaitForDagRunsSensor(
            task_id="wait_submit_to_sap",
            dag_runs= "{{ result('gather_child_run_ids') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        log_to_sumo_valid_export = rail.SendToSumoOperator(
            task_id="log_to_sumo_valid_export",
            data={
                'job_start_time': '{{ dag_run.conf.process_start_time }}',
                'job_end_time': f'{OPEN_BRACKETS} current_time_in_specified_tz("{config.time_zone}", "%Y-%m-%dT%H:%M:%S") {CLOSE_BRACKETS}',
                'export_type': '{{ dag_run.conf.time_export_run_type }}',
                'export_name': '{{ dag_run.conf.time_export_name }}',
                'twb_numberofrecords': "{{ dag_run.conf.twb_numberofrecords }}",
                'export_numberofrecords': "{{ result('final_export_data', 'length')}}"
            },
            sumo_conn_id=config.sumo_conn_id
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> finish
        can_run_batch_task >> rail.Label("No") >> query_records_to_post

        query_records_to_post >> get_all_billing_rates >> final_export_data >> create_s4hc_json_payload \
            >> create_h4s4_json_payload >> process_submit_to_s4hc >> process_submit_to_h4s4 >> gather_child_run_ids
        gather_child_run_ids >> wait_submit_to_sap >> log_to_sumo_valid_export >> finish
        
    return dag


rail.for_each_instance(create_main_dag)
