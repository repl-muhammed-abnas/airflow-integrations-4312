from datetime import timedelta
from pendulum import datetime
from capgemini.leave_data_extract_global_row_v5_leave_balance.utils import custom_methods, request_payload, response_filter
from airflow.models import Variable
import rail


def create_dag(config):
    # Create one DAG per region
    for region_config in config.REGION_COUNTRY_MAPPER:
        region_code = region_config['region_code']
        schedule_interval = region_config.get('schedule_interval')

        dag_id = f"{config.leave_balance_export_master_dag_id}_{region_code.lower()}"

        with rail.create_airflow_dag(
            dag_id=dag_id,
            description=f'Capgemini Leave Data Export Global Master {config.leave_status} {config.instance} - {region_config["region"]}',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            start_date=datetime(2026, 3, 23),
            schedule_interval=schedule_interval,
            max_active_runs=config.max_active_runs,
            default_args={'sftp_conn_id': config.sftp_conn_id},
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='logging_details'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(days=config.execution_timeout_days),
                start_task='logging_details',
                end_task='batch_end',
            )

            logging_details = rail.PythonOperator(
                task_id='logging_details',
                python_callable=custom_methods.get_logging_details,
                op_args=[config.time_zone]
            )

            get_location_hierarchy = rail.RepliconServicePageOperator(
                task_id='get_location_hierarchy',
                endpoint="/services/LocationListService1.svc/GetHierarchyData",
                data=request_payload.get_all_location_hierarchy_data,
                page_handler=request_payload.page_handler,
                all_result_data_handler=lambda response, rc=region_config: response_filter.build_location_hierarchy_map(
                    response, [rc]
                )
            )

            prepare_export_items = rail.PythonOperator(
                task_id='prepare_export_items',
                python_callable=response_filter.prepare_export_items,
                op_args=[[region_config], config.DEFAULT_MAX_BATCH_COUNT]
            )

            trigger_child_for_each_country = rail.TriggerDagRunForEachItemOperator(
                task_id='trigger_child_for_each_country',
                trigger_dag_id=lambda item: f"{config.leave_balance_export_child_dag_id}_batch_{item['batch_num']}",
                items='{{ result("prepare_export_items") | to_json }}',
                conf=lambda item: {
                    "region": item['region'],
                    "region_code": item['region_code'],
                    "country_code": item['country_code'],
                    "country_list": item['country_list'],
                    "filename": custom_methods.get_file_name(item['filename_format']),
                    "location_hierarchy_artifact": rail.result("get_location_hierarchy"),
                    "logging_details": rail.result("logging_details")
                },
                execution_timeout=timedelta(days=config.execution_timeout_days)
            )

            batch_end = rail.EmptyOperator(task_id='batch_end')

            can_run_batch_task >> rail.Label("Yes") >> batch_task >> batch_end
            can_run_batch_task >> rail.Label("No") >> logging_details

            logging_details >> get_location_hierarchy >> prepare_export_items >> trigger_child_for_each_country >> batch_end


rail.for_each_instance(create_dag)
