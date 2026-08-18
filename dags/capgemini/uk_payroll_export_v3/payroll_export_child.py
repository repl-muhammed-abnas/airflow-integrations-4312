from datetime import timedelta
from capgemini.uk_payroll_export_v3.utils import custom_methods, request_payload
import rail

# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.create_export_child_dag_id,
        description=f"Capgemini UK Overtime Payroll Export Create Export Child {config.instance} V3",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.payroll_export_max_active_runs,
        default_args={
            "sftp_conn_id": config.sftp_conn_id,
            'retries': 0
        }
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        

        get_cost_center_uri = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_cost_center_uri',
            endpoint="/services/CostCenterService1.svc/GetPageOfAvailableCostCentersByTextSearch",
            items= '{{ dag_run.conf.cost_centers_list | to_json }}',
            data=request_payload.get_cost_center_payload,
            data_handler=lambda response, dag_run, item: custom_methods.get_costcenter_uri(
                response, dag_run, item)
        )


        is_costcenters_present = rail.IfOperator(
            task_id='is_costcenters_present',
            test="{{ result('get_cost_center_uri') | is_truthy }}",
            yes_task='create_payrun_batch',
            no_task='finish_payroll_export'
        )

        create_payrun_batch = rail.RepliconServiceOperator(
            task_id="create_payrun_batch",
            endpoint="/services/PayRunService1.svc/CreatePayRunBatch",
            data=lambda dag_run: request_payload.get_create_payrun_batch_payload_multi_costcenter(
                dag_run, config.time_zone)
        )

        execute_payrun_batch, wait_forpayrun_batch = rail.batch_execution(
            'execute_payrun_batch', create_payrun_batch.task_id)

        get_payrun_batch_result = rail.RepliconServiceOperator(
            task_id="get_payrun_batch_result",
            endpoint="/services/PayRunService1.svc/GetCreatePayRunBatchResults",
            data={"payRunBatchUri": "{{ result('create_payrun_batch') }}"}
        )

        update_payrun_name = rail.RepliconServiceOperator(
            task_id="update_payrun_name",
            endpoint="/services/PayRunService1.svc/UpdatePayRunName",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}",
                },
                "name": "UK{{ dag_run.conf.cost_center_group_name }}_{{ dag_run.conf.exportdetails.payroll_name_suffix }}"
            }
        )

        create_payrun_download_batch = rail.RepliconServiceOperator(
            task_id="create_payrun_download_batch",
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data=request_payload.get_create_payrun_download_batch_payload
        )

        execute_payrun_download_batch, wait_for_payrun_download_batch = rail.batch_execution(
            'execute_payrun_download_batch', create_payrun_download_batch.task_id)

        get_payrun_download_batch_result = rail.RepliconServiceOperator(
            task_id="get_payrun_download_batch_result",
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={
                "payrollDownloadBatchUri": "{{ result('create_payrun_download_batch') }}"}
        )

        create_complete_payrun_status_batch = rail.RepliconServiceOperator(
            task_id='create_complete_payrun_status_batch',
            endpoint="/services/PayRunService1.svc/CreateMarkPayRunAsCompleteBatch",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}"
                }
            }
        )

        execute_complete_payrun_status_batch, wait_for_complete_payrun_status_batch = rail.batch_execution(
            'complete_payrun_status_batch', create_complete_payrun_status_batch.task_id)

        download_final_payload_file_from_url = rail.HTTPDownloadFileOperator(
            task_id="download_final_payload_file_from_url",
            url="{{ result('get_payrun_download_batch_result').downloadUrl }}"
        )

        load_final_payload_file = rail.LoadCSVFileOperator(
            task_id="load_final_payload_file",
            document="{{ result('download_final_payload_file_from_url') }}"
        )

        create_payrun_payroll_data_collection = rail.CreateCollectionOperator(
            task_id='create_payrun_payroll_data_collection',
            name='payrunpayrolldata',
            source="{{ result('load_final_payload_file') }}"
        )

        query_blank_emmployeeid_records = rail.QueryCollectionOperator(
            task_id='query_blank_emmployeeid_records',
            query="SELECT * FROM payrunpayrolldata WHERE NULLIF(Local_Employee_Number, '') IS NULL",
            name='invalid_records'
        )

        is_blank_empid_records_exists = rail.IfOperator(
            task_id='is_blank_empid_records_exists',
            test='{{ result("query_blank_emmployeeid_records", "length") > 0 }}',
            yes_task='mark_payrun_as_draft',
            no_task='process_payroll_exports'
        )

        process_payroll_exports = rail.EmptyOperator(
            task_id='process_payroll_exports'
        )

        trigger_overtime_export = rail.TriggerDagRunOperator(
            task_id='trigger_overtime_export',
            trigger_dag_id=config.overtime_export_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "exportdetails": dag_run.conf["exportdetails"],
                "payrunuri": rail.result("get_payrun_batch_result")["payRunUri"],
                "export_filename": f'UK{dag_run.conf["cost_center_group_name"]}_{dag_run.conf["exportdetails"]["overtime_export_filename_suffix"]}',
                "payrun_export_name": f'UK{dag_run.conf["cost_center_group_name"]}_{dag_run.conf["exportdetails"]["payroll_name_suffix"]}',
                "cost_center_group_name": dag_run.conf["cost_center_group_name"],
                "cost_centers_list": dag_run.conf["cost_centers_list"]
            }
        )

        trigger_oncall_export = rail.TriggerDagRunOperator(
            task_id='trigger_oncall_export',
            trigger_dag_id=config.oncall_export_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "exportdetails": dag_run.conf["exportdetails"],
                "payrunuri": rail.result("get_payrun_batch_result")["payRunUri"],
                "export_filename": f'UK{dag_run.conf["cost_center_group_name"]}_{dag_run.conf["exportdetails"]["oncall_export_filename_suffix"]}',
                "payrun_export_name": f'UK{dag_run.conf["cost_center_group_name"]}_{dag_run.conf["exportdetails"]["payroll_name_suffix"]}',
                "cost_center_group_name": dag_run.conf["cost_center_group_name"],
                "cost_centers_list": dag_run.conf["cost_centers_list"]
            }
        )

        mark_payrun_as_draft = rail.RepliconServiceOperator(
            task_id="mark_payrun_as_draft",
            endpoint="/services/PayRunService1.svc/MarkPayRunAsDraft",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}"
                }
            }
        )

        cancel_blank_empid_payrun = rail.RepliconServiceOperator(
            task_id="cancel_blank_empid_payrun",
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data=request_payload.get_payload
        )

        fail_blank_empid_payrun = rail.FailOperator(
            task_id="fail_blank_empid_payrun",
            message="Local Employee Number not present for some users. Users available to validate in payrun 'UK{{ dag_run.conf.cost_center_group_name }}_{{ dag_run.conf.exportdetails.payroll_name_suffix }}'"
        )

        finish_payroll_export = rail.EmptyOperator(
            task_id='finish_payroll_export'
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        cancel_payrun = rail.RepliconServiceOperator(
            task_id="cancel_payrun",
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data=request_payload.get_payload
        )

        fail_export = rail.FailOperator(
            task_id="fail_export",
            message="{{ get_error_message() }}"
        )

        # DYNAMIC TASK DEPENDENCIES - Set up dependencies for dynamically created URI tasks
        # All URI tasks run in parallel, then flow to collection
        get_cost_center_uri >> is_costcenters_present
        
        # Main workflow dependencies
        is_costcenters_present >> rail.Label("Yes") >> create_payrun_batch >> execute_payrun_batch \
            >> wait_forpayrun_batch >> get_payrun_batch_result
        is_costcenters_present >> rail.Label("No") >> finish_payroll_export
        
        get_payrun_batch_result >> update_payrun_name >> create_payrun_download_batch >> execute_payrun_download_batch \
            >> wait_for_payrun_download_batch >> get_payrun_download_batch_result >> create_complete_payrun_status_batch \
                >> execute_complete_payrun_status_batch
        
        wait_for_complete_payrun_status_batch >> rail.Label(
                "on_success") >> download_final_payload_file_from_url
        wait_for_complete_payrun_status_batch >> rail.Label(
            "on_error") >> on_error >> cancel_payrun >> fail_export
        
        download_final_payload_file_from_url >> load_final_payload_file >> create_payrun_payroll_data_collection

        create_payrun_payroll_data_collection >> query_blank_emmployeeid_records >> is_blank_empid_records_exists
        is_blank_empid_records_exists >> rail.Label("Yes") >> mark_payrun_as_draft >> cancel_blank_empid_payrun \
            >> fail_blank_empid_payrun
        is_blank_empid_records_exists >> rail.Label("No") >> process_payroll_exports

        process_payroll_exports >> trigger_overtime_export >> finish_payroll_export
        process_payroll_exports >> trigger_oncall_export >> finish_payroll_export

    return dag


rail.for_each_instance(create_child_dag)