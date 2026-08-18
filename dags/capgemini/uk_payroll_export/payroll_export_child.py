from datetime import timedelta
from capgemini.uk_payroll_export.utils import custom_methods, request_payload
import rail

# pylint: disable=too-many-statements


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.create_export_child_dag_id,
        description=f"Capgemini UK Overtime Payroll Export Create Export Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.payroll_export_max_active_runs,
        default_args={
            "sftp_conn_id": config.sftp_conn_id,
            'retries': 0
        }
    ) as dag:

        get_cost_center_uri = rail.RepliconServiceOperator(
            task_id='get_cost_center_uri',
            endpoint="/services/CostCenterService1.svc/GetPageOfAvailableCostCentersByTextSearch",
            data=request_payload.get_parent_cost_center_payload,
            data_handler=lambda response, dag_run: custom_methods.get_parent_costcenter_uri(
                response, dag_run)
        )

        is_costcenter_present = rail.IfOperator(
            task_id='is_costcenter_present',
            test="{{ result('get_cost_center_uri') | is_truthy }}",
            yes_task='get_costcenter_details',
            no_task='finish_payroll_export'
        )

        get_costcenter_details = rail.RepliconServiceOperator(
            task_id='get_costcenter_details',
            endpoint='services/costcenterservice1.svc/GetCostCenterDetails',
            data=lambda: {
                "costCenterUri": rail.result('get_cost_center_uri')
            }
        )

        create_payrun_batch = rail.RepliconServiceOperator(
            task_id="create_payrun_batch",
            endpoint="/services/PayRunService1.svc/CreatePayRunBatch",
            data=lambda dag_run: request_payload.get_create_payrun_batch_payload(
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
                "name": "{{ result('get_costcenter_details').code }}_{{ dag_run.conf.exportdetails.payroll_name_suffix }}"
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

        mark_payrun_as_complete = rail.RepliconServiceOperator(
            task_id="mark_payrun_as_complete",
            endpoint="/services/PayRunService1.svc/MarkPayRunAsComplete",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}"
                }
            }
        )

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
                "export_filename": f'{rail.result("get_costcenter_details")["code"]}_{dag_run.conf["exportdetails"]["overtime_export_filename_suffix"]}',
                "payrun_export_name": f'{rail.result("get_costcenter_details")["code"]}_{dag_run.conf["exportdetails"]["payroll_name_suffix"]}',
                "cost_center_name": dag_run.conf["cost_center_name"]
            }
        )

        trigger_oncall_export = rail.TriggerDagRunOperator(
            task_id='trigger_oncall_export',
            trigger_dag_id=config.oncall_export_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "exportdetails": dag_run.conf["exportdetails"],
                "payrunuri": rail.result("get_payrun_batch_result")["payRunUri"],
                "export_filename": f'{rail.result("get_costcenter_details")["code"]}_{dag_run.conf["exportdetails"]["oncall_export_filename_suffix"]}',
                "payrun_export_name": f'{rail.result("get_costcenter_details")["code"]}_{dag_run.conf["exportdetails"]["payroll_name_suffix"]}',
                "cost_center_name": dag_run.conf["cost_center_name"]
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
            message="Employee ID not present for some users. Users available to validate in payrun '{{ dag_run.conf.exportdetails.payroll_name_suffix }}'"
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

        get_cost_center_uri >> is_costcenter_present
        is_costcenter_present >> rail.Label("Yes") >> get_costcenter_details >> create_payrun_batch >> execute_payrun_batch \
            >> wait_forpayrun_batch >> get_payrun_batch_result
        is_costcenter_present >> rail.Label("No") >> finish_payroll_export
        get_payrun_batch_result >> update_payrun_name >> create_payrun_download_batch >> execute_payrun_download_batch \
            >> wait_for_payrun_download_batch >> get_payrun_download_batch_result >> mark_payrun_as_complete >> rail.Label(
                "on_success") >> download_final_payload_file_from_url
        mark_payrun_as_complete >> rail.Label(
            "on_error") >> on_error >> cancel_payrun >> fail_export
        download_final_payload_file_from_url >> load_final_payload_file >> create_payrun_payroll_data_collection

        create_payrun_payroll_data_collection >> query_blank_emmployeeid_records >> is_blank_empid_records_exists
        is_blank_empid_records_exists >> rail.Label("Yes") >> mark_payrun_as_draft >> cancel_blank_empid_payrun \
            >> fail_blank_empid_payrun
        is_blank_empid_records_exists >> rail.Label(
            "No") >> process_payroll_exports

        process_payroll_exports >> trigger_overtime_export >> finish_payroll_export
        process_payroll_exports >> trigger_oncall_export >> finish_payroll_export

    return dag


rail.for_each_instance(create_main_dag)
