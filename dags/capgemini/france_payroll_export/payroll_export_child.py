from datetime import timedelta
from pendulum import datetime
from capgemini.france_payroll_export.utils import request_payload
import rail

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.create_payroll_extract_child_dag_id,
        description=f"Capgemini France Payroll Export Child {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2024, 11, 1, tz=config.time_zone),
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            "sftp_conn_id": config.sftp_conn_id,
            'retries': 0
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_payrun_batch = rail.RepliconServiceOperator(
            task_id="create_payrun_batch",
            endpoint="/services/PayRunService1.svc/CreatePayRunBatch",
            data=lambda dag_run: request_payload.get_create_payrun_batch_payload(dag_run, config.time_zone)
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
                "name":  "{{ dag_run.conf.exportdetails.payroll_name }}"
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

        is_payroll_data_exists = rail.IfOperator(
            task_id='is_payroll_data_exists',
            test='{{ result("create_payrun_payroll_data_collection", "length") > 0 }}',
            yes_task='query_blank_emmployeeid_records',
            no_task='process_payroll_exports'
        )

        query_blank_emmployeeid_records = rail.QueryCollectionOperator(
            task_id='query_blank_emmployeeid_records',
            query="SELECT * FROM payrunpayrolldata WHERE NULLIF(Employee_ID, '') IS NULL",
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

        trigger_sopra_export = rail.TriggerDagRunOperator(
            task_id='trigger_sopra_export',
            trigger_dag_id=config.sopra_export_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "exportdetails": dag_run.conf["exportdetails"],
                "payrunuri": rail.result("get_payrun_batch_result")["payRunUri"]
            }
        )

        trigger_gfs_export = rail.TriggerDagRunOperator(
            task_id='trigger_gfs_export',
            trigger_dag_id=config.gfs_export_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "exportdetails": dag_run.conf["exportdetails"],
                "payrunuri": rail.result("get_payrun_batch_result")["payRunUri"]
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
            data=request_payload.get_payrun_uri_payload
        )

        fail_blank_empid_payrun = rail.FailOperator(
            task_id="fail_blank_empid_payrun",
            message="Employee ID not present for some users. Users available to validate in payrun '{{ dag_run.conf.exportdetails.payroll_name }}'"
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        cancel_payrun = rail.RepliconServiceOperator(
            task_id="cancel_payrun",
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data=request_payload.get_payrun_uri_payload
        )

        fail_export = rail.FailOperator(
            task_id="fail_export",
            message="{{ get_error_message() }}"
        )

        create_payrun_batch >> execute_payrun_batch >> wait_forpayrun_batch >> get_payrun_batch_result
        get_payrun_batch_result >> update_payrun_name >> create_payrun_download_batch >> execute_payrun_download_batch \
            >> wait_for_payrun_download_batch >> get_payrun_download_batch_result >> create_complete_payrun_status_batch \
                >> execute_complete_payrun_status_batch
        wait_for_complete_payrun_status_batch >> rail.Label(
                "on_success") >> download_final_payload_file_from_url
        wait_for_complete_payrun_status_batch >> rail.Label(
            "on_error") >> on_error >> cancel_payrun >> fail_export
        download_final_payload_file_from_url >> load_final_payload_file >> create_payrun_payroll_data_collection

        create_payrun_payroll_data_collection >> is_payroll_data_exists
        is_payroll_data_exists >> rail.Label("Yes") >> query_blank_emmployeeid_records >> is_blank_empid_records_exists
        is_payroll_data_exists >> rail.Label("No") >> process_payroll_exports
        is_blank_empid_records_exists >> rail.Label("Yes") >> mark_payrun_as_draft >> cancel_blank_empid_payrun \
            >> fail_blank_empid_payrun
        is_blank_empid_records_exists >> rail.Label("No") >> process_payroll_exports

        process_payroll_exports >> trigger_sopra_export
        process_payroll_exports >> trigger_gfs_export
    return dag

rail.for_each_instance(create_child_dag)
