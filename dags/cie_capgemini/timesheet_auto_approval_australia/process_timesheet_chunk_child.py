from datetime import timedelta
import rail
from cie_capgemini.timesheet_auto_approval_australia.utils import request_payload, python_callable


def create_child_dag_wbs(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_{config.country}_process_timesheet_chunk_child{dag_id_postfix}'.lower(),
        description=f'{dag_id_prefix}process_timesheet_chunk_child{dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=3,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id= "view_dagrun_config")

        # pylint: disable=too-many-function-args
        get_latest_recalc_ts_uris = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_latest_recalc_ts_uris',
            endpoint="/services/TimesheetService1.svc/GetTimesheetScriptCalculationStatusDetails",
            items="{{ dag_run.conf.item | to_json }}",
            flatten=True,
            data=lambda item: {
                "timesheet": {
                    "uri": item
                }
            },
            data_handler=lambda data: python_callable.check_reclac_status(config, data),
            all_result_data_handler=lambda data_handler: list(
                filter(lambda item: item, data_handler)
                ),

        )

        get_bulk_validation = rail.RepliconServiceOperator(
            task_id="get_bulk_validation",
            endpoint="/services/TimesheetService1.svc/BulkGetMostRecentValidationResults",
            data=lambda: {
                "timesheetUris": rail.result("get_latest_recalc_ts_uris")
            },
        )
        
        filter_validation_uris = rail.PythonOperator(
            task_id='filter_validation_uris',
            python_callable=python_callable.filter_error_uris,
            op_args=[config]
        )

        create_timesheet_batch = rail.RepliconServiceOperator(
            task_id='create_timesheet_batch',
            endpoint='/services/TimesheetApprovalService1.svc/CreateForcedApproveBatch',
            data=lambda: {
                "timesheetUris": rail.result("filter_validation_uris"),
                "comments": config.timesheet_approve_remarks
            },
            response_filter=lambda response: response.json()['d']
        )

        approve_timesheet = rail.RepliconServiceOperator(
            task_id="approve_timesheet",
            endpoint="/services/TimesheetApprovalService1.svc/ExecuteTimesheetApprovalBatch2",
            data=request_payload.execute_batch_timesheet_data(
                "{{ result('create_timesheet_batch') }}"),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        create_timesheet_batch_retries = rail.RepliconServiceOperator(
            task_id='create_timesheet_batch_retries',
            trigger_rule='one_failed',
            endpoint='/services/TimesheetApprovalService1.svc/CreateForcedApproveBatch',
            data=lambda: {
                "timesheetUris": rail.result("get_latest_recalc_ts_uris"),
                "comments": config.timesheet_approve_remarks
            },
            response_filter=lambda response: response.json()['d']
        )

        approve_timesheet_retries = rail.RepliconServiceOperator(
            task_id="approve_timesheet_retries",
            endpoint="/services/TimesheetApprovalService1.svc/ExecuteTimesheetApprovalBatch2",
            data=request_payload.execute_batch_timesheet_data(
                "{{ result('create_timesheet_batch_retries') }}"),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )
        check_for_log = rail.IfOperator(
            task_id='check_for_log',
            trigger_rule='all_done',
            test=python_callable.task_state,
            yes_task='write_logs_for_success',
            no_task='write_logs_for_failure'
        )
        write_logs_for_success = rail.WriteLogOperator(
            task_id='write_logs_for_success',
            log="{{ dag_run.conf.logid }}",
            message="na",
            severity="success",
            properties={
                "timesheet_batch": "{{ dag_run.conf.item }}",
                "status": "success",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        write_logs_for_failure = rail.WriteLogOperator(
            task_id='write_logs_for_failure',
            log="{{ dag_run.conf.logid }}",
            message="na",
            severity="failed",
            properties={
                "timesheet_batch":  "{{ dag_run.conf.item }}",
                "status": "failed",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        get_latest_recalc_ts_uris >> get_bulk_validation >> filter_validation_uris >> create_timesheet_batch >> approve_timesheet >> create_timesheet_batch_retries >> approve_timesheet_retries >> check_for_log
        check_for_log >> rail.Label(
            'No') >> write_logs_for_failure
        check_for_log >> rail.Label(
            'Yes') >> write_logs_for_success
        [write_logs_for_success, write_logs_for_failure] >> finish

    return dag


rail.for_each_instance(create_child_dag_wbs)
