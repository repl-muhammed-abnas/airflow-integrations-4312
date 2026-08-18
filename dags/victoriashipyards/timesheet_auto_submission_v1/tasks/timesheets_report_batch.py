import rail
from victoriashipyards.timesheet_auto_submission_v1.utils import request_payload


def report_batch(config):
    with rail.TaskGroup(group_id='timesheets_report_batch', prefix_group_id=False):

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.timesheet_report,
        )

        create_report_generation_batch = rail.RepliconServiceOperator(
            task_id='create_report_generation_batch',
            endpoint='/services/reportService1.svc/CreateReportGenerationBatch',
            data=request_payload.get_report_generate_batch_payload
        )

        process_batch_request = rail.batch_execution(
            group_id='process_batch_request',
            creation_task_id=create_report_generation_batch.task_id,
            replicon_conn_id=config.replicon_conn_id
        )

        get_batch_status = rail.RepliconServiceOperator(
            task_id='get_batch_status',
            endpoint='/services/reportService1.svc/GetBatchStatus',
            data={
                "batchUri": "{{ result('create_report_generation_batch') }}"
            },
        )

        check_execution_state = rail.IfOperator(
            task_id='check_execution_state',
            test='{{ result("get_batch_status")["executionState"] | matches("succeeded") }}',
            yes_task='get_batch_process_result',
            no_task='fail_batch_process'
        )

        fail_batch_process = rail.FailOperator(
            task_id="fail_batch_process",
            message='{{ result("get_batch_status")["message"] }}',
        )

        get_batch_process_result = rail.RepliconServiceOperator(
            task_id='get_batch_process_result',
            endpoint='/services/ReportService1.svc/GetReportGenerationBatchResults',
            data={
                "reportGenerationBatchUri": "{{ result('create_report_generation_batch') }}"
            }
        )

        check_payload_has_data = rail.IfOperator(
            task_id='check_payload_has_data',
            test="{{ result('get_batch_process_result').reportGenerationResults[0].payload.startswith('No Data') | is_falsy }}",
            yes_task='error_not_exists',
            no_task='finish_empty'
        )

        error_not_exists = rail.IfOperator(
            task_id='error_not_exists',
            test="{{ result('get_batch_process_result').reportGenerationResults[0].error | is_falsy }}",
            yes_task='load_csv',
            no_task='fail_error_exists'
        )

        fail_error_exists = rail.FailOperator(
            task_id="fail_error_exists",
            message='{{ result("get_batch_process_result").reportGenerationResults[0].error }}',
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id='load_csv',
            document='{{ result("get_batch_process_result").reportGenerationResults[0].payload }}'
        )

        create_timesheet_data = rail.CreateCollectionOperator(
            task_id='create_timesheet_data',
            source="{{ result('load_csv') }}",
            columns={
                'Timesheet Period': 'timesheetperiod',
                'Login Name': 'username',
                'Validation Message': 'validationmessages',
                'Approval Status': 'approvalstatus',
                'Timesheet URI': 'timesheeturi',
                'Timesheet Start Date': 'timesheetstartdate',
                'Timesheet End Date': 'timesheetenddate',
                'validationcheck': 'validationcheck',
            },
            name='timesheet_data'
        )

        finish_empty = rail.EmptyOperator(
            task_id='finish_empty'
        )

        get_report_details >> create_report_generation_batch >> process_batch_request >> get_batch_status >> check_execution_state
        check_execution_state >> rail.Label(
            'Yes') >> get_batch_process_result >> check_payload_has_data
        check_payload_has_data >> rail.Label('Yes') >> error_not_exists
        error_not_exists >> rail.Label(
            'Yes') >> load_csv >> create_timesheet_data
        error_not_exists >> rail.Label('No') >> fail_error_exists
        check_payload_has_data >> rail.Label('No') >> finish_empty
        check_execution_state >> rail.Label('No') >> fail_batch_process

        return get_report_details, create_timesheet_data
