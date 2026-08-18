import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'horizonmedia_timesheet_approval_for_disabled_users_child_{config.instance}',
        description=f'Horizonmedia_timesheet_approval_for_disabled_users_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dagrun_config')

        create_forced_approve_batch = rail.RepliconServiceOperator(
            task_id='create_forced_approve_batch',
            endpoint="/services/TimesheetApprovalService1.svc/CreateForcedApproveBatch",
            data=lambda dag_run:{
                "timesheetUris": list(set(map(lambda x: x['timesheetperioduri'], dag_run.conf['timesheet_batch_items']))),
                "comments": "Submitted by automation"
            }
        )

        batch_management = rail.batch_execution(
            group_id='execute_batch_management',
            creation_task_id='create_forced_approve_batch',

        )

        is_batch_success = rail.IfOperator(
            task_id='is_batch_success',
            test="{{ result('execute_batch_management.wait_for_batch').executionState == 'urn:replicon-service-model:batch-execution-state:succeeded'}}",
            yes_task="log_success_entries",
            no_task="log_failure_entries",
        )

        log_success_entries = rail.WriteLogOperator(
            task_id='log_success_entries',
            message="Adding the multiple entries",
            severity='Success',
            items="{{dag_run.conf.timesheet_batch_items | to_json}}",
            properties={
                'username': '{{item.username}}',
                'status': 'Success',
                'details': 'TimesheetApproved',
                'timesheetperiod': '{{item.timesheetperiod}}',
                'employeeid': '{{item.employeeid}}',
                'errormessage':''
            }
        )

        log_failure_entries = rail.WriteLogOperator(
            task_id='log_failure_entries',
            message="Adding the multiple entries",
            severity='Error',
            items="{{dag_run.conf.timesheet_batch_items | to_json}}",
            properties={
                'username': '{{item.username}}',
                'status': 'Failed',
                'details': 'TimesheetApproved',
                'timesheetperiod': '{{item.timesheetperiod}}',
                'employeeid': '{{item.employeeid}}',
                'errormessage': 'execution failed'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            items="{{dag_run.conf.timesheet_batch_items | to_json}}",
            message='{{ get_error_message() }}',
            properties={
                'username': '{{item.username}}',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
                'timesheetperiod': '{{item.timesheetperiod}}',
                'employeeid': '{{item.employeeid}}',
                'errormessage': '{{ get_error_message() }}',
            }
        )

        create_forced_approve_batch >> batch_management >> is_batch_success
        is_batch_success >> rail.Label(
            'Yes') >> log_success_entries >> catch_and_log_errors
        is_batch_success >> rail.Label(
            'No') >> log_failure_entries >> catch_and_log_errors
    return dag


rail.for_each_instance(create_dag)
