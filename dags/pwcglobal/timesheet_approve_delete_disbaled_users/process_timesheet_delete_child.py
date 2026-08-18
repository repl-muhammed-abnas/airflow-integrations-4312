import rail
from pwcglobal.timesheet_approve_delete_disbaled_users.utils import request_payload


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"pwc_timesheet_delete_data_process_each_record_child_{config.instance}",
        description=f"Pwc Timesheet Delete Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_timesheet_delete_batch = rail.RepliconServiceOperator(
            task_id='create_timesheet_delete_batch',
            endpoint="/services/TimesheetService1.svc/CreateTimesheetDeleteBatch",
            data=request_payload.get_timesheets_delete_payload
        )

        (execute_timesheet_batch, wait_for_timesheet_batch) = rail.batch_execution(
            group_id='execute_time_export',
            creation_task_id=create_timesheet_delete_batch.task_id,
            retries=0
        )

        timesheet_delete_success = rail.WriteLogOperator(
            task_id='timesheet_delete_success',
            message="Timesheet Delete completed",
            items="{{dag_run.conf.timesheetdetails | to_json}}",
            severity='Success',
            properties={
                'username': "{{item.User_Name}}",
                'timesheetenddate': "{{item.Timesheet_End_Date}}",
                'timesheetstartdate': "{{item.Timesheet_Start_Date}}",
                'timesheeturi': "{{item.TimesheetPeriodUri}}",
                'childecid': "{{dag_run_ecid()}}",
                'status': 'Timesheet Deleted Success',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            items="{{dag_run.conf.timesheetdetails | to_json}}",
            message='{{ get_error_message() }}',
            properties={
                'username': "{{item.User_Name}}",
                'timesheetenddate': "{{item.Timesheet_End_Date}}",
                'timesheetstartdate': "{{item.Timesheet_Start_Date}}",
                'timesheeturi': "{{item.TimesheetPeriodUri}}",
                'childecid': "{{dag_run_ecid()}}",
                'status': 'Failed',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        create_timesheet_delete_batch >> execute_timesheet_batch\
            >> wait_for_timesheet_batch >> timesheet_delete_success >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
