import rail
from ge.timesheet_recalc_portugal.utils import request_payload


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"ge_timesheet_data_process_each_record_child_{config.instance}",
        description=f"GE Timesheet Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        mark_timesheets_as_outofdate = rail.RepliconServiceOperator(
            task_id="mark_timesheets_as_outofdate",
            endpoint="/services/TimesheetService1.svc/MarkTimesheetsAsOutOfDate",
            data=request_payload.get_timesheets_payload
        )

        enqueue_recalculate_scriptdata = rail.RepliconServiceOperator(
            task_id="enqueue_recalculate_scriptdata",
            endpoint="/services/TimesheetService1.svc/CreateRecalculateScriptDataBatch2",
            data=request_payload.get_timesheets_payload
        )

        (execute_timesheet_batch, wait_for_timesheet_batch) = rail.batch_execution(
            group_id='execute_time_export',
            creation_task_id=enqueue_recalculate_scriptdata.task_id,
        )

        timesheet_recalc_success = rail.WriteLogOperator(
            task_id='timesheet_recalc_success',
            message="Recalculation completed",
            items="{{dag_run.conf.timesheetdetails | to_json}}",
            severity='Success',
            properties={
                'username': "{{item.User_Name}}",
                'timesheetstartdate': "{{item.Timesheet_Start_Date}}",
                'timesheetenddate': "{{item.Timesheet_End_Date}}",
                'timesheeturi': "{{item.timesheeturi}}",
                'childecid': "{{dag_run_ecid()}}",
                'status': 'Success',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            items="{{dag_run.conf.timesheetdetails | to_json}}",
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'username': "{{item.User_Name}}",
                'timesheetstartdate': "{{item.Timesheet_Start_Date}}",
                'timesheetenddate': "{{item.Timesheet_End_Date}}",
                'timesheeturi': "{{item.timesheeturi}}",
                'childecid': "{{dag_run.ecid()}}",
                'status': 'failed',
            },
        )

        mark_timesheets_as_outofdate >> enqueue_recalculate_scriptdata >> execute_timesheet_batch\
            >> wait_for_timesheet_batch >> timesheet_recalc_success >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
