import rail
from seaspanshipyards.timesheet_auto_submission.utils import custom_methods

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"seaspanshipyards_recalculate_timesheets_child_dag_{config.instance}",
        description=f"SeaspanShipyards Recalculate Timesheets {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.recalculate_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        mark_timesheet_out_of_date = rail.RepliconServiceOperator(
            task_id='mark_timesheet_out_of_date',
            endpoint='/services/TimesheetService1.svc/MarkTimesheetsAsOutOfDate',
            data=lambda: {
                "timesheets": list(map(lambda x: x['timesheeturi'], rail.get_current_context()['dag_run'].conf['items']))
            }
        )

        recalculate_script_data = rail.RepliconServiceCallForEachItemOperator(
            task_id='recalculate_script_data',
            endpoint='/services/TimesheetService1.svc/RecalculateScriptData',
            items=lambda: list(map(lambda x: x['timesheeturi'], rail.get_current_context()[
                               'dag_run'].conf['items'])),
            data=lambda item: {
                "timesheet": {
                    "uri": item,
                    "user": null,
                    "date": null
                }
            }
        )

        if_task_mark_timesheet_out_of_date_failed = rail.IfOperator(
            task_id = 'if_task_mark_timesheet_out_of_date_failed',
            trigger_rule='one_failed',
            test=custom_methods.is_task_mark_timesheet_out_of_date_failed,
            no_task="fail_dag_run"
        )

        fail_dag_run = rail.FailOperator(
            task_id='fail_dag_run',
            message='{{ get_error_message() }}'
        )
        
        mark_timesheet_out_of_date >> recalculate_script_data >> if_task_mark_timesheet_out_of_date_failed
        if_task_mark_timesheet_out_of_date_failed >> rail.Label('No') >> fail_dag_run

    return dag


rail.for_each_instance(create_child_dag)
