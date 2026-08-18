import rail

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcglobal_zero_hours_recalculate_timesheets_v4_child_dag_{config.instance}",
        description=f"PwCGlobal - Timesheet auto submission zero hours Recalculate Timesheets v4.0 {config.instance}",
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

        mark_timesheet_out_of_date >> recalculate_script_data

    return dag


rail.for_each_instance(create_child_dag)
