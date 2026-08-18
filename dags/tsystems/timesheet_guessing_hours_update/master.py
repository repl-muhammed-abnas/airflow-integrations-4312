import itertools
from datetime import datetime
import pendulum
import rail
from datetime import timedelta
from tsystems.timesheet_guessing_hours_update.utils import custom_methods
from tsystems.timesheet_guessing_hours_update.utils.response_filters import filter_group_data
from tsystems.timesheet_guessing_hours_update.utils.request_payload import get_location_payload
from pendulum import now

null = None

def create_master_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f"T-Systems | Guessing Hours Master ({config.instance})",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule,
        max_active_runs=config.max_active_runs_master,
    ) as dag:
        
        can_process_run = rail.IfOperator(
            task_id = "can_process_run",
            test=lambda: custom_methods.can_process_run_test(
                config.time_zone, 
                config.SCHEDULE_MAPPER, 
                config.MAPPER_DATE_FORMAT
            ),
            yes_task="log_start_time"
        )

        log_start_time = rail.PythonOperator(
            task_id="log_start_time",
            python_callable=lambda: {
                "start_time": now(config.time_zone).isoformat(),
                "log_filename": "Logs_Guessing_Hours_Update_"+ rail.render_template('{{ dag_run_ecid() | replace(":", "-") }}_') + \
                    now(config.time_zone).strftime("%Y%m%dT%H%M%S") + ".csv"
            }
        )

        create_org_log = rail.CreateLogOperator(
            task_id="create_org_log"
        )

        get_todays_mapper_records = rail.PythonOperator(
            task_id="get_todays_mapper_records",
            python_callable=lambda: custom_methods.get_todays_mapper_records(
                config.time_zone,
                config.SCHEDULE_MAPPER,
                config.MAPPER_DATE_FORMAT
            )
        )

        get_all_org_structures = rail.RepliconServiceOperator(
            task_id="get_all_org_structures",
            endpoint='/services/LocationListService1.svc/GetData',
            data=get_location_payload,
            data_handler=filter_group_data
        )

        trigger_process_each_orgstructure = rail.TriggerDagRunForEachItemOperator(
            task_id="trigger_process_each_orgstructure",
            items="{{ result('get_todays_mapper_records') | to_json }}",
            trigger_dag_id=config.process_each_orgstructure,
            conf=lambda item:{
                "input_data": {
                    "org_code": item.get("org_code"),
                    "org_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_org_structures"), "code", item.get("org_code"), "uri"),
                    "timesheet_start_date": custom_methods.change_date_format(item.get("start_date"), config.MAPPER_DATE_FORMAT, config.REPORT_DATE_FORMAT),
                    "timesheet_end_date": custom_methods.change_date_format(item.get("end_date"), config.MAPPER_DATE_FORMAT, config.REPORT_DATE_FORMAT),
                },
                "org_log": rail.result("create_org_log")
            }
        )

        wait_for_process_each_orgstructure = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_each_orgstructure",
            dag_runs='{{ result("trigger_process_each_orgstructure") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_time_entry_logs = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_time_entry_logs",
            dag_runs='{{ result("trigger_process_each_orgstructure") }}',
            dagrun_task_id="gather_process_users_logs",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True
        )

        trigger_log_generation = rail.TriggerDagRunOperator(
            task_id='trigger_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation,
            conf=lambda: {
                'timeentrylogs': rail.result("gather_time_entry_logs") if rail.result("gather_time_entry_logs") else None,
                'otherlogs': rail.result("create_org_log"),
                'log_filename': rail.result("log_start_time")["log_filename"],
                'start_time': rail.result("log_start_time")["start_time"]
            }
        )

        can_process_run >> rail.Label("Yes") >> log_start_time >> create_org_log \
            >> get_todays_mapper_records >> get_all_org_structures >> trigger_process_each_orgstructure \
                >> wait_for_process_each_orgstructure >> gather_time_entry_logs >> trigger_log_generation

    return dag

rail.for_each_instance(create_master_dag)
