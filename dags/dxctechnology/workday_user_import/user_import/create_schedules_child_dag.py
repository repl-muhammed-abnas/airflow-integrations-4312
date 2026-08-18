from pendulum import datetime
import rail
from airflow.models import Variable
from datetime import timedelta

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_process_schedule_creation_dag,
        description="dxctechnology workday user sync Master",
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.max_active_run_master
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="create_new_draft"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="create_new_draft",
            end_task="publish_draft",
            execution_timeout=timedelta(days=14)
        )

        create_new_draft = rail.RepliconServiceOperator(
            task_id = "create_new_draft",
            endpoint="/services/OfficeScheduleService1.svc/CreateNewDraft"
        )

        update_name = rail.RepliconServiceOperator(
            task_id = "update_name",
            endpoint="/services/OfficeScheduleService1.svc/updateName",
            data= {
                "officeScheduleUri": "{{ result('create_new_draft') }}",
                "name": "{{ dag_run.conf.work_schedule }}"
            }
        )

        def get_schedule_pattern(dag_run):
            decimal_value = float(float(dag_run.conf['work_schedule'].replace("WS-", "")) / 5)
            decimal_value_in_seconds = int(decimal_value * 3600)
            std_schedule_pattern, daily_schedule_pattern = {
                "hours": "0",
                "minutes": "0",
                "seconds": "0",
                "milliseconds": "0",
                "microseconds": "0"
            }, {
                "hours": "0",
                "minutes": "0",
                "seconds": decimal_value_in_seconds,
                "milliseconds": "0",
                "microseconds": "0"
            }
            return {
                "officeScheduleUri": rail.result("create_new_draft"),
                "pattern": {
                    "startDayOfWeekUri": "urn:replicon:day-of-week:sunday",
                    "day1WorkDuration": std_schedule_pattern,
                    "day2WorkDuration": daily_schedule_pattern,
                    "day3WorkDuration": daily_schedule_pattern,
                    "day4WorkDuration": daily_schedule_pattern,
                    "day5WorkDuration": daily_schedule_pattern,
                    "day6WorkDuration": daily_schedule_pattern,
                    "day7WorkDuration": std_schedule_pattern
                }
            }

        put_schedule_pattern = rail.RepliconServiceOperator(
            task_id="put_schedule_pattern",
            endpoint="/services/OfficeScheduleService1.svc/PutSimpleSchedulePattern",
            data =get_schedule_pattern
        )

        publish_draft = rail.RepliconServiceOperator(
            task_id= "publish_draft",
            endpoint="/services/OfficeScheduleService1.svc/PublishDraft",
            data={
                "officeScheduleDraftUri" : '{{result("create_new_draft")}}'
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> publish_draft
        can_run_batch_task >> rail.Label("No") >> create_new_draft

        create_new_draft >> update_name >> put_schedule_pattern >> publish_draft

    return dag

rail.for_each_instance(create_dag)
