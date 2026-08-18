from datetime import timedelta
from airflow.models import Variable
from ge.user_sync_poland.utils import custom_methods
import rail

null = None


def create_dag(config):
    # pylnot: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.sub_child_schedule_add_dag_id,
        description=f'GE POLAND User Import Schedule Add Sub-Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_sub_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_draft_for_new_schedule_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_draft_for_new_schedule_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_draft_for_new_schedule_3 = rail.RepliconServiceOperator(
            task_id='create_draft_for_new_schedule_3',
            endpoint="/services/OfficeScheduleService1.svc/CreateNewDraft",
        )

        update_name_for_new_schedule_4 = rail.RepliconServiceOperator(
            task_id='update_name_for_new_schedule_4',
            endpoint="/services/OfficeScheduleService1.svc/UpdateName",
            data={
                "officeScheduleUri": "{{ result('create_draft_for_new_schedule_3') }}",
                "name": "{{ dag_run.conf.name }}"
            }
        )

        publish_draftfornewschedule_5 = rail.RepliconServiceOperator(
            task_id='publish_draftfornewschedule_5',
            endpoint="/services/OfficeScheduleService1.svc/PublishDraft",
            data={
                "officeScheduleDraftUri": "{{ result('create_draft_for_new_schedule_3') }}"
            }
        )

        log_minutes_for_weekdays = rail.PythonOperator(
            task_id='log_minutes_for_weekdays',
            python_callable=custom_methods.get_minutes_for_weekdays
        )

        put_simple_schedule_pattern_for_new_schedule_13 = rail.RepliconServiceOperator(
            task_id='put_simple_schedule_pattern_for_new_schedule_13',
            endpoint="/services/OfficeScheduleService1.svc/PutSimpleSchedulePattern",
            data=lambda: {
                "officeScheduleUri": rail.result('publish_draftfornewschedule_5')['uri'],
                "pattern": {
                    "startDayOfWeekUri": "urn:replicon:day-of-week:monday",
                    "day1WorkDuration": {
                        "hours": "0",
                        "minutes": rail.result('log_minutes_for_weekdays')['minutes_for_monday'],
                        "seconds": "0",
                        "milliseconds": "0",
                        "microseconds": "0"
                    },
                    "day2WorkDuration": {
                        "hours": "0",
                        "minutes": rail.result('log_minutes_for_weekdays')['minutes_for_tuesday'],
                        "seconds": "0",
                        "milliseconds": "0",
                        "microseconds": "0"
                    },
                    "day3WorkDuration": {
                        "hours": "0",
                        "minutes": rail.result('log_minutes_for_weekdays')['minutes_for_wednesday'],
                        "seconds": "0",
                        "milliseconds": "0",
                        "microseconds": "0"
                    },
                    "day4WorkDuration": {
                        "hours": "0",
                        "minutes": rail.result('log_minutes_for_weekdays')['minutes_for_thursday'],
                        "seconds": "0",
                        "milliseconds": "0",
                        "microseconds": "0"
                    },
                    "day5WorkDuration": {
                        "hours": "0",
                        "minutes": rail.result('log_minutes_for_weekdays')['minutes_for_friday'],
                        "seconds": "0",
                        "milliseconds": "0",
                        "microseconds": "0"
                    },
                    "day6WorkDuration": {
                        "hours": "0",
                        "minutes": rail.result('log_minutes_for_weekdays')['minutes_for_saturday'],
                        "seconds": "0",
                        "milliseconds": "0",
                        "microseconds": "0"
                    },
                    "day7WorkDuration": {
                        "hours": "0",
                        "minutes": rail.result('log_minutes_for_weekdays')['minutes_for_sunday'],
                        "seconds": "0",
                        "milliseconds": "0",
                        "microseconds": "0"
                    }
                }
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> create_draft_for_new_schedule_3
        create_draft_for_new_schedule_3 >> update_name_for_new_schedule_4 >> publish_draftfornewschedule_5 >> log_minutes_for_weekdays >>\
            put_simple_schedule_pattern_for_new_schedule_13 >> finish

    return dag


rail.for_each_instance(create_dag)
