
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'gehealthcare_user_sync_ge_netherlands_child_schedule_add_v1_0_{config.instance}',
        description=f'GE_netherlands Child Schedule add V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_new_draftfornewschedule_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_new_draftfornewschedule_3',
            end_task='catch_14_14_14',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_new_draftfornewschedule_3 = rail.RepliconServiceOperator(
            task_id='create_new_draftfornewschedule_3',
            endpoint="/services/OfficeScheduleService1.svc/CreateNewDraft",
            data=None
        )

        update_namefornewschedule_4 = rail.RepliconServiceOperator(
            task_id='update_namefornewschedule_4',
            endpoint="/services/OfficeScheduleService1.svc/UpdateName",
            data={
                "officeScheduleUri": "{{ result('create_new_draftfornewschedule_3') }}",
                "name": "{{ dag_run.conf.name }}"
            }
        )

        publish_draftfornewschedule_5 = rail.RepliconServiceOperator(
            task_id='publish_draftfornewschedule_5',
            endpoint="/services/OfficeScheduleService1.svc/PublishDraft",
            data={
                "officeScheduleDraftUri": "{{ result('create_new_draftfornewschedule_3') }}"
            }
        )

        def get_minutes_of_day(day_hours):
            if day_hours:
                day_hour_minutes = str(day_hours).split('.')
                hour_minutes = int(day_hour_minutes[0]) * 60
                minute_minute = 0
                if len(day_hour_minutes) > 1:
                    minute_minute = int(day_hour_minutes[1]) * 6
                return hour_minutes + minute_minute
            return 0

        log_minutesfor_monday_6 = rail.PythonOperator(
            task_id='log_minutesfor_monday_6',
            python_callable=lambda dag_run:  get_minutes_of_day(
                dag_run.conf['monday'])
        )

        log_minutesfor_tuesday_7 = rail.PythonOperator(
            task_id='log_minutesfor_tuesday_7',
            python_callable=lambda dag_run:  get_minutes_of_day(
                dag_run.conf['tuesday'])
        )

        log_minutesfor_wednesday_8 = rail.PythonOperator(
            task_id='log_minutesfor_wednesday_8',
            python_callable=lambda dag_run:  get_minutes_of_day(
                dag_run.conf['wednesday'])
        )

        log_minutesfor_thursday_9 = rail.PythonOperator(
            task_id='log_minutesfor_thursday_9',
            python_callable=lambda dag_run:  get_minutes_of_day(
                dag_run.conf['thursday'])
        )

        log_minutesfor_friday_10 = rail.PythonOperator(
            task_id='log_minutesfor_friday_10',
            python_callable=lambda dag_run:  get_minutes_of_day(
                dag_run.conf['friday'])
        )

        log_minutesfor_saturday_11 = rail.PythonOperator(
            task_id='log_minutesfor_saturday_11',
            python_callable=lambda dag_run:  get_minutes_of_day(
                dag_run.conf['saturday'])
        )

        log_minutesfor_sunday_12 = rail.PythonOperator(
            task_id='log_minutesfor_sunday_12',
            python_callable=lambda dag_run:  get_minutes_of_day(
                dag_run.conf['sunday'])
        )

        put_simple_schedule_patternfornewschedule_13 = rail.RepliconServiceOperator(
            task_id='put_simple_schedule_patternfornewschedule_13',
            endpoint="/services/OfficeScheduleService1.svc/PutSimpleSchedulePattern",
            data={
                "officeScheduleUri": "{{ result('publish_draftfornewschedule_5').uri }}",
                "pattern": {
                    "startDayOfWeekUri": "urn:replicon:day-of-week:monday",
                    "day1WorkDuration": {
                        "hours": "0",
                        "minutes": "{{ result('log_minutesfor_monday_6') }}",
                        "seconds": "0",
                        "milliseconds": "0",
                        "microseconds": "0"
                    },
                    "day2WorkDuration": {
                        "hours": "0",
                        "minutes": "{{ result('log_minutesfor_tuesday_7') }}",
                        "seconds": "0",
                        "milliseconds": "0",
                        "microseconds": "0"
                    },
                    "day3WorkDuration": {
                        "hours": "0",
                        "minutes": "{{ result('log_minutesfor_wednesday_8') }}",
                        "seconds": "0",
                        "milliseconds": "0",
                        "microseconds": "0"
                    },
                    "day4WorkDuration": {
                        "hours": "0",
                        "minutes": "{{ result('log_minutesfor_thursday_9') }}",
                        "seconds": "0",
                        "milliseconds": "0",
                        "microseconds": "0"
                    },
                    "day5WorkDuration": {
                        "hours": "0",
                        "minutes": "{{ result('log_minutesfor_friday_10') }}",
                        "seconds": "0",
                        "milliseconds": "0",
                        "microseconds": "0"
                    },
                    "day6WorkDuration": {
                        "hours": "0",
                        "minutes": "{{ result('log_minutesfor_saturday_11') }}",
                        "seconds": "0",
                        "milliseconds": "0",
                        "microseconds": "0"
                    },
                    "day7WorkDuration": {
                        "hours": "0",
                        "minutes": "{{ result('log_minutesfor_sunday_12') }}",
                        "seconds": "0",
                        "milliseconds": "0",
                        "microseconds": "0"
                    }
                }
            }
        )

        catch_14_14_14 = rail.EmptyOperator(
            task_id='catch_14_14_14',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_14_14_14
        can_run_batch_task >> rail.Label(
            'No') >> create_new_draftfornewschedule_3
        create_new_draftfornewschedule_3 >> update_namefornewschedule_4 >> \
            publish_draftfornewschedule_5 >> log_minutesfor_monday_6 >> log_minutesfor_tuesday_7 >> \
            log_minutesfor_wednesday_8 >> log_minutesfor_thursday_9 >> log_minutesfor_friday_10 >> \
            log_minutesfor_saturday_11 >> log_minutesfor_sunday_12 >> put_simple_schedule_patternfornewschedule_13 >> \
            catch_14_14_14 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
