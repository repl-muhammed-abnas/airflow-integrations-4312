from datetime import datetime, timedelta
from pytz import timezone
from airflow.models import Variable
import rail
# pylint:disable = too-many-statements
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_initial_setup_main_{config.instance}',
        description=f'deltek_costpoint_initial_setup_main_{config.instance}',
        schedule_interval=timedelta(seconds=config.schedule_interval),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_last_run_date'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_last_run_date',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def do_get_last_run_date():
            current_time = datetime.now(timezone('UTC')) - timedelta(seconds=2)
            lookup_timestamp_value = Variable.get(
                config.last_run_date_var_name, default_var=None)
            last_run_date = (datetime.fromisoformat(
                lookup_timestamp_value) if lookup_timestamp_value else current_time).isoformat()
            Variable.set(config.last_run_date_var_name,
                         current_time.isoformat())
            return last_run_date

        get_last_run_date = rail.PythonOperator(
            task_id='get_last_run_date',
            python_callable=do_get_last_run_date
        )

        def get_time():
            time_zone = timezone(config.time_zone)
            datetime_in_timezone = datetime.fromisoformat(
                rail.result('get_last_run_date')).astimezone(time_zone)
            return (datetime_in_timezone).replace(tzinfo=None).isoformat()

        process_groups = rail.TriggerDagRunOperator(
            task_id='process_groups',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_costpoint_group_option_update_child_{config.instance}',
            conf=lambda: {
                    "last_modified": get_time()
            }
        )

        wait_for_completion_trigger_process_groups = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_process_groups',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_groups") }}'
        )

        process_oefs = rail.TriggerDagRunOperator(
            task_id='process_oefs',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_costpoint_oef_tag_update_child_{config.instance}',
            conf=lambda: {
                    "last_modified": get_time()
            }
        )

        wait_for_completion_trigger_process_oefs = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_process_oefs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_oefs") }}'
        )

        process_paycodes = rail.TriggerDagRunOperator(
            task_id='process_paycodes',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_costpoint_paycode_type_child_{config.instance}',
            conf=lambda: {
                    "last_modified": get_time()
            }
        )

        wait_for_completion_trigger_process_paycodes = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_process_paycodes',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_paycodes") }}'
        )

        process_timeoffs = rail.TriggerDagRunOperator(
            task_id='process_timeoffs',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_costpoint_timeoff_type_child_{config.instance}',
            conf=lambda: {
                    "last_modified": get_time()
            }
        )

        wait_for_completion_trigger_process_timeoffs = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_process_timeoffs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_timeoffs") }}'
        )

        process_tsperiods = rail.TriggerDagRunOperator(
            task_id='process_tsperiods',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_costpoint_timesheet_period_child_{config.instance}',
            conf=lambda: {
                    "last_modified": get_time()
            }
        )

        wait_for_completion_trigger_process_tsperiods = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_process_tsperiods',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_tsperiods") }}'
        )

        process_schedule_sync = rail.TriggerDagRunOperator(
            task_id='process_schedule_sync',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_costpoint_schedules_{config.instance}',
            conf=lambda: {
                    "last_modified": get_time()
            }
        )

        wait_for_completion_trigger_process_schedules = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_process_schedules',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_schedule_sync") }}'
        )

        process_holiday_calendar_sync = rail.TriggerDagRunOperator(
            task_id='process_holiday_calendar_sync',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_costpoint_holiday_calendars_{config.instance}',
            conf=lambda: {
                    "last_modified": get_time()
            }
        )

        wait_for_completion_trigger_process_holiday_calendars = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_process_holiday_calendars',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_holiday_calendar_sync") }}'
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: 'Error:' +
            rail.render_template("{{get_error_message()}}")
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_last_run_date >> process_groups >> wait_for_completion_trigger_process_groups >> \
            process_oefs >> wait_for_completion_trigger_process_oefs >> \
            process_paycodes >> wait_for_completion_trigger_process_paycodes >> \
            process_timeoffs >> wait_for_completion_trigger_process_timeoffs >> \
            process_tsperiods >> wait_for_completion_trigger_process_tsperiods >> \
            process_schedule_sync >> wait_for_completion_trigger_process_schedules >> \
            process_holiday_calendar_sync >> wait_for_completion_trigger_process_holiday_calendars >> catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
