"""
Schedule Add - Unisys Workday User Import Child DAG

Creates new office schedules in Replicon based on schedule types from Workday.
This child DAG is triggered when a new schedule type is detected that doesn't exist
in Replicon, creating the schedule with appropriate work duration patterns.

Key features:
    - Creates office schedule drafts
    - Sets schedule names from Workday data
    - Configures work duration patterns by day of week
    - Handles weekend (non-working) days
    - Distributes hours evenly across working days
    - Publishes schedules for use
    - Supports batch task execution

Functions:
    create_dag(config): Creates the schedule creation child DAG
"""
from datetime import timedelta
import rail
from unisys.workday_user_import.utils import custom_method


def create_dag(config):
    """
    Create child DAG for creating new office schedules.

    This DAG creates office schedules in Replicon using the OfficeScheduleService1.svc
    endpoints. Schedules are created as drafts, configured with work patterns, and
    published for assignment to users.

    Args:
        config: Configuration object containing DAG settings including:
            - process_new_schedule: DAG ID for this child DAG
            - company_key: Replicon company identifier
            - replicon_conn_id: Replicon connection ID
            - schedule_dag_max_active_runs: Max parallel DAG runs
            - max_active_runs_process_schedule: Max parallel tasks
            - execution_timeout_days: Task execution timeout

    Returns:
        DAG: Configured Airflow DAG object for schedule creation

    DAG Configuration:
        dag_run.conf should contain:
            - scheduletype: Schedule type value (e.g., "40" for 40 hours/week)

    Note:
        The scheduletype is expected to be numeric representing total weekly hours.
        Hours are distributed evenly across Monday-Friday, with weekends set to 0.
    """
    with rail.create_airflow_dag(
        dag_id=config.process_new_schedule,
        description=f'Unisys Workday User Import- Process New Schedule',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.schedule_dag_max_active_runs,
        max_active_tasks=config.max_active_runs_process_schedule,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        scheduletype = "{{ dag_run.conf.scheduletype }}"

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_new_draft',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_new_draft = rail.RepliconServiceOperator(
            task_id='create_new_draft',
            endpoint='/services/OfficeScheduleService1.svc/CreateNewDraft',
        )

        update_name = rail.RepliconServiceOperator(
            task_id='update_name',
            endpoint='/services/OfficeScheduleService1.svc/UpdateName',
            data={
                    "officeScheduleUri": "{{ result('create_new_draft') }}",
                    "name": "{{dag_run.conf.scheduletype}}"
            }
        )

        def get_work_duration(day, total_hours, days=5):
            day_index = ['sunday', 'monday', 'tuesday',
                         'wednesday', 'thursday', 'friday', 'saturday'].index(day)
            if day in ['sunday', 'saturday']:
                return {
                    "hours": 0,
                    "minutes": 0,
                    "seconds": 0,
                    "milliseconds": 0,
                    "microseconds": 0,
                }

            total_hours = float(total_hours)
            hours_per_day = total_hours / days
            
            hours = int(hours_per_day)
            remaining = hours_per_day - hours
            
            minutes = int(remaining * 60)
            remaining = (remaining * 60) - minutes
            
            seconds = int(remaining * 60)
            
            return {
                "hours": hours,
                "minutes": minutes,
                "seconds": seconds,
                "milliseconds": 0,
                "microseconds": 0,
            }

        put_schedule_pattern = rail.RepliconServiceOperator(
            task_id='put_schedule_pattern',
            endpoint='/services/OfficeScheduleService1.svc/PutSimpleSchedulePattern',
            data=lambda dag_run: {
                "officeScheduleUri": rail.result('create_new_draft'),
                "pattern": {
                    "startDayOfWeekUri": "urn:replicon:day-of-week:sunday",
                    "day1WorkDuration": get_work_duration('sunday', dag_run.conf['scheduletype']),
                    "day2WorkDuration": get_work_duration('monday', dag_run.conf['scheduletype']),
                    "day3WorkDuration": get_work_duration('tuesday', dag_run.conf['scheduletype']),
                    "day4WorkDuration": get_work_duration('wednesday', dag_run.conf['scheduletype']),
                    "day5WorkDuration": get_work_duration('thursday', dag_run.conf['scheduletype']),
                    "day6WorkDuration": get_work_duration('friday', dag_run.conf['scheduletype']),
                    "day7WorkDuration": get_work_duration('saturday', dag_run.conf['scheduletype']),
                }
            }
        )

        publish_draft = rail.RepliconServiceOperator(
            task_id='publish_draft',
            endpoint='/services/OfficeScheduleService1.svc/PublishDraft',
            data={
                    "officeScheduleDraftUri": "{{ result('create_new_draft') }}"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'schedulename': scheduletype,
                'userpartyid': 'na',
                'username': 'na',
                'legalentityid': 'na',
                'status': 'Error',
                'message': '{{ get_error_message() }}',

            },
        )
        batch_task >> create_new_draft
        batch_task >> catch_and_log_errors
        create_new_draft >> update_name >> put_schedule_pattern >> publish_draft >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)
