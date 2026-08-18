from datetime import timedelta
from airflow.models import Variable
import rail
import re

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_office_schedule_add_dag_id,
        description=f'Assured Partners User Import Office Schedule Add Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_split_schedule_for_processing'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_split_schedule_for_processing',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def hours_to_duration(hours_decimal):
            """Convert decimal hours to duration dict with hours, minutes, seconds, milliseconds, microseconds"""
            units = [('hours', 3600), ('minutes', 60), ('seconds', 1),
                     ('milliseconds', 0.001), ('microseconds', 0.000001)]
            remaining = hours_decimal * 3600  # Convert to seconds
            result = {}
            for name, divisor in units:
                if divisor >= 1:
                    result[name] = int(remaining // divisor)
                    remaining = remaining % divisor
                else:
                    result[name] = int(round(remaining / divisor))
                    remaining = 0
            return result

        def split_schedule_for_processing(dag_run):
            """Parse new format: US_M8_T8_W8_T8_F8_S0_S0 or US_M7P5_T7P5_W7P5_T7P5_F0_S0_S0"""
            schedule_name = dag_run.conf['schedulename']
            # ['US', 'M8', 'T8', 'W8', 'T8', 'F8', 'S0', 'S0']
            parts = schedule_name.split('_')

            # Expected order after US_: Mon, Tue, Wed, Thu, Fri, Sat, Sun
            day_keys = ['monday', 'tuesday', 'wednesday',
                        'thursday', 'friday', 'saturday', 'sunday']

            zero_duration = hours_to_duration(0)
            weekdays_schedule = {
                "sunday": zero_duration, "monday": zero_duration, "tuesday": zero_duration,
                "wednesday": zero_duration, "thursday": zero_duration, "friday": zero_duration, "saturday": zero_duration
            }

            for idx, day_key in enumerate(day_keys):
                part_idx = idx + 1  # Skip 'US' prefix
                if part_idx < len(parts):
                    # Extract hours: M8 -> 8, M7P5 -> 7.5, M2P88 -> 2.88
                    # Remove letter prefix
                    hours_str = re.sub(r'^[A-Za-z]+', '', parts[part_idx])
                    # Convert P back to decimal
                    hours_str = hours_str.replace('P', '.')
                    hours = float(hours_str) if hours_str else 0
                    weekdays_schedule[day_key] = hours_to_duration(hours)

            return weekdays_schedule

        log_split_schedule_for_processing = rail.PythonOperator(
            task_id='log_split_schedule_for_processing',
            python_callable=split_schedule_for_processing
        )

        create_new_draft_60 = rail.RepliconServiceOperator(
            task_id='create_new_draft_60',
            endpoint="/services/OfficeScheduleService1.svc/CreateNewDraft",
        )

        update_name_61 = rail.RepliconServiceOperator(
            task_id='update_name_61',
            endpoint="/services/OfficeScheduleService1.svc/updateName",
            data={
                "officeScheduleUri": "{{ result('create_new_draft_60') }}",
                "name": "{{ dag_run.conf.schedulename }}"
            }
        )

        put_simple_schedule_pattern_62 = rail.RepliconServiceOperator(
            task_id='put_simple_schedule_pattern_62',
            endpoint="/services/OfficeScheduleService1.svc/PutSimpleSchedulePattern",
            data=lambda: {
                "officeScheduleUri": rail.result('create_new_draft_60'),
                "pattern": {
                    "startDayOfWeekUri": "urn:replicon:day-of-week:sunday",
                    "day1WorkDuration": rail.result('log_split_schedule_for_processing')['sunday'],
                    "day2WorkDuration": rail.result('log_split_schedule_for_processing')['monday'],
                    "day3WorkDuration": rail.result('log_split_schedule_for_processing')['tuesday'],
                    "day4WorkDuration": rail.result('log_split_schedule_for_processing')['wednesday'],
                    "day5WorkDuration": rail.result('log_split_schedule_for_processing')['thursday'],
                    "day6WorkDuration": rail.result('log_split_schedule_for_processing')['friday'],
                    "day7WorkDuration": rail.result('log_split_schedule_for_processing')['saturday']
                }
            }
        )

        publish_draft_63 = rail.RepliconServiceOperator(
            task_id='publish_draft_63',
            endpoint="/services/OfficeScheduleService1.svc/PublishDraft",
            data={
                "officeScheduleDraftUri": "{{ result('create_new_draft_60') }}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{dag_run.conf.groups_table}}",
            message='na',
            severity='Error',
            properties={
                "jobid": "{{dag_run.conf.jobid}}",
                "name": "{{dag_run.conf.schedulename}}",
                "details": "Error in creating Shedule - {{dag_run.conf.schedulename}} ; {{get_error_message()}} "
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> log_split_schedule_for_processing

        log_split_schedule_for_processing >> create_new_draft_60

        create_new_draft_60 >> update_name_61 >> put_simple_schedule_pattern_62 >> publish_draft_63 >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
