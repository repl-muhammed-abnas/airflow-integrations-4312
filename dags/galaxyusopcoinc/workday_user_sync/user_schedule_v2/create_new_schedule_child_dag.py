import rail
from galaxyusopcoinc.workday_user_sync.user_schedule_v2.utils import python_callable_method
from airflow.models import Variable


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_schedule_creation,
        description=f'Vialto Partners New Schedule Child V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_user_schedule_runs,
    ) as dag:

        scheduletype = "{{ dag_run.conf.scheduletype }}"

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test = lambda: Variable.get(config.can_run_batch_task_var_name, deserialize_json=True, default_var={}).get('schedule_creation', True),
            yes_task="batch_task",
            no_task="validate_blank_schedule"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="validate_blank_schedule",
            end_task="catch_and_log_errors"
        )

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        def validate_blank_schedule_test(dag_run):
            schedule_list = dag_run.conf['scheduletype'].split("|")
            for schedule in schedule_list:
                if schedule not in ['', None]:
                    return False
            return True

        validate_blank_schedule = rail.IfOperator(
            task_id = "validate_blank_schedule",
            test=validate_blank_schedule_test,
            yes_task="skip_blank_schedule",
            no_task="has_valid_scheduletype"
        )

        skip_blank_schedule = rail.EmptyOperator(
            task_id = "skip_blank_schedule"
        )
 
        has_valid_scheduletype = rail.IfOperator(
            task_id='has_valid_scheduletype',
            test=lambda dag_run: python_callable_method.valid_schedule(
                dag_run.conf['scheduletype']),
            yes_task='create_new_draft',
            no_task='log_schedule_patter_not_valid'
        )

        log_schedule_patter_not_valid = rail.WriteLogOperator(
            task_id='log_schedule_patter_not_valid',
            message='Schedule pattern is not valid',
            severity='Exception',
            properties={
                'schedulename': scheduletype,
                'employeeid': "NA",
                'status': 'Exception',
                'message': "{{ dag_run.conf.scheduletype + ' Schedule pattern is not valid' }}"
            }
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
                    "name": scheduletype
            }
        )

        puts_impleschedule_pattern = rail.RepliconServiceOperator(
            task_id='puts_impleschedule_pattern',
            endpoint='/services/OfficeScheduleService1.svc/PutSimpleSchedulePattern',
            data=lambda: {
                "officeScheduleUri": rail.result('create_new_draft'),
                "pattern": {
                    "startDayOfWeekUri": "urn:replicon:day-of-week:sunday",
                    "day1WorkDuration": python_callable_method.get_work_duration('sunday'),
                    "day2WorkDuration": python_callable_method.get_work_duration('monday'),
                    "day3WorkDuration": python_callable_method.get_work_duration('tuesday'),
                    "day4WorkDuration": python_callable_method.get_work_duration('wednesday'),
                    "day5WorkDuration": python_callable_method.get_work_duration('thursday'),
                    "day6WorkDuration": python_callable_method.get_work_duration('friday'),
                    "day7WorkDuration": python_callable_method.get_work_duration('saturday'),
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
                'employeeid': "NA",
                'status': 'Error',
                'message': '{{ get_error_message() }}',

            },
        )


        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> validate_blank_schedule

        validate_blank_schedule >> rail.Label("Blank Schedule") >> skip_blank_schedule >> catch_and_log_errors
        validate_blank_schedule >> rail.Label("Non-Blank Schedule") >> has_valid_scheduletype >> rail.Label(
            "NO") >> log_schedule_patter_not_valid >> catch_and_log_errors
        has_valid_scheduletype >> rail.Label("YES") >> create_new_draft
        create_new_draft >> update_name >> puts_impleschedule_pattern
        puts_impleschedule_pattern >> publish_draft >> catch_and_log_errors

        return dag


rail.for_each_instance(create_child_dag)
