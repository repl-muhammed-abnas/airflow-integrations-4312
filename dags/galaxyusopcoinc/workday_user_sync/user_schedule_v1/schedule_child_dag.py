import rail
from galaxyusopcoinc.workday_user_sync.user_schedule_v1.utils import request_payload
from galaxyusopcoinc.workday_user_sync.user_schedule_v1.utils import python_callable_method


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_new_schedule_child_dag_{config.dag_id_postfix}',
        description=f'Vialto Partners New Schedule Child V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_user_schedule_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        scheduletype = "{{ dag_run.conf.scheduletype }}"

        has_valid_scheduletype = rail.IfOperator(
            task_id='has_valid_scheduletype',
            test=lambda: python_callable_method.valid_schedule(
                request_payload.get_dag_run_conf()['scheduletype']),
            yes_task='create_new_draft',
            no_task='log_error'
        )

        log_error = rail.WriteLogOperator(
            task_id='log_error',
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

        has_valid_scheduletype >> rail.Label(
            "NO") >> log_error >> catch_and_log_errors
        has_valid_scheduletype >> rail.Label("YES") >> create_new_draft
        create_new_draft >> update_name >> puts_impleschedule_pattern
        puts_impleschedule_pattern >> publish_draft >> catch_and_log_errors

        return dag


rail.for_each_instance(create_child_dag)
