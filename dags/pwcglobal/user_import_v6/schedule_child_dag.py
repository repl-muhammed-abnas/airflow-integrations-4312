from datetime import timedelta
import rail

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/user_import_v6/config.py


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.schedule_dag_id,
        description=f'PwCGlobal_User_Import_Child_Office Schedule Add',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.schedule_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        scheduletype = "{{ dag_run.conf.scheduletype }}"

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='has_valid_scheduletype',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        has_valid_scheduletype = rail.IfOperator(
            task_id='has_valid_scheduletype',
            test="{{ dag_run.conf.scheduletype | split('|') | length  == 7 }}",
            yes_task='create_new_draft',
            no_task='log_error'
        )

        log_error = rail.WriteLogOperator(
            task_id='log_error',
            message='Schedule pattern is received for incorrect number of days',
            severity='Exception',
            properties={
                'schedulename': scheduletype,
                'userpartyid': 'na',
                'username': 'na',
                'legalentityid': 'na',
                'status': 'Exception',
                'message': "{{ dag_run.conf.scheduletype + ' Schedule pattern is received for incorrect number of days' }}"
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

        def get_work_duration(day):
            day_index = ['sunday', 'monday', 'tuesday',
                         'wednesday', 'thursday', 'friday', 'saturday'].index(day)
            time = float(rail.get_current_context()[
                'dag_run'].conf['scheduletype'].split('|')[day_index])
            return {
                "hours": int(time),
                "minutes": int((time*60) % 60),
                "seconds": int((time*60*60) % 60),
                "milliseconds": 0,
                "microseconds": 0,
            }

        puts_impleschedule_pattern = rail.RepliconServiceOperator(
            task_id='puts_impleschedule_pattern',
            endpoint='/services/OfficeScheduleService1.svc/PutSimpleSchedulePattern',
            data=lambda: {
                "officeScheduleUri": rail.result('create_new_draft'),
                "pattern": {
                    "startDayOfWeekUri": "urn:replicon:day-of-week:sunday",
                    "day1WorkDuration": get_work_duration('sunday'),
                    "day2WorkDuration": get_work_duration('monday'),
                    "day3WorkDuration": get_work_duration('tuesday'),
                    "day4WorkDuration": get_work_duration('wednesday'),
                    "day5WorkDuration": get_work_duration('thursday'),
                    "day6WorkDuration": get_work_duration('friday'),
                    "day7WorkDuration": get_work_duration('saturday'),
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
        batch_task >> has_valid_scheduletype
        batch_task >> catch_and_log_errors
        has_valid_scheduletype >> rail.Label(
            'no') >> log_error >> catch_and_log_errors
        has_valid_scheduletype >> rail.Label('yes') >> create_new_draft >> \
            update_name >> puts_impleschedule_pattern >> publish_draft >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)
