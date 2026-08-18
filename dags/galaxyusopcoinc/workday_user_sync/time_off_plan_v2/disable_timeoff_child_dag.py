import rail
from galaxyusopcoinc.workday_user_sync.time_off_plan_v2.utils import request_payload
from galaxyusopcoinc.workday_user_sync.time_off_plan_v2.utils.custom_method import get_updated_timeoff_mapper_callable

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.disable_timeoff_type_dag_id,
        description=f'Vialto Partners Time Off Disable Child V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.disable_timeoff_type_max_active_run
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")


        can_update_timeoff_name = rail.IfOperator(
            task_id = "can_update_timeoff_name",
            test="{{ dag_run.conf.timeoff_name != dag_run.conf.feed_timeoff_name }}",
            yes_task="update_timeoff_name",
            no_task="disable_timeoff_in_replicon"
        )

        update_timeoff_name = rail.RepliconServiceOperator(
            task_id = "update_timeoff_name",
            endpoint="/services/TimeOffService1.svc/PutTimeOffType",
            data = request_payload.get_update_timeoff_name
        )

        disable_timeoff_in_replicon = rail.RepliconServiceOperator(
            task_id = "disable_timeoff_in_replicon",
            endpoint="/services/TimeOffService1.svc/DisableTimeOffType",
            data= {
                    "timeOffTypeUri": "{{ dag_run.conf.timeoff_uri }}"
                }
        )

        log_timeoff_disabled = rail.WriteLogOperator(
            task_id='log_timeoff_disabled',
            message='Time Off Type is Disabled in Replicon',
            severity='Success',
            properties={
                'time_off_type_desc': "{{ dag_run.conf.timeoff_description}}",
                'time_off_type_name': "{{ dag_run.conf.feed_timeoff_name}}",
                'unit_of_time': "{{ dag_run.conf.unit_of_time}}",
                'country': "{{ dag_run.conf.country}}",
                'status': 'Success'
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                'time_off_type_desc': "{{ dag_run.conf.timeoff_description}}",
                'time_off_type_name': "{{ dag_run.conf.feed_timeoff_name}}",
                'unit_of_time': "{{ dag_run.conf.unit_of_time}}",
                'country': "{{ dag_run.conf.country}}",
                'status': 'Error'
            },
        )

        can_update_timeoff_name >> rail.Label("Yes") >> update_timeoff_name >> disable_timeoff_in_replicon
        can_update_timeoff_name >> rail.Label("No") >> disable_timeoff_in_replicon \
            >> log_timeoff_disabled >> rail.Label("On Error") >> catch_and_log_error

    return dag

rail.for_each_instance(create_child_dag)
