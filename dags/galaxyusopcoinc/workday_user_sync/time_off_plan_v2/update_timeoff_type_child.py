import rail
from galaxyusopcoinc.workday_user_sync.time_off_plan_v2.utils import request_payload

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_timeoff_type_dag_id,
        description=f'Vialto Partners Time Off Enable Child V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.enable_timeoff_type_max_active_run
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        update_timeoff_name = rail.RepliconServiceOperator(
            task_id = "update_timeoff_name",
            endpoint="/services/TimeOffService1.svc/PutTimeOffType",
            data = request_payload.get_update_timeoff_name
        )

        log_update_success = rail.WriteLogOperator(
            task_id='log_update_success',
            message='Time Off Type is Updated in Replicon',
            severity='Success',
            properties={
                'time_off_type_desc': "{{ dag_run.conf.timeoff_description}}",
                'time_off_type_name': "{{ dag_run.conf.time_off_type_name}}",
                'unit_of_time': "{{ dag_run.conf.unit_of_time}}",
                'country': "{{ dag_run.conf.country}}",
                'status': 'Success'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                'time_off_type_desc': "{{ dag_run.conf.timeoff_description}}",
                'time_off_type_name': "{{ dag_run.conf.time_off_type_name}}",
                'unit_of_time': "{{ dag_run.conf.unit_of_time}}",
                'country': "{{ dag_run.conf.country}}",
                'status': 'Error'
            },
        )

        update_timeoff_name >> log_update_success >> rail.Label("On Error") >> catch_and_log_errors

        return dag

rail.for_each_instance(create_child_dag)
