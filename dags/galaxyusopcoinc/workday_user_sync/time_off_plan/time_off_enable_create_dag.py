import rail
from galaxyusopcoinc.workday_user_sync.time_off_plan.utils import request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_time_off_enable_create_child_{config.instance}',
        description=f'Vialto Partners Time Off Enable Or Create Child V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_enable_create_action = rail.IfOperator(
            task_id='is_enable_create_action',
            test=lambda: request_payload.get_dag_run_conf()['action'] == "enabled",
            yes_task='enable_time_off_type',
            no_task='put_time_off_type'
        )

        enable_time_off_type = rail.RepliconServiceOperator(
            task_id='enable_time_off_type',
            endpoint='/services/TimeOffService1.svc/EnableTimeOffType',
            data={
                "timeOffTypeUri": "{{ dag_run.conf.uri}}"
            }
        )

        log_enabled_success = rail.WriteLogOperator(
            task_id='log_enabled_success',
            message='Time Off Type is Enabled in Replicon',
            severity='Success',
            properties={
                'time_off_type_desc': "{{ dag_run.conf.time_off_type_desc}}",
                'time_off_type_name': "{{ dag_run.conf.time_off_type_name}}",
                'unit_of_time': "{{ dag_run.conf.unit_of_time}}",
                'country': "{{ dag_run.conf.country}}",
                'status': 'Success'
            }
        )

        put_time_off_type = rail.RepliconServiceOperator(
            task_id='put_time_off_type',
            endpoint='/services/TimeOffService1.svc/PutTimeOffType',
            data=request_payload.get_put_time_off_type_data
        )

        log_create_success = rail.WriteLogOperator(
            task_id='log_create_success',
            message='Time Off Type is Created in Replicon',
            severity='Success',
            properties={
                'time_off_type_desc': "{{ dag_run.conf.time_off_type_desc}}",
                'time_off_type_name': "{{ dag_run.conf.time_off_type_name}}",
                'unit_of_time': "{{ dag_run.conf.unit_of_time}}",
                'country': "{{ dag_run.conf.country}}",
                'status': 'Success'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'time_off_type_desc': "{{ dag_run.conf.time_off_type_desc}}",
                'time_off_type_name': "{{ dag_run.conf.time_off_type_name}}",
                'unit_of_time': "{{ dag_run.conf.unit_of_time}}",
                'country': "{{ dag_run.conf.country}}",
                'status': 'Error'
            },
        )

        is_enable_create_action >> rail.Label("Yes") >> enable_time_off_type
        is_enable_create_action >> rail.Label("No") >> put_time_off_type
        put_time_off_type >> log_create_success >> catch_and_log_errors
        enable_time_off_type >> log_enabled_success >> catch_and_log_errors

        return dag


rail.for_each_instance(create_child_dag)
