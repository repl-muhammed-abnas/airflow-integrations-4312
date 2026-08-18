import rail
from galaxyusopcoinc.workday_user_sync.time_off_plan_v1.utils import request_payload
from galaxyusopcoinc.workday_user_sync.time_off_plan_v1.utils import custom_method


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_user_timeoff_dag_id,
        description=f'Vialto Partners Update Each User Time Off Type V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.assign_newly_created_timeoff_type_max_active_run
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_conf_time_off_types_uri = rail.RepliconServiceOperator(
            task_id='get_conf_time_off_types_uri',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            response_filter=custom_method.map_conf_time_off_uri
        )

        get_all_assigned_time_off_type_user = rail.RepliconServiceOperator(
            task_id='get_all_assigned_time_off_type_user',
            endpoint='/services/TimeOffService1.svc/BulkGetTimeOffTypeAssignmentsForUsers',
            data={
                "userUris": ["{{ dag_run.conf.useruri }}"]
            },
            response_filter=custom_method.map_assigned_time_off_uri
        )

        put_time_off_type_user = rail.RepliconServiceOperator(
            task_id='put_time_off_type_user',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=request_payload.get_put_time_off_type_payload
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'time_off_type_desc': "NA",
                'time_off_type_name': "{{ dag_run.conf.time_off_types}}",
                'unit_of_time': "NA",
                'country': "{{ dag_run.conf.country}}",
                'status': 'Error'
            },
        )

        get_conf_time_off_types_uri >> get_all_assigned_time_off_type_user >> put_time_off_type_user
        put_time_off_type_user >> catch_and_log_errors

        return dag


rail.for_each_instance(create_child_dag)
