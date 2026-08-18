import rail
from dxctechnology.compass_attributes_1_and_2.utils import request_payload

null = None


def create_attribute_1_create_task_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_attribute_1_create_task_child_{config.dag_id_postfix}',
        description=f'DXC_Compass_Attribute 1 Child - Create Task {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_iwo_wbs_projecturi_present = rail.IfOperator(
            task_id='is_iwo_wbs_projecturi_present',
            test='{{ dag_run.conf.iwowbsprojecturi | is_falsy }}',
            yes_task='put_task_from_wbs',
            no_task='finish'
        )

        put_task_from_wbs = rail.RepliconServiceOperator(
            task_id='put_task_from_wbs',
            endpoint='/services/ProjectService1.svc/PutTask',
            data=lambda dag_run: request_payload.get_put_task_data(
                dag_run, False),
        )

        log_attribute_1_create = rail.WriteLogOperator(
            task_id='log_attribute_1_create',
            message='Attribute added successfully',
            properties={
                'wbs': '{{ dag_run.conf.projectname }}',
                'attributename': '{{ dag_run.conf.name }}',
                'attributenumber': '{{ dag_run.conf.level }}',
                'action': 'add',
                'status': 'Success',
                'details': 'Attribute added successfully',
                'recordcount': ''
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'wbs': '{{ dag_run.conf.projectname }}',
                'attributename': '{{ dag_run.conf.name }}',
                'attributenumber': '{{ dag_run.conf.level }}',
                'action': 'add',
                'status': 'Error',
                # pylint: disable=line-too-long
                'details': '{{ get_error_message() }}',
                'recordcount': ''
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'wbs ': '{{ dag_run.conf.projectname }}',
                'attribute': '{{ dag_run.conf.name  }}',
                'level': '{{ dag_run.conf.level  }}',
                'enddate': '{{ dag_run.conf.enddate  }}',
                'usercount': '{{ dag_run.conf.userlist | length }}',
                'iwousercount': '{{ dag_run.conf.iwouserlist | length }}',
                'details': '{{ "Attribute added successfully" if get_task_state("put_task_from_wbs") == "success" else "Attribute addition failed" }}',
                'filename': '{{ dag_run.conf.filename }}'
            }
        )

        is_iwo_wbs_projecturi_present >> rail.Label(
            'Yes') >> put_task_from_wbs >> log_attribute_1_create >> finish
        is_iwo_wbs_projecturi_present >> rail.Label(
            'No') >> finish

        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_attribute_1_create_task_child_dag)
