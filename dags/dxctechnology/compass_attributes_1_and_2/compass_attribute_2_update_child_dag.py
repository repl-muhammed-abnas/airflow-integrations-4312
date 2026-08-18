import rail
from dxctechnology.compass_attributes_1_and_2.utils import request_payload

null = None


def create_attribute_2_update_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_attribute_2_update_task_child_{config.dag_id_postfix}',
        description=f'DXC_Compass_Attribute 1 Child - Update Hierachy Task {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_or_modify_task_hierarchy = rail.RepliconServiceOperator(
            task_id='create_or_modify_task_hierarchy',
            endpoint='/services/TaskService1.svc/CreateTaskOrApplyModifications',
            data=request_payload.get_create_modify_task2
        )

        create_update_timeentry_date_range_batch = rail.RepliconServiceOperator(
            task_id='create_update_timeentry_date_range_batch',
            endpoint='/services/TaskService1.svc/CreateUpdateTimeEntryDateRangeForTaskHierarchyBatch',
            data=request_payload.get_create_update_timeentry_date_range_batch
        )

        create_update_timeentry_date_range_batch_group_entry, create_update_timeentry_date_range_batch_group_exit = rail.batch_execution(
            'execute_create_update_timeentry_date_range_batch', create_update_timeentry_date_range_batch.task_id)

        log_attribute_updated_successfully = rail.WriteLogOperator(
            task_id='log_attribute_updated_successfully',
            message='Attribute updated successfully',
            properties={
                'wbs': '{{ dag_run.conf.projectname }}',
                'attributename': '{{ dag_run.conf.name }}',
                'attributenumber': '{{ dag_run.conf.level }}',
                'action': 'update',
                'status': 'Success',
                'details': 'Attribute updated successfully',
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'wbs': '{{ dag_run.conf.projectname }}',
                'attributename': '{{ dag_run.conf.name }}',
                'attributenumber': '{{ dag_run.conf.level }}',
                'action': 'update',
                'status': 'Error',
                # pylint: disable=line-too-long
                'details': '{{ get_error_message() }}',
            }
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
                # pylint: disable=line-too-long
                'details': '{{ "Attribute updated successfully" if get_task_state("create_or_modify_task_hierarchy") == "success" else "Attribute updation failed" }}',
                'filename': '{{ dag_run.conf.filename }}'
            }
        )

        create_or_modify_task_hierarchy >> create_update_timeentry_date_range_batch \
            >> create_update_timeentry_date_range_batch_group_entry >> create_update_timeentry_date_range_batch_group_exit \
            >> log_attribute_updated_successfully >> finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_attribute_2_update_child_dag)
