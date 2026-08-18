from datetime import timedelta
import rail
from dxctechnology.compass_attributes_1_and_2.utils import request_payload
from dxctechnology.compass_attributes_1_and_2.utils import response_filter


def create_attribute_1_process_update_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_attribute_1_process_update_task_hierarchy_child_{config.dag_id_postfix}',
        description=f'DXC_Compass_Attribute 1 Child - Update Hierachy Task {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_or_modify_task_hierarchy = rail.RepliconServiceOperator(
            task_id='create_or_modify_task_hierarchy',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            data=request_payload.get_create_modify_task_hierarchy,
            response_filter=response_filter.get_success_error_messages
        )

        get_task_descendants_details_for_update = rail.RepliconServiceOperator(
            task_id='get_task_descendants_details_for_update',
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data={'parentUri': '{{ dag_run.conf["data"][0]["uri"] }}'},
            response_filter=response_filter.get_all_child_tasks_for_update
        )

        update_existing_attribute_1_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_existing_attribute_1_tasks',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            endpoint='/services/TaskService1.svc/CreateTaskOrApplyModifications',
            items=lambda: rail.result(
                'get_task_descendants_details_for_update')['child_tasks'],
            data=request_payload.get_update_task_data,
        )

        update_existing_child_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_existing_child_tasks',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            endpoint='/services/TaskService1.svc/CreateTaskOrApplyModifications',
            items=lambda: rail.result(
                'get_task_descendants_details_for_update')['child_child_tasks'],
            data=request_payload.get_update_task_data,
        )

        update_existing_child_child_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_existing_child_child_tasks',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            endpoint='/services/TaskService1.svc/CreateTaskOrApplyModifications',
            items=lambda: rail.result(
                'get_task_descendants_details_for_update')['child_child_child_tasks'],
            data=request_payload.get_update_task_data,
        )

        log_attribute_updated_successfully = rail.WriteLogOperator(
            task_id="log_attribute_updated_successfully",
            message='Attribute updated successfully',
            items='{{ result("create_or_modify_task_hierarchy")["success"] | to_json }}',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '{{ item.attributename }}',
                'attributenumber': '1',
                'action': 'update',
                'status': 'Success',
                'details': 'Attribute updated successfully',
                'recordcount': ''
            }
        )

        log_attribute_updated_error = rail.WriteLogOperator(
            task_id="log_attribute_updated_error",
            message='{{ result("create_or_modify_task_hierarchy") }}',
            items='{{ result("create_or_modify_task_hierarchy")["error"] | to_json }}',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '{{ item.attributename }}',
                'attributenumber': '1',
                'action': 'update',
                'status': 'Error',
                'details': '{{ item.message }}',
                'recordcount': ''
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
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '{{ dag_run.conf.data[0]["name"] }}',
                'attributenumber': '1',
                'action': 'update',
                'status': 'Error',
                # pylint: disable=line-too-long
                'details': '{{ get_error_message() }}',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'wbs ': '{{ dag_run.conf.wbs }}',
                'attribute': '{{ dag_run.conf.data[0]["name"] }}',
                'level': '1',
                'enddate': '{{ dag_run.conf.projectenddate  }}',
                'attributecount': '{{ dag_run.conf.data | length }}',
                # pylint: disable=line-too-long
                'filename': '{{ dag_run.conf.filename }}'
            }
        )

        create_or_modify_task_hierarchy >> get_task_descendants_details_for_update \
            >> update_existing_attribute_1_tasks >> update_existing_child_tasks \
            >> update_existing_child_child_tasks >> [log_attribute_updated_successfully, log_attribute_updated_error] \
            >> finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_attribute_1_process_update_child_dag)
