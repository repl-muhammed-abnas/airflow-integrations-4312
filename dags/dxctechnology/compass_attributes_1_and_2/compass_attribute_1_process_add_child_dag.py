import rail
from dxctechnology.compass_attributes_1_and_2.utils import request_payload
from dxctechnology.compass_attributes_1_and_2.utils import response_filter

null = None


def create_attribute_1_process_add_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_attribute_1_task_hierachy_add_child_{config.dag_id_postfix}',
        description=f'DXC_Compass_Attribute 1 Child - Add Hierachy Task {config.dag_id_postfix}',
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

        log_add_hierarchy_task_success = rail.WriteLogOperator(
            task_id='log_add_hierarchy_task_success',
            message='Attribute added successfully',
            items='{{ result("create_or_modify_task_hierarchy")["success"] | to_json }}',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '{{ item.attributename }}',
                'attributenumber': '1',
                'action': 'add',
                'status': 'Success',
                'details': 'Attribute added successfully',
                'recordcount': ''
            }
        )

        log_add_hierarchy_task_error = rail.WriteLogOperator(
            task_id="log_add_hierarchy_task_error",
            message='{{ result("create_or_modify_task_hierarchy") }}',
            items='{{ result("create_or_modify_task_hierarchy")["error"] | to_json }}',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '{{ item.attributename }}',
                'attributenumber': '1',
                'action': 'add',
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
            severity='Error',
            # pylint: disable=line-too-long
            items='{{  dag_run.conf.data  | to_json }}',
            message='{{ get_error_message() }}',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '{{ item["name"] }}',
                'attributenumber': '1',
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
                'wbs ': '{{ dag_run.conf.wbs }}',
                'attribute': '{{ dag_run.conf["data"][0]["name"] }}',
                'level': '1',
                'enddate': '{{ dag_run.conf.projectenddate  }}',
                'attributecount': '{{ dag_run.conf.data | length }}',
                # pylint: disable=line-too-long
                'details': '{{ "Attribute added successfully" if get_task_state("create_or_modify_task_hierarchy") == "success" else "Attribute addition failed" }}',
                'filename': '{{ dag_run.conf.filename }}'
            }
        )

        create_or_modify_task_hierarchy >> log_add_hierarchy_task_success >> log_add_hierarchy_task_error \
            >> finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_attribute_1_process_add_child_dag)
