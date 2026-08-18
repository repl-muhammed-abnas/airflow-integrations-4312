from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.gsap_iwo_resource_assignment.utils import request_payload
from dxctechnology.gsap_iwo_resource_assignment.utils import python_callable_method

null = None


def create_attribute_1_process_child_wbs_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsab_iwo_resource_assign_resource_task_{config.dag_id_postfix}',
        description=f'DXC_GSAB IWO Resource Child - GSAP resource Assignment {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='assignment_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='assignment_details',
            end_task='catch_and_log_errors',
        )

        assignment_details = rail.PythonOperator(
            task_id="assignment_details",
            python_callable=python_callable_method.assigment_json_details,
        )

        update_date_to_parent_task = rail.RepliconServiceOperator(
            task_id="update_date_to_parent_task",
            endpoint="/services/ResourceService1.svc/PutResourceTaskAllocationsForTask",
            data=request_payload.get_update_date_to_parent_task
        )

        get_children_task_details = rail.RepliconServiceOperator(
            task_id='get_children_task_details',
            endpoint='/services/TaskService1.svc/GetChildrenTaskDetails',
            data={
                'parentUri': '{{ dag_run.conf.parentTaskUri }}'
            }
        )

        get_child_tasks_from_project = rail.PythonOperator(
            task_id='get_child_tasks_from_project',
            python_callable=python_callable_method.retrieve_task_list,
            op_args=['get_children_task_details']
        )

        child_tasks_from_project_collection = rail.CreateCollectionOperator(
            task_id='child_tasks_from_project_collection',
            source='{{ result("get_child_tasks_from_project") | to_json }}',
            columns=[
                'name',
                'code',
                'enddate',
                'oef',
                'uri'],
            name='parent_child_task_project'
        )

        query_parent_child_task_list = rail.QueryCollectionOperator(
            task_id='query_parent_child_task_list',
            query="""SELECT * FROM parent_child_task_project"""
        )

        is_child_task_present = rail.IfOperator(
            task_id="is_child_task_present",
            test="{{result('query_parent_child_task_list', 'length') > 0 }}",
            no_task="catch_and_log_errors",
            yes_task="assign_resource_child_task_level"
        )

        assign_resource_child_task_level = rail.TriggerDagRunForEachItemOperator(
            task_id='assign_resource_child_task_level',
            retries=0,
            items="{{ result('query_parent_child_task_list') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_gsab_iwo_resource_assign_child_resource_task_{config.dag_id_postfix}',
            conf=request_payload.get_gsap_child_conf
        )

        wait_for_assign_resource_child_task_level = rail.WaitForDagRunsSensor(
            task_id='wait_for_assign_resource_child_task_level',
            dag_runs='{{ result("assign_resource_child_task_level") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ dag_run.conf.log_artifact }}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            "No") >> assignment_details >> update_date_to_parent_task >> get_children_task_details >> get_child_tasks_from_project
        get_child_tasks_from_project >> child_tasks_from_project_collection >> query_parent_child_task_list >> is_child_task_present
        is_child_task_present >> rail.Label(
            "Yes") >> assign_resource_child_task_level >> wait_for_assign_resource_child_task_level \
                >> catch_and_log_errors
        is_child_task_present >> rail.Label("No") >> catch_and_log_errors

        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_attribute_1_process_child_wbs_dag)
