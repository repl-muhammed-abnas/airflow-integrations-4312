from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.gsap_iwo_resource_assignment_v1.utils import request_payload
from dxctechnology.gsap_iwo_resource_assignment_v1.utils import python_callable_method

null = None


def create_attribute_1_process_child_wbs_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_gsap_resource_dag_id,
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
            yes_task="for_each_child_task"
        )

        # Process child tasks inline using ForEachOperator instead of separate child DAG
        for_each_child_task = rail.ForEachOperator(
            task_id='for_each_child_task',
            items="{{ result('query_parent_child_task_list') }}",
            start_task='update_date_to_child_task',
            end_task='for_each_child_task_end'
        )

        # Inline child task update - eliminates separate child DAG
        update_date_to_child_task = rail.RepliconServiceOperator(
            task_id='update_date_to_child_task',
            endpoint="/services/ResourceService1.svc/PutResourceTaskAllocationsForTask",
            data=request_payload.get_update_date_to_child_task_inline
        )

        for_each_child_task_end = rail.EmptyOperator(
            task_id='for_each_child_task_end'
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

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            "No") >> assignment_details >> update_date_to_parent_task >> get_children_task_details >> get_child_tasks_from_project
        get_child_tasks_from_project >> child_tasks_from_project_collection >> query_parent_child_task_list >> is_child_task_present

        # ForEach loop for child tasks - replaces child DAG triggering
        is_child_task_present >> rail.Label(
            "Yes") >> for_each_child_task >> update_date_to_child_task >> for_each_child_task_end
        for_each_child_task >> for_each_child_task_end
        for_each_child_task_end >> catch_and_log_errors

        is_child_task_present >> rail.Label("No") >> catch_and_log_errors

        catch_and_log_errors

    return dag


rail.for_each_instance(create_attribute_1_process_child_wbs_dag)
