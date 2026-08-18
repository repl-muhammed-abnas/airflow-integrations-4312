from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.gsap_iwo_resource_assignment_v1.utils import request_payload
from dxctechnology.gsap_iwo_resource_assignment_v1.utils import response_filter
from dxctechnology.gsap_iwo_resource_assignment_v1.utils import python_callable_method

null = None


def create_attribute_1_create_task_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_gsap_assignment_dag_id,
        description=f'DXC_GSAB IWO Resource Child - GSAP Resource Task {config.dag_id_postfix}',
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
            no_task='is_employee_present_replicon'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_employee_present_replicon',
            end_task='catch_and_log_errors',
        )

        is_employee_present_replicon = rail.IfOperator(
            task_id="is_employee_present_replicon",
            test="{{ dag_run.conf.useruri | is_truthy }}",
            no_task="log_user_validation_error",
            yes_task="get_project_info_based_on_wbs_element"
        )

        log_user_validation_error = rail.WriteLogOperator(
            task_id='log_user_validation_error',
            log='{{ dag_run.conf.log_artifact }}',
            message="Required user \"'{{dag_run.conf.empid}}'\" is not available in Replicon",
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'taskcode': '',
                'action': 'pre-check',
                'status': 'skipped',
                'details': 'Gsap IWO Resource Assignment skipped, since this Employee not present in Replicon.'
            }
        )

        get_project_info_based_on_wbs_element = rail.RepliconServiceOperator(
            task_id='get_project_info_based_on_wbs_element',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=request_payload.get_project_details_payload
        )

        put_key_value_project = rail.RepliconServiceOperator(
            task_id='put_key_value_project',
            endpoint='/services/ProjectService1.svc/PutKeyValueForProject',
            data=request_payload.get_put_key_value_project
        )

        get_all_project_team_assignment = rail.RepliconServiceOperator(
            task_id="get_all_project_team_assignment",
            endpoint="/services/ProjectService1.svc/GetAllProjectTeamMemberDetails2",
            data={
                "projectUri": "{{ result('get_project_info_based_on_wbs_element')[0]['projectDetails']['uri'] }}",
                "asOfDate": null
            },
            response_filter=response_filter.map_resource_assignment_list
        )

        assignment_details = rail.PythonOperator(
            task_id="assignment_details",
            python_callable=python_callable_method.assigment_json_details,
        )

        is_uri_present = rail.IfOperator(
            task_id='is_uri_present',
            test=lambda: len(rail.result(
                'get_all_project_team_assignment')) > 0,
            yes_task="updateProjectTeamMemberAssignmentDateRange",
            no_task="assign_user_to_project"
        )

        updateProjectTeamMemberAssignmentDateRange = rail.RepliconServiceOperator(
            task_id="updateProjectTeamMemberAssignmentDateRange",
            endpoint='/services/ProjectService1.svc/UpdateProjectTeamMemberAssignmentDateRange',
            data=request_payload.get_assignmentdaterange_payload
        )

        assign_user_to_project = rail.RepliconServiceOperator(
            task_id="assign_user_to_project",
            endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
            data=request_payload.get_assign_user_payload
        )

        get_children_task_details = rail.RepliconServiceOperator(
            task_id='get_children_task_details',
            endpoint='/services/TaskService1.svc/GetChildrenTaskDetails',
            data={
                'parentUri': '{{ result("get_project_info_based_on_wbs_element")[0]["projectDetails"]["uri"] }}'
            }
        )

        get_tasks_from_project = rail.PythonOperator(
            task_id='get_tasks_from_project',
            python_callable=python_callable_method.retrieve_task_list,
            op_args=['get_children_task_details']
        )

        tasks_from_project_collection = rail.CreateCollectionOperator(
            task_id='tasks_from_project_collection',
            source='{{ result("get_tasks_from_project") | to_json }}',
            columns=[
                'name',
                'code',
                'enddate',
                'oef',
                'uri'],
            name='parent_task_project'
        )

        query_parent_task_list = rail.QueryCollectionOperator(
            task_id='query_parent_task_list',
            query="""SELECT * FROM parent_task_project WHERE oef = 'GSAP Billing Key' AND name = :task """,
            query_params={
                "task": "{{dag_run.conf.taskName}}"
            }
        )

        is_name_present_in_create_list = rail.IfOperator(
            task_id="is_name_present_in_create_list",
            test="{{result('query_parent_task_list', 'length') > 0 }}",
            no_task="catch_and_log_errors",
            yes_task="assign_resource_task_level"
        )

        assign_resource_task_level = rail.TriggerDagRunForEachItemOperator(
            task_id='assign_resource_task_level',
            retries=0,
            items="{{ result('query_parent_task_list') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_gsap_resource_dag_id,
            conf=request_payload.get_gsap_conf
        )

        wait_for_assign_resource_task_level = rail.WaitForDagRunsSensor(
            task_id='wait_for_assign_resource_task_level',
            dag_runs='{{ result("assign_resource_task_level") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        log_success_record = rail.WriteLogOperator(
            task_id='log_success_record',
            log='{{ dag_run.conf.log_artifact }}',
            message='Gsap IWO Resource Assignment Sync successfull',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'taskcode': '',
                'action': 'Sync',
                'status': 'Success',
                'details': 'Gsap IWO Resource Assignment Sync successfull'
            }
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
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> is_employee_present_replicon
        is_employee_present_replicon >> rail.Label(
            "No") >> log_user_validation_error >> catch_and_log_errors
        is_employee_present_replicon >> rail.Label(
            "Yes") >> get_project_info_based_on_wbs_element >> put_key_value_project >> get_all_project_team_assignment >> assignment_details >> is_uri_present
        is_uri_present >> rail.Label(
            "Yes") >> updateProjectTeamMemberAssignmentDateRange
        is_uri_present >> rail.Label(
            "No") >> assign_user_to_project >> updateProjectTeamMemberAssignmentDateRange >> get_children_task_details
        get_children_task_details >> get_tasks_from_project >> tasks_from_project_collection >> query_parent_task_list
        query_parent_task_list >> is_name_present_in_create_list >> rail.Label(
            'Yes') >> assign_resource_task_level >> wait_for_assign_resource_task_level \
                >> log_success_record >> catch_and_log_errors
        is_name_present_in_create_list >> rail.Label(
            'No') >> catch_and_log_errors

        catch_and_log_errors

    return dag


rail.for_each_instance(create_attribute_1_create_task_child_dag)
