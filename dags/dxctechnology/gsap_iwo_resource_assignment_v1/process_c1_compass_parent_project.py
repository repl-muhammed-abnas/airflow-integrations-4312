from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.gsap_iwo_resource_assignment_v1.utils import request_payload
from dxctechnology.gsap_iwo_resource_assignment_v1.utils import python_callable_method
from dxctechnology.gsap_iwo_resource_assignment_v1.utils import response_filter

null = None


def create_attribute_1_process_child_wbs_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_c1_compass_assignment_dag_id,
        description=f'DXC_GSAB IWO Resource Child - Process C1 Compass Assignment {config.dag_id_postfix}',
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
                'status': 'Exception',
                'details': 'Gsap IWO Resource Assignment skipped, since this Employee not present in Replicon.'
            }
        )

        get_project_info_based_on_wbs_element = rail.RepliconServiceOperator(
            task_id='get_project_info_based_on_wbs_element',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=request_payload.get_project_details_payload
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

        log_success_record = rail.WriteLogOperator(
            task_id='log_success_record',
            log='{{ dag_run.conf.log_artifact }}',
            message='Gsap IWO Resource Assignment Sync successfull',
            severity='Success',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'taskcode': '',
                'action': 'sync',
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
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            "No") >> is_employee_present_replicon
        is_employee_present_replicon >> rail.Label(
            "No") >> log_user_validation_error >> catch_and_log_errors
        is_employee_present_replicon >> rail.Label(
            "Yes") >> get_project_info_based_on_wbs_element
        get_project_info_based_on_wbs_element >> get_all_project_team_assignment >> assignment_details >> is_uri_present
        is_uri_present >> rail.Label(
            "Yes") >> updateProjectTeamMemberAssignmentDateRange
        is_uri_present >> rail.Label(
            "No") >> assign_user_to_project >> updateProjectTeamMemberAssignmentDateRange >> log_success_record >> catch_and_log_errors
        catch_and_log_errors

    return dag


rail.for_each_instance(create_attribute_1_process_child_wbs_dag)
