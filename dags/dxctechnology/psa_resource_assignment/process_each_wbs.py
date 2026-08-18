from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.psa_resource_assignment.utils import request_payload
from dxctechnology.psa_resource_assignment.utils import python_callable_method
from dxctechnology.psa_resource_assignment.utils import response_filter

null = None

# pylint: disable=too-many-statements


def create_attribute_1_process_wbs_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_psa_resource_process_wbs_{config.dag_id_postfix}',
        description=f'DXC PSA Resource Child - Process each WBS V1.0 {config.dag_id_postfix}',
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

        is_assignment_present = rail.IfOperator(
            task_id='is_assignment_present',
            test=lambda: bool(rail.result('assignment_details')[
                              'startDate'] and rail.result('assignment_details')['endDate']),
            yes_task='is_employeee_present',
            no_task='log_assignment_not_present',
        )

        log_assignment_not_present = rail.WriteLogOperator(
            task_id='log_assignment_not_present',
            message='Gsap PSA Resource Assignment Sync Skipped',
            severity='Success',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'action': 'Validation',
                'status': 'Exception',
                'details': 'Gsap PSA Resource Assignment Sync Skipped - Either Start Date or End Date is not in correct format'
            }
        )

        is_employeee_present = rail.IfOperator(
            task_id='is_employeee_present',
            test='{{ dag_run.conf.useruri | is_truthy}}',
            yes_task='get_project_details_based_on_wbs',
            no_task='log_employee_not_present',
        )

        log_employee_not_present = rail.WriteLogOperator(
            task_id='log_employee_not_present',
            message='Gsap PSA Resource Assignment Sync - Employee is not present in Replicon',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'action': 'Validation',
                'status': 'Exception',
                'details': 'Gsap PSA Resource Assignment Sync - Employee is not present in Replicon'
            }
        )

        get_project_details_based_on_wbs = rail.RepliconServiceOperator(
            task_id='get_project_details_based_on_wbs',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": '{{ dag_run.conf.wbs }}',
                        "code": null,
                        "parameterCorrelationId": null
                    }
                ]
            },
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                                          {"projectDetails": null}])[0]['projectDetails']
        )

        check_wbs_exists = rail.IfOperator(
            task_id='check_wbs_exists',
            test=lambda: bool(rail.result('get_project_details_based_on_wbs') and
                              rail.result(
                'get_project_details_based_on_wbs')['uri']),
            yes_task='check_wbs_is_archived',
            no_task='log_wbs_not_available'
        )

        log_wbs_not_available = rail.WriteLogOperator(
            task_id='log_wbs_not_available',
            message='Failed to sync, since WBS not available in Replicon',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'action': 'Validation',
                'status': 'Exception',
                'details': 'Gsap PSA Resource Assignment Sync - since WBS not available in Replicon'
            }
        )

        check_wbs_is_archived = rail.IfOperator(
            task_id='check_wbs_is_archived',
            test=lambda: rail.result('get_project_details_based_on_wbs')[
                'status']['name'] == 'Archived',
            yes_task='log_wbs_is_archived',
            no_task='get_project_user_division',
        )

        log_wbs_is_archived = rail.WriteLogOperator(
            task_id='log_wbs_is_archived',
            message='Gsap PSA Resource Assignment Sync skipped, since this WBS is in Archive status.',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'action': 'Validation',
                'status': 'Exception',
                'details': 'Gsap PSA Resource Assignment Sync skipped, since this WBS is in Archive status.'
            }
        )

        get_project_user_division = rail.PythonOperator(
            task_id='get_project_user_division',
            python_callable=lambda: python_callable_method.project_division(
                'get_project_details_based_on_wbs')
        )

        check_same_division = rail.IfOperator(
            task_id='check_same_division',
            test=lambda: rail.result('get_project_user_division'),
            yes_task='get_all_project_team_assignment',
            no_task='get_all_filter_defination',
        )

        get_all_filter_defination = rail.RepliconServiceOperator(
            task_id="get_all_filter_defination",
            endpoint="services/ProjectListService1.svc/GetAllFilterDefinitions",
            data={},
            response_filter=response_filter.map_parent_wbs_oef_uri
        )

        get_all_columns = rail.RepliconServiceOperator(
            task_id="get_all_columns",
            endpoint="services/ProjectListService1.svc/GetAllColumns",
            data={},
            response_filter=response_filter.map_parent_column_uri
        )

        get_all_child_wbs_details = rail.RepliconServiceOperator(
            task_id="get_all_child_wbs_details",
            endpoint="services/ProjectListService1.svc/GetData",
            data=request_payload.get_child_wbs_payload,
            response_filter=response_filter.map_child_wbs
        )

        check_child_wbs_exist = rail.IfOperator(
            task_id='check_child_wbs_exist',
            test=lambda: rail.result('get_all_child_wbs_details'),
            yes_task='process_each_child_wbs',
            no_task='log_no_child_wbs_exception',
        )

        log_no_child_wbs_exception = rail.WriteLogOperator(
            task_id='log_no_child_wbs_exception',
            message='Gsap PSA Resource Assignment Sync Skipped',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'action': 'Validation',
                'status': 'Exception',
                # pylint: disable=line-too-long
                'details': "Gsap PSA Resource Assignment Sync Skipped for this User as no child WBS present in the parent"
            }
        )

        get_all_project_team_assignment = rail.RepliconServiceOperator(
            task_id="get_all_project_team_assignment",
            endpoint="/services/ProjectService1.svc/GetAllProjectTeamMemberDetails2",
            data={
                "projectUri": "{{ result('get_project_details_based_on_wbs')['uri'] }}",
                "asOfDate": null
            },
            response_filter=response_filter.map_resource_assignment_list
        )

        is_uri_present = rail.IfOperator(
            task_id='is_uri_present',
            test=lambda: len(rail.result(
                'get_all_project_team_assignment')) > 0,
            yes_task="log_update_user_success",
            no_task="assign_user_to_project"
        )

        updateProjectTeamMemberAssignmentDateRange = rail.RepliconServiceOperator(
            task_id="updateProjectTeamMemberAssignmentDateRange",
            endpoint='/services/ProjectService1.svc/UpdateProjectTeamMemberAssignmentDateRange',
            data=lambda dag_run: request_payload.get_assignmentdaterange_payload(
                dag_run, 'get_project_details_based_on_wbs')
        )

        assign_user_to_project = rail.RepliconServiceOperator(
            task_id="assign_user_to_project",
            endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
            data=lambda dag_run: request_payload.get_assign_user_payload(
                dag_run, 'get_project_details_based_on_wbs')
        )

        log_add_user_success = rail.WriteLogOperator(
            task_id='log_add_user_success',
            message='Gsap PSA Resource Assignment Sync Successful',
            severity='Success',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'action': 'Add',
                'status': 'Success',
                'details': 'Gsap PSA Resource Assignment Sync Successful for this User'
            }
        )

        log_update_user_success = rail.WriteLogOperator(
            task_id='log_update_user_success',
            message='Gsap PSA Resource Assignment Sync Successful',
            severity='Success',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'action': 'Update',
                'status': 'Success',
                'details': 'Gsap PSA Resource Assignment Sync Successful for this User'
            }
        )

        process_each_child_wbs = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_child_wbs',
            retries=0,
            items=lambda: rail.result('get_all_child_wbs_details'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_psa_resource_process_child_wbs_{config.dag_id_postfix}',
            conf=request_payload.get_process_each_child_wbs
        )

        wait_for_process_each_child_wbs = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_child_wbs',
            dag_runs='{{ result("process_each_child_wbs") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'action': 'Sync',
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
            "No") >> assignment_details >> is_assignment_present
        is_assignment_present >> rail.Label("Yes") >> is_employeee_present
        is_assignment_present >> rail.Label(
            "No") >> log_assignment_not_present >> catch_and_log_errors
        is_employeee_present >> rail.Label(
            "Yes") >> get_project_details_based_on_wbs
        is_employeee_present >> rail.Label(
            "No") >> log_employee_not_present >> catch_and_log_errors
        get_project_details_based_on_wbs >> check_wbs_exists

        check_wbs_exists >> rail.Label(
            'Yes') >> log_wbs_not_available >> catch_and_log_errors
        check_wbs_exists >> rail.Label('No') >> check_wbs_is_archived

        check_wbs_is_archived >> rail.Label(
            'Yes') >> log_wbs_is_archived >> catch_and_log_errors
        check_wbs_is_archived >> rail.Label(
            'No') >> get_project_user_division >> check_same_division
        check_same_division >> rail.Label(
            "Yes") >> get_all_project_team_assignment >> is_uri_present
        check_same_division >> rail.Label("No") >> get_all_filter_defination
        get_all_filter_defination >> get_all_columns >> get_all_child_wbs_details >> check_child_wbs_exist
        check_child_wbs_exist >> rail.Label(
            "Yes") >> process_each_child_wbs >> wait_for_process_each_child_wbs >> catch_and_log_errors
        check_child_wbs_exist >> rail.Label(
            "No") >> log_no_child_wbs_exception >> catch_and_log_errors
        is_uri_present >> rail.Label(
            "Yes") >> log_update_user_success >> updateProjectTeamMemberAssignmentDateRange
        is_uri_present >> rail.Label(
            "No") >> assign_user_to_project >> log_add_user_success >> updateProjectTeamMemberAssignmentDateRange >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_attribute_1_process_wbs_child_dag)
