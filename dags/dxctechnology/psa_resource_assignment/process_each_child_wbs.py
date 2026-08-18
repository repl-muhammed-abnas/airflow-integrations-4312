from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.psa_resource_assignment.utils import request_payload
from dxctechnology.psa_resource_assignment.utils import python_callable_method
from dxctechnology.psa_resource_assignment.utils import response_filter

null = None


def create_attribute_1_process_child_wbs_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_psa_resource_process_child_wbs_{config.dag_id_postfix}',
        description=f'DXC PSA Resource Child - PSA Child Resource Assignment {config.dag_id_postfix}',
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
            no_task='get_child_project_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_child_project_details',
            end_task='catch_and_log_errors',
        )

        get_child_project_details = rail.RepliconServiceOperator(
            task_id='get_child_project_details',
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

        get_project_wbs_type = rail.PythonOperator(
            task_id='get_project_wbs_type',
            python_callable=python_callable_method.project_wbs_type
        )

        check_child_wbs_type_diwo = rail.IfOperator(
            task_id='check_child_wbs_type_diwo',
            test=lambda: rail.result('get_project_wbs_type') == "DIWO",
            yes_task='check_wbs_exists',
            no_task='log_invalid_user_exception',
        )

        check_wbs_exists = rail.IfOperator(
            task_id='check_wbs_exists',
            test=lambda: bool(rail.result('get_child_project_details') and
                              rail.result(
                'get_child_project_details')['uri']),
            yes_task='check_wbs_is_archived',
            no_task='log_wbs_not_available'
        )

        log_wbs_not_available = rail.WriteLogOperator(
            task_id='log_wbs_not_available',
            message='Failed to sync, since Child WBS {{dag_run.conf.wbs}} not available in Replicon',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.parentWbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'action': 'Validation',
                'status': 'Exception',
                'details': 'Gsap PSA Resource Assignment Sync Skipped for this record - since Child WBS {{dag_run.conf.wbs}} not available in Replicon'
            }
        )

        check_wbs_is_archived = rail.IfOperator(
            task_id='check_wbs_is_archived',
            test=lambda: rail.result('get_child_project_details')[
                'status']['name'] == 'Archived',
            yes_task='log_wbs_is_archived',
            no_task='get_child_project_user_division',
        )

        log_wbs_is_archived = rail.WriteLogOperator(
            task_id='log_wbs_is_archived',
            # pylint: disable=line-too-long
            message='Gsap PSA Resource Assignment Sync skipped, since this Child WBS {{dag_run.conf.wbs}} is in Archive status.',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.parentWbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'action': 'Validation',
                'status': 'Exception',
                'details': 'Gsap PSA Resource Assignment Sync skipped, since this Child WBS {{dag_run.conf.wbs}} is in Archive status.'
            }
        )

        get_child_project_user_division = rail.PythonOperator(
            task_id='get_child_project_user_division',
            python_callable=lambda: python_callable_method.project_division(
                'get_child_project_details')
        )

        check_child_same_division = rail.IfOperator(
            task_id='check_child_same_division',
            test=lambda: rail.result('get_child_project_user_division'),
            yes_task='get_child_project_team_assignment',
            no_task='log_division_exception'
        )

        log_division_exception = rail.WriteLogOperator(
            task_id='log_division_exception',
            message='Company code of Child WBS {{dag_run.conf.wbs}} does not match with User company code',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.parentWbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'action': 'Validation',
                'status': 'Exception',
                'details': 'Company code of Child WBS {{dag_run.conf.wbs}} does not match with User company code'
            }
        )

        get_child_project_team_assignment = rail.RepliconServiceOperator(
            task_id="get_child_project_team_assignment",
            endpoint="/services/ProjectService1.svc/GetAllProjectTeamMemberDetails2",
            data={
                "projectUri": "{{ result('get_child_project_details')['uri'] }}",
                "asOfDate": null
            },
            response_filter=response_filter.map_resource_assignment_list
        )

        assignment_details = rail.PythonOperator(
            task_id="assignment_details",
            python_callable=python_callable_method.assigment_json_details,
        )

        is_child_uri_present = rail.IfOperator(
            task_id='is_child_uri_present',
            test=lambda: len(rail.result(
                'get_child_project_team_assignment')) > 0,
            yes_task="log_update_user_success",
            no_task="assign_user_to_project"
        )

        log_update_user_success = rail.WriteLogOperator(
            task_id='log_update_user_success',
            message='Gsap PSA Resource Assignment Sync Successful',
            severity='Success',
            properties={
                'wbs': '{{ dag_run.conf.parentWbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'action': 'Update',
                'status': 'Success',
                'details': 'Gsap PSA Resource Assignment Sync Successful for this User on Child WBS {{dag_run.conf.wbs}}'
            }
        )

        updateChildProjectTeamMemberAssignmentDateRange = rail.RepliconServiceOperator(
            task_id="updateChildProjectTeamMemberAssignmentDateRange",
            endpoint='/services/ProjectService1.svc/UpdateProjectTeamMemberAssignmentDateRange',
            data=lambda dag_run: request_payload.get_assignmentdaterange_payload(
                dag_run, 'get_child_project_details')
        )

        assign_user_to_project = rail.RepliconServiceOperator(
            task_id="assign_user_to_project",
            endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
            data=lambda dag_run: request_payload.get_assign_user_payload(
                dag_run, 'get_child_project_details')
        )

        log_add_user_success = rail.WriteLogOperator(
            task_id='log_add_user_success',
            message='Gsap PSA Resource Assignment Sync Successful',
            severity='Success',
            properties={
                'wbs': '{{ dag_run.conf.parentWbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'action': 'Add',
                'status': 'Success',
                'details': 'Gsap PSA Resource Assignment Sync Successful for this User on Child WBS {{dag_run.conf.wbs}}'
            }
        )

        log_invalid_user_exception = rail.WriteLogOperator(
            task_id='log_invalid_user_exception',
            message='Gsap PSA Resource Assignment Sync Skipped',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.parentWbs }}',
                'empid': '{{ dag_run.conf.empid }}',
                'action': 'Validation',
                'status': 'Exception',
                'details': "Gsap PSA Resource Assignment Sync Skipped for this User as Child WBS - {{dag_run.conf.wbs}}, Type is not DIWO"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'wbs': '{{ dag_run.conf.parentWbs }}',
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
            "No") >> get_child_project_details >> get_project_wbs_type >> check_child_wbs_type_diwo >> rail.Label("Yes") >> check_wbs_exists
        check_child_wbs_type_diwo >> rail.Label(
            "No") >> log_invalid_user_exception >> catch_and_log_errors
        check_wbs_exists >> rail.Label("Yes") >> check_wbs_is_archived
        check_wbs_exists >> rail.Label(
            "No") >> log_wbs_not_available >> catch_and_log_errors
        check_wbs_is_archived >> rail.Label(
            "Yes") >> log_wbs_is_archived >> catch_and_log_errors
        check_wbs_is_archived >> rail.Label(
            "No") >> get_child_project_user_division >> check_child_same_division
        check_child_same_division >> rail.Label('No') >> log_division_exception >> catch_and_log_errors
        check_child_same_division >> rail.Label(
            "Yes") >> get_child_project_team_assignment >> assignment_details >> is_child_uri_present
        is_child_uri_present >> rail.Label(
            "Yes") >> log_update_user_success >> updateChildProjectTeamMemberAssignmentDateRange
        is_child_uri_present >> rail.Label(
            "No") >> assign_user_to_project >> log_add_user_success >> updateChildProjectTeamMemberAssignmentDateRange >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_attribute_1_process_child_wbs_dag)
