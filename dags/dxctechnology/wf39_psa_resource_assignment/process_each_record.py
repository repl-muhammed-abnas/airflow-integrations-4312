import rail
from dxctechnology.wf39_psa_resource_assignment.utils import python_callable_method
from dxctechnology.wf39_psa_resource_assignment.utils import response_filter
from dxctechnology.wf39_psa_resource_assignment.utils import request_payload
# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_wf39_psa_resource_assignment_process_each_record_child_{config.instance}',
        description=f'DXC_WF39 PSA Resource Assignment Automation Child V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_wbs_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        null = None

        validate_field = rail.IfOperator(
            task_id='validate_field',
            test=lambda: not (bool(request_payload.get_dag_run_conf()['user']) and bool(
                request_payload.get_dag_run_conf()['wbselement'])),
            yes_task="log_validation_error",
            no_task="validate_useruri"
        )

        log_validation_error = rail.WriteLogOperator(
            task_id='log_validation_error',
            message=python_callable_method.logMessage,
            properties={
                'employeeid': '{{dag_run.conf.user}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Exception',
                'action': 'Validation',
                'employee': '{{dag_run.conf.name}}'
            }
        )

        validate_useruri = rail.IfOperator(
            task_id='validate_useruri',
            test=lambda: not bool(
                request_payload.get_dag_run_conf()['useruri']),
            yes_task="log_user_validation_error",
            no_task="validate_status"
        )

        log_user_validation_error = rail.WriteLogOperator(
            task_id='log_user_validation_error',
            message="Required user '{{dag_run.conf.user}}' is not available in Replicon",
            properties={
                'employeeid': '{{dag_run.conf.user}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Exception',
                'action': 'Validation',
                'employee': '{{dag_run.conf.name}}'
            }
        )

        validate_status = rail.IfOperator(
            task_id='validate_status',
            test=lambda: request_payload.get_dag_run_conf()[
                'status'] != "Enabled",
            yes_task="log_status_validation_error",
            no_task="get_project_info_based_on_wbs_element"
        )

        log_status_validation_error = rail.WriteLogOperator(
            task_id='log_status_validation_error',
            message="Required user '{{dag_run.conf.user}}' is disable in Replicon",
            properties={
                'employeeid': '{{dag_run.conf.user}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Exception',
                'action': 'Validation',
                'employee': '{{dag_run.conf.name}}'
            }
        )

        get_project_info_based_on_wbs_element = rail.RepliconServiceOperator(
            task_id='get_project_info_based_on_wbs_element',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=request_payload.get_project_details_payload
        )

        get_user_info = rail.RepliconServiceOperator(
            task_id='get_user_info',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "uri": "{{dag_run.conf.useruri}}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            },
            response_filter=lambda res: res.json()['d'][0]
        )

        get_user_division_detail = rail.RepliconServiceOperator(
            task_id='get_user_division_detail',
            endpoint='/services/DivisionService1.svc/GetDivisionDetails',
            data=request_payload.get_user_division_detail
        )

        is_user_c1 = rail.IfOperator(
            task_id="is_user_c1",
            test=python_callable_method.is_user_c1,
            yes_task="project_exist_validation",
            no_task="log_user_not_c1",
        )

        log_user_not_c1 = rail.WriteLogOperator(
            task_id='log_user_not_c1',
            message="Required user '{{dag_run.conf.user}}' is not C1 User",
            properties={
                'employeeid': '{{dag_run.conf.user}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Exception',
                'action': 'Validation',
                'employee': '{{dag_run.conf.name}}'
            }
        )

        project_exist_validation = rail.IfOperator(
            task_id="project_exist_validation",
            test=lambda: bool(rail.result(
                'get_project_info_based_on_wbs_element')[0]['error']),
            yes_task="log_project_validation",
            no_task="project_status_validation",
        )

        log_project_validation = rail.WriteLogOperator(
            task_id='log_project_validation',
            message="Required WBS '{{dag_run.conf.wbselement}}' is not available in Replicon",
            properties={
                'employeeid': '{{dag_run.conf.user}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Exception',
                'action': 'Validation',
                'employee': '{{dag_run.conf.name}}'
            }
        )

        project_status_validation = rail.IfOperator(
            task_id="project_status_validation",
            test=lambda: python_callable_method.project_status(
                'get_project_info_based_on_wbs_element'),
            yes_task="get_all_project_team_assignment",
            no_task="log_project_status_validation",
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

        log_project_status_validation = rail.WriteLogOperator(
            task_id='log_project_status_validation',
            # pylint: disable=line-too-long
            message="Required WBS '{{ dag_run.conf.wbselement }}' is in '{{ result('get_project_info_based_on_wbs_element')[0]['projectDetails']['status']['name'] }}' status in Replicon",
            properties={
                'employeeid': '{{dag_run.conf.user}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Exception',
                'action': 'Validation',
                'employee': '{{dag_run.conf.name}}'
            }
        )

        is_uri_present = rail.IfOperator(
            task_id='is_uri_present',
            test=lambda: len(rail.result(
                'get_all_project_team_assignment')) > 0,
            yes_task="is_assignment_date_present",
            no_task="assign_user_to_project"
        )

        assign_user_to_project = rail.RepliconServiceOperator(
            task_id="assign_user_to_project",
            endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
            data=request_payload.get_assign_user_payload
        )

        is_assignment_date_present = rail.IfOperator(
            task_id="is_assignment_date_present",
            test=lambda: (bool(request_payload.get_dag_run_conf()['startdate']) or bool(
                request_payload.get_dag_run_conf()['enddate'])),
            yes_task="updateProjectTeamMemberAssignmentDateRange",
            no_task="is_assignment_uri_present"
        )

        updateProjectTeamMemberAssignmentDateRange = rail.RepliconServiceOperator(
            task_id="updateProjectTeamMemberAssignmentDateRange",
            endpoint='/services/ProjectService1.svc/UpdateProjectTeamMemberAssignmentDateRange',
            data=request_payload.get_assignmentdaterange_payload
        )

        is_assignment_uri_present = rail.IfOperator(
            task_id="is_assignment_uri_present",
            test=lambda: bool(rail.result('get_all_project_team_assignment')),
            yes_task="assignedBillingRates",
            no_task="is_labour_type_assigned"
        )

        assignedBillingRates = rail.PythonOperator(
            task_id="assignedBillingRates",
            python_callable=python_callable_method.assignedBillingRates,
            op_args=['get_all_project_team_assignment']
        )

        is_labour_type_assigned = rail.IfOperator(
            task_id="is_labour_type_assigned",
            test=python_callable_method.is_LT_assigned,
            yes_task="bulkUpdateProjectTeamMemberBillingRatesAllowedForBillingTime",
            no_task="is_assignemnt_date_out_of_range")

        bulkUpdateProjectTeamMemberBillingRatesAllowedForBillingTime = rail.RepliconServiceOperator(
            task_id="bulkUpdateProjectTeamMemberBillingRatesAllowedForBillingTime",
            endpoint="/services/TimeAndMaterialsProjectService1.svc/BulkUpdateProjectTeamMemberBillingRatesAllowedForBillingTime",
            data=request_payload.assigned_payload
        )

        def successPayload():
            value = python_callable_method.is_LT_assigned()
            data_item = request_payload.get_dag_run_conf()['labourtype']
            return {
                'employeeid': '{{dag_run.conf.user}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': [data_item+extension for extension in ["|Billable", "|Non-Billable"]] if value else "nill",
                'status': 'Success',
                'action': 'Add',
                'employee': '{{dag_run.conf.name}}'
            }

        is_assignemnt_date_out_of_range = rail.IfOperator(
            task_id="is_assignemnt_date_out_of_range",
            test=python_callable_method.is_assignemnt_date_out_of_range,
            yes_task="log_user_date_range_exception",
            no_task="is_success_record"
        )

        def get_message(dag_run):
            msg = python_callable_method.get_out_of_range_message(dag_run)
            return "User added to the WBS but " + str(msg)

        log_user_date_range_exception = rail.WriteLogOperator(
            task_id='log_user_date_range_exception',
            message=get_message,
            properties={
                'employeeid': '{{dag_run.conf.user}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Exception',
                'action': 'Validation',
                'employee': '{{dag_run.conf.name}}'
            }
        )

        is_success_record = rail.IfOperator(
            task_id="is_success_record",
            test=python_callable_method.is_assignemnt_date_out_of_range,
            yes_task="catch_and_log_errors",
            no_task="log_success_bilingtype")

        log_success_bilingtype = rail.WriteLogOperator(
            task_id='log_success_bilingtype',
            message="Completed Successfully",
            properties=successPayload
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                'employeeid': '{{dag_run.conf.user}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Error',
                'action': 'Validation',
                'employee': '{{dag_run.conf.name}}'
            })

        validate_field >> rail.Label(
            "YES") >> log_validation_error >> catch_and_log_errors
        validate_field >> rail.Label("NO") >> validate_useruri
        validate_useruri >> rail.Label(
            "YES") >> log_user_validation_error >> catch_and_log_errors
        validate_useruri >> rail.Label("NO") >> validate_status
        validate_status >> rail.Label(
            "YES") >> log_status_validation_error >> catch_and_log_errors
        validate_status >> rail.Label(
            "NO") >> get_project_info_based_on_wbs_element
        get_project_info_based_on_wbs_element >> get_user_info >> get_user_division_detail >> is_user_c1 >> rail.Label(
            "Yes") >> project_exist_validation
        is_user_c1 >> rail.Label("No") >> log_user_not_c1 >> catch_and_log_errors
        project_exist_validation >> rail.Label(
            "YES") >> log_project_validation >> catch_and_log_errors
        project_exist_validation >> rail.Label(
            "NO") >> project_status_validation
        project_status_validation >> rail.Label(
            "YES") >> get_all_project_team_assignment
        project_status_validation >> rail.Label(
            "NO") >> log_project_status_validation >> catch_and_log_errors
        get_all_project_team_assignment >> is_uri_present
        is_uri_present >> rail.Label(
            "YES") >> is_assignment_date_present
        is_uri_present >> rail.Label(
            "NO") >> assign_user_to_project
        assign_user_to_project >> is_assignment_date_present
        is_assignment_date_present >> rail.Label(
            "YES") >> updateProjectTeamMemberAssignmentDateRange
        is_assignment_date_present >> rail.Label(
            "NO") >> is_assignment_uri_present
        updateProjectTeamMemberAssignmentDateRange >> is_assignment_uri_present
        is_assignment_uri_present >> rail.Label("YES") >> assignedBillingRates
        assignedBillingRates >> is_labour_type_assigned
        is_assignment_uri_present >> rail.Label(
            "NO") >> is_labour_type_assigned
        is_labour_type_assigned >> rail.Label(
            "YES") >> bulkUpdateProjectTeamMemberBillingRatesAllowedForBillingTime
        bulkUpdateProjectTeamMemberBillingRatesAllowedForBillingTime >> is_assignemnt_date_out_of_range
        is_assignemnt_date_out_of_range >> rail.Label(
            'YES') >> log_user_date_range_exception >> is_success_record
        is_assignemnt_date_out_of_range >> rail.Label(
            'No') >> is_success_record
        is_success_record >> rail.Label(
            "No") >> log_success_bilingtype >> catch_and_log_errors
        is_success_record >> rail.Label("Yes") >> catch_and_log_errors
        is_labour_type_assigned >> rail.Label(
            "NO") >> is_assignemnt_date_out_of_range
    return dag


rail.for_each_instance(create_dag)
