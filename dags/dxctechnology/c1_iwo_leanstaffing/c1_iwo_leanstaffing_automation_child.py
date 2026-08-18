import rail
from dxctechnology.c1_iwo_leanstaffing import response_filter
from dxctechnology.c1_iwo_leanstaffing import python_callable_method
from dxctechnology.c1_iwo_leanstaffing import request_payload
# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_c1_iwo_leanstaffing_automation_child_{config.instance}',
        description=f'DXC_C1_Lean Staffing_Automation Child V3.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_leanstaffing_automation_child_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        null = None

        validate_field = rail.IfOperator(
            task_id='validate_field',
            test=lambda: not(bool(request_payload.get_dag_run_conf()['personnelnumber']) and bool(
                request_payload.get_dag_run_conf()['wbselement'])),
            yes_task="log_validation_error",
            no_task="validate_useruri"
        )

        log_validation_error = rail.WriteLogOperator(
            task_id='log_validation_error',
            message=python_callable_method.logMessage,
            properties={
                'employeeid': '{{dag_run.conf.personnelnumber}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Exception',
                'childwbs': '{{dag_run.conf.childwbs}}'
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
            message="Required user \"'{{dag_run.conf.personnelnumber}}'\" is not available in Replicon",
            properties={
                'employeeid': '{{dag_run.conf.personnelnumber}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Exception',
                'childwbs': '{{dag_run.conf.childwbs}}'
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
            message="Required user \"'{{dag_run.conf.personnelnumber}}'\" is disable in Replicon",
            properties={
                'employeeid': '{{dag_run.conf.personnelnumber}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Exception',
                'childwbs': '{{dag_run.conf.childwbs}}'
            }
        )

        get_project_info_based_on_wbs_element = rail.RepliconServiceOperator(
            task_id='get_project_info_based_on_wbs_element',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=request_payload.get_project_details_payload
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
            message="Required WBS \"'{{dag_run.conf.wbselement}}'\" is not available in Replicon",
            properties={
                'employeeid': '{{dag_run.conf.personnelnumber}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Exception',
                'childwbs': '{{dag_run.conf.childwbs}}'
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
            #pylint: disable=line-too-long
            message="Required WBS \"'{{ dag_run.conf.wbselement }}'\" is in \"'{{ result('get_project_info_based_on_wbs_element')[0]['projectDetails']['status']['name'] }}'\" status in Replicon",
            properties={
                'employeeid': '{{dag_run.conf.personnelnumber}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Exception',
                'childwbs': '{{dag_run.conf.childwbs}}'
            }
        )

        assignment_details = rail.PythonOperator(
            task_id="assignment_details",
            python_callable=python_callable_method.assigment_json_details,
        )

        is_uri_present = rail.IfOperator(
            task_id='is_uri_present',
            test=lambda: len(rail.result(
                'get_all_project_team_assignment')) > 0,
            yes_task="empty_is_assignment_date_present",
            no_task="empty_is_compass_user"
        )

        is_c1_user = rail.IfOperator(
            task_id="is_c1_user",
            test='{{ dag_run.conf.companycode | matches(["C1"]) }}',
            yes_task='assign_user_to_project',
            no_task='log_compass_user_validation',
        )

        log_compass_user_validation = rail.WriteLogOperator(
            task_id='log_compass_user_validation',
            #pylint: disable=line-too-long
            message="Required user \"'{{dag_run.conf.personnelnumber}}'\" is not available in project",
            properties={
                'employeeid': '{{dag_run.conf.personnelnumber}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Exception',
                'childwbs': '{{dag_run.conf.childwbs}}'
            }
        )

        assign_user_to_project = rail.RepliconServiceOperator(
            task_id="assign_user_to_project",
            endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
            data=request_payload.get_assign_user_payload
        )

        empty_is_compass_user = rail.EmptyOperator(
            task_id="empty_is_compass_user")

        empty_is_assignment_date_present = rail.EmptyOperator(
            task_id="empty_is_assignment_date_present")

        is_assignment_date_present = rail.IfOperator(
            task_id="is_assignment_date_present",
            test=lambda: request_payload.get_dag_run_conf()['companycode'] != "COMPASS" and (bool(rail.result(
                'assignment_details')['startdate']) or bool(rail.result('assignment_details')['enddate'])),
            yes_task="updateProjectTeamMemberAssignmentDateRange",
            no_task="is_item_present"
        )

        updateProjectTeamMemberAssignmentDateRange = rail.RepliconServiceOperator(
            task_id="updateProjectTeamMemberAssignmentDateRange",
            endpoint='/services/ProjectService1.svc/UpdateProjectTeamMemberAssignmentDateRange',
            data=request_payload.get_assignmentdaterange_payload
        )

        is_item_present = rail.IfOperator(
            task_id="is_item_present",
            test=lambda: bool(request_payload.get_dag_run_conf()['items']),
            yes_task="is_assignment_uri_present",
            no_task="log_success_biilingtype"
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
            no_task="log_success_biilingtype")

        bulkUpdateProjectTeamMemberBillingRatesAllowedForBillingTime = rail.RepliconServiceOperator(
            task_id="bulkUpdateProjectTeamMemberBillingRatesAllowedForBillingTime",
            endpoint="/services/TimeAndMaterialsProjectService1.svc/BulkUpdateProjectTeamMemberBillingRatesAllowedForBillingTime",
            data=request_payload.assigned_payload
        )

        def successPayload():
            value = python_callable_method.is_LT_assigned()
            data_item = request_payload.get_dag_run_conf()['items']
            labour_types_payload = [i for i in list(
                map(lambda x: x['LaborType'], data_item)) if bool(i)]
            return {
                'employeeid': '{{dag_run.conf.personnelnumber}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': [labour_type+extension for labour_type in labour_types_payload for extension
                                in ["|Billable", "|Non-Billable"]] if value else "nill",
                'status': 'Success',
                'childwbs': '{{dag_run.conf.childwbs}}'
            }

        log_success_bilingtype = rail.WriteLogOperator(
            task_id='log_success_biilingtype',
            message="Completed Successfully",
            properties=successPayload
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                'employeeid': '{{dag_run.conf.personnelnumber}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Error',
                'childwbs': '{{dag_run.conf.childwbs}}'
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
        get_project_info_based_on_wbs_element >> project_exist_validation
        project_exist_validation >> rail.Label(
            "YES") >> log_project_validation >> catch_and_log_errors
        project_exist_validation >> rail.Label(
            "NO") >> project_status_validation
        project_status_validation >> rail.Label(
            "YES") >> get_all_project_team_assignment
        project_status_validation >> rail.Label(
            "NO") >> log_project_status_validation >> catch_and_log_errors
        get_all_project_team_assignment >> assignment_details
        assignment_details >> is_uri_present
        is_uri_present >> rail.Label(
            "YES") >> empty_is_assignment_date_present >> is_assignment_date_present
        is_uri_present >> rail.Label(
            "NO") >> empty_is_compass_user >> is_c1_user
        is_c1_user >> rail.Label(
            "NO") >> log_compass_user_validation >> catch_and_log_errors
        is_c1_user >> rail.Label("YES") >> assign_user_to_project
        assign_user_to_project >> is_assignment_date_present
        is_assignment_date_present >> rail.Label(
            "YES") >> updateProjectTeamMemberAssignmentDateRange
        updateProjectTeamMemberAssignmentDateRange >> is_item_present
        is_assignment_date_present >> rail.Label("NO") >> is_item_present
        is_item_present >> rail.Label("YES") >> is_assignment_uri_present
        is_assignment_uri_present >> rail.Label("YES") >> assignedBillingRates
        assignedBillingRates >> is_labour_type_assigned
        is_assignment_uri_present >> rail.Label(
            "NO") >> is_labour_type_assigned
        is_labour_type_assigned >> rail.Label(
            "YES") >> bulkUpdateProjectTeamMemberBillingRatesAllowedForBillingTime
        bulkUpdateProjectTeamMemberBillingRatesAllowedForBillingTime >> log_success_bilingtype
        is_labour_type_assigned >> rail.Label("NO") >> log_success_bilingtype
        is_item_present >> rail.Label(
            "NO") >> log_success_bilingtype >> catch_and_log_errors
    return dag


rail.for_each_instance(create_dag)
