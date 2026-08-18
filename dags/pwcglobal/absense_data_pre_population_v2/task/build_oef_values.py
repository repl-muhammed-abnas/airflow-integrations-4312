import rail
from pwcglobal.absense_data_pre_population_v2.utils import request_payload
from pwcglobal.absense_data_pre_population_v2.utils import python_callable_method

null = None


def get_build_oef_values(caller, worktype_mapper):
    with rail.TaskGroup(group_id=f'build_oef_values_group_{caller}', prefix_group_id=False) as build_oef_values_group:

        start_oef_build = rail.EmptyOperator(
            task_id=f'start_oef_build_{caller}'
        )

        build_oef_for_wdid = rail.PythonOperator(
            task_id=f'build_oef_for_wdid_{caller}',
            python_callable=python_callable_method.get_oef_for_wdid,
        )

        check_work_location_present = rail.IfOperator(
            task_id=f'check_work_location_present_{caller}',
            test='{{ dag_run.conf.WorkLocation| is_truthy }}' and '{{ dag_run.conf.worklocationoefuri| is_truthy }}',
            yes_task=f'get_oef_tags_filtered_by_search_{caller}',
            no_task=f'get_work_type_oef_uri_{caller}'
        )

        get_oef_tags_filtered_by_search = rail.RepliconServiceOperator(
            task_id=f'get_oef_tags_filtered_by_search_{caller}',
            endpoint='/services/ObjectExtensionTagService1.svc/GetPageOfObjectExtensionTagsFilteredBySearch',
            data=request_payload.get_oef_tags_payload
        )

        build_oef_for_work_location = rail.PythonOperator(
            task_id=f'build_oef_for_work_location_{caller}',
            python_callable=python_callable_method.get_oef_for_work_location,
            op_args=[f'get_oef_tags_filtered_by_search_{caller}']
        )

        get_work_type_oef_uri = rail.PythonOperator(
            task_id=f'get_work_type_oef_uri_{caller}',
            python_callable=lambda dag_run: python_callable_method.get_work_type_oef_uri(dag_run, worktype_mapper)
        )

        check_work_type_oef_uri = rail.IfOperator(
            task_id=f'check_work_type_oef_uri_{caller}',
            test=lambda: bool(rail.result(get_work_type_oef_uri.task_id)),
            yes_task=f'get_oef_tags_filtered_by_custom_search_{caller}',
            no_task=f'build_oef_for_comments_{caller}'
        )

        get_oef_tags_filtered_by_custom_search = rail.RepliconServiceOperator(
            task_id=f'get_oef_tags_filtered_by_custom_search_{caller}',
            endpoint='/services/ObjectExtensionTagService1.svc/GetPageOfObjectExtensionTagsFilteredBySearch',
            data={
                "page": "1",
                "pageSize": "10000",
                "objectExtensionTagDefinitionUri": '{{ result(\'' + get_work_type_oef_uri.task_id + '\') }}',
                "textSearch": {
                    "queryText": '{{ dag_run.conf.WorkType }}',
                    "searchInDisplayText": "1",
                    "searchInName": "1"
                }
            }
        )

        build_oef_for_work_type = rail.PythonOperator(
            task_id=f'build_oef_for_work_type_{caller}',
            python_callable=python_callable_method.get_oef_for_work_type,
            op_args=[f'get_oef_tags_filtered_by_custom_search_{caller}',
                     f'get_work_type_oef_uri_{caller}']
        )

        build_oef_values_list = rail.PythonOperator(
            task_id=f'build_oef_values_list_{caller}',
            python_callable=python_callable_method.add_items_to_oef_values,
            op_args=[f'build_oef_for_wdid_{caller}',
                     f'build_oef_for_work_location_{caller}',
                     f'build_oef_for_work_type_{caller}']
        )

        build_oef_for_comments = rail.PythonOperator(
            task_id=f'build_oef_for_comments_{caller}',
            python_callable=python_callable_method.get_comments
        )

        build_time_transaction_details = rail.PythonOperator(
            task_id=f'build_time_transaction_details_{caller}',
            python_callable=python_callable_method.get_time_transaction_details,
            op_args=[f'build_oef_values_list_{caller}',
                     'add_item_to_team_members_list']
        )

        check_existing_teammembers_uri = rail.IfOperator(
            task_id=f'check_existing_teammembers_uri_{caller}',
            test=lambda: bool(
                python_callable_method.check_existing_teammembers_uri),
            yes_task=f'check_project_type_{caller}',
            no_task=f'end_oef_build_{caller}'
        )

        check_project_type = rail.IfOperator(
            task_id=f'check_project_type_{caller}',
            test=lambda: bool(rail.result('search_project_with_code') and
                              rail.result('search_project_with_code')[0]['extensionfieldvalues'] != 'Statistical' and
                              rail.result('search_project_with_code')[0]['extensionfieldvalues'] != 'Leave'),
            yes_task=f'update_project_team_member_assignment_{caller}',
            no_task=f'end_oef_build_{caller}'
        )

        update_project_team_member_assignment = rail.RepliconServiceOperator(
            task_id=f'update_project_team_member_assignment_{caller}',
            endpoint='/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment',
            data={
                "projectUri": '{{ result("search_project_with_code")[0]["projecturi"] }}',
                "resourceUri": '{{ result("search_users")[0].useruri }}',
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            }
        )

        check_task_uri_present = rail.IfOperator(
            task_id=f'check_task_uri_present_{caller}',
            test=lambda: bool(rail.result('get_task_uri')
                              and 'urn' in rail.result('get_task_uri')),
            yes_task=f'update_resource_assignment_{caller}',
            no_task=f'end_oef_build_{caller}'
        )

        update_resource_assignment = rail.RepliconServiceOperator(
            task_id=f'update_resource_assignment_{caller}',
            endpoint='/services/TaskService1.svc/UpdateResourceAssignment',
            data={
                "taskUri": '{{ result("get_task_uri") }}',
                "resourceUri": '{{ result("search_users")[0].useruri }}',
                "isAssigned": "1"
            }
        )

        end_oef_build = rail.EmptyOperator(
            task_id=f'end_oef_build_{caller}'
        )

        start_oef_build >> build_oef_for_wdid >> check_work_location_present

        check_work_location_present >> rail.Label(
            'Yes') >> get_oef_tags_filtered_by_search >> build_oef_for_work_location >> get_work_type_oef_uri
        check_work_location_present >> rail.Label(
            'No') >> get_work_type_oef_uri >> check_work_type_oef_uri

        check_work_type_oef_uri >> rail.Label(
            'Yes') >> get_oef_tags_filtered_by_custom_search >> build_oef_for_work_type >> build_oef_for_comments
        check_work_type_oef_uri >> rail.Label('No') >> build_oef_for_comments

        build_oef_for_comments >> build_oef_values_list >> build_time_transaction_details >> check_existing_teammembers_uri

        check_existing_teammembers_uri >> rail.Label(
            'Yes') >> check_project_type
        check_existing_teammembers_uri >> rail.Label(
            'No') >> end_oef_build

        check_project_type >> rail.Label(
            'Yes') >> update_project_team_member_assignment >> check_task_uri_present >> update_resource_assignment \
            >> end_oef_build
        check_project_type >> rail.Label(
            'No') >> end_oef_build

        check_task_uri_present >> rail.Label(
            'Yes') >> update_resource_assignment >> end_oef_build
        check_task_uri_present >> rail.Label(
            'No') >> end_oef_build

        return build_oef_values_group
