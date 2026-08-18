from datetime import timedelta
import rail
from pwcglobal.project_import_api_v6 import request_payload, python_callable_method, response_filter
from pwcglobal.project_import_api_v6.request_payload import get_user_list_for_team_members, get_user_list_for_managers


def get_team_members_project_managers(caller):
    with rail.TaskGroup(group_id='get_team_members_project_managers', prefix_group_id=False) as get_user_uris:

        validate_legal_entity = rail.IfOperator(
            task_id="validate_legal_entity",
            test=lambda dag_run: len([x['PwCLegalEntity']['pwclegalentityuri']
                                     for x in request_payload.get_internal_work_relationship_if_valid(
                dag_run.conf['internalpersonrole'])]) > 0,
            yes_task="get_user_list",
            no_task="dummy_finish_team_member_project_manager"
        )

        get_user_list = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_user_list",
            endpoint='/services/UserListService1.svc/GetData',
            items=lambda dag_run: request_payload.get_internal_work_relationship_if_valid(
                dag_run.conf['internalpersonrole']),
            execution_timeout=timedelta(days=14),
            flatten=True,
            data=lambda item, dag_run : get_user_list_for_team_members(item,dag_run,caller),
            data_handler=response_filter.map_user_list
        )

        get_user_list_for_manager_and_comanager = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_user_list_for_manager_and_comanager",
            endpoint='/services/UserListService1.svc/GetData',
            items=lambda dag_run: request_payload.get_internal_work_relationship_if_valid(
                dag_run.conf['internalpersonrole']),
            execution_timeout=timedelta(days=14),
            flatten=True,
            data=lambda item : get_user_list_for_managers(item),
            data_handler=response_filter.map_user_list
        )

        is_users_list_present = rail.IfOperator(
            task_id="is_users_list_present",
            test=lambda : bool((len(rail.result('get_user_list')) > 0) or (len(rail.result('get_user_list_for_manager_and_comanager')) > 0)),
            yes_task="get_team_member_to_assign",
            no_task="dummy_finish_team_member_project_manager"
        )

        get_team_member_to_assign = rail.PythonOperator(
            task_id="get_team_member_to_assign",
            python_callable=python_callable_method.add_to_respective_user_list,
            op_args=[('Charge Code Team Member', 'Team Member',
                      'Charge Code Delivery Manager', 'Engagement Line Delivery Manager', 'Project Delivery Manager',
                      'Charge Code Delivery Partner', 'Engagement Line Delivery Partner', 'Project Delivery Partner')]
        )

        get_project_manager_to_assign = rail.PythonOperator(
            task_id="get_project_manager_to_assign",
            python_callable=python_callable_method.add_to_respective_user_list,
            op_args=[('Charge Code Delivery Manager',
                      'Engagement Line Delivery Manager', 'Project Delivery Manager')]
        )

        get_project_co_manager_to_assign = rail.PythonOperator(
            task_id="get_project_co_manager_to_assign",
            python_callable=python_callable_method.add_to_respective_user_list,
            op_args=[('Charge Code Delivery Partner',
                      'Engagement Line Delivery Partner', 'Project Delivery Partner')]
        )

        get_individual_team_member_uris = rail.PythonOperator(
            task_id="get_individual_team_member_uris",
            python_callable=python_callable_method.add_to_individual_team_member_list
        )

        dummy_finish_team_member_project_manager = rail.EmptyOperator(
            task_id='dummy_finish_team_member_project_manager'
        )

        validate_legal_entity >> rail.Label(
            "Yes") >> get_user_list >> get_user_list_for_manager_and_comanager >> is_users_list_present

        is_users_list_present >> rail.Label(
            "Yes") >> get_team_member_to_assign >> get_project_manager_to_assign >> \
            get_project_co_manager_to_assign >> \
            get_individual_team_member_uris >> dummy_finish_team_member_project_manager

        is_users_list_present >> rail.Label(
            "No") >> dummy_finish_team_member_project_manager

        validate_legal_entity >> rail.Label(
            "No") >> dummy_finish_team_member_project_manager

        return get_user_uris
