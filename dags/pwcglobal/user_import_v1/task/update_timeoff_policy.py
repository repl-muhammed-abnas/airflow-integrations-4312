import rail
from pwcglobal.user_import_v1 import request_payload
from pwcglobal.user_import_v1.mapper.general import general_mapper


def get_update_timeoff_policy():
    user_uri = request_payload.get_user_uri_template_exp()
    with rail.TaskGroup(group_id='update_timeoff_policy_task', prefix_group_id=False) as update_timeoff_policy:

        can_assign_timeoff_policy = rail.IfOperator(
            task_id='can_assign_timeoff_policy',
            test=lambda: len(list(
                filter(lambda x: x['Country'] == 'Global' and
                       x['Type'] == 'Time off Template' and
                       x['Identifier'] == request_payload.get_conf()['country'] and
                       x['Value'],
                       general_mapper))) == 0 and
            not (rail.result('bulk_get_user3')[
                'timeOffTemplate'] or {}).get('name', None),
            yes_task='assign_timeoff_policy',
            no_task='can_remove_timeoff_policy',
        )

        assign_timeoff_policy = rail.RepliconServiceOperator(
            task_id='assign_timeoff_policy',
            endpoint='/services/PolicySetService1.svc/AssignPolicySetToUser',
            data={
                "userUri": user_uri,
                "policySetUri": "{{ dag_run.conf.timeoffpolicyuri}}"
            }
        )

        can_remove_timeoff_policy = rail.IfOperator(
            task_id='can_remove_timeoff_policy',
            test=lambda: len(list(
                filter(lambda x: x['Country'] == 'Global' and
                       x['Type'] == 'Time off Template' and
                       x['Identifier'] == request_payload.get_conf()['country'] and
                       x['Value'],
                       general_mapper))) == 1 and
            (rail.result('bulk_get_user3')[
                'timeOffTemplate'] or {}).get('name', None),
            yes_task='remove_timeoff_policy',
            no_task='update_timeoff_policy_complete'
        )

        remove_timeoff_policy = rail.RepliconServiceOperator(
            task_id='remove_timeoff_policy',
            endpoint='/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser',
            data={
                "userUri": user_uri,
                "policySetUri": "{{ dag_run.conf.timeoffpolicyuri}}"
            }
        )

        update_timeoff_policy_complete = rail.EmptyOperator(
            task_id='update_timeoff_policy_complete'
        )

        can_assign_timeoff_policy >> rail.Label(
            'Yes') >> assign_timeoff_policy >> can_remove_timeoff_policy
        can_assign_timeoff_policy >> rail.Label(
            'No') >> can_remove_timeoff_policy

        can_remove_timeoff_policy >> rail.Label(
            'Yes') >> remove_timeoff_policy >> update_timeoff_policy_complete
        can_remove_timeoff_policy >> rail.Label(
            'No') >> update_timeoff_policy_complete

    return update_timeoff_policy
