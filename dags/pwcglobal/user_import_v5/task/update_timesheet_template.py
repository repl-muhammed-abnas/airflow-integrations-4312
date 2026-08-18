import rail
from pwcglobal.user_import_v5 import config
from pwcglobal.user_import_v5.utils import request_payload
from pwcglobal.user_import_v5.mapper.timesheet_policy import timesheet_policy_mapper


def get_update_timesheet_template():
    user_uri = request_payload.get_user_uri_template_exp()
    with rail.TaskGroup(group_id='update_timesheet_template_task', prefix_group_id=False) as update_timesheet_template_task:

        is_timesheet_template_changed = rail.IfOperator(
            task_id='is_timesheet_template_changed',
            test=lambda: request_payload.get_conf()['timesheettemplate'] and request_payload.get_conf()['timesheettemplateuri'] and (
                request_payload.get_conf()['timesheettemplate'] != (rail.result('bulk_get_user3')['timesheetTemplate'] or {}).get('name', None)),
            yes_task='update_timsheet_template',
            no_task='finish_timesheet_template_update',
        )

        update_timsheet_template = rail.RepliconServiceOperator(
            task_id='update_timsheet_template',
            endpoint='/services/PolicySetService1.svc/AssignPolicySetToUser',
            data={
                "userUri": user_uri,
                "policySetUri": "{{ dag_run.conf.timesheettemplateuri }}",
            }
        )

        has_timesheet_policy_map_changed = rail.IfOperator(
            task_id='has_timesheet_policy_map_changed',
            test=lambda: next(filter(lambda x: x['Country'] == 'Europe' and
                                     x['timesheet'] == (rail.result('bulk_get_user3')['timesheetTemplate'] or {}).get('name', None) and
                                     x['policy'],
                                     timesheet_policy_mapper), {}).get('policy', None) !=
            next(filter(lambda x: x['Country'] == 'Europe' and
                        x['timesheet'] == request_payload.get_conf()['timesheettemplate'] and
                        x['policy'],
                        timesheet_policy_mapper), {}).get('policy', None),
            yes_task='get_all_policy_sets',
            no_task='finish_timesheet_template_update'
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets',
            endpoint='/services/PolicySetService1.svc/GetAllPolicySets',
        )

        get_next_timesheet_duedate = rail.RepliconServiceOperator(
            task_id='get_next_timesheet_duedate',
            endpoint='/services/TimesheetService1.svc/GetNextTimesheetDueDate',
            data={
                "userUri": user_uri,
                "asOfDate": request_payload.get_today_date(),
            }
        )

        get_punch_entrypolicy_log = rail.CreateLogOperator(
            task_id="get_punch_entrypolicy_log",
            tenant_wide_name=config.punch_entrypolicy_log_name,
            existing_log_mode="append",
        )

        def get_policy_name():
            policy_set_name = next(filter(lambda x: x['Country'] == 'Europe' and
                                          x['timesheet'] == request_payload.get_conf()['timesheettemplate'] and
                                          x['policy'],
                                          timesheet_policy_mapper), {}).get('policy', '')

            return {
                'policy_set': policy_set_name,
                'get_policy_set_name': policy_set_name if policy_set_name == "Historical Punch Entry Access" else "All Devices Access"
            }

        write_punch_entrypolicy_to_log = rail.WriteLogOperator(
            task_id="write_punch_entrypolicy_to_log",
            log="{{ result('get_punch_entrypolicy_log') }}",
            message="{{ (dag_run.conf.useruri or dag_run.conf.search_user_uri) }} queued user_import_punch_entrypolicy",
            properties=lambda: {
                'policyuri': rail.find_first_by_attr_and_get_attr(
                    rail.result(get_all_policy_sets.task_id), 'name', get_policy_name()['get_policy_set_name'], 'uri'),
                'action': 'remove' if 'No' in get_policy_name()['policy_set'] else 'add',
                'effectivedate': rail.result(get_next_timesheet_duedate.task_id),
                'user_uri': request_payload.get_user_uri()
            }
        )

        finish_timesheet_template_update = rail.EmptyOperator(
            task_id='finish_timesheet_template_update'
        )

        is_timesheet_template_changed >> rail.Label(
            'Yes') >> update_timsheet_template >> has_timesheet_policy_map_changed
        is_timesheet_template_changed >> rail.Label(
            'no') >> finish_timesheet_template_update
        has_timesheet_policy_map_changed >> \
            rail.Label('Yes') >> get_all_policy_sets >> get_next_timesheet_duedate >> \
            get_punch_entrypolicy_log >> write_punch_entrypolicy_to_log >> finish_timesheet_template_update
        has_timesheet_policy_map_changed >> \
            rail.Label('No') >> finish_timesheet_template_update

    return update_timesheet_template_task
