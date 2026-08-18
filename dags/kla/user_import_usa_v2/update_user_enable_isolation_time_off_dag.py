import json
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'kla_user_import_usa_update_user_enable_isolation_time_off_v2_{config.instance}',
        description=f'USA KLATencor Update User - enable_Isolation_Time Off v2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        def get_conf():
            return rail.get_current_context()['dag_run'].conf

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_time_off_type_policy_summary'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_time_off_type_policy_summary',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_time_off_type_policy_summary = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{dag_run.conf.useruri}}"
            }
        )

        getenabled_time_offtypes = rail.RepliconServiceOperator(
            task_id='getenabled_time_offtypes',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        has_isolation_timeoff = rail.IfOperator(
            task_id='has_isolation_timeoff',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_type_policy_summary')[
                              'policiesByTimeOffType'], 'timeOffType.displayText', 'Isolation Leave')),
            yes_task="put_time_off_type_assignments_for_user",
            no_task="has_blank_isolation_timeoff",
        )

        put_time_off_type_assignments_for_user = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {
                "userUri": get_conf()['useruri'],
                "timeOffTypeUris":  list(map(lambda x: x['timeOffType']['uri'], filter(lambda x: x['timeOffType']['displayText'] != 'Isolation Leave', rail.result('get_user_time_off_type_policy_summary')['policiesByTimeOffType']))) +
                [rail.find_first_by_attr_and_get_attr(rail.result(
                    'getenabled_time_offtypes'), 'displayText', 'Isolation Leave', 'uri')]
            }
        )

        has_blank_isolation_timeoff = rail.IfOperator(
            task_id='has_blank_isolation_timeoff',
            test=lambda: not bool(rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_type_policy_summary')[
                'policiesByTimeOffType'], 'timeOffType.displayText', 'Isolation Leave')),
            yes_task="put_time_off_type_assignments_for_user2",
        )

        put_time_off_type_assignments_for_user2 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user2',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {
                "userUri": get_conf()['useruri'],
                "timeOffTypeUris":  list(map(lambda x: x['timeOffType']['uri'], filter(lambda x: x['timeOffType']['displayText'] != 'Isolation Leave', rail.result('get_user_time_off_type_policy_summary')['policiesByTimeOffType'])))
            }
        )

        get_default_time_off_type_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": get_conf()['useruri'],
                    "timeOffTypeUri": rail.find_first_by_attr_and_get_attr(rail.result(
                        'getenabled_time_offtypes'), 'displayText', 'Isolation Leave', 'uri')
                }
            }
        )

        assign_default_policy = rail.RepliconServiceOperator(
            task_id='assign_default_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": get_conf()['useruri'],
                    "timeOffTypeUri": rail.find_first_by_attr_and_get_attr(rail.result(
                        'getenabled_time_offtypes'), 'displayText', 'Isolation Leave', 'uri')
                },
                "policySetScheduleEntries": json.loads(json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user'))
                                                       .replace('"script"', '"scriptTarget"')
                                                       .replace('"description": null', '"description": "effective"'))
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> get_user_time_off_type_policy_summary

        get_user_time_off_type_policy_summary >> getenabled_time_offtypes >> has_isolation_timeoff

        has_isolation_timeoff >> rail.Label(
            'Yes') >> put_time_off_type_assignments_for_user >> has_blank_isolation_timeoff
        has_isolation_timeoff >> rail.Label(
            'No') >> has_blank_isolation_timeoff

        has_blank_isolation_timeoff >> rail.Label(
            'Yes') >> put_time_off_type_assignments_for_user2 >> get_default_time_off_type_policy_schedule_for_user >> assign_default_policy >> finish
        has_blank_isolation_timeoff >> rail.Label(
            'No') >> finish
        finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
