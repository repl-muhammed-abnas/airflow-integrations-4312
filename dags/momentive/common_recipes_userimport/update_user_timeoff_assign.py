# pylint: disable=line-too-long
from datetime import timedelta, datetime
from airflow.models import Variable
import rail
from momentive.common_recipes_userimport.utils import python_callable, request_payload

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.momentive_othercountries_user_sync_update_user_timeoff_assign_child_dag_id,
        description=f'momentive_othercountries_user_sync_update_user_timeoff_assign_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_assigned_timeofftypes'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_assigned_timeofftypes',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_assigned_timeofftypes = rail.RepliconServiceOperator(
            task_id='get_assigned_timeofftypes',
            endpoint="/services/TimeOffService1.svc/BulkGetTimeOffTypeAssignmentsForUsers",
            data={
                "userUris": [
                    "{{ dag_run.conf.useruri }}"
                ]
            },
            data_handler=lambda response: response[0] if response else ''
        )

        get_alltimeoff_types = rail.RepliconServiceOperator(
            task_id='get_alltimeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        get_years_of_service = rail.PythonOperator(
            task_id='get_years_of_service',
            python_callable=lambda dag_run: ((datetime.now() - datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d')).days)/365
        )

        get_previoustimeofflist = rail.PythonOperator(
            task_id="get_previoustimeofflist",
            python_callable=python_callable.get_previoustimeoff_list
        )

        if_timeofftypes_not_present = rail.IfOperator(
            task_id='if_timeofftypes_not_present',
            test="{{ dag_run.conf.timeofftypes | is_falsy }}",
            yes_task="trigger_child_0_balance_timeoff",
            no_task="get_final_set_timeoff_29",
        )

        trigger_child_0_balance_timeoff = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_0_balance_timeoff',
            items="{{ result('get_previoustimeofflist') }}",
            trigger_dag_id=config.momentive_othercountries_user_sync_zero_balance_timeoff_update_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.trigger_child_0_balance_timeoff_payload
        )

        remove_all_timeoffs = rail.RepliconServiceOperator(
            task_id='remove_all_timeoffs',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'timeOffTypeUris': []
            }
        )

        get_final_set_timeoff_29 = rail.PythonOperator(
            task_id="get_final_set_timeoff_29",
            python_callable=python_callable.get_final_timeoff
        )

        # Recipe nodes 29-32: previously-assigned types no longer requested -> zero balance.
        if_timeoff_previously_assigned_to_notassigned_present_32 = rail.IfOperator(
            task_id='if_timeoff_previously_assigned_to_notassigned_present_32',
            test="{{ result('get_final_set_timeoff_29').timeoff_previously_assigned_to_be_notassigned | is_truthy }}",
            yes_task="trigger_child_0_balance_timeoff_33",
            no_task="if_final_set_timeoff_uri_present",
        )

        trigger_child_0_balance_timeoff_33 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_0_balance_timeoff_33',
            items=lambda: rail.result('get_final_set_timeoff_29')['timeoff_previously_assigned_to_be_notassigned'],
            trigger_dag_id=config.momentive_othercountries_user_sync_zero_balance_timeoff_update_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.trigger_child_0_balance_timeoff_payload
        )

        # Recipe nodes 33-35: full-replace assignment of the requested set.
        if_final_set_timeoff_uri_present = rail.IfOperator(
            task_id='if_final_set_timeoff_uri_present',
            test="{{ result('get_final_set_timeoff_29').final_timeoff_assign_val | is_truthy }}",
            yes_task="assign_req_timeofftypes_36",
            no_task="if_timeoff_not_previously_assigned_present",
        )

        assign_req_timeofftypes_36 = rail.RepliconServiceOperator(
            task_id='assign_req_timeofftypes_36',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'timeOffTypeUris': rail.result('get_final_set_timeoff_29')['final_timeoff_assign_val']
            }
        )

        # Recipe nodes 38-41: newly-requested types -> add/rehire child (1435238).
        if_timeoff_not_previously_assigned_present = rail.IfOperator(
            task_id='if_timeoff_not_previously_assigned_present',
            test="{{ result('get_final_set_timeoff_29').timeoff_not_previously_assigned | is_truthy }}",
            yes_task="trigger_timeoff_add_rehire_user_42",
            no_task="if_timeoff_is_KOR_annual_or_monthly",
        )

        trigger_timeoff_add_rehire_user_42 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_timeoff_add_rehire_user_42',
            items=lambda: rail.result('get_final_set_timeoff_29')['timeoff_not_previously_assigned'],
            trigger_dag_id=config.momentive_othercountries_user_sync_timeoff_rehire_user_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.trigger_timeoff_add_rehire_payload
        )

        # Recipe nodes 42-44: rehire, KOR annual/monthly -> add/rehire child (1435238).
        if_timeoff_is_KOR_annual_or_monthly = rail.IfOperator(
            task_id='if_timeoff_is_KOR_annual_or_monthly',
            test="{{ result('get_final_set_timeoff_29').timeoff_add_rehire | is_truthy }}",
            yes_task="trigger_timeoff_add_rehire_user_45",
            no_task="if_timeoff_is_BEL",
        )

        trigger_timeoff_add_rehire_user_45 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_timeoff_add_rehire_user_45',
            items=lambda: rail.result('get_final_set_timeoff_29')['timeoff_add_rehire'],
            trigger_dag_id=config.momentive_othercountries_user_sync_timeoff_rehire_user_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.trigger_timeoff_add_rehire_payload
        )

        # Recipe nodes 46-47: rehire, name startswith '[BEL]' -> BEL policy child (1362490 - STUB).
        # (Recipe nodes 48-49, the UK_Holiday Paid -> 1435242 branch, are ON HOLD - not wired.)
        if_timeoff_is_BEL = rail.IfOperator(
            task_id='if_timeoff_is_BEL',
            test="{{ result('get_final_set_timeoff_29').bel_policy_rehire | is_truthy }}",
            yes_task="trigger_bel_policy_rehire_47",
            no_task="if_timeoff_generic_policy_rehire",
        )

        trigger_bel_policy_rehire_47 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_bel_policy_rehire_47',
            items=lambda: rail.result('get_final_set_timeoff_29')['bel_policy_rehire'],
            trigger_dag_id=config.momentive_othercountries_user_sync_bel_policy_rehire_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.trigger_bel_policy_rehire_payload
        )

        # Recipe nodes 50-51: rehire, generic (non-KOR/BEL/UK) -> annual-leave policy days child (1435236).
        if_timeoff_generic_policy_rehire = rail.IfOperator(
            task_id='if_timeoff_generic_policy_rehire',
            test="{{ result('get_final_set_timeoff_29').annual_leave_policy_rehire | is_truthy }}",
            yes_task="trigger_child_annual_leave_policy_52",
            no_task="catch_error",
        )

        trigger_child_annual_leave_policy_52 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_annual_leave_policy_52',
            items=lambda: rail.result('get_final_set_timeoff_29')['annual_leave_policy_rehire'],
            trigger_dag_id=config.momentive_othercountries_user_sync_policy_rehire_update_days_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['hiredate'],
                "actualstartdate": dag_run.conf['oldstartdate'],
                "timeofftype": item['name'],
                "update": 'update',
                "type": 'rehire',
                "timeoffuri": item['uri'],
            }
        )

        # Leaf error reply (gathered by the parent on failure).
        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Update user timeoff assign - Dag_Run Error - {{ get_error_message() }}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_error') if rail.result('catch_error') else ""
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error >> final_response_from_dag
        can_run_batch_task >> rail.Label('No') >> get_assigned_timeofftypes

        get_assigned_timeofftypes >> get_alltimeoff_types >> get_years_of_service >> get_previoustimeofflist >> if_timeofftypes_not_present

        if_timeofftypes_not_present >> rail.Label('Yes') >> trigger_child_0_balance_timeoff >> remove_all_timeoffs >> get_final_set_timeoff_29
        if_timeofftypes_not_present >> rail.Label('No') >> get_final_set_timeoff_29

        get_final_set_timeoff_29 >> if_timeoff_previously_assigned_to_notassigned_present_32

        if_timeoff_previously_assigned_to_notassigned_present_32 >> rail.Label('Yes') >> trigger_child_0_balance_timeoff_33 >> if_final_set_timeoff_uri_present
        if_timeoff_previously_assigned_to_notassigned_present_32 >> rail.Label('No') >> if_final_set_timeoff_uri_present

        if_final_set_timeoff_uri_present >> rail.Label('Yes') >> assign_req_timeofftypes_36 >> if_timeoff_not_previously_assigned_present
        if_final_set_timeoff_uri_present >> rail.Label('No') >> if_timeoff_not_previously_assigned_present

        if_timeoff_not_previously_assigned_present >> rail.Label('Yes') >> trigger_timeoff_add_rehire_user_42 >> if_timeoff_is_KOR_annual_or_monthly
        if_timeoff_not_previously_assigned_present >> rail.Label('No') >> if_timeoff_is_KOR_annual_or_monthly

        if_timeoff_is_KOR_annual_or_monthly >> rail.Label('Yes') >> trigger_timeoff_add_rehire_user_45 >> if_timeoff_is_BEL
        if_timeoff_is_KOR_annual_or_monthly >> rail.Label('No') >> if_timeoff_is_BEL

        if_timeoff_is_BEL >> rail.Label('Yes') >> trigger_bel_policy_rehire_47 >> if_timeoff_generic_policy_rehire
        if_timeoff_is_BEL >> rail.Label('No') >> if_timeoff_generic_policy_rehire

        if_timeoff_generic_policy_rehire >> rail.Label('Yes') >> trigger_child_annual_leave_policy_52 >> catch_error
        if_timeoff_generic_policy_rehire >> rail.Label('No') >> catch_error

        catch_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
