from datetime import timedelta, datetime
from airflow.models import Variable
import rail
from momentive.user_import_south_korea.utils import python_callable, request_payload

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'momentive_userimport_update_user_timeoff_assign_child_{config.instance}',
        description=f'momentive_userimport_update_user_timeoff_assign_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.update_user_timeoff_assign_child_dag_active_runs,
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
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
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
            task_id = "get_previoustimeofflist",
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
            items= "{{ result('get_previoustimeofflist') }}",
            trigger_dag_id=f'momentive_userimport_0_balance_for_timeoff_update_child_{config.instance}',
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
            task_id = "get_final_set_timeoff_29",
            python_callable=python_callable.get_final_timeoff
        )

        if_timeoff_previously_assigned_to_notassigned_present_32 = rail.IfOperator(
            task_id='if_timeoff_previously_assigned_to_notassigned_present_32',
            test="{{ result('get_final_set_timeoff_29').timeoff_previously_assigned_to_be_notassigned | is_truthy }}",
            yes_task="trigger_child_0_balance_timeoff_33",
            no_task="if_final_set_timeoff_uri_present",
        )

        trigger_child_0_balance_timeoff_33 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_0_balance_timeoff_33',
            items=lambda: rail.result('get_final_set_timeoff_29')['timeoff_previously_assigned_to_be_notassigned'],
            trigger_dag_id=f'momentive_userimport_0_balance_for_timeoff_update_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.trigger_child_0_balance_timeoff_payload
        )

        if_final_set_timeoff_uri_present = rail.IfOperator(
            task_id='if_final_set_timeoff_uri_present',
            test="{{ result('get_final_set_timeoff_29').final_timeoff_assign_val | is_truthy }}",
            yes_task="assign_req_timeofftypes_36",
            no_task="if_timeoff_not_previously_assigned_present",
        )

        assign_req_timeofftypes_36 = rail.RepliconServiceOperator(
            task_id='assign_req_timeofftypes_36',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'timeOffTypeUris': ["{{ result('get_final_set_timeoff_29').final_timeoff_assign_val }}"]
            }
        )

        if_timeoff_not_previously_assigned_present = rail.IfOperator(
            task_id='if_timeoff_not_previously_assigned_present',
            test="{{ result('get_final_set_timeoff_29').timeoff_not_previously_assigned | is_truthy }}",
            yes_task="trigger_timeoff_add_rehire_user_42",
            no_task="if_timeoff_is_KOR_annual_or_monthly",
        )

        trigger_timeoff_add_rehire_user_42 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_timeoff_add_rehire_user_42',
            items=lambda: rail.result('get_final_set_timeoff_29')['timeoff_not_previously_assigned'],
            trigger_dag_id=f'momentive_userimport_timeoff_add_newuser_rehire_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.trigger_timeoff_add_rehire_payload
        )

        if_timeoff_is_KOR_annual_or_monthly = rail.IfOperator(
            task_id='if_timeoff_is_KOR_annual_or_monthly',
            test="{{ result('get_final_set_timeoff_29').timeoff_add_rehire | is_truthy }}",
            yes_task="trigger_timeoff_add_rehire_user_45",
            no_task="if_timeoff_not_KOR_annual_or_monthly_or_BEL_UK",
        )

        trigger_timeoff_add_rehire_user_45 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_timeoff_add_rehire_user_45',
            items=lambda: rail.result('get_final_set_timeoff_29')['timeoff_add_rehire'],
            trigger_dag_id=f'momentive_userimport_timeoff_add_newuser_rehire_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.trigger_timeoff_add_rehire_payload
        )

        if_timeoff_not_KOR_annual_or_monthly_or_BEL_UK = rail.IfOperator(
            task_id='if_timeoff_not_KOR_annual_or_monthly_or_BEL_UK',
            test="{{ result('get_final_set_timeoff_29').annual_leave_policy_rehire | is_truthy }}",
            yes_task="trigger_child_annual_leave_policy_52",
            no_task="catch_and_log_error",
        )

        trigger_child_annual_leave_policy_52 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_annual_leave_policy_52',
            items=lambda: rail.result('get_final_set_timeoff_29')['annual_leave_policy_rehire'],
            trigger_dag_id=f'momentive_userimport_policy_assignment_rehire_update_days_child_{config.instance}',
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

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "details":"{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
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

        if_timeoff_is_KOR_annual_or_monthly >> rail.Label('Yes') >> trigger_timeoff_add_rehire_user_45 >> if_timeoff_not_KOR_annual_or_monthly_or_BEL_UK
        if_timeoff_is_KOR_annual_or_monthly >> rail.Label('No') >> if_timeoff_not_KOR_annual_or_monthly_or_BEL_UK

        if_timeoff_not_KOR_annual_or_monthly_or_BEL_UK >> rail.Label('Yes') >> trigger_child_annual_leave_policy_52 >> catch_and_log_error
        if_timeoff_not_KOR_annual_or_monthly_or_BEL_UK >> rail.Label('No') >> catch_and_log_error

        catch_and_log_error

        catch_and_log_error >> log_to_sumo


    return dag

rail.for_each_instance(create_dag)
