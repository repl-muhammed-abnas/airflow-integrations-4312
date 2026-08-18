from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail
from momentive.user_import_japan.utils import python_callable, request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_japan_user_sync_child_update_user_timeoff_assign_id,
        description=f'Momentive_user_sync_Timeoff_add_update_user_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        # =====================================================================
        # PHASE 1: Setup/Validation Operators
        # =====================================================================

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_child_trigger_list'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_child_trigger_list',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        create_child_trigger_list = rail.SetVariableOperator(
            task_id='create_child_trigger_list',
            name='childtriggeredlist',
            append=False,
            value=[]
        )

        # =====================================================================
        # PHASE 2: Preprocessing Operators - Workato Steps 1-22
        # =====================================================================

        get_assigned_timeoff_types = rail.RepliconServiceOperator(
            task_id='get_assigned_timeoff_types',
            endpoint="/services/TimeOffService1.svc/BulkGetTimeOffTypeAssignmentsForUsers",
            data={
                "userUris": [
                    "{{ dag_run.conf.useruri }}"
                ]
            }
        )

        get_enabled_timeoff_types = rail.RepliconServiceOperator(
            task_id='get_enabled_timeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        if_timeofftypes_tobeassigned_present = rail.IfOperator(
            task_id='if_timeofftypes_tobeassigned_present',
            test=lambda dag_run: bool(dag_run.conf.get('timeofftypes')),
            yes_task='build_update_timeoff_lists',
            no_task='catch_error'
        )

        build_update_timeoff_lists = rail.PythonOperator(
            task_id='build_update_timeoff_lists',
            python_callable=lambda dag_run: python_callable.build_update_timeoff_lists(
                rail.result('get_enabled_timeoff_types'),
                dag_run.conf.get('timeofftypes', ''),
                rail.result('get_assigned_timeoff_types')
            )
        )

        # =====================================================================
        # PHASE 3: Assignment Operator - Workato Step 30
        # =====================================================================

        if_final_uris_present = rail.IfOperator(
            task_id='if_final_uris_present',
            test=lambda: bool(rail.result('build_update_timeoff_lists')['final_uris']),
            yes_task='remove_unassigned_timeoff_types',
            no_task='create_child_trigger_list2'
        )

        remove_unassigned_timeoff_types = rail.RepliconServiceOperator(
            task_id='remove_unassigned_timeoff_types',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('build_update_timeoff_lists')['final_uris']
            }
        )

        # =====================================================================
        # PHASE 4: Payout Loop Operators - Workato Steps 23-26
        # =====================================================================

        # Workato Step 23: payout loop over PREVIOUSLY ASSIGNED types that are no
        # longer in the requested set (recipe #25: previous uri blank in new list)
        foreach_timeoff_payout = rail.ForEachOperator(
            task_id='foreach_timeoff_payout',
            items=lambda: rail.result("build_update_timeoff_lists")["to_remove"],
            start_task='check_not_previously_assigned_payout',
            end_task='payout_loop_end'
        )

        check_not_previously_assigned_payout = rail.IfOperator(
            task_id='check_not_previously_assigned_payout',
            test=lambda: bool(rail.result('foreach_timeoff_payout').get('uri')),
            yes_task='trigger_child_payout_for_timeoff_update',
            no_task='payout_loop_end'
        )

        # Workato Step 26: Call China_Japan_Momentive_Child_payout for timeoff_update v3.0
        trigger_child_payout_for_timeoff_update = rail.TriggerDagRunOperator(
            task_id='trigger_child_payout_for_timeoff_update',
            trigger_dag_id=config.momentive_japan_child_payout_for_timeoff_update_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['useruri'],
                "hiredate": dag_run.conf.get('hiredate', ''),
                "terminationdate": dag_run.conf.get('terminationdate', ''),
                "active": dag_run.conf.get('active', ''),
                "useruri": dag_run.conf['useruri'],
                "timeoffupdate": "yes",
                "timeoffuri": rail.result('foreach_timeoff_payout').get('uri')
            }
        )

        insert_childid_to_wait_list_1 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_1',
            name="{{result('create_child_trigger_list').name}}",
            append=True,
            value="{{result('trigger_child_payout_for_timeoff_update')}}"
        )

        payout_loop_end = rail.EmptyOperator(
            task_id='payout_loop_end'
        )
        

        child_dag_ids = rail.PythonOperator(
            task_id='child_dag_ids',
            python_callable=lambda: [
                int(item) for item in rail.get_dag_run_var('childtriggeredlist')] if rail.get_dag_run_var('childtriggeredlist') else []
        )

        wait_for_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('child_dag_ids') | to_json}}"
        )

        gather_responses_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_responses_from_child',
            dag_runs='{{ result("child_dag_ids") }}',
            dagrun_task_id='final_response_from_dag',
            execution_timeout=timedelta(
                hours=config.responses_from_child_timeout),
            flatten=True
        )

        filter_error_responses = rail.PythonOperator(
            task_id='filter_error_responses',
            python_callable=lambda: [item for item in rail.result(
                'gather_responses_from_child') if item]
        )

        create_child_trigger_list2 = rail.SetVariableOperator(
            task_id='create_child_trigger_list2',
            name='childtriggeredlist2',
            append=False,
            value=[]
        )

        # =====================================================================
        # PHASE 5: Rehire/Schedule Loop Operators - Workato Steps 33-41
        # =====================================================================

        # Workato Step 33: Post-processing loop over assigned timeoffs
        foreach_assigned_timeoff = rail.ForEachOperator(
            task_id='foreach_assigned_timeoff',
            items=lambda: rail.result("build_update_timeoff_lists")["matched"],
            start_task='check_not_previously_assigned',
            end_task='assignment_loop_end'
        )

        # Workato Step 35: Check if timeoff was NOT previously assigned
        check_not_previously_assigned = rail.IfOperator(
            task_id='check_not_previously_assigned',
            test=lambda: rail.result('foreach_assigned_timeoff').get('action') == 'add',
            yes_task='trigger_timeoff_add_rehire_user',
            no_task='check_schedule_change_and_previously_assigned'
        )

        # Workato Step 36: Call Japan_Momentive_Timeoff_add_rehire_user
        trigger_timeoff_add_rehire_user = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_add_rehire_user',
            trigger_dag_id=config.momentive_japan_timeoff_add_rehire_user_update_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['hiredate'],
                "continuous_service_date": dag_run.conf.get('continious_service_date'),
                "timeoff_service_date": dag_run.conf.get('timeoff_service_date'),
                "timeoffuri": rail.result('foreach_assigned_timeoff').get('uri'),
                "timeoffname": rail.result('foreach_assigned_timeoff').get('name'),
                "workshift_changeddate": dag_run.conf.get('workshift_change_effective_date'),
                "lastname": None,
                "firstname": None,
                "middlename": None,
                "loginname": dag_run.conf['useruri'],
                "employeeid": dag_run.conf['useruri'],
                "emailaddress": dag_run.conf['useruri']
            }
        )

        insert_childid_to_wait_list_3 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_3',
            name="{{result('create_child_trigger_list2').name}}",
            append=True,
            value="{{result('trigger_timeoff_add_rehire_user')}}"
        )

        # Workato Step 37: schedule change AND previously assigned; recipe does
        # NOTHING when false (the hire-date check #40 lives inside this gate)
        check_schedule_change_and_previously_assigned = rail.IfOperator(
            task_id='check_schedule_change_and_previously_assigned',
            test=lambda dag_run: (
                (dag_run.conf.get('schedulechange') or '').lower() == 'yes' and
                rail.result('foreach_assigned_timeoff').get('action') == 'update'
            ),
            yes_task='check_shift_worker_holiday_types',
            no_task='check_hire_date_changed'
        )

        # Workato Step 38: Check for specific shift worker holiday types
        check_shift_worker_holiday_types = rail.IfOperator(
            task_id='check_shift_worker_holiday_types',
            test=lambda: rail.result('foreach_assigned_timeoff').get('name') in [
                '07. JPN_個別休日 - 個別連休（上期） Shift Worker Holiday - Consecutive Holiday - 1st Half',
                '08. JPN_個別休日 - 個別連休（下期） Shift Worker Holiday - Consecutive Holiday - 2nd Half',
                '09. JPN_個別休日 - 個別休日 Shift Worker Holiday - Inconsecutive Holiday',
                '06. JPN_個別休日 - 誕生日休日 Shift Worker Holiday - Birthday Holiday',
                '10. JPN_個別休暇 - 夏期 Shift Worker Special Vacation (Summer)'
            ],
            yes_task='assignment_loop_end',
            no_task='assignment_loop_end'
        )

        # Workato Step 40: Check if hire date changed (rehire scenario)
        check_hire_date_changed = rail.IfOperator(
            task_id='check_hire_date_changed',
            test=lambda dag_run: bool(dag_run.conf.get('old_startdate') and datetime.strptime(
                dag_run.conf['old_startdate'], '%Y-%m-%d') != datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d')),
            yes_task='trigger_policy_assignment_rehire',
            no_task='assignment_loop_end'
        )

        # Workato Step 41: Call Child Annual Leave policy_days Assignment_rehire_v3.0
        trigger_policy_assignment_rehire = rail.TriggerDagRunOperator(
            task_id='trigger_policy_assignment_rehire',
            trigger_dag_id=config.momentive_japan_policy_assignment_rehire_update_days_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['hiredate'],
                "type": "rehire",
                "timeoffuri": rail.result('foreach_assigned_timeoff').get('uri'),
                "timeofftype": rail.result('foreach_assigned_timeoff').get('name'),
                "actualstartdate": dag_run.conf.get('old_startdate'),  # Old hire date
                "update": "update"
            }
        )

        insert_childid_to_wait_list_4 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_4',
            name="{{result('create_child_trigger_list2').name}}",
            append=True,
            value="{{result('trigger_policy_assignment_rehire')}}"
        )

        assignment_loop_end = rail.EmptyOperator(
            task_id='assignment_loop_end'
        )

        child_dag_ids2 = rail.PythonOperator(
            task_id='child_dag_ids2',
            python_callable=lambda: [
                int(item) for item in rail.get_dag_run_var('childtriggeredlist2')] if rail.get_dag_run_var('childtriggeredlist2') else []
        )

        wait_for_child_dags2 = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('child_dag_ids2') | to_json}}"
        )

        gather_responses_from_child2 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_responses_from_child2',
            dag_runs='{{ result("child_dag_ids2") }}',
            dagrun_task_id='final_response_from_dag',
            execution_timeout=timedelta(
                hours=config.responses_from_child_timeout),
            flatten=True
        )

        filter_error_responses2 = rail.PythonOperator(
            task_id='filter_error_responses2',
            python_callable=lambda: [item for item in rail.result(
                'gather_responses_from_child2') if item]
        )

        errorsfrom_both_child_dags = rail.PythonOperator(
            task_id='errorsfrom_both_child_dags',
            python_callable=lambda: (rail.result('filter_error_responses') or []) + (rail.result('filter_error_responses2') or [])
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in timeoff update for user ; {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_error') if rail.result('catch_error') else (rail.result('errorsfrom_both_child_dags') or null)
        )
        # =====================================================================
        # PHASE 7: Task Flow Dependencies
        # =====================================================================

        # Batch Task Flow
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error >> final_response_from_dag
        can_run_batch_task >> rail.Label('No') >> create_child_trigger_list

        # Workato Steps 1-22: Preprocessing - build timeoff lists
        create_child_trigger_list >> get_assigned_timeoff_types >> get_enabled_timeoff_types >> if_timeofftypes_tobeassigned_present >> rail.Label('Yes') >> build_update_timeoff_lists
        if_timeofftypes_tobeassigned_present >> rail.Label('No') >> catch_error

        # Workato Steps 23-26: payout loop runs BEFORE the assignment PUT (recipe order)
        build_update_timeoff_lists >> foreach_timeoff_payout
        foreach_timeoff_payout >> check_not_previously_assigned_payout
        check_not_previously_assigned_payout >> rail.Label('Yes') >> trigger_child_payout_for_timeoff_update >> insert_childid_to_wait_list_1 >> payout_loop_end
        check_not_previously_assigned_payout >> rail.Label('No') >> payout_loop_end

        # Workato Steps 28-30: assign the new set only when at least one uri matched
        foreach_timeoff_payout >> payout_loop_end >> child_dag_ids >> wait_for_child_dags >> gather_responses_from_child >> filter_error_responses >> if_final_uris_present
        if_final_uris_present >> rail.Label('Yes') >> remove_unassigned_timeoff_types >> create_child_trigger_list2
        if_final_uris_present >> rail.Label('No') >> create_child_trigger_list2

        # Workato Steps 33-41: Rehire/Schedule Change Loop Flow
        create_child_trigger_list2 >> foreach_assigned_timeoff
        foreach_assigned_timeoff >> check_not_previously_assigned
        check_not_previously_assigned >> rail.Label('Yes') >> trigger_timeoff_add_rehire_user >> insert_childid_to_wait_list_3 >> check_schedule_change_and_previously_assigned
        check_not_previously_assigned >> rail.Label('No') >> check_schedule_change_and_previously_assigned

        # Schedule Change Sub-flow (recipe #37 / #39):
        #   schedulechange == yes -> shift-worker holiday types (06-10) are skipped (no reassignment)
        #   ELSE (schedulechange != yes) -> if the start date changed (rehire), run the rehire
        #   annual-leave assignment.
        # The recipe passes schedulechange = nil unconditionally (update recipe #311), so the ELSE
        # is the active path and the rehire check must live under the No branch (not the Yes branch).
        check_schedule_change_and_previously_assigned >> rail.Label('Yes') >> check_shift_worker_holiday_types
        check_schedule_change_and_previously_assigned >> rail.Label('No') >> check_hire_date_changed  # ELSE (recipe #39/#40)
        check_shift_worker_holiday_types >> rail.Label('Yes') >> assignment_loop_end  # shift holiday -> skip
        check_shift_worker_holiday_types >> rail.Label('No') >> assignment_loop_end   # #38 has no rehire action
        check_hire_date_changed >> rail.Label('Yes') >> trigger_policy_assignment_rehire >> insert_childid_to_wait_list_4 >> assignment_loop_end
        check_hire_date_changed >> rail.Label('No') >> assignment_loop_end

        # Final Flow
        foreach_assigned_timeoff >> assignment_loop_end >> child_dag_ids2 >> wait_for_child_dags2 >> gather_responses_from_child2 >> filter_error_responses2 >> errorsfrom_both_child_dags >> catch_error

        return dag


rail.for_each_instance(create_dag)