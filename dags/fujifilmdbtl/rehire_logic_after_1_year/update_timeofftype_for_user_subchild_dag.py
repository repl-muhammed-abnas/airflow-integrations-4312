import rail
from fujifilmdbtl.rehire_logic_after_1_year.mappers.fujifilmdbtl_timeoff_balance_mapper import fdt_timeoff_balance_mapper
from fujifilmdbtl.rehire_logic_after_1_year.utils import request_payload

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.subchild_dag,
        description=f'Fujifilmdbtl | Rehire Logic | Update Timeofftype For User Subchild {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.update_timeofftype_dag_max_active_runs
    ) as dag:
        
        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

       
        get_tenure = rail.PythonOperator(
            task_id='get_tenure',
            python_callable=request_payload.get_tenure_method
        )

        is_timeofftype_allowed = rail.IfOperator(
            task_id = "is_timeofftype_allowed",
            test = "{{ dag_run.conf.is_timeoff_allowed | is_truthy }}",
            yes_task="get_default_timeofftype_policy_schedule_for_user",
            no_task="log_skipped_timeofftypes"
        )

        log_skipped_timeofftypes = rail.WriteLogOperator(
            task_id='log_skipped_timeofftypes',
            log="{{ dag_run.conf.log }}",
            message="Skipped | Time off type is not enabled in Replicon",
            severity="Skipped",
            properties=lambda dag_run: {
                "user": dag_run.conf['login_name'],
                "timeofftype": dag_run.conf['timeofftypename'],
                "details": "Time off type is not enabled in the user's profile",
                "status": "Exception"
            }
        )
        
        #step 7
        get_default_timeofftype_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id = "get_default_timeofftype_policy_schedule_for_user",
            endpoint = "/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data = {
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
                }
            }
        )

        #step 8 to 10
        check_if_policy_is_present = rail.IfOperator(
            task_id = "check_if_policy_is_present",
            test = request_payload.check_if_policy_is_present_method,
            yes_task="get_default_policy_from_global_level",
            no_task="end_task"
        )
     

        #From Step 11 to 18 
        get_reset_balance_amount_policy = rail.PythonOperator(
            task_id='get_reset_balance_amount_policy',
            python_callable=request_payload.get_reset_balance_amount_policy_method
        )


        #step 20
        get_default_policy_from_global_level = rail.RepliconServiceOperator(
            task_id = "get_default_policy_from_global_level",
            endpoint = "/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data = {
                    "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )


        #step 22
        if_timeofftype_sick_or_floating = rail.IfOperator(
            task_id = "if_timeofftype_sick_or_floating",
            test = lambda dag_run: dag_run.conf["timeofftypename"].lower() in ["sick leave", "floating holiday"], 
            yes_task = 'get_timeoff_balance_from_mapper',
            no_task = 'if_timeofftype_not_sick_and_floating'
        )

        #step 23 and 24
        get_timeoff_balance_from_mapper = rail.IfOperator(
            task_id="get_timeoff_balance_from_mapper",
            test= request_payload.get_timeoff_balance_from_mapper_method,
            yes_task='get_default_starting_balance_set_to_policy',
            no_task='if_timeofftype_not_sick_and_floating'
        )

        #step 25 to 30
        get_default_starting_balance_set_to_policy = rail.SetVariableOperator(
            task_id='get_default_starting_balance_set_to_policy',
            append=False,
            name='default_starting_balance_policy_task',
            value=request_payload.get_default_starting_balance_set_to_policy_method
        )
            

        #step 31
        get_updated_starting_balance_set_to_policy = rail.SetVariableOperator(
            task_id='get_updated_starting_balance_set_to_policy',
            append=False,
            name='starting_balance_policy_task',
            value=request_payload.get_updated_starting_balance_set_to_policy_method
        )
            

        #From 32 to 41st step  
        create_old_policy_schedules_list = rail.SetVariableOperator(
            task_id='create_old_policy_schedules_list',
            append=False,
            name='old_policy_schedules_list,', 
            value=request_payload.create_old_policy_schedules_list_method
        )


        #From step 43 to 45
        create_count_of_last_policy_list = rail.SetVariableOperator(
            task_id= 'create_count_of_last_policy_list',
            append=False,
            name='last_policy_list',
            value=request_payload.create_count_of_last_policy_list_method
        )

        #step 46
        find_the_least_difference = rail.PythonOperator(
            task_id="find_the_least_difference",
            python_callable=request_payload.find_the_least_difference_method
        )

        #step 42, 47 and 48, 49, 50 and 51
        create_count_of_new_policy_list = rail.SetVariableOperator(
            task_id="create_count_of_new_policy_list",
            append=False,
            name="count_of_new_policy_list",
            value=request_payload.create_count_of_new_policy_list_method
        )


        #from step 52 to 60
        create_new_policy_schedule_list = rail.SetVariableOperator(
            task_id="create_new_policy_schedule_list",
            append=False,
            name="new_policy_schedule_list",
            value=request_payload.create_new_policy_schedule_list_method
        )


        #to overcome append issue
        check_if_default_policy_and_new_policy_schedule_list_is_not_present = rail.IfOperator(
            task_id='check_if_default_policy_and_new_policy_schedule_list_is_not_present',
            yes_task='update_new_policy_schedule_list_if_list_size_0',
            no_task='get_existing_time_off_policies',
            test=lambda: (rail.result('get_default_policy_from_global_level') is not None and len(rail.result('create_new_policy_schedule_list')['value'])==0)
        )

        #from step 61 to 66         
        update_new_policy_schedule_list_if_list_size_0 = rail.SetVariableOperator(
            task_id='update_new_policy_schedule_list_if_list_size_0',
            append=True,
            name="new_policy_schedule_list",
            value=request_payload.update_policy_for_list_size_zero
        )

        
        #step 67
        get_existing_time_off_policies = rail.SetVariableOperator(
            task_id='get_existing_time_off_policies',
            append=False,
            name='existing_TO_policies',
            value=request_payload.calculate_existing_TO_policies
        )

        #step 68
        get_new_time_off_policies = rail.SetVariableOperator(
            task_id='get_new_time_off_policies',
            append=False,
            name='new_TO_policies',
            value=request_payload.calculate_new_TO_policies
        )


        #step 69
        new_policies = rail.SetVariableOperator(
            task_id='new_policies',
            append=False,
            name='n_policies',
            value=request_payload.calculate_new_policies
        )

        #step 70
        put_user_time_off_check = rail.IfOperator(
            task_id='put_user_time_off_check',
            yes_task='put_user_time_off_policy',
            no_task='if_timeofftype_not_sick_and_floating',
            test=request_payload.check_if_message_67_present
        )

        #step 71
        put_user_time_off_policy = rail.RepliconServiceOperator(
            task_id = "put_user_time_off_policy",
            endpoint = "/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data = lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": (rail.result('get_existing_time_off_policies')['value'] +
                                            rail.result('new_policies')['value'])
            }
        )

        #Step72
        if_timeofftype_not_sick_and_floating = rail.IfOperator(
            task_id = "if_timeofftype_not_sick_and_floating",
            test = lambda dag_run: dag_run.conf["timeofftypename"].lower() not in ["sick leave", "floating holiday"], 
            yes_task = 'check_if_policyset_is_present',
            no_task = 'end_task'
        )

        #step73 and 74
        check_if_policyset_is_present = rail.IfOperator(
            task_id = "check_if_policyset_is_present",
            test = lambda: bool(rail.result('get_default_timeofftype_policy_schedule_for_user') and rail.result('get_default_timeofftype_policy_schedule_for_user')[0]["policySet"]), 
            yes_task = 'create_policyschedules_list',
            no_task = 'end_task'
        )

        #step 75 to 83
        create_policyschedules_list = rail.SetVariableOperator(
            task_id='create_policyschedules_list',
            append=False,
            name='policyschedules',
            value=request_payload.calculate_create_policyschedules_list
        )

        #step 84 to 88
        create_count_of_new_policy_list2 = rail.SetVariableOperator(
            task_id='create_count_of_new_policy_list2',
            append=False,
            name='count_of_new_policy2',
            value=request_payload.calculate_create_count_of_new_policy_list2
        )

        #step 91 and 92
        last_policy_list2 = rail.PythonOperator(
            task_id='last_policy_list2',
            python_callable=request_payload.calculate_last_policy_list2
        )


        #step 93
        find_the_least_difference2 = rail.PythonOperator(
            task_id="find_the_least_difference2",
            python_callable=request_payload.find_least_difference2
        )


        #to prevent append issue
        if_diff_equals_least_difference2 = rail.IfOperator(
            task_id='if_diff_equals_least_difference2',
            test=request_payload.if_diff_equals_least_difference2_method,
            yes_task='update_count_of_new_policy2',
            no_task='get_count_of_new_policy2'
        )

        #step 94 and 95. 
        update_count_of_new_policy2 = rail.SetVariableOperator(
            task_id='update_count_of_new_policy2',
            append=True,
            name='count_of_new_policy2',
            value=request_payload.calculate_update_count_of_new_policy2
        )

        
        get_count_of_new_policy2 = rail.GetVariableOperator(
            task_id='get_count_of_new_policy2',
            name='count_of_new_policy2'
        )

        #to prevent append issue
        check_if_count_of_new_policy2_present = rail.IfOperator(
            task_id='check_if_count_of_new_policy2_present',
            test=lambda: bool(rail.result('get_count_of_new_policy2')['value']),
            yes_task='foreach_count_of_new_policy2',
            no_task='get_policyschedules'
        )


        foreach_count_of_new_policy2 = rail.ForEachOperator(
            task_id='foreach_count_of_new_policy2',
            items=lambda: rail.result('get_count_of_new_policy2')['value'],
            start_task='update_create_policyschedules_list',
            end_task='foreach_count_of_new_policy2_end'
        )

        foreach_count_of_new_policy2_end = rail.EmptyOperator(
            task_id='foreach_count_of_new_policy2_end',
        )


        update_create_policyschedules_list = rail.SetVariableOperator(
            task_id="update_create_policyschedules_list",
            append=True,
            name='policyschedules',
            value=request_payload.calculate_update_create_policyschedules_list
        )

        get_policyschedules = rail.GetVariableOperator(
            task_id='get_policyschedules',
            name='policyschedules'
        )

        #To overcome None append issue
        check_if_default_policy_and_new_policy_schedule_list_is_not_present2 = rail.IfOperator(
            task_id='check_if_default_policy_and_new_policy_schedule_list_is_not_present2',
            yes_task='update_if_policyschedulelist_size_equals_0',
            no_task='get_policyschedules2',
            test=lambda: (rail.result('get_default_policy_from_global_level') is not None and len(rail.result('get_policyschedules')['value'])==0)
        )

        #step 102 to 107
        update_if_policyschedulelist_size_equals_0 = rail.SetVariableOperator(
            task_id='update_if_policyschedulelist_size_equals_0',
            append=True,
            name='policyschedules',
            value=request_payload.calculate_update_if_policyschedulelist_size_equals_0
        )

        get_policyschedules2 = rail.GetVariableOperator(
            task_id='get_policyschedules2',
            name='policyschedules'
        )


        update_policy_schedule_payload_for_putusertimeoff = rail.PythonOperator(
            task_id='update_policy_schedule_payload_for_putusertimeoff',
            python_callable=request_payload.calculate_update_policy_schedule_payload_for_putusertimeoff
        )


        #step 110
        putUserTimeOffAccountPolicySetSchedule = rail.RepliconServiceOperator(
            task_id = "putUserTimeOffAccountPolicySetSchedule",
            endpoint = "/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data = lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('update_policy_schedule_payload_for_putusertimeoff')['value']
            }
        ) 


        end_task = rail.EmptyOperator(
            task_id="end_task"
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.log }}",
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'user': "{{dag_run.conf.userloginname}}",
                'timeofftype':"{{ dag_run.conf.timeofftypename }}",
                'details': "{{ get_error_message() }}",
                "status": "Error"
            }
        )



        get_tenure >> is_timeofftype_allowed
        
        is_timeofftype_allowed >> rail.Label('No') >> log_skipped_timeofftypes
        is_timeofftype_allowed >> rail.Label('Yes') >> get_default_timeofftype_policy_schedule_for_user >> check_if_policy_is_present >> rail.Label('Yes') >> get_default_policy_from_global_level >> \
            get_reset_balance_amount_policy >> if_timeofftype_sick_or_floating >> rail.Label("Yes") >> get_timeoff_balance_from_mapper >> rail.Label("Yes") >> get_default_starting_balance_set_to_policy >> \
                get_updated_starting_balance_set_to_policy >> create_old_policy_schedules_list >> create_count_of_last_policy_list >> find_the_least_difference >> create_count_of_new_policy_list >> \
                    create_new_policy_schedule_list >> check_if_default_policy_and_new_policy_schedule_list_is_not_present >> rail.Label("Yes") >> update_new_policy_schedule_list_if_list_size_0 >> \
                        get_existing_time_off_policies >> get_new_time_off_policies >> new_policies >> put_user_time_off_check >> rail.Label("Yes") >> put_user_time_off_policy
        put_user_time_off_policy >> if_timeofftype_not_sick_and_floating >> rail.Label("Yes") >> check_if_policyset_is_present >> rail.Label("Yes") >> create_policyschedules_list >> \
            create_count_of_new_policy_list2 >> last_policy_list2 >> find_the_least_difference2 >> if_diff_equals_least_difference2 >> rail.Label('Yes') >> update_count_of_new_policy2 >> \
                get_count_of_new_policy2 >> check_if_count_of_new_policy2_present >> rail.Label('Yes') >> foreach_count_of_new_policy2 >> update_create_policyschedules_list >> \
                    foreach_count_of_new_policy2_end >> get_policyschedules >> check_if_default_policy_and_new_policy_schedule_list_is_not_present2 >> rail.Label("Yes") >> update_if_policyschedulelist_size_equals_0 >> \
                        get_policyschedules2 >> update_policy_schedule_payload_for_putusertimeoff >> putUserTimeOffAccountPolicySetSchedule >> end_task >> catch_and_log_errors


        check_if_policy_is_present >> rail.Label('No') >> end_task
        if_timeofftype_sick_or_floating >> rail.Label("No") >> if_timeofftype_not_sick_and_floating
        get_timeoff_balance_from_mapper >> rail.Label("No") >> if_timeofftype_not_sick_and_floating
        check_if_default_policy_and_new_policy_schedule_list_is_not_present >> rail.Label("No") >> get_existing_time_off_policies
        put_user_time_off_check >> rail.Label("No") >> if_timeofftype_not_sick_and_floating
        if_timeofftype_not_sick_and_floating >> rail.Label("No") >> end_task
        check_if_policyset_is_present >> rail.Label("No") >> end_task
        if_diff_equals_least_difference2 >> rail.Label('No') >> get_count_of_new_policy2
        check_if_count_of_new_policy2_present >> rail.Label('No') >> get_policyschedules
        check_if_default_policy_and_new_policy_schedule_list_is_not_present2 >> rail.Label("No") >> get_policyschedules2
        foreach_count_of_new_policy2 >> foreach_count_of_new_policy2_end


    return dag

rail.for_each_instance(create_dag)

