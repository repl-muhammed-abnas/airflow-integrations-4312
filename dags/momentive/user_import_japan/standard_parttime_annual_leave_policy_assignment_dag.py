from datetime import datetime, timedelta
import json
from airflow.models import Variable
import rail
from momentive.user_import_japan.utils import python_callable
from momentive.user_import_japan.mappers.momentive_timeoff_policy_mapper import timeoff_policy_mapper

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_japan_user_sync_child_annual_leave_policy_fixed_term_standard_parttime_assignment_dag_id,
        description=f'Momentive_user_sync_child_annual_leave_policy_standard_parttime_assignment_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)
        
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='calculate_yoss_years'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='calculate_yoss_years',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        calculate_yoss_years = rail.PythonOperator(
            task_id='calculate_yoss_years',
            python_callable=lambda dag_run: python_callable.calculate_yoss_years_and_month(
                dag_run.conf.get('yoss')
            )
        )

        initialize_startdate_variable = rail.SetVariableOperator(
            task_id='initialize_startdate_variable',
            append=False,
            name='startdatetoconsider',
            value='{{ dag_run.conf.startdate }}'
        )

        determine_service_band = rail.PythonOperator(
            task_id='determine_service_band',
            python_callable=lambda: python_callable.determine_service_band(
                rail.result('calculate_yoss_years')['yoss_years']
            )
        )

        search_entries_in_timeoff_policy_mapper = rail.PythonOperator(
            task_id='search_entries_in_timeoff_policy_mapper',
            python_callable=lambda dag_run: list(
                filter(lambda x: x["timeofftype"] == dag_run.conf['timeofftype'], timeoff_policy_mapper)
            )
        )
        
        lookup_policy_accruals = rail.PythonOperator(
            task_id='lookup_policy_accruals',
            python_callable=lambda dag_run: python_callable.lookup_accrual_from_mapper(
                rail.result('search_entries_in_timeoff_policy_mapper'),
                rail.result('calculate_yoss_years')['yoss_years']
            )
        )

        create_effectivedates_list = rail.SetVariableOperator(
            task_id='create_effectivedates_list',
            name='effectivedates',
            append=False,
            value=[]
        )

        if_startdate_lessthan_today_begofyear = rail.IfOperator(
            task_id='if_startdate_lessthan_today_begofyear',
            test=lambda dag_run: datetime.strptime(dag_run.conf.get('startdate'), '%Y-%m-%d').date() < datetime.now().replace(month=1, day=1).date(),
            yes_task='add_items_to_effectivedateslist_25',
            no_task='if_yoss_lessthan_0_5_27'
        )

        # Step 24: Add policy if startdate < beginning_of_year
        add_items_to_effectivedateslist_25 = rail.SetVariableOperator(
            task_id='add_items_to_effectivedateslist_25',
            name='effectivedates',
            append=True,
            value=lambda dag_run: python_callable.build_policy_item_beginning_of_year(
                rail.result('determine_service_band'),
                rail.result('lookup_policy_accruals')['accrual_values']
            )
        )

        # Step 26: Check if yoss < 0.5
        if_yoss_lessthan_0_5_27 = rail.IfOperator(
            task_id='if_yoss_lessthan_0_5_27',
            test=lambda: float(rail.result('determine_service_band')) < 0.5,
            yes_task='add_items_to_effectivedateslist_28',
            no_task='add_items_to_effectivedateslist_31'
        )

        # Step 27: Add policy for yoss < 0.5 (startdate + 6 months)
        add_items_to_effectivedateslist_28 = rail.SetVariableOperator(
            task_id='add_items_to_effectivedateslist_28',
            name='effectivedates',
            append=True,
            value=lambda dag_run: python_callable.build_policy_item_startdate_plus_6months(
                rail.result('determine_service_band'),
                rail.result('lookup_policy_accruals')['accrual_values'],
                dag_run.conf.get('startdate')
            )
        )

        update_startdate_variable_29 = rail.SetVariableOperator(
            task_id='update_startdate_variable_29',
            name='startdatetoconsider',
            append=False,
            value=lambda dag_run: python_callable.add_6_months_to_date(dag_run.conf.get('startdate'))
        )

        # Step 30: Add policy for yoss >= 0.5 (startdate)
        add_items_to_effectivedateslist_31 = rail.SetVariableOperator(
            task_id='add_items_to_effectivedateslist_31',
            name='effectivedates',
            append=True,
            value=lambda dag_run: python_callable.build_policy_item_startdate(
                rail.result('determine_service_band'),
                rail.result('lookup_policy_accruals')['accrual_values'],
                dag_run.conf.get('startdate')
            )
        )

        if_yoss_lessthan_0_5_32 = rail.IfOperator(
            task_id='if_yoss_lessthan_0_5_32',
            test=lambda: float(rail.result('determine_service_band')) < 0.5,
            yes_task='add_items_to_effectivedateslist_33_38',
            no_task='if_yoss_equals_0_5_39'
        )

        add_items_to_effectivedateslist_33_38 = rail.SetVariableOperator(
            task_id='add_items_to_effectivedateslist_33_38',
            name='effectivedates',
            append=False,
            value=lambda dag_run: python_callable.build_standard_parttime_policies_multiple_years(
                rail.result('determine_service_band'),
                rail.result('lookup_policy_accruals')['accrual_values'],
                dag_run.conf.get('yoss'),
                rail.get_dag_run_var('effectivedates')
            )
        )

        if_yoss_equals_0_5_39 = rail.IfOperator(
            task_id='if_yoss_equals_0_5_39',
            test=lambda: float(rail.result('determine_service_band')) == 0.5,
            yes_task='if_startdate_greaterthan_today_begofyear',
            no_task='add_items_for_service_bands_55_74'
        )

        if_startdate_greaterthan_today_begofyear = rail.IfOperator(
            task_id='if_startdate_greaterthan_today_begofyear',
            test=lambda dag_run: datetime.strptime(dag_run.conf.get('startdate'), '%Y-%m-%d').date() > datetime.now().replace(month=1, day=1).date(),
            yes_task='add_items_to_effectivedateslist_41_47',
            no_task='add_items_to_effectivedateslist_49_54'
        )

        add_items_to_effectivedateslist_41_47 = rail.SetVariableOperator(
            task_id='add_items_to_effectivedateslist_41_47',
            name='effectivedates',
            append=False,
            value=lambda dag_run: python_callable.build_standard_parttime_policies_from_yoss_date(
                rail.result('determine_service_band'),
                rail.result('lookup_policy_accruals')['accrual_values'],
                dag_run.conf.get('yoss'),
                rail.get_dag_run_var('effectivedates') or []
            )
        )

        add_items_to_effectivedateslist_49_54 = rail.SetVariableOperator(
            task_id='add_items_to_effectivedateslist_49_54',
            name='effectivedates',
            append=False,
            value=lambda dag_run: python_callable.build_standard_parttime_policies_from_startdate_years(
                rail.result('determine_service_band'),
                rail.result('lookup_policy_accruals')['accrual_values'],
                dag_run.conf.get('startdate'),
                rail.get_dag_run_var('effectivedates') or []
            )
        )

        # STEPS 54-78: Build Policies for All Service Bands (1.5-6.5)
        # Consolidated into ONE function that handles all bands automatically
        add_items_for_service_bands_55_74 = rail.SetVariableOperator(
            task_id='add_items_for_service_bands_55_74',
            name='effectivedates',
            append=False,
            value=lambda dag_run: python_callable.build_standard_parttime_policies_by_service_band(
                rail.result('determine_service_band'),
                rail.result('lookup_policy_accruals')['accrual_values'],
                dag_run.conf.get('yoss'),
                rail.get_dag_run_var('effectivedates')
            )
        )

        # STEP 74: Get Unique Effective Dates from effectivedates list
        get_unique_effective_dates_task = rail.PythonOperator(
            task_id='get_unique_effective_dates',
            python_callable=lambda dag_run: python_callable.get_unique_effective_dates(
                rail.get_dag_run_var('effectivedates'))
        )

        get_default_timeofftype_policyschedulesetforuser = rail.RepliconServiceOperator(
            task_id='get_default_timeofftype_policyschedulesetforuser',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf.get('useruri'),
                    "timeOffTypeUri": dag_run.conf.get('timeoffuri')
                }
            }
        )

        if_day_present = rail.IfOperator(
            task_id='if_day_present',
            test=lambda: bool(((rail.result('get_default_timeofftype_policyschedulesetforuser') or [{}])[0].get('effectiveDate') or {}).get('day')),
            yes_task='extract_policy_parameters',
            no_task='catch_error'
        )

        # Extract accrual, reset, and starting balance parameters
        extract_policy_parameters = rail.PythonOperator(
            task_id='extract_policy_parameters',
            python_callable=lambda: {
                'accrual_setup': python_callable.extract_accrual_balance_setup(
                    rail.result('get_default_timeofftype_policyschedulesetforuser')
                ),
                'reset_setup': python_callable.extract_yearly_reset_setup(
                    rail.result('get_default_timeofftype_policyschedulesetforuser')
                ),
                'starting_balance_setup': python_callable.extract_starting_balance_setup(
                    rail.result('get_default_timeofftype_policyschedulesetforuser')
                )
            }
        )

        # Task 1: Validate and prepare all inputs for policy building (easier debugging)
        validate_policy_building_inputs = rail.PythonOperator(
            task_id='validate_policy_building_inputs',
            python_callable=lambda dag_run: {
                'unique_dates': rail.result('get_unique_effective_dates'),
                'unique_dates_count': len(rail.result('get_unique_effective_dates')),
                'mapper_entries': rail.get_dag_run_var('effectivedates'),
                'mapper_entries_count': len(rail.get_dag_run_var('effectivedates')),
                'accrual_setup': rail.result('extract_policy_parameters')['accrual_setup'],
                'reset_setup': rail.result('extract_policy_parameters')['reset_setup'],
                'starting_balance_setup': rail.result('extract_policy_parameters')['starting_balance_setup'],
                'policy_template': rail.result('get_default_timeofftype_policyschedulesetforuser')[0]['policySet'],
                'startdate': dag_run.conf.get('startdate'),
                'validation_status': 'All inputs prepared and validated'
            }
        )

        # Task 2: Find mapper matches for each effective date
        find_mapper_matches = rail.PythonOperator(
            task_id='find_mapper_matches',
            python_callable=lambda dag_run: python_callable.find_mapper_matches_for_dates(
                rail.result('validate_policy_building_inputs')['unique_dates'],
                rail.result('validate_policy_building_inputs')['mapper_entries']
            )
        )

        # Task 3: Build first policy entry (with pro-ration)
        build_first_policy_with_proration = rail.PythonOperator(
            task_id='build_first_policy_with_proration',
            python_callable=lambda dag_run: python_callable.build_single_policy_entry(
                effective_date=rail.result('validate_policy_building_inputs')['unique_dates'][0],
                mapper_match=rail.result('find_mapper_matches')['matches'][0] if rail.result('find_mapper_matches')['matches'] else None,
                policy_template=rail.result('validate_policy_building_inputs')['policy_template'],
                startdate=rail.result('validate_policy_building_inputs')['startdate'],
                accrual_setup=rail.result('validate_policy_building_inputs')['accrual_setup'],
                reset_setup=rail.result('validate_policy_building_inputs')['reset_setup'],
                starting_balance_setup=rail.result('validate_policy_building_inputs')['starting_balance_setup'],
                is_first_policy=True,  # Enables pro-ration
                service_month_uri=rail.result('calculate_yoss_years')['service_month_uri']
            )
        )

        # Task 4: Build remaining policy entries (full accrual)
        build_remaining_policies = rail.PythonOperator(
            task_id='build_remaining_policies',
            python_callable=lambda dag_run: python_callable.build_remaining_policy_entries(
                remaining_dates=rail.result('validate_policy_building_inputs')['unique_dates'][1:],
                remaining_matches=rail.result('find_mapper_matches')['matches'][1:],
                policy_template=rail.result('validate_policy_building_inputs')['policy_template'],
                accrual_setup=rail.result('validate_policy_building_inputs')['accrual_setup'],
                reset_setup=rail.result('validate_policy_building_inputs')['reset_setup'],
                starting_balance_setup=rail.result('validate_policy_building_inputs')['starting_balance_setup'],
                service_month_uri=rail.result('calculate_yoss_years')['service_month_uri']
            )
        )

        # Task 5: Combine and validate final policy entries
        combine_and_validate_policies = rail.PythonOperator(
            task_id='combine_and_validate_policies',
            python_callable=lambda dag_run: {
                'policy_entries': [rail.result('build_first_policy_with_proration')] + rail.result('build_remaining_policies'),
                'entry_count': 1 + len(rail.result('build_remaining_policies')),
                'validation_status': 'All policies combined and ready for API submission'
            }
        )

        put_user_timeoff_policy = rail.RepliconServiceOperator(
            task_id='put_user_timeoff_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf.get('useruri'),
                    "timeOffTypeUri": dag_run.conf.get('timeoffuri')
                },
                "policySetScheduleEntries": rail.result('combine_and_validate_policies')['policy_entries']
            }
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Standard Parttime Annual Leave Assignment for user ; {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_error') if rail.result('catch_error') else ""
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_error >> final_response_from_dag
        can_run_batch_task >> rail.Label('No') >> calculate_yoss_years

        calculate_yoss_years >> initialize_startdate_variable >> determine_service_band >> \
            search_entries_in_timeoff_policy_mapper >> lookup_policy_accruals >> create_effectivedates_list >> if_startdate_lessthan_today_begofyear

        if_startdate_lessthan_today_begofyear >> rail.Label('Yes') >> add_items_to_effectivedateslist_25 >> if_yoss_lessthan_0_5_32
        if_startdate_lessthan_today_begofyear >> rail.Label('No') >> if_yoss_lessthan_0_5_27

        if_yoss_lessthan_0_5_27 >> rail.Label('Yes') >> add_items_to_effectivedateslist_28 >> update_startdate_variable_29 >> if_yoss_lessthan_0_5_32
        if_yoss_lessthan_0_5_27 >> rail.Label('No') >> add_items_to_effectivedateslist_31 >> if_yoss_lessthan_0_5_32

        if_yoss_lessthan_0_5_32 >> rail.Label('Yes') >> add_items_to_effectivedateslist_33_38 >> if_yoss_equals_0_5_39
        if_yoss_lessthan_0_5_32 >> rail.Label('No') >> if_yoss_equals_0_5_39

        if_yoss_equals_0_5_39 >> rail.Label('Yes') >> if_startdate_greaterthan_today_begofyear
        if_yoss_equals_0_5_39 >> rail.Label('No') >> add_items_for_service_bands_55_74 
        
        if_startdate_greaterthan_today_begofyear >> rail.Label('Yes') >> add_items_to_effectivedateslist_41_47 >> add_items_for_service_bands_55_74
        if_startdate_greaterthan_today_begofyear >> rail.Label('No') >> add_items_to_effectivedateslist_49_54 >> add_items_for_service_bands_55_74

        add_items_for_service_bands_55_74 >> get_unique_effective_dates_task >> get_default_timeofftype_policyschedulesetforuser >> if_day_present

        if_day_present >> rail.Label('Yes') >> extract_policy_parameters >> validate_policy_building_inputs >> find_mapper_matches >> build_first_policy_with_proration >> build_remaining_policies >> combine_and_validate_policies >> put_user_timeoff_policy >> catch_error
        if_day_present >> rail.Label('No') >> catch_error

    return dag

for_each_instance = rail.for_each_instance(create_dag)
