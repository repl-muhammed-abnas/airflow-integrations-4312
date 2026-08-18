"""
GuestTek Talent to Replicon User Import Integration - Master DAG

This module defines the master DAG that orchestrates the Talent to Replicon user import process.

The DAG workflow:
    1. Initialize logging
    2. Fetch all users from Talent API (paginated)
    3. Filter to delta users (user_last_modified >= yesterday)
    4. If no delta -> send "no data" email, end
    5. Get Replicon prerequisites (users, groups, templates, etc.)
    6. Create missing groups (UserTypes, Roles, Service Centers) - only if deltas exist
    7. Refresh prerequisites
    8. Categorize users: New, Update, Disable, Skip
    9. Trigger child DAGs in parallel (add, update, disable)
    10. Wait for all processing
    11. Trigger supervisor double-check
    12. Aggregate logs and send completion email

Schedule: Daily at 12:15 AM MST (cron: 15 0 * * *)
Delta Detection: user_last_modified (last 24 hours)
"""
from datetime import timedelta
import pendulum
import rail
import itertools
from airflow.models import Variable
from guesttekinteractive.talent_user_import.utils import custom_method, request_payload, response_filters
from guesttekinteractive.talent_user_import.task.get_talent_users import get_talent_users_task_group
from guesttekinteractive.talent_user_import.task.get_user_prereqs import get_user_prereqs_task_group, get_updated_user_prereqs_task_group
from guesttekinteractive.talent_user_import.mappers.user_sync_mapper import is_valid_mapper_key
from guesttekinteractive.talent_user_import import config as base_config

null = None


def create_main_dag(config):
    """Create the master DAG for Talent user import integration."""
    with rail.create_airflow_dag(
        dag_id=config.master_dag,
        description=f'GuestTek Talent User Import - Master DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.master_dag_interval,
        max_active_runs=config.max_active_run_master,
    ) as dag:
        
        # Initialize logging
        create_log = rail.CreateLogOperator(task_id='create_log')
        
        # Fetch users from Talent API
        get_talent_users_start, get_talent_users_group = get_talent_users_task_group(config)
        
        # Check if there are delta users
        has_delta_users = rail.IfOperator(
            task_id='has_delta_users',
            test="{{ result('get_delta_count') > 0 }}",
            yes_task='dummy_get_user_prereqs_start',
            no_task='send_no_delta_email'
        )
        
        send_no_delta_email = rail.EmailOperator(
            task_id='send_no_delta_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | GuestTek Talent User Sync - No Records - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/no_delta_users.html"
        )
        
        # Get Replicon prerequisites
        get_prereqs_start, get_prereqs_group = get_user_prereqs_task_group(config)
        
        # Prepare data for groups processing
        prepare_groups_data = rail.PythonOperator(
            task_id='prepare_groups_data',
            python_callable=lambda: _get_unique_usertypes_from_delta()
        )
        
        prepare_roles_data = rail.PythonOperator(
            task_id='prepare_roles_data',
            python_callable=lambda: _get_unique_roles_from_delta()
        )
        
        prepare_service_centers_data = rail.PythonOperator(
            task_id='prepare_service_centers_data',
            python_callable=lambda: _get_unique_service_centers_from_delta()
        )
        
        # Conditional checks for each entity type
        has_usertypes = rail.IfOperator(
            task_id='has_usertypes',
            test="{{ result('prepare_groups_data') | length > 0 }}",
            yes_task='trigger_process_groups',
            no_task='skip_usertypes'
        )
        
        has_roles = rail.IfOperator(
            task_id='has_roles',
            test="{{ result('prepare_roles_data') | length > 0 }}",
            yes_task='trigger_process_roles',
            no_task='skip_roles'
        )
        
        has_service_centers = rail.IfOperator(
            task_id='has_service_centers',
            test="{{ result('prepare_service_centers_data') | length > 0 }}",
            yes_task='trigger_process_service_centers',
            no_task='skip_service_centers'
        )
        
        skip_usertypes = rail.EmptyOperator(task_id='skip_usertypes')
        skip_roles = rail.EmptyOperator(task_id='skip_roles')
        skip_service_centers = rail.EmptyOperator(task_id='skip_service_centers')
        
        # Process groups (create missing UserTypes)
        trigger_process_groups = rail.TriggerDagRunOperator(
            task_id='trigger_process_groups',
            trigger_dag_id=config.process_groups_dag_id,
            conf=lambda: {
                'delta_usertypes': rail.result('prepare_groups_data'),
                'replicon_usertypes_details': rail.write_json_artifact(rail.result('get_employeetype_groups_data')),
            },
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        
        # Process roles (create missing Roles)
        trigger_process_roles = rail.TriggerDagRunOperator(
            task_id='trigger_process_roles',
            trigger_dag_id=config.process_roles_dag_id,
            conf=lambda: {
                'delta_roles': rail.result('prepare_roles_data'),
                'replicon_roles_details': rail.write_json_artifact(rail.result('get_all_project_roles')),
            },
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        
        # Process service centers (create missing Service Centers)
        trigger_process_service_centers = rail.TriggerDagRunOperator(
            task_id='trigger_process_service_centers',
            trigger_dag_id=config.process_service_centers_dag_id,
            conf=lambda: {
                'delta_service_centers': rail.result('prepare_service_centers_data'),
                'replicon_service_centers_details': rail.write_json_artifact(rail.result('get_all_service_centers')),
            },
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        
        # Join point after entity creation
        entity_creation_done = rail.EmptyOperator(task_id='entity_creation_done')
        
        # Refresh prerequisites after group creation
        get_updated_prereqs_start, get_updated_prereqs_group = get_updated_user_prereqs_task_group(config)
        
        # Categorize users
        categorize_users = rail.PythonOperator(
            task_id='categorize_users',
            python_callable=lambda: custom_method.categorize_users(
                rail.result('filter_delta_users'),
                rail.result('get_all_replicon_users')
            )
        )
        
        # Check if there are users to process
        has_users_to_process = rail.IfOperator(
            task_id='has_users_to_process',
            test=lambda: (len(rail.result('categorize_users').get('new', [])) > 0 or 
                         len(rail.result('categorize_users').get('update', [])) > 0 or
                         len(rail.result('categorize_users').get('disable', [])) > 0),
            yes_task='dummy_process_users',
            no_task='trigger_log_generation_empty'
        )
        
        dummy_process_users = rail.EmptyOperator(task_id='dummy_process_users')
        
        # Process new users
        process_new_users = rail.trigger_parallel_dagrun(
            task_id='process_new_users',
            items=lambda: _prepare_new_user_items(config),
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            trigger_dag_id=config.process_each_user,
            conf=lambda item: item,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        
        # Process update users
        process_update_users = rail.trigger_parallel_dagrun(
            task_id='process_update_users',
            items=lambda: _prepare_update_user_items(config),
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            trigger_dag_id=config.process_each_user,
            conf=lambda item: item,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        
        # Process disable users
        process_disable_users = rail.trigger_parallel_dagrun(
            task_id='process_disable_users',
            items=lambda: _prepare_disable_user_items(config),
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            trigger_dag_id=config.process_each_user,
            conf=lambda item: item,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Wait for all user processing to complete
        wait_for_processing = rail.EmptyOperator(task_id='wait_for_processing')

        get_process_users_dag_ids = rail.PythonOperator(
            task_id='get_process_users_dag_ids',
            python_callable=lambda: list(itertools.chain(
                *[rail.result(f'process_new_users_{x+1}')
                  for x in range(config.trigger_parallel_dagrun_count_process_users)
                  if rail.result(f'process_new_users_{x+1}')],
                *[rail.result(f'process_update_users_{x+1}')
                  for x in range(config.trigger_parallel_dagrun_count_process_users)
                  if rail.result(f'process_update_users_{x+1}')],
                *[rail.result(f'process_disable_users_{x+1}')
                  for x in range(config.trigger_parallel_dagrun_count_process_users)
                  if rail.result(f'process_disable_users_{x+1}')]
            )),
            show_return_value_in_logs=False
        )

        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs='{{ result("get_process_users_dag_ids") }}',
            dagrun_task_id='create_process_user_log',
            execution_timeout=timedelta(hours=config.gather_user_logs_timeout_hours),
            flatten=True
        )

        # Trigger supervisor check
        trigger_supervisor_check = rail.TriggerDagRunOperator(
            task_id='trigger_supervisor_check',
            trigger_dag_id=config.processs_supervisor,
            conf=lambda: {
                'user_log': rail.result('create_log'),
                'supervisor_permission_uri': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_permission_sets'),
                    'name', base_config.SUPERVISOR_PERMISSION, 'uri'),
            },
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        
        # Generate logs and send completion email
        trigger_log_generation = rail.TriggerDagRunOperator(
            task_id='trigger_log_generation',
            trigger_dag_id=config.process_log_generation,
            conf=lambda: {
                'userlogs': rail.result('gather_user_logs'),
                'otherlogs': rail.result('create_log'),
                'log_filename': f"guesttek_talent_user_sync_{pendulum.now().format('YYYYMMDD_HHmmss')}.csv",
                'total_processed': _get_total_processed(),
                'new_users': len(rail.result('categorize_users').get('new', [])),
                'updated_users': len(rail.result('categorize_users').get('update', [])),
                'disabled_users': len(rail.result('categorize_users').get('disable', [])),
                'skipped_users': len(rail.result('categorize_users').get('skip', [])),
            },
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        
        trigger_log_generation_empty = rail.TriggerDagRunOperator(
            task_id='trigger_log_generation_empty',
            trigger_dag_id=config.process_log_generation,
            conf=lambda: {
                'userlogs': [],
                'otherlogs': rail.result('create_log'),
                'log_filename': f"guesttek_talent_user_sync_{pendulum.now().format('YYYYMMDD_HHmmss')}.csv",
                'total_processed': 0,
                'new_users': 0,
                'updated_users': 0,
                'disabled_users': 0,
                'skipped_users': len(rail.result('categorize_users').get('skip', [])),
            },
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        
        # Update last processed time in Airflow Variable (runs on all completion paths)
        update_last_processed_time = rail.PythonOperator(
            task_id='update_last_processed_time',
            python_callable=lambda: _update_last_processed_time(config)
        )
        
        finish = rail.EmptyOperator(task_id='finish')
        
        # Define flow
        create_log >> get_talent_users_start
        get_talent_users_group >> has_delta_users
        has_delta_users >> [get_prereqs_start, send_no_delta_email]
        send_no_delta_email >> update_last_processed_time
        
        # Entity creation with conditional triggers
        get_prereqs_group >> [prepare_groups_data, prepare_roles_data, prepare_service_centers_data]
        prepare_groups_data >> has_usertypes >> [trigger_process_groups, skip_usertypes]
        prepare_roles_data >> has_roles >> [trigger_process_roles, skip_roles]
        prepare_service_centers_data >> has_service_centers >> [trigger_process_service_centers, skip_service_centers]
        [trigger_process_groups, skip_usertypes, trigger_process_roles, skip_roles, trigger_process_service_centers, skip_service_centers] >> entity_creation_done
        entity_creation_done >> get_updated_prereqs_start
        
        get_updated_prereqs_group >> categorize_users >> has_users_to_process
        
        has_users_to_process >> [dummy_process_users, trigger_log_generation_empty]
        dummy_process_users >> [process_new_users, process_update_users, process_disable_users]
        [process_new_users, process_update_users, process_disable_users] >> wait_for_processing
        wait_for_processing >> get_process_users_dag_ids >> gather_user_logs >> trigger_supervisor_check >> trigger_log_generation >> update_last_processed_time
        trigger_log_generation_empty >> update_last_processed_time
        update_last_processed_time >> finish
    
    return dag


def _get_unique_usertypes_from_delta():
    """Extract unique employee types from delta users for group processing.
    
    Note: We extract ALL employee types regardless of mapper validity.
    Users without valid mapper keys will be skipped during processing,
    but the employee types still need to exist in Replicon.
    """
    delta_users = rail.result('filter_delta_users')
    employee_types = set()
    
    for user in delta_users:
        # Extract employee type regardless of mapper validity
        emp_type = user.get('employee_work_schedule_value', '')
        if emp_type:
            employee_types.add(emp_type)
    
    return [{'usertype': ut} for ut in employee_types]


def _get_unique_roles_from_delta():
    """Extract unique roles (job titles) from delta users.
    
    Note: We extract ALL roles regardless of mapper validity.
    """
    delta_users = rail.result('filter_delta_users')
    roles = set()
    
    for user in delta_users:
        role = user.get('job_title', '')
        if role:
            roles.add(role)
    
    return [{'role_name': r} for r in roles]


def _get_unique_service_centers_from_delta():
    """Extract unique service centers (job types) from delta users.
    
    Note: We extract ALL service centers regardless of mapper validity.
    """
    delta_users = rail.result('filter_delta_users')
    service_centers = set()
    
    for user in delta_users:
        sc = user.get('job_type', '')
        if sc:
            service_centers.add(sc)
    
    return [{'service_center_name': sc} for sc in service_centers]


def _prepare_new_user_items(config):
    """Prepare configuration items for new user processing."""
    categorized = rail.result('categorize_users')
    new_users = categorized.get('new', [])
    
    prereqs = {
        'locations': rail.result('get_updated_locations'),
        'department_groups': rail.result('get_all_department_groups'),
        'employeetypes': rail.result('get_updated_employeetypes'),
        'timesheet_templates': rail.result('get_all_timesheet_templates'),
        'holiday_calendars': rail.result('get_all_holiday_calendars'),
        'schedules': rail.result('get_updated_schedules'),
        'payrules': rail.result('get_all_payrules'),
        'time_off_types': rail.result('get_all_time_off_types'),
        'timezones': rail.result('get_all_timezones'),
    }

    supervisor_permission_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_permission_sets'),
        'name', base_config.SUPERVISOR_PERMISSION, 'uri')

    items = []
    for user in new_users:
        item = custom_method.prepare_new_user_conf(user, config, prereqs)
        item['supervisor_permission_uri'] = supervisor_permission_uri
        items.append(item)

    return items


def _prepare_update_user_items(config):
    categorized = rail.result('categorize_users')
    update_data = categorized.get('update', [])

    prereqs = {
        'locations': rail.result('get_updated_locations'),
        'department_groups': rail.result('get_all_department_groups'),
    }

    supervisor_permission_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_permission_sets'),
        'name', base_config.SUPERVISOR_PERMISSION, 'uri')

    items = []
    for data in update_data:
        item = custom_method.prepare_update_user_conf(
            data['talent_user'],
            data['replicon_user'],
            config,
            prereqs
        )
        item['supervisor_permission_uri'] = supervisor_permission_uri
        items.append(item)

    return items


def _prepare_disable_user_items(config):
    """Prepare configuration items for disable user processing."""
    categorized = rail.result('categorize_users')
    disable_data = categorized.get('disable', [])

    items = []
    for data in disable_data:
        talent_user = data['talent_user']
        items.append({
            'employee_id': talent_user.get('user_employee_id', ''),
            'login_name': talent_user.get('user_email', ''),
            'first_name': talent_user.get('user_firstname', ''),
            'last_name': talent_user.get('user_lastname', ''),
            'user_deactivated': talent_user.get('user_deactivated', 0),
        })

    return items


def _get_total_processed():
    """Get total number of users processed."""
    categorized = rail.result('categorize_users')
    return (len(categorized.get('new', [])) + 
            len(categorized.get('update', [])) + 
            len(categorized.get('disable', [])))


def _update_last_processed_time(config):
    """
    Update the Airflow Variable with the current timestamp.
    
    Called at the end of every successful DAG run (including no-delta runs)
    so the next run picks up from where this one left off.
    """
    Variable.set(
        config.last_processed_time_var,
        pendulum.now('UTC').format('YYYY-MM-DD HH:mm:ss')
    )


rail.for_each_instance(create_main_dag)