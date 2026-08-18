from datetime import timedelta, datetime
import rail
import json
from airflow.models import Variable
from tsystems.project_team_assignment_v2.utils import request_payload
from tsystems.project_team_assignment_v2.utils import python_callable


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_event_data_dag_id,
        description=f'T-Systems Project Team Assignment - process each unique event {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        each_event_log = rail.CreateLogOperator(
            task_id='each_event_log',
        )

        if_enddate_is_before_startdate = rail.IfOperator(
            task_id='if_enddate_is_before_startdate',
            test=lambda: bool(datetime.fromisoformat(
                rail.get_dag_run_conf().get("search_period_end").replace('Z', '+00:00')) < datetime.fromisoformat(
                    rail.get_dag_run_conf().get("search_period_start").replace('Z', '+00:00'))),
            yes_task='log_enddate_is_before_startdate',
            no_task='get_assignment_id_data_from_blob'
        )

        log_enddate_is_before_startdate = rail.WriteLogOperator(
            task_id='log_enddate_is_before_startdate',
            log='{{ result("each_event_log") }}',
            message='Start date is after End date in search period',
            severity='Exception',
            properties={
                'assignment_id':'{{ dag_run.conf.assignment_id }}',
                'decidalo_project_id': '{{ dag_run.conf.decidalo_project_id }}',
                'individual_id': '{{ dag_run.conf.individual_id }}',
                'cost_object_id': '{{ dag_run.conf.cost_object_id }}',
                'search_period_start': '{{ dag_run.conf.search_period_start }}',
                'search_period_end': '{{ dag_run.conf.search_period_end }}',
                'hours': '',
                'status': 'Exception',
                'details': 'Start date is after End date in search period',
            }
        )

        get_assignment_id_data_from_blob = rail.RepliconServiceOperator(
            task_id='get_assignment_id_data_from_blob',
            endpoint='/services/GenericKeyValueStoreService1.svc/GetKeyValue',
            data={
                "keyNamespace": 'project_team_assignment_id',
                "key": "{{ dag_run.conf.assignment_id }}"
            }
        )

        if_assignment_id_key_present = rail.IfOperator(   
            task_id='if_assignment_id_key_present',
            test="{{ result('get_assignment_id_data_from_blob') | is_truthy }}",
            yes_task='get_updated_daterange_based_on_blob',
            no_task='get_per_day_capacity_from_api'
        )

        get_updated_daterange_based_on_blob = rail.PythonOperator(
            task_id="get_updated_daterange_based_on_blob",
            python_callable=python_callable.get_updated_daterange_from_blob
        )

        get_per_day_capacity_from_api = rail.SimpleHttpOperator(
            task_id='get_per_day_capacity_from_api',
            method='POST',
            endpoint=config.search_capacity_endpoint,
            http_conn_id=config.http_conn_id,
            headers={
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}",
                "Content-Type": "application/json"
            },
            data=json.dumps({
                "projectId": "{{ dag_run.conf.decidalo_project_id }}",
                "costObjectId": "{{ dag_run.conf.cost_object_id }}",
                "assigneeId": "{{ dag_run.conf.individual_id }}",
                "searchPeriod": {
                    "startDateTime":  "{% if result('get_assignment_id_data_from_blob') | is_truthy %}{{ result('get_updated_daterange_based_on_blob').search_period_start }}{% else %}{{ dag_run.conf.search_period_start }}{% endif %}",
                    "endDateTime": "{% if result('get_assignment_id_data_from_blob') | is_truthy %}{{ result('get_updated_daterange_based_on_blob').search_period_end }}{% else %}{{ dag_run.conf.search_period_end }}{% endif %}"
                }
            }),
            extra_options={
                'verify': False
            }
        )

        check_allocation_data_available = rail.PythonOperator(
            task_id = 'check_allocation_data_available',
            python_callable=python_callable.check_allocation_details_available,
            trigger_rule='all_done',
        )

        if_allocation_data_present_in_decidalo = rail.IfOperator(
            task_id = 'if_allocation_data_present_in_decidalo',
            test= "{{ result('check_allocation_data_available') | is_truthy }}",
            yes_task='get_user_from_individual_id',
            no_task='log_no_allocation_data_found'
        )

        log_no_allocation_data_found = rail.WriteLogOperator(
            task_id='log_no_allocation_data_found',
            log='{{ result("each_event_log") }}',
            message='No allocation details found for Individual ID: {{ dag_run.conf.individual_id }}',
            severity='Exception',
            properties={
                'assignment_id':'{{ dag_run.conf.assignment_id }}',
                'decidalo_project_id': '{{ dag_run.conf.decidalo_project_id }}',
                'individual_id': '{{ dag_run.conf.individual_id }}',
                'cost_object_id': '{{ dag_run.conf.cost_object_id }}',
                'search_period_start': '{{ dag_run.conf.search_period_start }}',
                'search_period_end': '{{ dag_run.conf.search_period_end }}',
                'hours': '',
                'status': 'Exception',
                'details': 'No allocation details found for Individual ID: {{ dag_run.conf["individual_id"] }}',
            }
        )

        get_user_from_individual_id = rail.RepliconServiceOperator(
            task_id='get_user_from_individual_id',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "employeeId": "{{ dag_run.conf.individual_id }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=python_callable.get_user_data_from_response
        )

        if_user_present = rail.IfOperator(   
            task_id='if_user_present',
            test="{{ result('get_user_from_individual_id') | is_truthy }}",
            yes_task='if_multiple_user_present',
            no_task='log_no_user_found'
        )

        log_no_user_found = rail.WriteLogOperator(
            task_id='log_no_user_found',
            log='{{ result("each_event_log") }}',
            message='No user found for Individual ID: {{ dag_run.conf.individual_id }}',
            severity='Exception',
            properties={
                'assignment_id':'{{ dag_run.conf.assignment_id }}',
                'decidalo_project_id': '{{ dag_run.conf.decidalo_project_id }}',
                'individual_id': '{{ dag_run.conf.individual_id }}',
                'cost_object_id': '{{ dag_run.conf.cost_object_id }}',
                'search_period_start': '{{ dag_run.conf.search_period_start }}',
                'search_period_end': '{{ dag_run.conf.search_period_end }}',
                'hours': '',
                'status': 'Exception',
                'details': 'No user found for Individual ID: {{ dag_run.conf["individual_id"] }}',
            }
        )

        if_multiple_user_present = rail.IfOperator(   
            task_id='if_multiple_user_present',
            test=lambda: bool(len(rail.result('get_user_from_individual_id')) > 1),
            yes_task='log_multiple_user_found',
            no_task='if_user_permission_present'
        )

        log_multiple_user_found = rail.WriteLogOperator(
            task_id='log_multiple_user_found',
            log='{{ result("each_event_log") }}',
            message='Multiple users found for Individual ID: {{ dag_run.conf.individual_id }}',
            severity='Exception',
            properties={
                'assignment_id':'{{ dag_run.conf.assignment_id }}',
                'decidalo_project_id': '{{ dag_run.conf.decidalo_project_id }}',
                'individual_id': '{{ dag_run.conf.individual_id }}',
                'cost_object_id': '{{ dag_run.conf.cost_object_id }}',
                'search_period_start': '{{ dag_run.conf.search_period_start }}',
                'search_period_end': '{{ dag_run.conf.search_period_end }}',
                'hours': '',
                'status': 'Exception',
                'details': 'Multiple users found for Individual ID: {{ dag_run.conf["individual_id"] }}',
            }
        )

        if_user_permission_present = rail.IfOperator(
            task_id='if_user_permission_present',
            test=lambda: bool(set(rail.result('get_user_from_individual_id')[0]['permission_set_uris']) & set(rail.get_dag_run_conf().get('user_permission_list', []))),
            yes_task='get_project_from_costobject_id',
            no_task='log_no_user_permission_found'
        )

        log_no_user_permission_found = rail.WriteLogOperator(
            task_id='log_no_user_permission_found',
            log='{{ result("each_event_log") }}',
            message='No permission set found for Individual ID: {{ dag_run.conf.individual_id }}',
            severity='Exception',
            properties={
                'assignment_id':'{{ dag_run.conf.assignment_id }}',
                'decidalo_project_id': '{{ dag_run.conf.decidalo_project_id }}',
                'individual_id': '{{ dag_run.conf.individual_id }}',
                'cost_object_id': '{{ dag_run.conf.cost_object_id }}',
                'search_period_start': '{{ dag_run.conf.search_period_start }}',
                'search_period_end': '{{ dag_run.conf.search_period_end }}',
                'hours': '',
                'status': 'Exception',
                'details': 'No permission set found for Individual ID: {{ dag_run.conf["individual_id"] }}'
            }
        )

        get_project_from_costobject_id = rail.RepliconServiceOperator(
            task_id='get_project_from_costobject_id',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "code": "{{ dag_run.conf.cost_object_id }}"
                    }
                ]
            },
            data_handler=python_callable.get_project_data_from_response
        )

        if_project_present = rail.IfOperator(   
            task_id='if_project_present',
            test="{{ result('get_project_from_costobject_id') | is_truthy }}",
            yes_task='get_assign_team_from_for_the_project',
            no_task='log_no_project_found'
        )

        log_no_project_found = rail.WriteLogOperator(
            task_id='log_no_project_found',
            log='{{ result("each_event_log") }}',
            message='No project found for Cost Object ID: {{ dag_run.conf.cost_object_id }}',
            severity='Exception',
            properties={
                'assignment_id':'{{ dag_run.conf.assignment_id }}',
                'decidalo_project_id': '{{ dag_run.conf.decidalo_project_id }}',
                'individual_id': '{{ dag_run.conf.individual_id }}',
                'cost_object_id': '{{ dag_run.conf.cost_object_id }}',
                'search_period_start': '{{ dag_run.conf.search_period_start }}',
                'search_period_end': '{{ dag_run.conf.search_period_end }}',
                'hours': '',
                'status': 'Exception',
                'details': 'No project found for Cost Object ID: {{ dag_run.conf["cost_object_id"] }}',
            }
        )

        get_assign_team_from_for_the_project = rail.RepliconServiceOperator(
            task_id='get_assign_team_from_for_the_project',
            endpoint='/services/ProjectService1.svc/GetEligibleProjectTeamMemberDataAccessScopeDetailsForProject',
            data={
                "projectUri": "{{ result('get_project_from_costobject_id')['project_uri'] }}"
            },
            data_handler=python_callable.get_assign_team_from_for_the_project_data
        )

        remove_employee_type_restriction = rail.RepliconServiceOperator(
            task_id='remove_employee_type_restriction',
            endpoint='/services/ProjectService1.svc/PutEligibleProjectTeamMemberDataAccessScopesForProject',
            data=request_payload.remove_employee_type_restriction_payload
        )

        extract_capacity_date_range = rail.PythonOperator(
            task_id='extract_capacity_date_range',
            python_callable=python_callable.extract_capacity_date_range
        )

        get_user_assigned_to_project = rail.RepliconServiceOperator(
            task_id='get_user_assigned_to_project',
            endpoint='/services/ProjectService1.svc/GetProjectTeamMemberDetails',
            data={
                "projectUri": "{{ result('get_project_from_costobject_id')['project_uri'] }}",
                "resourceUri": "{{ result('get_user_from_individual_id')[0]['user_uri'] }}"
            },
            data_handler=python_callable.get_user_assignment_data
        )

        if_user_assigned_to_project = rail.IfOperator(
            task_id='if_user_assigned_to_project',
            test="{{ result('get_user_assigned_to_project') | is_truthy }}",
            yes_task='compare_assignment_date_range',
            no_task="assign_user_to_project"
        )

        compare_assignment_date_range = rail.PythonOperator(
            task_id='compare_assignment_date_range',
            python_callable=python_callable.compare_assignment_date_range
        )

        update_user_assignment_date_range = rail.RepliconServiceOperator(
            task_id='update_user_assignment_date_range',
            endpoint='/services/ProjectService1.svc/UpdateProjectTeamMemberAssignmentDateRange',
            data=request_payload.update_user_assignment_date_range_payload
        )

        assign_user_to_project = rail.RepliconServiceOperator(
            task_id='assign_user_to_project',
            endpoint='/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
            data=request_payload.assign_user_to_project_payload
        )

        get_capacity_items = rail.PythonOperator(
            task_id='get_capacity_items',
            python_callable=python_callable.get_capacity_items_for_trigger
        )

        def flatten_capacity_items_callable():
            capacity_items = rail.result('get_capacity_items')
            # Parse JSON string if needed
            if isinstance(capacity_items, str):
                capacity_items = json.loads(capacity_items)

            return [
                {
                    "record_id": idx,
                    "allocation_date": item.get("applicableTimePeriod", {}).get("validFor", {}).get("startDateTime"),
                    "capacity_amount": item.get("humanCapacityAmount", {}).get("capacityAmount")
                }
                for idx, item in enumerate(capacity_items)
            ]

        flatten_capacity_items = rail.PythonOperator(
            task_id='flatten_capacity_items',
            python_callable=flatten_capacity_items_callable
        )

        def get_process_allocation_trigger_id(item):
            # Use record_id to distribute across batches
            modulo = int(item['record_id']) % config.ALLOCATION_BATCH_COUNT
            if modulo == 0:
                return config.individual_allocation_per_day_dag_id
            return f"{config.individual_allocation_per_day_dag_id}_batch_{modulo}"

        trigger_indidual_allocation_per_day = rail.trigger_parallel_dagrun(
            task_id='trigger_indidual_allocation_per_day',
            items=lambda: rail.result('flatten_capacity_items'),
            parallel_count= config.parallel_count,
            trigger_dag_id=get_process_allocation_trigger_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "allocation_date": item["allocation_date"],
                "capacity_amount": item["capacity_amount"],
                "assignment_id": dag_run.conf['assignment_id'],
                "decidalo_project_id": dag_run.conf['decidalo_project_id'],
                "cost_object_id": dag_run.conf['cost_object_id'],
                "individual_id": dag_run.conf['individual_id'],
                "project_uri": rail.result('get_project_from_costobject_id')['project_uri'],
                "user_uri": rail.result('get_user_from_individual_id')[0]['user_uri'],
                "logger": rail.result("each_event_log")
            }
        )

        add_employee_type_restriction = rail.RepliconServiceOperator(
            task_id='add_employee_type_restriction',
            endpoint='/services/ProjectService1.svc/PutEligibleProjectTeamMemberDataAccessScopesForProject',
            data=request_payload.add_employee_type_restriction_payload
        )

        get_all_tasks_for_project = rail.RepliconServiceOperator(
            task_id='get_all_tasks_for_project',
            endpoint='/services/ProjectService1.svc/BulkGetTaskDetails',
            data=request_payload.get_all_task,
            data_handler=python_callable.get_all_tasks_uris_for_project
        )

        if_tasks_present = rail.IfOperator(
            task_id='if_tasks_present',
            test="{{ result('get_all_tasks_for_project') | is_truthy }}",
            yes_task='assign_user_to_all_task',
            no_task="add_assignment_id_data_to_blob"
        )

        assign_user_to_all_task = rail.RepliconServiceOperator(
            task_id='assign_user_to_all_task',
            endpoint='/services/ProjectService1.svc/PutTaskAssignmentsForResource',
            data=request_payload.add_users_to_all_task
        )

        add_assignment_id_data_to_blob = rail.RepliconServiceOperator(
            task_id='add_assignment_id_data_to_blob',
            endpoint='/services/GenericKeyValueStoreService1.svc/PutKeyValue',
            data=lambda dag_run: request_payload.add_project_data_to_blob_param(dag_run, config.assignment_id_blob_key_name)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("each_event_log") }}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties={
                'assignment_id':'{{ dag_run.conf.assignment_id }}',
                'decidalo_project_id': '{{ dag_run.conf.decidalo_project_id }}',
                'individual_id': '{{ dag_run.conf.individual_id }}',
                'cost_object_id': '{{ dag_run.conf.cost_object_id }}',
                'search_period_start': '{{ dag_run.conf.search_period_start }}',
                'search_period_end': '{{ dag_run.conf.search_period_end }}',
                'hours': '',
                'status': "Error",
                'details': '{{ get_error_message() }}'
            }
        )

        each_event_log >> if_enddate_is_before_startdate
        
        if_enddate_is_before_startdate >> rail.Label("Yes") >> log_enddate_is_before_startdate >> catch_and_log_errors
        if_enddate_is_before_startdate >> rail.Label("No") >>  get_assignment_id_data_from_blob >> if_assignment_id_key_present

        if_assignment_id_key_present >> rail.Label("Yes") >> get_updated_daterange_based_on_blob >> get_per_day_capacity_from_api
        if_assignment_id_key_present >> rail.Label("No") >> get_per_day_capacity_from_api

        get_per_day_capacity_from_api >> check_allocation_data_available >> if_allocation_data_present_in_decidalo
        
        if_allocation_data_present_in_decidalo >> rail.Label("No") >> log_no_allocation_data_found >> catch_and_log_errors
        if_allocation_data_present_in_decidalo >> rail.Label("Yes") >> get_user_from_individual_id >> if_user_present

        if_user_present >> rail.Label("Yes") >> if_multiple_user_present
        if_user_present >> rail.Label("No") >> log_no_user_found >> catch_and_log_errors

        if_multiple_user_present >> rail.Label("Yes") >> log_multiple_user_found >> catch_and_log_errors
        if_multiple_user_present >> rail.Label("No") >> if_user_permission_present

        if_user_permission_present >> rail.Label("Yes") >> get_project_from_costobject_id >> if_project_present
        if_user_permission_present >> rail.Label("No") >> log_no_user_permission_found >> catch_and_log_errors

        if_project_present >> rail.Label("Yes") >> get_assign_team_from_for_the_project >> remove_employee_type_restriction >> extract_capacity_date_range
        if_project_present >> rail.Label("No") >> log_no_project_found >> catch_and_log_errors

        extract_capacity_date_range >> get_user_assigned_to_project >> if_user_assigned_to_project

        if_user_assigned_to_project >> rail.Label("Yes") >> compare_assignment_date_range >> update_user_assignment_date_range             
        if_user_assigned_to_project >> rail.Label("No") >> assign_user_to_project >> update_user_assignment_date_range

        update_user_assignment_date_range >> get_capacity_items

        get_capacity_items >> flatten_capacity_items >> trigger_indidual_allocation_per_day >> \
            add_employee_type_restriction >> get_all_tasks_for_project >> if_tasks_present
            
        if_tasks_present >> rail.Label("Yes") >> assign_user_to_all_task >> add_assignment_id_data_to_blob >> catch_and_log_errors
        if_tasks_present >> rail.Label("No") >> add_assignment_id_data_to_blob >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
