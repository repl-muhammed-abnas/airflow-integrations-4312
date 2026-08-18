from datetime import datetime, timedelta
from airflow.models import Variable
import rail
from pwcglobal.project_import_file_based_v1.task.get_team_members_project_manager import get_team_members_project_managers
from pwcglobal.project_import_file_based_v1.task.add_user_permission_policy import get_add_permission_policy
from pwcglobal.project_import_file_based_v1.task.child_dag_exception_logs import log_exception_field_task
from pwcglobal.project_import_file_based_v1 import request_payload
from pwcglobal.project_import_file_based_v1 import custom_method
from pwcglobal.project_import_file_based_v1 import python_callable_method
from pwcglobal.project_import_file_based_v1 import response_filter


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/project_import_file_based_v1/config.py


# pylint:disable = too-many-statements
def create_child_update_project_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pwc_project_import_child_update_project_flat_file_based_{config.instance}_v1',
        description=f'Update Project {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_update_project_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='put_key_value_to_project'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='put_key_value_to_project',
            end_task='catch_and_log_errors',
        )

        put_key_value_to_project = rail.RepliconServiceOperator(
            task_id="put_key_value_to_project",
            endpoint="/services/ProjectService1.svc/PutKeyValueForProject",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['project_uri'],
                "keyValue": {
                    "keyUri": "urn:replicon:project-key-value-key:source-input-reference-id",
                    "value": {
                        "text": dag_run.conf['md5']
                    }
                }
            },
        )

        bulk_get_project_team_members = rail.RepliconServiceOperator(
            task_id="bulk_get_project_team_members",
            endpoint="/services/ProjectService1.svc/BulkGetAllProjectTeamMembers2",
            data={
                "projectUris": ["{{ dag_run.conf.project_uri }}"]
            }
        )

        should_update_description = rail.IfOperator(
            task_id="should_update_description",
            test=lambda dag_run: bool(dag_run.conf.get('engagementline')) and dag_run.conf['engagementline'][0].get('EngagementLineDescription') and (
                dag_run.conf['engagementline'][0]['EngagementLineDescription'] != dag_run.conf['load_project']['description']),
            yes_task="update_project_description",
            no_task="should_update_project_name"
        )

        update_project_description = rail.RepliconServiceOperator(
            task_id="update_project_description",
            endpoint="/services/ProjectService1.svc/UpdateDescription",
            data={
                "projectUri": "{{ dag_run.conf.project_uri }}",
                "description": "{{ dag_run.conf.engagementline | first_or_default | \
                    attr_or_default('EngagementLineDescription') }}"
            }
        )

        should_update_project_name = rail.IfOperator(
            task_id="should_update_project_name",
            test=lambda dag_run: dag_run.conf['chargecodename'] !=
            dag_run.conf['load_project']['name'],
            yes_task="update_project_name",
            no_task="should_process_team_members_managers"
        )

        update_project_name = rail.RepliconServiceOperator(
            task_id="update_project_name",
            endpoint="/services/ProjectService1.svc/UpdateName",
            data={
                "projectUri": "{{ dag_run.conf.project_uri }}",
                "name": "{{ dag_run.conf.chargecodename }}"
            }
        )

        should_process_team_members_managers = rail.IfOperator(
            task_id="should_process_team_members_managers",
            test="{{ dag_run.conf.internalpersonrole | length > 0 }}",
            yes_task='process_project_team_managers',
            no_task='should_process_project_status_update'
        )

        process_project_team_managers = rail.EmptyOperator(
            task_id='process_project_team_managers',
        )

        get_project_team_managers = get_team_members_project_managers('update')

        get_permissionuri_useruri_project_manager = rail.PythonOperator(
            task_id='get_permissionuri_useruri_project_manager',
            python_callable=python_callable_method.get_permission_user_uri,
            op_args=['project_manager', 'get_project_manager_to_assign']
        )

        is_project_manager_to_be_assigned = rail.IfOperator(
            task_id="is_project_manager_to_be_assigned",
            test=lambda dag_run: rail.result('get_permissionuri_useruri_project_manager') and
            (rail.result('get_permissionuri_useruri_project_manager')['user_uri'] != (dag_run.conf[
                'load_project']['projectLeader'].get('user', {}).get('uri') if dag_run.conf[
                'load_project']['projectLeader'] else None)),
            yes_task="process_add_permission_policy_project_manager",
            no_task="get_permissionuri_useruri_project_comanager"
        )

        process_add_permission_policy_project_manager = rail.EmptyOperator(
            task_id='process_add_permission_policy_project_manager',
        )

        add_permission_policy_project_manager = get_add_permission_policy(
            'project_manager', 'update')

        get_permissionuri_useruri_project_comanager = rail.PythonOperator(
            task_id='get_permissionuri_useruri_project_comanager',
            python_callable=python_callable_method.get_permission_user_uri,
            op_args=['project_comanager', 'get_project_co_manager_to_assign']
        )

        is_project_comanager_to_be_assigned = rail.IfOperator(
            task_id="is_project_comanager_to_be_assigned",
            test=lambda dag_run: (bool(rail.result('get_permissionuri_useruri_project_comanager')) and
                                  len(rail.result('get_project_co_manager_to_assign')) > 0) and
            rail.result('get_project_co_manager_to_assign')[0]['user_name'] != rail.find_first_by_attr_and_get_attr(
                dag_run.conf['load_project']['customFields'], 'customField.displayText', 'Co Project Manager', 'text'),
            yes_task="get_explicit_sharing_assignments_project",
            no_task="should_process_project_status_update"
        )

        get_explicit_sharing_assignments_project = rail.RepliconServiceOperator(
            task_id="get_explicit_sharing_assignments_project",
            endpoint="/services/ProjectService1.svc/GetExplicitSharingAssignments",
            data={
                "projectUri": "{{ dag_run.conf.project_uri }}"
            }
        )

        compare_with_project_comanager = rail.IfOperator(
            task_id="compare_with_project_comanager",
            test=lambda: not bool(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_explicit_sharing_assignments_project'), 'user.uri', rail.result(
                    'get_permissionuri_useruri_project_comanager')['user_uri']) if rail.result(
                        'get_explicit_sharing_assignments_project') else None),
            yes_task="process_add_permission_policy_project_comanager",
            no_task="should_process_project_status_update"
        )

        process_add_permission_policy_project_comanager = rail.EmptyOperator(
            task_id='process_add_permission_policy_project_comanager',
        )

        add_permission_policy_project_comanager = get_add_permission_policy(
            'project_comanager', 'update')

        should_process_project_status_update = rail.IfOperator(
            task_id="should_process_project_status_update",
            test=lambda dag_run: ("In Progress" if dag_run.conf['openfortime'] == 'true' else
                                  "Completed") != dag_run.conf['load_project'].get('status', {}).get('name'),
            yes_task="all_project_status_label",
            no_task="get_project_date_range_logs"
        )

        all_project_status_label = rail.RepliconServiceOperator(
            task_id="all_project_status_label",
            endpoint="/services/ProjectStatusService1.svc/GetAllProjectStatusLabels",
        )

        update_project_status = rail.RepliconServiceOperator(
            task_id="update_project_status",
            endpoint="/services/ProjectService1.svc/UpdateStatus",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['project_uri'],
                "projectStatusUri": rail.find_first_by_attr_and_get_attr(rail.result('all_project_status_label'),
                                                                         "name", "In Progress", "uri")
                if dag_run.conf['openfortime'] == "true" else
                rail.find_first_by_attr_and_get_attr(rail.result('all_project_status_label'),
                                                     "name", "Completed", "uri")
            }
        )

        get_project_date_range_logs = rail.PythonOperator(
            task_id="get_project_date_range_logs",
            python_callable=request_payload.get_date_range_and_logs
        )

        should_update_project_date_range = rail.IfOperator(
            task_id="should_update_project_date_range",
            test=lambda: bool(rail.result(
                'get_project_date_range_logs')['date_range']),
            yes_task="update_project_date_range",
            no_task="should_process_confidential_flag"
        )

        update_project_date_range = rail.RepliconServiceOperator(
            task_id="update_project_date_range",
            endpoint="/services/ProjectService1.svc/UpdateTimeEntryDateRange",
            data=lambda dag_run: {
                'projectUri': dag_run.conf['project_uri'],
                'dateRange': rail.result('get_project_date_range_logs')['date_range']
            }
        )

        should_process_confidential_flag = rail.IfOperator(
            task_id="should_process_confidential_flag",
            test=lambda dag_run: bool(dag_run.conf.get('confidential_flag')) and
            (rail.find_first_by_attr_and_get_attr(dag_run.conf['load_project']['customFields'],
                                                  'customField.displayText', 'Confidential Project', 'text') !=
             ("Yes" if dag_run.conf['confidential_flag'] == "true" else "No")),
            yes_task="update_project_custom_field_confidential_flag",
            no_task="should_process_mandatory_flag"
        )

        update_project_custom_field_confidential_flag = rail.RepliconServiceOperator(
            task_id="update_project_custom_field_confidential_flag",
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.project_uri }}",
                "customFieldUri": "{{ dag_run.conf.confidential_flag_uri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.confidentiality_flag_dropdown_uri }}"
            }
        )

        should_process_mandatory_flag = rail.IfOperator(
            task_id="should_process_mandatory_flag",
            test=lambda dag_run: bool(dag_run.conf.get('mandatorytextflag')) and
            (rail.find_first_by_attr_and_get_attr(dag_run.conf['load_project']['customFields'],
                                                  'customField.displayText', 'Mandatory Text', 'text') !=
             ("Yes" if dag_run.conf['mandatorytextflag'] == "true" else "No")),
            yes_task="update_project_custom_field_mandatory_flag",
            no_task="should_process_client_update"
        )

        update_project_custom_field_mandatory_flag = rail.RepliconServiceOperator(
            task_id="update_project_custom_field_mandatory_flag",
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.project_uri }}",
                "customFieldUri": "{{ dag_run.conf.mandatory_flag_uri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.mandatory_text_dropdown_uri }}"
            }
        )

        should_update_text_effective_udf_value = rail.IfOperator(
            task_id="should_update_text_effective_udf_value",
            test=lambda dag_run: bool(
                dag_run.conf.get('text_effective_date_uri')) and dag_run.conf['mandatorytextflag'] == 'true' and rail.find_first_by_attr_and_get_attr(
                    dag_run.conf[
                        'load_project']['customFields'], 'customField.displayText', 'Text Effective Date', 'text') != datetime.utcnow().strftime('%Y-%m-%d'),
            yes_task="update_text_effective_udf_value",
            no_task="should_process_client_update"
        )

        update_text_effective_udf_value = rail.RepliconServiceOperator(
            task_id="update_text_effective_udf_value",
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=request_payload.get_update_text_effective_udf_value_payload
        )

        def should_update_client_update_test(dag_run):
            client_uri = None
            if dag_run.conf['client_name'] and dag_run.conf['client_code']:
                if dag_run.conf['client_uri']:
                    client_uri = dag_run.conf['client_uri']
            if client_uri:
                return (not rail.find_first_by_attr_and_get_attr(dag_run.conf['load_project']['clients'],
                                                      'client.uri', client_uri, 'client.uri'))
            return False


        should_process_client_update = rail.IfOperator(
            task_id="should_process_client_update",
            test=should_update_client_update_test,
            yes_task="update_project_client",
            no_task="should_process_work_items"
        )

        update_project_client = rail.RepliconServiceOperator(
            task_id="update_project_client",
            endpoint="/services/ProjectService1.svc/ApplyNewClient",
            data={
                "projectUri": "{{ dag_run.conf.project_uri }}",
                "clientUri": "{{ dag_run.conf.client_uri}}",
                "optionUri": "urn:replicon:project-apply-new-client-option:keep-existing-billing-rates-and-expense-codes"
            }
        )

        should_process_work_items = rail.IfOperator(
            task_id="should_process_work_items",
            test=lambda dag_run: len(
                dag_run.conf.get('workitem', [])) > 0,
            yes_task="get_all_project_tasks",
            no_task="should_update_project_team_members"
        )

        get_all_project_tasks = rail.RepliconServiceOperator(
            task_id="get_all_project_tasks",
            endpoint="/services/ProjectService1.svc/BulkGetTaskDetails2",
            data={
                "pageIndex": 1,
                "pageSize": 1000,
                "projectUris": ["{{ dag_run.conf.project_uri }}"]
            },
            data_handler=response_filter.map_existing_project_tasks
        )

        existing_task_list = rail.CreateCollectionOperator(
            task_id="existing_task_list",
            source="{{ result('get_all_project_tasks') | to_json }}",
            name="existingtasklist"
        )

        new_task_list = rail.CreateCollectionOperator(
            task_id="new_task_list",
            source=lambda dag_run: dag_run.conf['workitem'],
            name="newtasklist",
            columns={
                'WorkItemType': 'taskname',
                'WorkItemTypeId': 'taskcode'
            }
        )

        get_tasks_to_update = rail.QueryCollectionOperator(
            task_id="get_tasks_to_update",
            query="""SELECT * FROM existingtasklist WHERE name IN (SELECT DISTINCT (taskname || ' - ' || taskcode) FROM newtasklist)""",
        )

        get_tasks_to_close = rail.QueryCollectionOperator(
            task_id="get_tasks_to_close",
            query="""SELECT * FROM existingtasklist WHERE name NOT IN (SELECT DISTINCT (taskname || ' - ' || taskcode) FROM newtasklist) AND name <> 'nil'""",
        )

        get_tasks_to_add = rail.QueryCollectionOperator(
            task_id="get_tasks_to_add",
            query="""SELECT * FROM newtasklist WHERE (taskname || ' - ' || taskcode) NOT IN (SELECT DISTINCT name FROM existingtasklist)""",
        )

        has_tasks_to_update = rail.IfOperator(
            task_id='has_tasks_to_update',
            test='{{ result("get_tasks_to_update", "length") > 0 }}',
            yes_task="update_tasks_in_replicon",
            no_task="has_tasks_to_close",
        )

        update_tasks_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_tasks_in_replicon',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            items=lambda: rail.load_all_records(
                rail.result('get_tasks_to_update')),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=lambda dag_run, item: request_payload.get_task_payload(
                item, dag_run, update_action_type='update'),
            data_handler=lambda response: response[0]
        )

        log_updated_tasks_in_replicon = rail.WriteLogOperator(
            task_id="log_updated_tasks_in_replicon",
            log='{{ dag_run.conf.log }}',
            message="Task Updated successfully",
            items=lambda: rail.result('update_tasks_in_replicon'),
            properties=lambda item, dag_run: python_callable_method.log_tasks(
                item, dag_run, message='Task Updated successfully')
        )

        has_tasks_to_close = rail.IfOperator(
            task_id='has_tasks_to_close',
            test='{{ result("get_tasks_to_close", "length") > 0 }}',
            yes_task="close_tasks_in_replicon",
            no_task="has_tasks_to_add",
        )

        close_tasks_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='close_tasks_in_replicon',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            items=lambda: rail.load_all_records(
                rail.result('get_tasks_to_close')),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=lambda dag_run, item: request_payload.get_task_payload(
                item, dag_run, update_action_type='close'),
            data_handler=lambda response: response[0]
        )

        has_tasks_to_add = rail.IfOperator(
            task_id='has_tasks_to_add',
            test='{{ result("get_tasks_to_add", "length") > 0 }}',
            yes_task="add_tasks_in_replicon",
            no_task="should_update_project_team_members",
        )

        add_tasks_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_tasks_in_replicon',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            items=lambda: rail.load_all_records(
                rail.result('get_tasks_to_add')),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=lambda dag_run, item: request_payload.get_task_payload(
                item, dag_run),
            data_handler=lambda response: response[0]
        )

        log_added_tasks_in_replicon = rail.WriteLogOperator(
            task_id="log_added_tasks_in_replicon",
            log='{{ dag_run.conf.log }}',
            message="Task added Successfully",
            items=lambda: rail.result('add_tasks_in_replicon'),
            properties=lambda item, dag_run:
            python_callable_method.log_tasks(
                item, dag_run, message='Task created with the team member assignment' if
                len(custom_method.get_new_project_team_members()) > 0 else 'Task created without team assignment')
        )

        should_update_project_team_members = rail.IfOperator(
            task_id="should_update_project_team_members",
            test=lambda: len(custom_method.get_new_project_team_members()) > 0,
            yes_task="bulk_update_project_team_members",
            no_task="get_success_logs"
        )

        bulk_update_project_team_members = rail.RepliconServiceOperator(
            task_id="bulk_update_project_team_members",
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment2",
            data=request_payload.get_bulk_update_team_members_payload
        )

        update_project_team_member_billing_rate = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_project_team_member_billing_rate',
            endpoint='/services/TimeAndMaterialsProjectService1.svc/UpdateProjectTeamMemberBillingRateAllowedForBillingTime',
            items=custom_method.get_new_project_team_members,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=request_payload.get_update_project_team_member_billing_rate_payload
        )

        get_success_logs = rail.PythonOperator(
            task_id="get_success_logs",
            python_callable=python_callable_method.do_get_success_logs_update
        )

        get_exception_logs = rail.PythonOperator(
            task_id='get_exception_logs',
            python_callable=python_callable_method.do_get_exception_logs_update
        )

        log_exception_for_update_project = log_exception_field_task(
            "Update")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log }}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                'SenderID': "{{ dag_run.conf.sender }} | Project",
                'Project Name|Project Code': "{{ dag_run.conf.chargecodename }} | {{ dag_run.conf.chargecode }}",
                'Client Name|Client Code': "{{ dag_run.conf.client_name }} | {{ dag_run.conf.client_code }}",
                'Task Name|Task Code': 'nil',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
                'UnitLoggedDateTime': "{{ current_time() }}",
                'Action': 'Update'
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> put_key_value_to_project

        put_key_value_to_project >> bulk_get_project_team_members >> \
            should_update_description >> should_update_project_name >> should_process_team_members_managers

        should_update_description >> rail.Label(
            "Yes") >> update_project_description >> should_update_project_name

        should_update_description >> rail.Label(
            "No") >> should_update_project_name

        should_update_project_name >> rail.Label(
            "Yes") >> update_project_name >> should_process_team_members_managers

        should_update_project_name >> rail.Label(
            "No") >> should_process_team_members_managers

        should_process_team_members_managers >> rail.Label(
            "Yes") >> process_project_team_managers >> get_project_team_managers >> \
            get_permissionuri_useruri_project_manager >> is_project_manager_to_be_assigned

        is_project_manager_to_be_assigned >> rail.Label(
            "Yes") >> process_add_permission_policy_project_manager >> \
            add_permission_policy_project_manager >> get_permissionuri_useruri_project_comanager

        is_project_manager_to_be_assigned >> rail.Label(
            "No") >> get_permissionuri_useruri_project_comanager

        get_permissionuri_useruri_project_comanager >> is_project_comanager_to_be_assigned >> rail.Label(
            "Yes") >> get_explicit_sharing_assignments_project >> compare_with_project_comanager

        compare_with_project_comanager >> rail.Label(
            "Yes") >> process_add_permission_policy_project_comanager >> \
            add_permission_policy_project_comanager >> should_process_project_status_update

        compare_with_project_comanager >> rail.Label(
            "No") >> should_process_project_status_update

        is_project_comanager_to_be_assigned >> rail.Label(
            "No") >> should_process_project_status_update

        should_process_team_members_managers >> rail.Label(
            "No") >> should_process_project_status_update

        should_process_project_status_update >> rail.Label(
            "Yes") >> all_project_status_label >> update_project_status >> \
            get_project_date_range_logs

        should_process_project_status_update >> rail.Label(
            "No") >> get_project_date_range_logs

        get_project_date_range_logs >> should_update_project_date_range

        should_update_project_date_range >> rail.Label(
            "Yes") >> update_project_date_range >> should_process_confidential_flag

        should_update_project_date_range >> rail.Label(
            "No") >> should_process_confidential_flag

        should_process_confidential_flag >> rail.Label(
            "Yes") >> update_project_custom_field_confidential_flag >> should_process_mandatory_flag

        should_process_confidential_flag >> rail.Label(
            "No") >> should_process_mandatory_flag

        should_process_mandatory_flag >> rail.Label(
            "Yes") >> update_project_custom_field_mandatory_flag >> should_update_text_effective_udf_value

        should_update_text_effective_udf_value >> rail.Label(
            "Yes") >> update_text_effective_udf_value >> should_process_client_update

        should_update_text_effective_udf_value >> rail.Label(
            "No") >> should_process_client_update

        should_process_mandatory_flag >> rail.Label(
            "No") >> should_process_client_update

        should_process_client_update >> rail.Label(
            "Yes") >> update_project_client >> should_process_work_items

        should_process_client_update >> rail.Label(
            "No") >> should_process_work_items

        should_process_work_items >> rail.Label(
            "No") >> should_update_project_team_members

        should_process_work_items >> rail.Label(
            "Yes") >> get_all_project_tasks >> existing_task_list >> new_task_list >> \
            get_tasks_to_update >> get_tasks_to_close >> get_tasks_to_add >> has_tasks_to_update

        has_tasks_to_update >> rail.Label(
            "Yes") >> update_tasks_in_replicon >> log_updated_tasks_in_replicon >> \
            has_tasks_to_close

        has_tasks_to_update >> rail.Label(
            "No") >> has_tasks_to_close

        has_tasks_to_close >> rail.Label(
            "Yes") >> close_tasks_in_replicon >> has_tasks_to_add

        has_tasks_to_close >> rail.Label(
            "No") >> has_tasks_to_add

        has_tasks_to_add >> rail.Label(
            "Yes") >> add_tasks_in_replicon >> log_added_tasks_in_replicon >> \
            should_update_project_team_members

        has_tasks_to_add >> rail.Label(
            "No") >> should_update_project_team_members

        should_update_project_team_members >> rail.Label(
            "Yes") >> bulk_update_project_team_members >> \
            update_project_team_member_billing_rate >> get_success_logs

        should_update_project_team_members >> rail.Label(
            "No") >> get_success_logs

        get_success_logs >> get_exception_logs >> \
            log_exception_for_update_project >> catch_and_log_errors

        return dag


rail.for_each_instance(create_child_update_project_dag)
