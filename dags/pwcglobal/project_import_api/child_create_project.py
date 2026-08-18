from datetime import timedelta
from airflow.models import Variable
import rail
from pwcglobal.project_import_api.task.get_team_members_project_manager import get_team_members_project_managers
from pwcglobal.project_import_api.task.add_user_permission_policy import get_add_permission_policy
from pwcglobal.project_import_api.task.child_dag_exception_logs import log_exception_field_task
from pwcglobal.project_import_api import request_payload
from pwcglobal.project_import_api import custom_method
from pwcglobal.project_import_api import response_filter
from pwcglobal.project_import_api import python_callable_method

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/project_import_api/config.py


# pylint:disable = too-many-statements
def create_child_add_project_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pwc_project_import_child_create_project_b1_{config.instance}',
        description=f'Create Project {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_create_project_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_department_list'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_department_list',
            end_task='catch_and_log_errors',
        )

        get_department_list = rail.RepliconServiceOperator(
            task_id="get_department_list",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.get_department_list_from_cost_center_code,
            response_filter=response_filter.map_company_codes_list
        )

        is_department_not_present = rail.IfOperator(
            task_id="is_department_not_present",
            test="{{ result('get_department_list') | length < 1 }}",
            yes_task='log_department_not_present',
            no_task='map_department_list_data'
        )

        log_department_not_present = rail.WriteLogOperator(
            task_id="log_department_not_present",
            log='{{ dag_run.conf.log }}',
            severity='Exception',
            message="Project not created as no company code found with the cost center code: {{ dag_run.conf.costcentre.CostCentreCode }}",
            properties={
                'SenderID': "{{ dag_run.conf.sender }} | Project",
                'Project Name|Project Code': "{{ dag_run.conf.chargecodename }} | {{ dag_run.conf.chargecode }}",
                'Client Name|Client Code': "{{ dag_run.conf.client_name }} | {{ dag_run.conf.client_code }}",
                'Task Name|Task Code': 'nil',
                'status': 'Exception',
                'details': "Project not created as no company code found with the cost center code: {{ dag_run.conf.costcentre.CostCentreCode }}",
                'UnitLoggedDateTime': "{{ current_time() }}",
                'Action': 'Add'
            }
        )

        map_department_list_data = rail.PythonOperator(
            task_id='map_department_list_data',
            python_callable=python_callable_method.do_map_department_list_data,
            op_args=[config.pwc_global_access_scope_mapper]
        )

        should_log_company_code_exception = rail.IfOperator(
            task_id="should_log_company_code_exception",
            test=lambda: bool(rail.result('map_department_list_data').get('exception1')) or
            bool(rail.result('map_department_list_data').get('exception2')),
            yes_task='log_company_code_exception',
            no_task='should_process_team_members_managers'
        )

        log_company_code_exception = rail.WriteLogOperator(
            task_id="log_company_code_exception",
            log='{{ dag_run.conf.log }}',
            severity='Exception',
            message='\
                {%- if result("map_department_list_data").exception1 | is_truthy -%} \
                    Project not created as no company code found with the cost center code: {{ dag_run.conf.costcentre.CostCentreCode }} \
                {%- else -%} \
                    Project not created since multiple company codes found with the cost center code: {{ dag_run.conf.costcentre.CostCentreCode }} \
                {%- endif -%}',
            properties={
                'SenderID': "{{ dag_run.conf.sender }} | Project",
                'Project Name|Project Code': "{{ dag_run.conf.chargecodename }} | {{ dag_run.conf.chargecode }}",
                'Client Name|Client Code': "{{ dag_run.conf.client_name }} | {{ dag_run.conf.client_code }}",
                'Task Name|Task Code': 'nil',
                'status': 'Exception',
                'details': '\
                    {%- if result("map_department_list_data").exception1 | is_truthy -%} \
                        Project not created as no company code found with the cost center code: {{ dag_run.conf.costcentre.CostCentreCode }} \
                    {%- else -%} \
                        Project not created since multiple company codes found with the cost center code: {{ dag_run.conf.costcentre.CostCentreCode }} \
                    {%- endif -%}',
                'Action': 'Add'
            }
        )

        should_process_team_members_managers = rail.IfOperator(
            task_id="should_process_team_members_managers",
            test="{{ dag_run.conf.internalpersonrole | length > 0 }}",
            yes_task='process_project_team_managers',
            no_task='create_project_with_payload'
        )

        process_project_team_managers = rail.EmptyOperator(
            task_id='process_project_team_managers',
        )

        get_project_team_managers = get_team_members_project_managers()

        get_permissionuri_useruri_project_manager = rail.PythonOperator(
            task_id='get_permissionuri_useruri_project_manager',
            python_callable=python_callable_method.get_permission_user_uri,
            op_args=['project_manager', 'get_project_manager_to_assign']
        )

        add_permission_policy_project_manager = get_add_permission_policy(
            'project_manager')

        create_project_with_payload = rail.RepliconServiceOperator(
            task_id="create_project_with_payload",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=request_payload.get_create_project_payload,
            data_handler=lambda response: response['uri'] if response else None
        )

        should_update_engagement_party_udf_value = rail.IfOperator(
            task_id="should_update_engagement_party_udf_value",
            test=lambda dag_run: bool(dag_run.conf['engagement_manager_party_uri']) and bool(
                rail.result('get_project_co_manager_to_assign')),
            yes_task="update_engagement_party_udf_value",
            no_task="should_update_text_effective_udf_value"
        )

        update_engagement_party_udf_value = rail.RepliconServiceOperator(
            task_id="update_engagement_party_udf_value",
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_project_with_payload') }}",
                "customFieldUri": "{{ dag_run.conf.engagement_manager_party_uri }}",
                "value": "{{ result('get_project_co_manager_to_assign') | map_to_attr('user_name') | first_or_default }}"
            }
        )

        should_update_text_effective_udf_value = rail.IfOperator(
            task_id="should_update_text_effective_udf_value",
            test=lambda dag_run: bool(
                dag_run.conf.get('text_effective_date_uri')) and dag_run.conf['mandatorytextflag'] == 'true',
            yes_task="update_text_effective_udf_value",
            no_task="should_update_column_view_setting_manager"
        )

        update_text_effective_udf_value = rail.RepliconServiceOperator(
            task_id="update_text_effective_udf_value",
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=request_payload.get_update_text_effective_udf_value_payload
        )

        should_update_column_view_setting_manager = rail.IfOperator(
            task_id="should_update_column_view_setting_manager",
            test=lambda: bool(
                rail.result('get_permissionuri_useruri_project_manager')),
            yes_task="impersonate_and_create_interactive_session_project_manager",
            no_task="process_add_permission_policy_project_comanager"
        )

        impersonate_and_create_interactive_session_project_manager = rail.RepliconServiceOperator(
            task_id='impersonate_and_create_interactive_session_project_manager',
            endpoint='/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession',
            data=lambda: {
                "impersonatedUserUri": rail.result('get_permissionuri_useruri_project_manager')['user_uri']
            },
            response_filter=custom_method.map_impersonate_and_create_interactive_session
        )

        put_column_view_settings_for_user_project_manager = rail.RepliconServiceCallForEachItemOperator(
            task_id='put_column_view_settings_for_user_project_manager',
            endpoint='/services/ListSettingsService1.svc/PutColumnSettingsForUser',
            items=lambda: request_payload.all_column_setting_payloads(
                'project_manager'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=lambda item: item,
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session_project_manager'),
        )

        process_add_permission_policy_project_comanager = rail.EmptyOperator(
            task_id='process_add_permission_policy_project_comanager',
        )

        get_permissionuri_useruri_project_comanager = rail.PythonOperator(
            task_id='get_permissionuri_useruri_project_comanager',
            python_callable=python_callable_method.get_permission_user_uri,
            op_args=['project_comanager', 'get_project_co_manager_to_assign']
        )

        add_permission_policy_project_comanager = get_add_permission_policy(
            'project_comanager')

        should_process_legal_entity = rail.IfOperator(
            task_id="should_process_legal_entity",
            test=lambda dag_run: bool(dag_run.conf['partyrole']) and bool(rail.find_first_by_attr_and_get_attr(
                dag_run.conf['partyrole'], 'PartyRoleType', 'PwC Delivery Legal Entity', 'PartyId')) and len(
                    [x['partyiduri'] for x in dag_run.conf['partyrole'] if x['partyiduri']]) == 1 and len(
                        dag_run.conf['partyrole'][0]['partyiduri'].split(',')) == 1,
            yes_task="update_legal_entity",
            no_task="should_process_company_code"
        )

        update_legal_entity = rail.RepliconServiceOperator(
            task_id="update_legal_entity",
            endpoint="/services/ProjectService1.svc/UpdateDivision",
            data={
                "projectUri": "{{ result('create_project_with_payload') }}",
                "division": {
                    "uri": "{{ dag_run.conf.partyrole | \
                        find_first_by_attr_and_get_attr('PartyRoleType' ,'PwC Delivery Legal Entity', 'partyiduri') }}"
                }
            }
        )

        should_process_company_code = rail.IfOperator(
            task_id="should_process_company_code",
            test=lambda: bool(rail.result(
                'map_department_list_data')['companycodeuri']) and rail.result('map_department_list_data')['companycodeurilength'] == 1,
            yes_task="update_company_code_for_project",
            no_task="should_update_location"
        )

        update_company_code_for_project = rail.RepliconServiceOperator(
            task_id="update_company_code_for_project",
            endpoint="/services/ProjectService1.svc/UpdateDepartmentGroup",
            data={
                "projectUri": "{{ result('create_project_with_payload') }}",
                "departmentGroup": {
                    "uri": "{{ result('map_department_list_data').companycodeuri }}"
                }
            }
        )

        should_update_location = rail.IfOperator(
            task_id="should_update_location",
            test=lambda dag_run: bool(rail.result('map_department_list_data')['secondlevelterritorylookupvalue']) and bool(
                rail.result('map_department_list_data')['country']) and len(
                    [x for x in dag_run.conf['replicon_locations'] if x['displayText'] == rail.result('map_department_list_data')['country']]) == 1,
            yes_task="update_location_for_project",
            no_task="should_update_client"
        )

        update_location_for_project = rail.RepliconServiceOperator(
            task_id="update_location_for_project",
            endpoint="/services/ProjectService1.svc/UpdateLocation",
            data={
                "projectUri": "{{ result('create_project_with_payload') }}",
                "location": {
                    "uri": "{{ dag_run.conf.replicon_locations | find_first_by_attr_and_get_attr('displayText', \
                result('map_department_list_data').country, 'uri') }}"
                }
            }
        )

        should_update_client = rail.IfOperator(
            task_id="should_update_client",
            test=lambda dag_run: bool(dag_run.conf['client_uri']),
            yes_task="update_client_for_project",
            no_task="should_create_tasks"
        )

        update_client_for_project = rail.RepliconServiceOperator(
            task_id="update_client_for_project",
            endpoint="/services/ProjectService1.svc/ApplyNewClient",
            data={
                "projectUri": "{{ result('create_project_with_payload') }}",
                "clientUri": "{{ dag_run.conf.client_uri }}",
                "optionUri": "urn:replicon:project-apply-new-client-option:keep-existing-billing-rates-and-expense-codes"
            }
        )

        should_create_tasks = rail.IfOperator(
            task_id="should_create_tasks",
            test=lambda dag_run: len(
                dag_run.conf.get('workitem', [])) > 0,
            yes_task="create_tasks_for_project",
            no_task="should_update_project_team_members"
        )

        create_tasks_for_project = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_tasks_for_project',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            items=lambda dag_run: dag_run.conf['workitem'],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=lambda dag_run, item: request_payload.get_task_payload(
                item, dag_run),
            data_handler=lambda response: response[0]
        )

        log_tasks = rail.WriteLogOperator(
            task_id="log_tasks",
            log='{{ dag_run.conf.log }}',
            message="Task added Successfully",
            items=lambda: rail.result('create_tasks_for_project'),
            properties=lambda item, dag_run: python_callable_method.log_tasks(
                item, dag_run, message='Task created with the team member assignment' if rail.result(
                    'get_individual_team_member_uris') else 'Task created without team assignment')

        )

        should_update_project_team_members = rail.IfOperator(
            task_id="should_update_project_team_members",
            test=lambda: bool(rail.result('get_individual_team_member_uris')),
            yes_task="bulk_update_project_team_members",
            no_task="should_apply_company_code_to_project_resource"
        )

        bulk_update_project_team_members = rail.RepliconServiceOperator(
            task_id="bulk_update_project_team_members",
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment2",
            data=lambda: {
                "projectUri": rail.result('create_project_with_payload'),
                "userUris": rail.result('get_individual_team_member_uris'),
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            }
        )

        update_project_team_member_billing_rate = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_project_team_member_billing_rate',
            endpoint='/services/TimeAndMaterialsProjectService1.svc/UpdateProjectTeamMemberBillingRateAllowedForBillingTime',
            items=lambda: rail.result('get_individual_team_member_uris'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=request_payload.get_update_project_team_member_billing_rate_payload
        )

        should_apply_company_code_to_project_resource = rail.IfOperator(
            task_id="should_apply_company_code_to_project_resource",
            test=lambda dag_run: dag_run.conf['confidential_flag'] == "false" and bool(
                rail.result('map_department_list_data')['accessscopemapper']),
            yes_task="has_company_code_uri_for_project_resource",
            no_task="get_exception_logs"
        )

        has_company_code_uri_for_project_resource = rail.IfOperator(
            task_id="has_company_code_uri_for_project_resource",
            test="{{ result('map_department_list_data').companycodeuritoassign | is_truthy }}",
            yes_task="apply_department_resource_to_project_tasks",
            no_task="get_exception_logs"
        )

        apply_department_resource_to_project_tasks = rail.RepliconServiceOperator(
            task_id="apply_department_resource_to_project_tasks",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=request_payload.get_apply_department_resource_to_project_payload
        )

        apply_department_resource_to_team_members = rail.RepliconServiceOperator(
            task_id="apply_department_resource_to_team_members",
            endpoint="/services/TimeAndMaterialsProjectService1.svc/UpdateProjectTeamMemberBillingRateAllowedForBillingTime",
            data=lambda dag_run: {
                "projectUri": rail.result('create_project_with_payload'),
                "resourceUri": rail.result('map_department_list_data')['companycodeuritoassign'],
                "billingRateUri": dag_run.conf['project_rate_uri'],
                "assigned": "true"
            }
        )

        get_exception_logs = rail.PythonOperator(
            task_id='get_exception_logs',
            python_callable=python_callable_method.do_get_exception_logs_add
        )

        log_exception_for_create_project = log_exception_field_task(
            "Add")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log }}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                'SenderID': "{{ dag_run.conf.sender }} | Project",
                'Project Name|Project Code': "{{ dag_run.conf.chargecodename }} | {{ dag_run.conf.chargecode }}",
                'Client Name|Client Code': 'nil',
                'Task Name|Task Code': 'nil',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
                'UnitLoggedDateTime': "{{ current_time() }}",
                'Action': 'Add'
            }
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.dagrun_log_conn_id,
            trigger_rule='all_done',
            extra_info={
                'MD5': '{{ dag_run.conf.md5 }}',
                'Name': '{{ dag_run.conf.chargecodename }}',
                'Code': '{{ dag_run.conf.chargecode }}',
                'Projecttype': '{{ dag_run.conf.project_type }}',
                'Payloadidentifier': '{{ dag_run.conf.identifier }}',
                'Sender': '{{ dag_run.conf.sender }}'
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors >> log_dagrun_to_sumo
        can_run_batch_task >> rail.Label('No') >> get_department_list

        get_department_list >> is_department_not_present

        is_department_not_present >> rail.Label(
            "Yes") >> log_department_not_present >> catch_and_log_errors
        is_department_not_present >> rail.Label(
            "No") >> map_department_list_data >> \
            should_log_company_code_exception >> rail.Label(
                "Yes") >> log_company_code_exception >> catch_and_log_errors

        should_log_company_code_exception >> rail.Label(
            "No") >> should_process_team_members_managers

        should_process_team_members_managers >> rail.Label(
            "Yes") >> process_project_team_managers >> \
            get_project_team_managers >> get_permissionuri_useruri_project_manager >> \
            add_permission_policy_project_manager >> \
            create_project_with_payload

        should_process_team_members_managers >> rail.Label(
            "No") >> create_project_with_payload

        create_project_with_payload >> should_update_engagement_party_udf_value

        should_update_engagement_party_udf_value >> rail.Label(
            "Yes") >> update_engagement_party_udf_value >> should_update_text_effective_udf_value

        should_update_engagement_party_udf_value >> rail.Label(
            "No") >> should_update_text_effective_udf_value

        should_update_text_effective_udf_value >> rail.Label(
            "Yes") >> update_text_effective_udf_value >> should_update_column_view_setting_manager

        should_update_text_effective_udf_value >> rail.Label(
            "No") >> should_update_column_view_setting_manager

        should_update_column_view_setting_manager >> rail.Label(
            "Yes") >> impersonate_and_create_interactive_session_project_manager >> \
            put_column_view_settings_for_user_project_manager >> process_add_permission_policy_project_comanager

        should_update_column_view_setting_manager >> rail.Label(
            "No") >> process_add_permission_policy_project_comanager

        process_add_permission_policy_project_comanager >> get_permissionuri_useruri_project_comanager >> \
            add_permission_policy_project_comanager >> should_process_legal_entity

        should_process_legal_entity >> rail.Label(
            "Yes") >> update_legal_entity >> should_process_company_code

        should_process_legal_entity >> rail.Label(
            "No") >> should_process_company_code

        should_process_company_code >> rail.Label(
            "Yes") >> update_company_code_for_project >> should_update_location

        should_process_company_code >> rail.Label(
            "No") >> should_update_location

        should_update_location >> rail.Label(
            "Yes") >> update_location_for_project >> should_update_client

        should_update_location >> rail.Label(
            "No") >> should_update_client

        should_update_client >> rail.Label(
            "Yes") >> update_client_for_project >> should_create_tasks

        should_update_client >> rail.Label(
            "No") >> should_create_tasks

        should_create_tasks >> rail.Label(
            "Yes") >> create_tasks_for_project >> log_tasks >> \
            should_update_project_team_members

        should_create_tasks >> rail.Label(
            "No") >> should_update_project_team_members

        should_update_project_team_members >> rail.Label(
            "Yes") >> bulk_update_project_team_members >> update_project_team_member_billing_rate >> \
            should_apply_company_code_to_project_resource

        should_update_project_team_members >> rail.Label(
            "No") >> should_apply_company_code_to_project_resource

        should_apply_company_code_to_project_resource >> rail.Label(
            "Yes") >> has_company_code_uri_for_project_resource

        has_company_code_uri_for_project_resource >> rail.Label(
            "Yes") >> apply_department_resource_to_project_tasks >> \
            apply_department_resource_to_team_members >> get_exception_logs

        has_company_code_uri_for_project_resource >> rail.Label(
            "No") >> get_exception_logs

        should_apply_company_code_to_project_resource >> rail.Label(
            "No") >> get_exception_logs

        get_exception_logs >> log_exception_for_create_project >> \
            catch_and_log_errors >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_child_add_project_dag)
