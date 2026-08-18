from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.c1_wbs_import_v7.utils import python_callable_method
from dxctechnology.c1_wbs_import_v7.utils import response_filter
from dxctechnology.c1_wbs_import_v7.tasks import field_config
from dxctechnology.c1_wbs_import_v7.utils import request_payload

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_wbs_import_v7/config.py

null = None


# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id_project,
        description=f'DXC_C1 WBS Automation Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.project_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        project_name = '{{ dag_run.conf.WBSElement if dag_run.conf.type == "WBS" else dag_run.conf.ServiceOrderNumberActivityOperation }}'

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='validate_field'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='validate_field',
            end_task='catch_and_log_errors',
        )

        validate_field = rail.PythonOperator(
            task_id='validate_field',
            python_callable=field_config.validate_conf_field
        )

        has_validation_error = rail.IfOperator(
            task_id="has_validation_error",
            test="{{ result('validate_field') | length > 0 }}",
            yes_task="log_validation_error",
            no_task="has_wbs_number",
        )

        has_wbs_number = rail.IfOperator(
            task_id="has_wbs_number",
            test="{{ True if dag_run.conf.ICWBSNumber else False }}",
            yes_task="get_project_info_based_on_icwbsnumber",
            no_task="get_project_info_based_on_wbs_element",
        )

        get_project_info_based_on_icwbsnumber = rail.RepliconServiceOperator(
            task_id='get_project_info_based_on_icwbsnumber',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "name": "{{ dag_run.conf.ICWBSNumber }}"
                    }
                ]
            }
        )

        is_project_exist = rail.IfOperator(
            task_id="is_project_exist",
            test=request_payload.is_icwbs_project_exist,
            yes_task="get_all_project_task",
            no_task="get_project_info_based_on_wbs_element",
        )

        get_all_project_task = rail.RepliconServiceOperator(
            task_id='get_all_project_task',
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data={
                "parentUri": "{{ result('get_project_info_based_on_icwbsnumber')[0]['projectDetails']['uri'] }}",
            }
        )

        get_all_attribute_1_project_dependant_fields = rail.RepliconServiceOperator(
            task_id= "get_all_attribute_1_project_dependant_fields",
            endpoint="services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/GetPageOfEnabledProjectDependentTimeEntryObjectExtensionTags",
            data={
                "page": "1",
                "pageSize": "10000",
                "textSearch": null,
                "project": {
                    "uri": "{{ result('get_project_info_based_on_icwbsnumber')[0]['projectDetails']['uri'] }}",
                    "name": null,
                    "code": null,
                    "parameterCorrelationId": null
                },
                "objectExtensionFieldDefinition": {
                    "uri": null,
                    "name": "Attribute 1"
                }
            }
        )

        get_all_attribute_2_project_dependant_fields = rail.RepliconServiceOperator(
            task_id= "get_all_attribute_2_project_dependant_fields",
            endpoint="services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/GetPageOfEnabledProjectDependentTimeEntryObjectExtensionTags",
            data={
                "page": "1",
                "pageSize": "10000",
                "textSearch": null,
                "project": {
                    "uri": "{{ result('get_project_info_based_on_icwbsnumber')[0]['projectDetails']['uri'] }}",
                    "name": null,
                    "code": null,
                    "parameterCorrelationId": null
                },
                "objectExtensionFieldDefinition": {
                    "uri": null,
                    "name": "Attribute 2"
                }
            }
        )

        get_all_gsap_tasks_project_dependant_fields = rail.RepliconServiceOperator(
            task_id= "get_all_gsap_tasks_project_dependant_fields",
            endpoint="services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/GetPageOfEnabledProjectDependentTimeEntryObjectExtensionTags",
            data= {
                "page": "1",
                "pageSize": "100000",
                "textSearch": null,
                "project": {
                    "uri": "{{ result('get_project_info_based_on_icwbsnumber')[0]['projectDetails']['uri'] }}",
                    "name": null,
                    "code": null,
                    "parameterCorrelationId": null
                },
                "objectExtensionFieldDefinition": {
                    "uri": null,
                    "name": "GSAP Task"
                }
            }
        )

        get_project_info_based_on_wbs_element = rail.RepliconServiceOperator(
            task_id='get_project_info_based_on_wbs_element',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "name": project_name
                    }
                ]
            }
        )

        validate_wbs_source_input_reference = rail.IfOperator(
            task_id='validate_wbs_source_input_reference',
            test=lambda dag_run: bool(dag_run.conf.get('Wbsmd5')) and request_payload.is_wbs_project_exist() and
            rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_project_info_based_on_wbs_element')[0]['projectDetails']['keyValues'],
                'keyUri', 'urn:replicon:project-key-value-key:source-input-reference-id', 'value.text'
            ) == dag_run.conf['Wbsmd5'],
            yes_task='can_update_tnm_indicator_oef_value',
            no_task='should_not_get_user_info'
        )

        can_update_tnm_indicator_oef_value = rail.IfOperator(
            task_id="can_update_tnm_indicator_oef_value",
            test=lambda: bool(request_payload.get_tnm_indicator_oef_param()),
            yes_task="update_tnm_indicator_oef_value",
            no_task="validate_changed_on_date",
        )

        update_tnm_indicator_oef_value = rail.RepliconServiceOperator(
            task_id='update_tnm_indicator_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_tnm_indicator_oef_param
        )

        validate_changed_on_date = rail.IfOperator(
            task_id='validate_changed_on_date',
            test=lambda dag_run: bool(dag_run.conf.get('Changedon')) and
            rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_project_info_based_on_wbs_element')[0]['projectDetails']['customFields'],
                'customField.uri', dag_run.conf['changedonuri'], 'date'
            ) != request_payload.get_replicon_date(dag_run.conf['Changedon']),
            yes_task='update_changed_on_custom_field',
            no_task='validate_service_order_created_date'
        )

        update_changed_on_custom_field = rail.RepliconServiceOperator(
            task_id='update_changed_on_custom_field',
            endpoint='/services/CustomFieldService1.svc/UpdateDateValue',
            data=lambda dag_run: {
                "objectUri": rail.result('get_project_info_based_on_wbs_element')[0]['projectDetails']['uri'],
                "customFieldUri": dag_run.conf['changedonuri'],
                "value": request_payload.get_replicon_date(dag_run.conf['Changedon'])
            }
        )

        validate_service_order_created_date = rail.IfOperator(
            task_id='validate_service_order_created_date',
            test=lambda dag_run: not request_payload.is_wbs_project() and
            bool(dag_run.conf.get('CreatedOnDate')) and rail.find_first_by_attr_and_get_attr(rail.result(
                'get_project_info_based_on_wbs_element')[0]['projectDetails']['customFields'],
                'customField.uri', dag_run.conf['wocreateddateuri'], 'date'
            ) != request_payload.get_replicon_date(dag_run.conf['CreatedOnDate']),
            yes_task='update_service_order_created_date',
            no_task='validate_service_order_changed_date'
        )

        update_service_order_created_date = rail.RepliconServiceOperator(
            task_id='update_service_order_created_date',
            endpoint='/services/CustomFieldService1.svc/UpdateDateValue',
            data=lambda dag_run: {
                "objectUri": rail.result('get_project_info_based_on_wbs_element')[0]['projectDetails']['uri'],
                "customFieldUri": dag_run.conf['wocreateddateuri'],
                "value": request_payload.get_replicon_date(dag_run.conf['CreatedOnDate'])
            }
        )

        validate_service_order_changed_date = rail.IfOperator(
            task_id='validate_service_order_changed_date',
            test=lambda dag_run: not request_payload.is_wbs_project() and
            bool(dag_run.conf.get('ChangedOnDate')) and rail.find_first_by_attr_and_get_attr(rail.result(
                'get_project_info_based_on_wbs_element')[0]['projectDetails']['customFields'],
                'customField.uri', dag_run.conf['wochangeddateuri'], 'date'
            ) != request_payload.get_replicon_date(dag_run.conf['ChangedOnDate']),
            yes_task='update_service_order_changed_date',
            no_task='log_same_project_payload'
        )

        update_service_order_changed_date = rail.RepliconServiceOperator(
            task_id='update_service_order_changed_date',
            endpoint='/services/CustomFieldService1.svc/UpdateDateValue',
            data=lambda dag_run: {
                "objectUri": rail.result('get_project_info_based_on_wbs_element')[0]['projectDetails']['uri'],
                "customFieldUri": dag_run.conf['wochangeddateuri'],
                "value": request_payload.get_replicon_date(dag_run.conf['ChangedOnDate'])
            }
        )

        log_same_project_payload = rail.WriteLogOperator(
            task_id='log_same_project_payload',
            message='No change in the project payload',
            properties={
                'projectname': project_name,
                'projecttype': '{{ dag_run.conf.type }}',
                'status': 'Skipped',
            }
        )

        should_not_get_user_info = rail.IfOperator(
            task_id='should_not_get_user_info',
            test=python_callable_method.validate_responsible_person_field,
            yes_task='validate_user_based_on_empid',
            no_task='get_user_based_on_empid'
        )

        get_user_based_on_empid = rail.RepliconServiceOperator(
            task_id='get_user_based_on_empid',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_user_based_on_empid_param,
            response_filter=response_filter.map_user_based_on_empid
        )

        validate_user_based_on_empid = rail.PythonOperator(
            task_id="validate_user_based_on_empid",
            python_callable=python_callable_method.validate_user_based_on_empid_method,
            op_args=['get_user_based_on_empid']
        )

        has_user_exception_validation_error = rail.IfOperator(
            task_id="has_user_exception_validation_error",
            test=lambda: bool(len(list(filter(lambda x: x['status'] == 'Exception', rail.result(
                "validate_user_based_on_empid")))) > 0),
            yes_task="log_user_validation_error",
            no_task="can_enable_project_owner_user"
        )

        can_enable_project_owner_user = rail.IfOperator(
            task_id="can_enable_project_owner_user",
            test=lambda: bool(
                rail.result('get_user_based_on_empid') and
                not rail.result('get_user_based_on_empid')['userstatus'] and
                rail.result('validate_user_based_on_empid', 'can_assign_manager')),
            yes_task="enable_project_owner_user",
            no_task="can_enable_project_comanager_user"
        )

        enable_project_owner_user = rail.RepliconServiceOperator(
            task_id='enable_project_owner_user',
            endpoint='/services/SecurityService1.svc/EnableLogin',
            data={"userUri": "{{ result('get_user_based_on_empid').useruri }}"}
        )

        can_enable_project_comanager_user = rail.IfOperator(
            task_id="can_enable_project_comanager_user",
            test=lambda: bool(
                rail.result('get_user_based_on_empid') and
                not rail.result('get_user_based_on_empid')['comanagerstatus'] and
                rail.result('validate_user_based_on_empid', 'can_assign_co_manager')),
            yes_task="enable_project_comanager_user",
            no_task="can_get_permission_sets_for_user"
        )

        enable_project_comanager_user = rail.RepliconServiceOperator(
            task_id='enable_project_comanager_user',
            endpoint='/services/SecurityService1.svc/EnableLogin',
            data={
                "userUri": "{{ result('get_user_based_on_empid').comanageruri }}"}
        )

        can_get_permission_sets_for_user = rail.IfOperator(
            task_id="can_get_permission_sets_for_user",
            test="{{ result('validate_user_based_on_empid','can_assign_manager') or result('validate_user_based_on_empid','can_assign_co_manager') }}",
            yes_task="get_permission_sets_for_user",
            no_task="can_copy_project_from_icwbs"
        )

        get_permission_sets_for_user = rail.RepliconServiceOperator(
            task_id='get_permission_sets_for_user',
            endpoint='/services/PermissionSetService1.svc/BulkGetAssignedPermissionSetsForUsers',
            data=request_payload.get_permission_sets_for_user_param
        )

        def get_items():
            user_data = rail.result('get_user_based_on_empid')
            uris = []
            if user_data['useruri']:
                uris.append(user_data['useruri'])
            if user_data['comanageruri']:
                uris.append(user_data['comanageruri'])
            return uris

        get_manager_co_manager_effective_groups = rail.RepliconServiceCallForEachItemOperator(
            task_id = "get_manager_co_manager_effective_groups",
            endpoint = "/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            items=get_items,
            data= lambda item: {
                "userUri": item,
                "dateRange": null
            }
        )

        get_assign_permissionset_to_user_param = rail.PythonOperator(
            task_id='get_assign_permissionset_to_user_param',
            python_callable=request_payload.get_assign_permissionset_to_user_param,
        )

        has_assign_permissionset_to_user = rail.IfOperator(
            task_id="has_assign_permissionset_to_user",
            test=lambda: len(rail.result('get_assign_permissionset_to_user_param')[
                'permissionSets']) > 0,
            yes_task="assign_permissionset_to_user",
            no_task="has_assign_policyDataAccessScopes_to_user",
        )

        assign_permissionset_to_user = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_permissionset_to_user',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result(
                'get_assign_permissionset_to_user_param')['permissionSets'],
            data=lambda item: item
        )

        has_assign_policyDataAccessScopes_to_user = rail.IfOperator(
            task_id="has_assign_policyDataAccessScopes_to_user",
            test=lambda: len(rail.result(
                'get_assign_permissionset_to_user_param')['policydataaccessscopes']) > 0,
            yes_task="assign_policyDataAccessScopes_to_user",
            no_task="can_copy_project_from_icwbs",
        )

        assign_policyDataAccessScopes_to_user = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_policyDataAccessScopes_to_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            endpoint='/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser',
            items=lambda: rail.result('get_assign_permissionset_to_user_param')[
                'policydataaccessscopes'],
            data=lambda item: item
        )

        can_copy_project_from_icwbs = rail.IfOperator(
            task_id="can_copy_project_from_icwbs",
            test=request_payload.can_copy_project_from_icwbs,
            yes_task="create_project_copy_batch",
            no_task="create_projectorapply_modifications",
        )

        create_project_copy_batch = rail.RepliconServiceOperator(
            task_id='create_project_copy_batch',
            endpoint='/services/ProjectService1.svc/CreateProjectCopyBatch2',
            data=request_payload.get_project_copy_batch_param
        )

        project_copy_batch_group_entry, project_copy_batch_group_exit = rail.batch_execution(
            'execute_project_copy_batch', create_project_copy_batch.task_id)

        get_projectcopy_batch_results = rail.RepliconServiceOperator(
            task_id='get_projectcopy_batch_results',
            endpoint='/services/ProjectService1.svc/GetProjectCopyBatchResults',
            data=lambda: {"projectCopyBatchUri": rail.result(
                'create_project_copy_batch')}
        )

        create_projectorapply_modifications = rail.RepliconServiceOperator(
            task_id='create_projectorapply_modifications',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=lambda: request_payload.create_projectorapply_modification_param(Variable.get(
                config.can_create_client_var_name, default_var='true').lower() == "true")
        )

        can_assign_comanager = rail.IfOperator(
            task_id="can_assign_comanager",
            test="{{ True if result('validate_user_based_on_empid','can_assign_co_manager') else False }}",
            yes_task="assign_comanager",
            no_task="update_divsion",
        )

        assign_comanager = rail.RepliconServiceOperator(
            task_id='assign_comanager',
            endpoint='/services/ProjectService1.svc/PutExplicitSharingAssignments',
            data=lambda: {
                "projectUri": rail.result('create_projectorapply_modifications')['uri'],
                "sharedUris": [
                    rail.result('get_user_based_on_empid')['comanageruri']]}
        )

        update_divsion = rail.RepliconServiceOperator(
            task_id='update_divsion',
            endpoint='/services/ProjectService1.svc/UpdateDivision2',
            data=lambda: {
                "projectUri": rail.result('create_projectorapply_modifications')['uri'],
                "division": {
                    "uri": request_payload.get_dag_run_conf()['companycodeuri'] if request_payload.is_wbs_project() else
                    request_payload.get_dag_run_conf(
                    )['serviceordercompanycodeuri']
                }
            }
        )

        can_update_cost_center = rail.IfOperator(
            task_id="can_update_cost_center",
            test=lambda: bool(
                request_payload.get_dag_run_conf()['costcenter']),
            yes_task="update_cost_center",
            no_task="put_eligibleprojectteammember_dataaccessscopes",
        )

        update_cost_center = rail.RepliconServiceOperator(
            task_id='update_cost_center',
            endpoint='/services/ProjectService1.svc/UpdateCostCenter2',
            data=lambda: {
                "projectUri": rail.result('create_projectorapply_modifications')['uri'],
                "costCenter": {
                    "name": request_payload.get_dag_run_conf()['costcenter']}}
        )

        put_eligibleprojectteammember_dataaccessscopes = rail.RepliconServiceOperator(
            task_id='put_eligibleprojectteammember_dataaccessscopes',
            endpoint='/services/ProjectService1.svc/PutEligibleProjectTeamMemberDataAccessScopesForProject',
            data=request_payload.get_eligibleprojectteammember_dataaccessscopes
        )

        get_project_created_status = rail.PythonOperator(
            task_id="get_project_created_status",
            python_callable=lambda: not request_payload.is_wbs_project_exist()
        )

        is_project_created = rail.IfOperator(
            task_id='is_project_created',
            test=lambda: not request_payload.is_wbs_project_exist(),
            yes_task='put_keyvalue_for_project',
            no_task='can_update_tnm_indicator_oef'
        )

        put_keyvalue_for_project = rail.RepliconServiceOperator(
            task_id='put_keyvalue_for_project',
            endpoint='/services/ProjectService1.svc/PutKeyValueForProject',
            data=request_payload.get_keyvalue_for_project
        )

        can_update_tnm_indicator_oef = rail.IfOperator(
            task_id="can_update_tnm_indicator_oef",
            test=lambda: bool(request_payload.get_tnm_indicator_oef_param()),
            yes_task="update_tnm_indicator_oef",
            no_task="can_update_time_tracking_oef",
        )

        update_tnm_indicator_oef = rail.RepliconServiceOperator(
            task_id='update_tnm_indicator_oef',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_tnm_indicator_oef_param
        )

        can_update_time_tracking_oef = rail.IfOperator(
            task_id="can_update_time_tracking_oef",
            test=lambda: bool(request_payload.get_time_tracking_oef_param()),
            yes_task="update_time_tracking_oef",
            no_task="update_russia_udf",
        )

        update_time_tracking_oef = rail.RepliconServiceOperator(
            task_id='update_time_tracking_oef',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_time_tracking_oef_param
        )

        update_russia_udf = rail.RepliconServiceOperator(
            task_id='update_russia_udf',
            endpoint='/services/CustomFieldService1.svc/UpdateDropdownValue',
            data=request_payload.get_update_russia_udf_param
        )

        is_compass_updated_wbsofferinggrp_psaflag = rail.IfOperator(
            task_id="is_compass_updated_wbsofferinggrp_psaflag",
            test=request_payload.check_wbsofferinggrp_psaflag,
            yes_task="can_update_client",
            no_task="can_update_project_identifier",
        )

        can_update_project_identifier = rail.IfOperator(
            task_id="can_update_project_identifier",
            test='{{ dag_run.conf.ProjectIdentifier | is_truthy }}',
            yes_task="update_project_identifier_oef",
            no_task="can_update_client",
        )

        update_project_identifier_oef = rail.RepliconServiceOperator(
            task_id='update_project_identifier_oef',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_project_identifier_oef_param
        )

        can_update_psa_flag = rail.IfOperator(
            task_id="can_update_psa_flag",
            test='{{ dag_run.conf.ProjectType == "RP" }}',
            yes_task="update_psa_flag",
            no_task="can_update_client",
        )

        update_psa_flag = rail.RepliconServiceOperator(
            task_id='update_psa_flag',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_psa_flag_oef_param
        )

        update_psa_team_assignment= rail.RepliconServiceOperator(
            task_id='update_psa_team_assignment',
            endpoint='/services/ProjectService1.svc/PutEligibleProjectTeamMemberDataAccessScopesForProject',
            data=request_payload.get_assign_team_psa_param
        )

        can_update_client = rail.IfOperator(
            task_id="can_update_client",
            test=lambda: request_payload.can_update_client() and Variable.get(
                config.can_create_client_var_name, default_var='true').lower() == "true",
            yes_task="get_client_info",
            no_task="can_inherit_psa_flag",
        )

        get_client_info = rail.RepliconServiceOperator(
            task_id='get_client_info',
            endpoint='/services/ClientListService1.svc/GetData',
            data=request_payload.get_client_list_search_param('{{ dag_run.conf.client }}'))

        update_client = rail.RepliconServiceOperator(
            task_id='update_client',
            endpoint='/services/ProjectService1.svc/ApplyNewClient2',
            data=request_payload.get_update_client_param
        )

        can_inherit_psa_flag = rail.IfOperator(
            task_id = 'can_inherit_psa_flag',
            test = request_payload.is_icwbs_project_exist,
            yes_task= 'inherit_psa_flag_from_parent',
            no_task = 'log_completion'
        )

        inherit_psa_flag_from_parent = rail.RepliconServiceOperator(
            task_id='inherit_psa_flag_from_parent',
             endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_inherit_psa_flag_payload
        )

        log_validation_error = rail.WriteLogOperator(
            task_id='log_validation_error',
            message='{{result("validate_field")}}',
            properties={
                'projectname': project_name,
                'projecttype': '{{ dag_run.conf.type }}',
                'status': 'Exception',
            }
        )

        log_user_validation_error = rail.WriteLogOperator(
            task_id='log_user_validation_error',
            message='{{result("validate_user_based_on_empid")|map(attribute="message")|join(", ")}}',
            properties={
                'projectname': project_name,
                'projecttype': '{{ dag_run.conf.type }}',
                'status': 'Exception',
            }
        )

        log_completion = rail.WriteLogOperator(
            task_id='log_completion',
            message='\
                {%- if result("validate_user_based_on_empid") | length == 0 -%} \
                    {{ "Project created successfully" if result("get_project_created_status") | is_truthy else "Project updated sucessfully" }} \
                {%- else -%} \
                    {{ "Project created partially, " if result("get_project_created_status") | is_truthy else "Project updated partially, " -}} \
                    {{ result("validate_user_based_on_empid") | map_to_attr("message") | join(", ") }} \
                {%- endif -%}',
            properties={
                'projectname': project_name,
                'projecttype': '{{ dag_run.conf.type }}',
                'status': '{{ "Success" if result("validate_user_based_on_empid") | length == 0  else "Exception" }}',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                'projectname': project_name,
                'projecttype': '{{ dag_run.conf.type }}',
                'status': 'Error',
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> validate_field

        validate_field >> has_validation_error

        has_validation_error >> rail.Label(
            "Yes") >> log_validation_error >> catch_and_log_errors
        has_validation_error >> rail.Label(
            "No") >> has_wbs_number

        has_wbs_number >> rail.Label(
            "Yes") >> get_project_info_based_on_icwbsnumber >> is_project_exist
        has_wbs_number >> rail.Label(
            "No") >> get_project_info_based_on_wbs_element

        is_project_exist >> rail.Label(
            "Yes") >> get_all_project_task >> get_all_attribute_1_project_dependant_fields\
                >> get_all_attribute_2_project_dependant_fields >>  get_all_gsap_tasks_project_dependant_fields

        get_all_gsap_tasks_project_dependant_fields >> get_project_info_based_on_wbs_element

        is_project_exist >> rail.Label(
            "No") >> get_project_info_based_on_wbs_element

        get_project_info_based_on_wbs_element >> validate_wbs_source_input_reference

        validate_wbs_source_input_reference >> rail.Label(
            "Yes") >> can_update_tnm_indicator_oef_value

        can_update_tnm_indicator_oef_value >> rail.Label(
            "Yes") >> update_tnm_indicator_oef_value >> validate_changed_on_date
        can_update_tnm_indicator_oef_value >> rail.Label("No") >> validate_changed_on_date

        validate_changed_on_date >> rail.Label(
            "Yes") >> update_changed_on_custom_field >> validate_service_order_created_date

        validate_changed_on_date >> rail.Label(
            "No") >> validate_service_order_created_date

        validate_service_order_created_date >> rail.Label(
            "Yes") >> update_service_order_created_date >> validate_service_order_changed_date

        validate_service_order_created_date >> rail.Label(
            "No") >> validate_service_order_changed_date

        validate_service_order_changed_date >> rail.Label(
            "Yes") >> update_service_order_changed_date >> log_same_project_payload >> catch_and_log_errors

        validate_service_order_changed_date >> rail.Label(
            "No") >> log_same_project_payload >> catch_and_log_errors

        validate_wbs_source_input_reference >> rail.Label(
            "No") >> should_not_get_user_info

        should_not_get_user_info >> rail.Label(
            "No") >> get_user_based_on_empid >> validate_user_based_on_empid
        should_not_get_user_info >> rail.Label(
            "Yes") >> validate_user_based_on_empid

        validate_user_based_on_empid >> has_user_exception_validation_error

        has_user_exception_validation_error >> rail.Label(
            "Yes") >> log_user_validation_error >> catch_and_log_errors
        has_user_exception_validation_error >> rail.Label(
            "No") >> can_enable_project_owner_user

        can_enable_project_owner_user >> rail.Label(
            "Yes") >> enable_project_owner_user >> can_enable_project_comanager_user
        can_enable_project_owner_user >> rail.Label(
            "No") >> can_enable_project_comanager_user

        can_enable_project_comanager_user >> rail.Label(
            "Yes") >> enable_project_comanager_user >> can_get_permission_sets_for_user
        can_enable_project_comanager_user >> rail.Label(
            "No") >> can_get_permission_sets_for_user

        can_get_permission_sets_for_user >> rail.Label(
            "Yes") >> get_permission_sets_for_user >> get_manager_co_manager_effective_groups >> get_assign_permissionset_to_user_param >> \
            has_assign_permissionset_to_user
        can_get_permission_sets_for_user >> rail.Label(
            "No") >> can_copy_project_from_icwbs

        has_assign_permissionset_to_user >> rail.Label(
            "Yes") >> assign_permissionset_to_user >> has_assign_policyDataAccessScopes_to_user
        has_assign_permissionset_to_user >> rail.Label(
            "No") >> has_assign_policyDataAccessScopes_to_user

        has_assign_policyDataAccessScopes_to_user >> rail.Label(
            "Yes") >> assign_policyDataAccessScopes_to_user >> can_copy_project_from_icwbs
        has_assign_policyDataAccessScopes_to_user >> rail.Label(
            "No") >> can_copy_project_from_icwbs

        can_copy_project_from_icwbs >> rail.Label(
            "Yes") >> create_project_copy_batch >> project_copy_batch_group_entry >> \
            project_copy_batch_group_exit >> get_projectcopy_batch_results >> create_projectorapply_modifications  # has_valid_status_if_new_project
        can_copy_project_from_icwbs >> rail.Label(
            "No") >> create_projectorapply_modifications  # has_valid_status_if_new_project

        create_projectorapply_modifications >> get_project_created_status >> can_assign_comanager
        can_assign_comanager >> rail.Label(
            "Yes") >> assign_comanager >> update_divsion
        can_assign_comanager >> rail.Label(
            "No") >> update_divsion

        update_divsion >> can_update_cost_center
        can_update_cost_center >> rail.Label(
            "Yes") >> update_cost_center >> put_eligibleprojectteammember_dataaccessscopes
        can_update_cost_center >> rail.Label(
            "No") >> put_eligibleprojectteammember_dataaccessscopes

        put_eligibleprojectteammember_dataaccessscopes >> is_project_created

        is_project_created >> rail.Label(
            'yes') >> put_keyvalue_for_project >> can_update_tnm_indicator_oef
        is_project_created >> rail.Label('No') >> can_update_tnm_indicator_oef

        can_update_tnm_indicator_oef >> rail.Label(
            "Yes") >> update_tnm_indicator_oef >> can_update_time_tracking_oef
        can_update_tnm_indicator_oef >> rail.Label("No") >> can_update_time_tracking_oef


        can_update_time_tracking_oef >> rail.Label(
            "Yes") >> update_time_tracking_oef >> update_russia_udf
        can_update_time_tracking_oef >> rail.Label(
            "No") >> update_russia_udf >> is_compass_updated_wbsofferinggrp_psaflag
        is_compass_updated_wbsofferinggrp_psaflag >> rail.Label('Yes') >> can_update_client
        is_compass_updated_wbsofferinggrp_psaflag >> rail.Label('No') >> can_update_project_identifier >> rail.Label(
            "Yes") >> update_project_identifier_oef >> can_update_psa_flag
        can_update_psa_flag >> rail.Label(
            "Yes") >> update_psa_flag >> update_psa_team_assignment >> can_update_client
        can_update_psa_flag >> rail.Label(
            "No") >> can_update_client
        can_update_project_identifier >> rail.Label(
            "No") >> can_update_client
        can_update_client >> rail.Label(
            "Yes") >> get_client_info >> update_client >> can_inherit_psa_flag

        can_update_client >> rail.Label("No") >> can_inherit_psa_flag
        can_inherit_psa_flag >> rail.Label('Yes') >> inherit_psa_flag_from_parent >> log_completion
        can_inherit_psa_flag >> rail.Label('No') >> log_completion

        log_completion >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
