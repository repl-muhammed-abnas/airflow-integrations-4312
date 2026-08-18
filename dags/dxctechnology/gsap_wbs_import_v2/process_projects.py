from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.gsap_wbs_import_v2.utils import request_payload
from dxctechnology.gsap_wbs_import_v2.utils import response_filter
from dxctechnology.gsap_wbs_import_v2.utils import python_callable_methods


# pylint: disable=too-many-statements
def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_wbs_import_child_process_projects_{config.instance}_v2',
        description='DXC_GSAP_WBS_Automation Process WBS',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_projects,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        project_name = '{{ dag_run.conf.wbsname }}'

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_wbs_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_wbs_log',
            end_task='catch_and_log_errors',
        )

        create_wbs_log = rail.CreateLogOperator(
            task_id='create_wbs_log'
        )

        has_mandatory_fields = rail.IfOperator(
            task_id='has_mandatory_fields',
            test=request_payload.mandatory_fields_check,
            yes_task="create_exception_log",
            no_task="log_madatory_fields_not_present"
        )

        log_madatory_fields_not_present = rail.WriteLogOperator(
            task_id='log_madatory_fields_not_present',
            log='{{ result("create_wbs_log") }}',
            message='\
                {%- if dag_run.conf.wbsname | is_falsy -%} \
                    WBS Name is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.companycode | is_falsy -%} \
                    Company code doesn"t exist - WBS is not created, \
                {%- endif -%}\
                {%- if dag_run.conf.startdate | is_truthy -%} \
                    Project Start Date in incorrect format, \
                {%- endif -%}\
                {%- if dag_run.conf.startdate | is_falsy -%} \
                    Project Start Date is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.enddate | is_truthy -%} \
                    Project End Date in incorrect format, \
                {%- endif -%}\
                {%- if dag_run.conf.enddate | is_falsy -%} \
                    Project End Date is not present in payload, \
                {%- endif -%}',
            severity='Exception',
            properties={
                'projectname': '{{dag_run.conf.wbsname}}',
                'status': 'Exception'
            }
        )

        create_exception_log = rail.CreateLogOperator(
            task_id='create_exception_log'
        )

        has_project_manager_in_feed_file = rail.IfOperator(
            task_id='has_project_manager_in_feed_file',
            test=lambda dag_run: dag_run.conf['projectmanagerid'] and dag_run.conf['projectmanagerid']!='00000000',
            yes_task="get_user_info_on_empid",
            no_task="log_no_projectmanger_in_feed_file"
        )

        log_no_projectmanger_in_feed_file = rail.WriteLogOperator(
            task_id='log_no_projectmanger_in_feed_file',
            log='{{ result("create_exception_log") }}',
            message='Project Manager ID not present in feed file',
            severity='Exception',
            properties={
                'projectname': '{{dag_run.conf.wbsname}}',
                'status': 'Exception'
            }
        )

        get_user_info_on_empid = rail.RepliconServiceOperator(
            task_id="get_user_info_on_empid",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_user_info_on_empid,
            data_handler=response_filter.get_filtered_user_info
        )

        is_project_manager_available = rail.IfOperator(
            task_id='is_project_manager_available',
            test=lambda: bool(rail.result('get_user_info_on_empid')),
            yes_task="is_end_date_in_past",
            no_task="is_user_found_using_perner"
        )

        is_user_found_using_perner = rail.IfOperator(
            task_id = "is_user_found_using_perner",
            test="{{ dag_run.conf.perner_user | is_truthy }}",
            yes_task= "empty_get_user_info",
            no_task="log_no_projectmanger_not_available"
        )

        empty_get_user_info = rail.EmptyOperator(
            task_id = "empty_get_user_info"
        )

        get_user_info = rail.RepliconServiceOperator(
            task_id="get_user_info",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{dag_run.conf.perner_user.user_uri }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=response_filter.get_user_details_filter
        )

        log_no_projectmanger_not_available = rail.WriteLogOperator(
            task_id='log_no_projectmanger_not_available',
            log='{{ result("create_exception_log") }}',
            message='Project Manager is not available/disabled in replicon',
            severity='Exception',
            properties={
                'projectname': '{{dag_run.conf.wbsname}}',
                'status': 'Exception'
            }
        )

        is_end_date_in_past = rail.IfOperator(
            task_id='is_end_date_in_past',
            test=request_payload.test_enddate,
            yes_task="log_end_date_in_past",
            no_task="can_enable_project_manager"
        )

        can_enable_project_manager = rail.IfOperator(
            task_id='can_enable_project_manager',
            test=request_payload.can_enable_project_manager,
            yes_task="enable_project_manager",
            no_task="is_project_manager_contractor"
        )

        enable_project_manager = rail.RepliconServiceOperator(
            task_id='enable_project_manager',
            endpoint='/services/SecurityService1.svc/EnableLogin',
            data=lambda:{
                "userUri": (rail.result('get_user_info_on_empid') or rail.result('get_user_info'))[0]['uri']
                }
        )

        log_end_date_in_past = rail.WriteLogOperator(
            task_id='log_end_date_in_past',
            log='{{ result("create_exception_log") }}',
            message='Project Manager was not assigned as the user end date is in past',
            severity='Exception',
            properties={
                'projectname': '{{dag_run.conf.wbsname}}',
                'status': 'Exception'
            }
        )

        is_project_manager_contractor = rail.IfOperator(
            task_id='is_project_manager_contractor',
            test=request_payload.test_contractor,
            yes_task="is_division_in_australia",
            no_task="get_permission_sets_for_project_manager"
        )

        is_division_in_australia = rail.IfOperator(
            task_id='is_division_in_australia',
            test=lambda: bool((rail.result('get_user_info_on_empid') or rail.result('get_user_info'))[
                              0]['division'] in config.contractor_company_codes)
            if (rail.result('get_user_info_on_empid') or rail.result('get_user_info'))[0]['division'] else True,
            yes_task="get_permission_sets_for_project_manager",
            no_task="log_project_manager_outside_australia"
        )

        log_project_manager_outside_australia = rail.WriteLogOperator(
            task_id='log_project_manager_outside_australia',
            log='{{ result("create_exception_log") }}',
            message='WBS Owner (Contractor) belongs to company code outside of Australia',
            severity='Exception',
            properties={
                'projectname': '{{dag_run.conf.wbsname}}',
                'status': 'Exception'
            }
        )

        get_permission_sets_for_project_manager = rail.RepliconServiceOperator(
            task_id='get_permission_sets_for_project_manager',
            endpoint='/services/PermissionSetService1.svc/BulkGetAssignedPermissionSetsForUsers',
            data=request_payload.get_permission_sets_for_project_manager,
        )

        check_for_required_permissions = rail.PythonOperator(
            task_id='check_for_required_permissions',
            python_callable=python_callable_methods.check_for_required_permissions,
        )

        has_required_permissions = rail.IfOperator(
            task_id="has_required_permissions",
            test=lambda: len(rail.result('check_for_required_permissions')[
                'permissionSets']) > 0,
            yes_task='assign_permissionset_to_projectmanager',
            no_task="has_parent_wbs",
        )

        assign_permissionset_to_projectmanager = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_permissionset_to_projectmanager',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            items=lambda: rail.result(
                'check_for_required_permissions')['permissionSets'],
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout_days),
            data=lambda item: {
                "userUri": (rail.result('get_user_info_on_empid') or rail.result('get_user_info'))[0]['uri'],
                "permissionSetUri": item
            }
        )

        assign_policyDataAccessScopes_to_projectmanager = rail.RepliconServiceOperator(
            task_id='assign_policyDataAccessScopes_to_projectmanager',
            endpoint='/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser',
            data=request_payload.assign_policyDataAccessScopes_to_projectmanager
        )

        has_parent_wbs = rail.IfOperator(
            task_id="has_parent_wbs",
            test=lambda dag_run: bool(
                dag_run.conf['c1compassparentwbs'] or dag_run.conf['gsapparentwbs']),
            yes_task="get_project_info_on_parentwbs",
            no_task="get_project_info_based_on_wbs_element",
        )

        get_project_info_on_parentwbs = rail.RepliconServiceOperator(
            task_id='get_project_info_on_parentwbs',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=request_payload.get_project_info_on_parentwbs
        )

        does_parentwbs_exist =  rail.IfOperator(
            task_id="does_parentwbs_exist",
            test=request_payload.does_parent_project_exist,
            yes_task="get_project_info_based_on_wbs_element",
            no_task="log_parent_doesnt_exist",
        )

        log_parent_doesnt_exist = rail.WriteLogOperator(
            task_id='log_parent_doesnt_exist',
            log='{{ result("create_exception_log") }}',
            # pylint: disable=line-too-long
            message='Parent Project "{{dag_run.conf.c1compassparentwbs if dag_run.conf.c1compassparentwbs else dag_run.conf.gsapparentwbs}}" is not available in Replicon',
            severity='Exception',
            properties={
                'projectname': '{{dag_run.conf.wbsname}}',
                'status': 'Exception'
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

        can_copy_project_from_parent = rail.IfOperator(
            task_id="can_copy_project_from_parent",
            test=request_payload.can_copy_project_from_parent,
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

        is_parent_c1 = rail.IfOperator(
            task_id="is_parent_c1",
            test=lambda dag_run: dag_run.conf['c1compassparentwbs'] and request_payload.check_parent_division(dag_run) == 'C1',
            yes_task="get_all_labour_types",
            no_task="create_projectorapply_modifications",
        )

        get_all_labour_types = rail.RepliconServiceOperator(
            task_id='get_all_labour_types',
            endpoint='/services/ImportService1.svc/BulkGetProjects2',
            data=request_payload.get_all_labour_types,
            data_handler=response_filter.get_all_labour_types
        )

        create_projectorapply_modifications = rail.RepliconServiceOperator(
            task_id='create_projectorapply_modifications',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.create_projectorapply_modifications
        )

        can_update_blob = rail.IfOperator(
            task_id="can_update_blob",
            test=lambda dag_run: dag_run.conf['c1compassparentwbs'] and request_payload.does_parent_project_exist()
             and request_payload.check_parent_division(dag_run) == 'C1'
            and not request_payload.does_wbs_exist(),
            yes_task="process_blob_update",
            no_task="update_division",
        )

        process_blob_update = rail.TriggerDagRunOperator(
            task_id='process_blob_update',
            retries=0,
            execution_timeout=timedelta(days=config.child_wait_execution_timeout_days),
            trigger_dag_id=f'dxctechnology_gsap_wbs_import_child_process_blob_{config.instance}_v2',
            conf=request_payload.get_blob_update,
        )

        wait_for_process_blob_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_blob_update',
            dag_runs='{{ result("process_blob_update") }}',
            execution_timeout=timedelta(days=config.child_wait_execution_timeout_days),
        )

        update_division = rail.RepliconServiceOperator(
            task_id='update_division',
            endpoint='/services/ProjectService1.svc/UpdateDivision2',
            data=lambda dag_run: {
                "projectUri": rail.result('create_projectorapply_modifications')['uri'],
                "division": {
                    "uri": dag_run.conf['companycodeuri']
                }
            }
        )

        put_eligibleprojectteammember_dataaccessscopes = rail.RepliconServiceOperator(
            task_id='put_eligibleprojectteammember_dataaccessscopes',
            endpoint='/services/ProjectService1.svc/PutEligibleProjectTeamMemberDataAccessScopesForProject',
            data=request_payload.get_eligibleprojectteammember_dataaccessscopes
        )

        can_update_tnm_indicator_oef_value = rail.IfOperator(
            task_id="can_update_tnm_indicator_oef_value",
            test= lambda dag_run: request_payload.does_parent_project_exist() and
                request_payload.check_parent_division(dag_run) == "COMPASS",
            yes_task="update_tnm_indicator_oef_value",
            no_task="has_client",
        )

        update_tnm_indicator_oef_value = rail.RepliconServiceOperator(
            task_id='update_tnm_indicator_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_tnm_indicator_oef_param
        )

        has_client = rail.IfOperator(
            task_id="has_client",
            test=lambda dag_run: bool(dag_run.conf['clientname']),
            yes_task="get_client_info",
            no_task="remove_client",
        )

        get_client_info = rail.RepliconServiceOperator(
            task_id='get_client_info',
            endpoint='/services/ClientListService1.svc/GetData',
            data=request_payload.get_client_list_search_param(
                "{{dag_run.conf.clientname}}"),
            data_handler=response_filter.get_filtered_client_data
        )
        
        if_client_info_exists = rail.IfOperator(
            task_id='if_client_info_exists',
            test=lambda: bool(rail.result('get_client_info')),
            yes_task='update_client',
            no_task='log_client_info_not_found'
        )

        update_client = rail.RepliconServiceOperator(
            task_id='update_client',
            endpoint='/services/ProjectService1.svc/ApplyNewClient2',
            data=lambda: request_payload.update_client('update')
        )
        
        log_client_info_not_found = rail.WriteLogOperator(
            task_id='log_client_info_not_found',
            log='{{ result("create_exception_log") }}',
            message="Client info for client name: '{{dag_run.conf.clientname}}' , not found in Replicon",
            severity='Exception',
            properties={
                'projectname': '{{dag_run.conf.wbsname}}',
                'status': 'Exception'
            }
        )

        remove_client = rail.RepliconServiceOperator(
            task_id='remove_client',
            endpoint='/services/ProjectService1.svc/ApplyNewClient2',
            data=lambda: request_payload.update_client('remove')
        )

        can_be_potential_parent = rail.IfOperator(
            task_id="can_be_potential_parent",
            test=request_payload.can_be_potential_parent,
            yes_task="get_data_of_child_wbs",
            no_task="get_all_exception_logs",
        )

        get_data_of_child_wbs = rail.RepliconServiceOperator(
            task_id='get_data_of_child_wbs',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=request_payload.get_project_list_payload,
            response_filter=response_filter.get_filtered_data
        )

        has_childs_wbs = rail.IfOperator(
            task_id="has_childs_wbs",
            test=lambda: bool(rail.result('get_data_of_child_wbs')),
            yes_task="update_iwo_wbs_element",
            no_task="get_all_exception_logs",
        )

        update_iwo_wbs_element = rail.RepliconServiceOperator(
            task_id='update_iwo_wbs_element',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.update_iwo_wbs_element
        )

        process_child_wbs = rail.TriggerDagRunForEachItemOperator(
            task_id='process_child_wbs',
            retries=0,
            items="{{ result('get_data_of_child_wbs') | to_json }}",
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout_days),
            trigger_dag_id=f'dxctechnology_gsap_wbs_import_child_process_child_projects_{config.instance}_v2',
            conf=request_payload.get_process_child_wbs_conf
        )

        get_all_exception_logs = rail.PythonOperator(
            task_id='get_all_exception_logs',
            python_callable=python_callable_methods.get_exception_logs,
            op_args=['create_exception_log']
        )

        log_completion = rail.WriteLogOperator(
            task_id='log_completion',
            log='{{ result("create_wbs_log") }}',
            message=request_payload.get_completion_message,
            severity=request_payload.get_severity,
            properties={
                'projectname': project_name,
                'status': "{{'Exception' if result('get_all_exception_logs') else 'Success' }}",
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ result("create_wbs_log") }}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'projectname': project_name,
                'status': 'Error',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_wbs_log

        create_wbs_log >> has_mandatory_fields >> rail.Label(
            'No') >> log_madatory_fields_not_present >> catch_and_log_errors >> log_to_sumo
        has_mandatory_fields >> rail.Label(
            'Yes') >> create_exception_log >> has_project_manager_in_feed_file

        has_project_manager_in_feed_file >> rail.Label(
            'No') >> log_no_projectmanger_in_feed_file >> has_parent_wbs
        has_project_manager_in_feed_file >> rail.Label(
            'Yes') >> get_user_info_on_empid >> is_project_manager_available
        is_project_manager_available >> is_user_found_using_perner >> rail.Label("Yes") >> empty_get_user_info >> get_user_info >> is_end_date_in_past
        is_user_found_using_perner >> rail.Label(
            'No') >> log_no_projectmanger_not_available >> has_parent_wbs
        is_project_manager_available >> rail.Label('Yes') >> is_end_date_in_past >> rail.Label(
            'Yes') >> log_end_date_in_past >> has_parent_wbs
        is_end_date_in_past >> rail.Label('No') >> can_enable_project_manager >> rail.Label('No') >> is_project_manager_contractor
        can_enable_project_manager >> rail.Label('Yes') >> enable_project_manager >> is_project_manager_contractor
        is_project_manager_contractor >> rail.Label(
            'Yes') >> is_division_in_australia
        is_project_manager_contractor >> rail.Label(
            'No') >> get_permission_sets_for_project_manager
        is_division_in_australia >> rail.Label(
            'No') >> log_project_manager_outside_australia >> has_parent_wbs
        is_division_in_australia >> rail.Label(
            'Yes') >> get_permission_sets_for_project_manager
        get_permission_sets_for_project_manager >> check_for_required_permissions >> has_required_permissions
        has_required_permissions >> rail.Label('Yes') >> has_parent_wbs
        create_project_copy_batch >> project_copy_batch_group_entry
        has_required_permissions >> rail.Label(
            'No') >> assign_permissionset_to_projectmanager >> assign_policyDataAccessScopes_to_projectmanager
        assign_policyDataAccessScopes_to_projectmanager >> has_parent_wbs
        has_parent_wbs >> rail.Label(
            'No') >> get_project_info_based_on_wbs_element
        has_parent_wbs >> rail.Label(
            'Yes') >> get_project_info_on_parentwbs >> does_parentwbs_exist >> rail.Label('Yes') >> get_project_info_based_on_wbs_element
        get_project_info_based_on_wbs_element >> can_copy_project_from_parent
        does_parentwbs_exist >> rail.Label('No') >> log_parent_doesnt_exist >> get_project_info_based_on_wbs_element
        can_copy_project_from_parent >> rail.Label(
            'Yes') >> create_project_copy_batch
        create_project_copy_batch >> project_copy_batch_group_entry
        project_copy_batch_group_exit >> get_projectcopy_batch_results >> is_parent_c1 >> rail.Label('No') >> create_projectorapply_modifications
        is_parent_c1 >> rail.Label('Yes') >> get_all_labour_types >> create_projectorapply_modifications
        can_copy_project_from_parent >> rail.Label(
            'No') >> create_projectorapply_modifications >> can_update_blob >> rail.Label('No') >> update_division
        can_update_blob >> rail.Label('Yes') >> process_blob_update >> wait_for_process_blob_update >> update_division
        update_division >> put_eligibleprojectteammember_dataaccessscopes >> can_update_tnm_indicator_oef_value

        can_update_tnm_indicator_oef_value >> rail.Label(
            'No') >> has_client
        can_update_tnm_indicator_oef_value >> rail.Label(
            'Yes') >> update_tnm_indicator_oef_value >> has_client

        has_client >> rail.Label(
            'No') >> remove_client >> can_be_potential_parent
        has_client >> rail.Label(
            'Yes') >> get_client_info >> if_client_info_exists
        if_client_info_exists >> rail.Label(
            'No') >> log_client_info_not_found >> can_be_potential_parent
        if_client_info_exists >> rail.Label(
            'Yes') >> update_client >> can_be_potential_parent
        can_be_potential_parent >> rail.Label(
            'No') >> get_all_exception_logs >> log_completion >> catch_and_log_errors
        can_be_potential_parent >> rail.Label('Yes') >> get_data_of_child_wbs >> has_childs_wbs >> rail.Label(
            'No') >> get_all_exception_logs >> log_completion
        has_childs_wbs >> rail.Label(
            'Yes') >> update_iwo_wbs_element >> process_child_wbs >> get_all_exception_logs >> log_completion

    return dag


rail.for_each_instance(create_child_dag_wbs)
