from datetime import timedelta
import json
import rail
from dxctechnology.compass_iwo_details_v3.utils import request_payload
from dxctechnology.compass_iwo_details_v3.utils import response_filter
from dxctechnology.compass_iwo_details_v3.utils import python_callable_method
from dxctechnology.compass_iwo_details_v3.utils import custom_methods
from airflow.models import Variable

null = None

# pylint: disable=too-many-statements


def create_iwo_details_wbs_update_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_iwo_wbs_update_child_{config.dag_id_postfix}',
        description=f'DXC_COMPASS_IWO_WBS_Update_Child - V3.0 {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_child_project_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_child_project_details',
            end_task='catch_and_log_errors',
        )

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        column_names = ['taskname', 'uri', 'enabled', 'task_fullpath', 'parent_present',
                        'parent_task_name', 'parent_task_uri', 'levels', 'code', 'start_date', 'end_date']

        get_child_project_details = rail.RepliconServiceOperator(
            task_id='get_child_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=request_payload.get_project_details_payload,
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails']
        )

        does_project_exist = rail.IfOperator(
            task_id='does_project_exist',
            test='{{ result("get_child_project_details") is not none  }}',
            yes_task='is_parent_company_code_present',
            no_task='log_required_wbs_doesnot_exist',
        )

        log_required_wbs_doesnot_exist = rail.WriteLogOperator(
            task_id='log_required_wbs_doesnot_exist',
            message='Required WBS not available in Replicon',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'employeeid': '',
                'action': 'update',
                'status': 'Exception',
                'details': 'Required WBS not available in Replicon',
            }
        )

        get_reprocess_update_log = rail.CreateLogOperator(
            task_id='get_reprocess_update_log',
            tenant_wide_name=f'{config.reprocess_update_log}_{config.dag_id_postfix}',
            existing_log_mode='append',
        )

        write_dagconf_to_update_log = rail.WriteLogOperator(
            task_id='write_dagconf_to_update_log',
            log='{{ result("get_reprocess_update_log") }}',
            message='{{ dag_run.conf.wbs }} queued for reprocess',
            properties=python_callable_method.get_process_iwo_wbs_update_conf
        )

        is_parent_company_code_present = rail.IfOperator(
            task_id='is_parent_company_code_present',
            test=lambda dag_run: bool(dag_run.conf['parentcompanycode']),
            yes_task='build_oef_for_parentcompanycode',
            no_task='add_company_code_is_blank',
        )

        add_company_code_is_blank = rail.PythonOperator(
            task_id='add_company_code_is_blank',
            python_callable=lambda: 'Provided Parent company code is blank or null'
        )

        build_oef_for_parentcompanycode = rail.PythonOperator(
            task_id='build_oef_for_parentcompanycode',
            python_callable=lambda dag_run: python_callable_method.get_oef_object(
                uri=dag_run.conf['parentcompanycodeuri'],
                textvalue=dag_run.conf['parentcompanycode']),
        )

        is_parent_details_present = rail.IfOperator(
            task_id='is_parent_details_present',
            test=lambda dag_run: bool(dag_run.conf['parentproject'] or dag_run.conf['parentwbs']
                                      or dag_run.conf['parentserviceorder']),
            yes_task='build_parent_details_oef',
            no_task='log_parent_details_blank',
        )

        log_parent_details_blank = rail.WriteLogOperator(
            task_id='log_parent_details_blank',
            message="The Provided \'Parent Project\', \'Parent WBS\' and \'Parent Service Order\' values are blank or null",
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'employeeid': '',
                'action': 'update',
                'status': 'Exception',
                'details': "The Provided \'Parent Project\', \'Parent WBS\' and \'Parent Service Order\' values are blank or null",
            }
        )

        get_parent_project_details = rail.RepliconServiceOperator(
            task_id='get_parent_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=request_payload.get_parent_project_details_payload,
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails']
        )

        get_companycode_list = rail.RepliconServiceOperator(
            task_id='get_companycode_list',
            endpoint='/services/DivisionListService1.svc/GetData',
            data=request_payload.get_companycode_details,
            response_filter=response_filter.division_code_check

        )

        build_parent_details_oef = rail.EmptyOperator(
            task_id='build_parent_details_oef',
        )

        build_all_oef = rail.PythonOperator(
            task_id='build_all_oef',
            python_callable=python_callable_method.get_all_oef_payload
        )

        update_oef_fields = rail.RepliconServiceOperator(
            task_id='update_oef_fields',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.get_update_oef_payload,
        )

        can_inherit_psa_flag = rail.IfOperator(
            task_id = 'can_inherit_psa_flag',
            test = lambda: bool(rail.result(
                'get_parent_project_details') and rail.result(
                'get_parent_project_details')['uri']),
            yes_task= 'inherit_psa_flag_from_parent',
            no_task = 'is_tasks_list_present'
        )

        inherit_psa_flag_from_parent = rail.RepliconServiceOperator(
            task_id='inherit_psa_flag_from_parent',
             endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_inherit_psa_flag_payload
        )

        is_tasks_list_present = rail.IfOperator(
            task_id='is_tasks_list_present',
            test=lambda dag_run: len(dag_run.conf['taskdetails']) > 0,
            yes_task='create_valid_task_list',
            no_task='finish',
        )

        create_valid_task_list = rail.PythonOperator(
            task_id='create_valid_task_list',
            python_callable=python_callable_method.get_valid_tasks_list
        )

        get_all_project_team_assignment = rail.RepliconServiceOperator(
            task_id='get_all_project_team_assignment',
            endpoint='/services/ProjectService1.svc/GetAllProjectTeamMemberDetails2',
            data=request_payload.get_all_project_team_assignment_payload,
            response_filter=response_filter.map_resource_assignment_list
        )

        is_user_uri_present = rail.IfOperator(
            task_id='is_user_uri_present',
            test=lambda: len(rail.result(
                'create_valid_task_list')['valid_tasks']) > 0,
            yes_task='update_project_team_members_assignment',
            no_task='get_all_project_team_assignment_after_update',
        )

        update_project_team_members_assignment = rail.RepliconServiceOperator(
            task_id='update_project_team_members_assignment',
            endpoint='/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
            data=request_payload.get_bulk_update_team_members_payload
        )

        get_all_project_team_assignment_after_update = rail.RepliconServiceOperator(
            task_id='get_all_project_team_assignment_after_update',
            endpoint='/services/ProjectService1.svc/GetAllProjectTeamMemberDetails2',
            data=request_payload.get_all_project_team_assignment_payload,
            response_filter=response_filter.map_resource_assignment_list_after_update
        )

        create_assignment_date = rail.PythonOperator(
            task_id='create_assignment_date',
            python_callable=python_callable_method.get_assignment_date,
        )

        update_project_team_members_assignment_daterange = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_project_team_members_assignment_daterange',
            endpoint='/services/ProjectService1.svc/UpdateProjectTeamMemberAssignmentDateRange',
            items='{{ result("create_assignment_date") | to_json}}',
            data=request_payload.get_project_team_members_assignment_daterange
        )

        does_parent_project_uri_exist = rail.IfOperator(
            task_id='does_parent_project_uri_exist',
            test=lambda: bool(rail.result(
                'get_parent_project_details') and rail.result(
                'get_parent_project_details')['uri']),
            yes_task='update_allow_timeentry_against_taskonly',
            no_task='add_parent_project_not_present',
        )

        update_allow_timeentry_against_taskonly = rail.RepliconServiceOperator(
            task_id='update_allow_timeentry_against_taskonly',
            endpoint='/services/ProjectService1.svc/UpdateAllowTimeEntryAgainstTasksOnly',
            data=request_payload.get_allow_timeentry_against_taskonly_payload
        )

        get_all_tasks_of_parent_project = rail.RepliconServiceOperator(
            task_id='get_all_tasks_of_parent_project',
            endpoint='/services/TaskListService1.svc/GetData',
            data=lambda: request_payload.get_all_project_tasks_payload(
                rail.result("get_parent_project_details")['uri']),
            response_filter=response_filter.all_task_response_filter
        )

        dummy_get_all_tasks_of_parent_project = rail.EmptyOperator(
            task_id = "dummy_get_all_tasks_of_parent_project"
        )

        does_parent_project_tasks_exist = rail.IfOperator(
            task_id='does_parent_project_tasks_exist',
            test=lambda: len(rail.result(
                'get_all_tasks_of_parent_project')) > 0,
            yes_task='parent_project_task_collection',
            no_task='get_details_for_parentproject_billing_rates',
        )

        get_all_gsap_task_for_parent = rail.RepliconServiceOperator(
            task_id = "get_all_gsap_task_for_parent",
            endpoint="/services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/GetPageOfProjectDependentTimeEntryObjectExtensionTags",
            data={
                    "page": "1",
                    "pageSize": "100000",
                    "textSearch": null,
                    "project": {
                        "uri": "{{result('get_parent_project_details').uri}}",
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

        has_any_gsap_task = rail.IfOperator(
            task_id = "has_any_gsap_task",
            test="{{ result('get_all_gsap_task_for_parent') | is_truthy}}",
            yes_task="get_all_gsap_task_for_child",
            no_task="dummy_get_all_tasks_of_parent_project"
        )

        get_all_gsap_task_for_child = rail.RepliconServiceOperator(
            task_id= "get_all_gsap_task_for_child",
            endpoint = "/services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/GetPageOfProjectDependentTimeEntryObjectExtensionTags",
            data={
                "page": "1",
                "pageSize": "100000",
                "textSearch": null,
                "project": {
                    "uri": "{{result('get_child_project_details').uri}}",
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

        def add_project_dependant_field(tag):
            return {
                "target": {
                    "uri":  tag['tag']['uri'],
                    "tagName": null
                },
                "isEnabled": tag['isEnabled'],
                "dateRange": tag['dateRange']
            }
        def get_gsap_task_to_add_callable():
            def get_task_to_add():
                parent_wbs_gsap_tasks = rail.result("get_all_gsap_task_for_parent")
                child_wbs_gsap_tasks = rail.result("get_all_gsap_task_for_child")

                if not child_wbs_gsap_tasks:
                    return list(map(add_project_dependant_field, parent_wbs_gsap_tasks))

                child_tasks = list(map(lambda child_tasks: child_tasks['tag']['uri'], child_wbs_gsap_tasks))
                return list(map(add_project_dependant_field ,filter(lambda parent_task: parent_task['tag']['uri'] not in child_tasks, parent_wbs_gsap_tasks)))

            return {
                "project": {
                    "uri": rail.result('get_child_project_details')['uri']
                },
                "objectExtensionFieldTags": {
                    "tagsToAdd": get_task_to_add(),
                    "tagsToRemove": []
                }
            }

        get_gsap_task_to_add = rail.PythonOperator(
            task_id = "get_gsap_task_to_add",
            python_callable= get_gsap_task_to_add_callable
        )

        has_any_gsap_task_to_add = rail.IfOperator(
            task_id = "has_any_gsap_task_to_add",
            test= "{{result('get_gsap_task_to_add').objectExtensionFieldTags.tagsToAdd | is_truthy}}",
            yes_task="add_gsap_task_to_child",
            no_task="dummy_get_all_tasks_of_parent_project"
        )

        add_gsap_task_to_child = rail.RepliconServiceOperator(
            task_id = "add_gsap_task_to_child",
            endpoint="/services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/ApplyModificationsForProjectTimeEntryDependentObjectExtensionTags",
            data= '{{result("get_gsap_task_to_add") | to_json}}'
        )

        parent_project_task_collection = rail.CreateCollectionOperator(
            task_id='parent_project_task_collection',
            source='{{result("get_all_tasks_of_parent_project") | to_json }}',
            columns=column_names,
            name='newtasklist'
        )

        get_all_tasks_of_child_project = rail.RepliconServiceOperator(
            task_id='get_all_tasks_of_child_project',
            endpoint='/services/TaskListService1.svc/GetData',
            data=lambda: request_payload.get_all_project_tasks_payload(
                rail.result("get_child_project_details")['uri']),
            response_filter=response_filter.all_task_response_filter
        )

        child_project_task_collection = rail.CreateCollectionOperator(
            task_id='child_project_task_collection',
            source='{{result("get_all_tasks_of_child_project") | to_json }}',
            columns=column_names,
            name='existingtasklist'
        )

        query_tasks_not_present_in_child_project = rail.QueryCollectionOperator(
            task_id='query_tasks_not_present_in_child_project',
            query="""SELECT * FROM newtasklist WHERE task_fullpath NOT IN (SELECT DISTINCT task_fullpath FROM existingtasklist)"""
        )

        does_tasks_list_to_be_created = rail.IfOperator(
            task_id='does_tasks_list_to_be_created',
            test='{{ result("query_tasks_not_present_in_child_project", "length") > 0}}',
            yes_task='get_all_unique_levels',
            no_task='get_details_for_parentproject_billing_rates'
        )

        get_all_unique_levels = rail.QueryCollectionOperator(
            task_id="get_all_unique_levels",
            query="SELECT DISTINCT levels FROM query_tasks_not_present_in_child_project ORDER BY levels ASC"
        )

        process_task_by_level = rail.TriggerDagRunForEachItemOperator(
            task_id="process_task_by_level",
            trigger_dag_id=f"dxctechnology_compass_iwo_process_tasks_by_level_child_{config.dag_id_postfix}",
            items="{{result('get_all_unique_levels')}}",
            conf=lambda item, dag_run: {
                "level": item['levels'],
                "file_name": dag_run.conf['file_name'],
                "parent_wbs": dag_run.conf['parentwbs'],
                "parent_wbs_uri": rail.result('get_parent_project_details')["uri"],
                "processing_wbs": dag_run.conf["wbs"],
                "processing_wbs_uri": rail.result('get_child_project_details')["uri"],
                "resource_list": request_payload.get_user_assignment_uris(),
                "task_type": dag_run.conf['task_type']
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_tasks_by_level = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_tasks_by_level",
            dag_runs="{{result('process_task_by_level')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_details_for_parentproject_billing_rates = rail.RepliconServiceOperator(
            task_id='get_details_for_parentproject_billing_rates',
            endpoint='/services/ImportService1.svc/BulkGetProjects2',
            data=request_payload.get_details_for_parentproject_billing_rates_payload,
            response_filter=response_filter.map_billing_rates_name_list
        )

        does_billing_rates_exist = rail.IfOperator(
            task_id='does_billing_rates_exist',
            test=lambda: len(rail.result(
                'get_details_for_parentproject_billing_rates')) > 0,
            yes_task='update_billing_rates',
            no_task='check_item_category_present',
        )

        update_billing_rates = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_billing_rates',
            endpoint='/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers',
            items=lambda: rail.result(
                'get_details_for_parentproject_billing_rates'),
            data=request_payload.get_update_billing_rates_payload
        )

        check_item_category_present = rail.IfOperator(
            task_id='check_item_category_present',
            test=custom_methods.check_item_category_present,
            yes_task='update_item_category',
            no_task='is_parent_c1_SO'
        )

        update_item_category = rail.RepliconServiceOperator(
            task_id='update_item_category',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.get_update_item_category_payload
        )

        is_parent_c1_SO = rail.IfOperator(
            task_id='is_parent_c1_SO',
            test=custom_methods.check_parent_c1_SO,
            yes_task='update_billing_rates_on_project_team_member',
            no_task='check_parent_uri_present'
        )

        update_billing_rates_on_project_team_member = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_billing_rates_on_project_team_member',
            endpoint='/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime3',
            items=lambda: rail.result('create_valid_task_list')['valid_tasks'],
            data=lambda item:{
                    "projectTeamMemberBillingRate": {
                        "projectUri": rail.result("get_child_project_details")['uri'],
                        "resourceUri": item['useruri'],
                        "billingRateUris": list(set(list(map(lambda x:x['billingRate']['uri'],rail.result("get_details_for_parentproject_billing_rates"))))),
                        "billingRateCopyOptionUri": "urn:replicon:billing-rate-copy-option:do-not-copy-billing-rates-from-client",
                        "defaultBillingRateUri": null
                    }
                }
        )

        add_parent_project_not_present = rail.PythonOperator(
            task_id='add_parent_project_not_present',
            python_callable=lambda:
                'Task and Labour Types Assignment skipped as Parentwbs or SO received is not present in Replicon'
        )

        check_parent_uri_present = rail.IfOperator(
            task_id='check_parent_uri_present',
            test=lambda: bool(rail.result('get_parent_project_details') and rail.result(
                'get_parent_project_details')['uri']),
            yes_task='process_iwo_blob_update',
            no_task='finish'
        )

        process_iwo_blob_update = rail.TriggerDagRunForEachItemOperator(
            task_id='process_iwo_blob_update',
            retries=0,
            items=[0],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_compass_iwo_blob_update_child_{config.dag_id_postfix}',
            conf=request_payload.get_iwo_blob_update,
        )

        wait_for_process_iwo_blob_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_iwo_blob_update',
            dag_runs='{{ result("process_iwo_blob_update") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_exception_messages = rail.PythonOperator(
            task_id='get_exception_messages',
            python_callable=python_callable_method.get_exception_messages
        )

        log_exception_or_success = rail.WriteLogOperator(
            task_id='log_exception_or_success',
            message='\
                {%- if result("get_exception_messages") | is_truthy -%} \
                    WBS Partialy updated - {{ result("get_exception_messages") }}\
                {%- else -%} \
                    WBS Updated successfully \
                {%- endif -%}',
            items='{{ dag_run.conf.taskdetails | to_json }}',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'employeeid': '{{ item.employeeid }}',
                'action': 'update',
                'status': '\
                    {%- if result("get_exception_messages") | is_truthy -%} \
                         Exception\
                    {%- else -%} \
                         Success\
                    {%- endif -%}',
                'details': '\
                    {%- if result("get_exception_messages") | is_truthy -%} \
                        WBS Partialy updated - {{ result("get_exception_messages") }}\
                    {%- else -%} \
                        WBS Updated successfully \
                    {%- endif -%}',
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'employeeid': '',
                'status': 'Error',
                # pylint: disable=line-too-long
                'details': '{{ get_error_message() }}',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'wbs ': '{{ dag_run.conf.wbs }}',
                'parentcompanycode': '{{ dag_run.conf.parentcompanycode }}',
                'parentwbs': '{{ dag_run.conf.parentwbs }}',
                'parentserviceorder': '{{ dag_run.conf.parentserviceorder }}',
                'parentproject': '{{ dag_run.conf.parentproject }}',
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_child_project_details

        get_child_project_details >> does_project_exist

        does_project_exist >> rail.Label(
            'Yes') >> is_parent_company_code_present
        does_project_exist >> rail.Label(
            'No') >> log_required_wbs_doesnot_exist >> get_reprocess_update_log >> write_dagconf_to_update_log >> catch_and_log_errors

        is_parent_company_code_present >> rail.Label(
            'Yes') >> build_oef_for_parentcompanycode >> is_parent_details_present
        is_parent_company_code_present >> rail.Label(
            'No') >> add_company_code_is_blank >> is_parent_details_present

        is_parent_details_present >> rail.Label(
            'Yes') >> build_parent_details_oef >> get_parent_project_details

        get_parent_project_details >> get_companycode_list >> build_all_oef >> update_oef_fields >> can_inherit_psa_flag

        can_inherit_psa_flag >> rail.Label(
            "Yes") >> inherit_psa_flag_from_parent >> is_tasks_list_present

        can_inherit_psa_flag >> rail.Label(
            "No") >> is_tasks_list_present

        is_parent_details_present >> rail.Label(
            'No') >> log_parent_details_blank >> get_parent_project_details

        is_tasks_list_present >> rail.Label(
            'Yes') >> create_valid_task_list >> get_all_project_team_assignment >> is_user_uri_present
        is_tasks_list_present >> rail.Label(
            'No') >> finish

        is_user_uri_present >> rail.Label(
            'Yes') >> update_project_team_members_assignment >> get_all_project_team_assignment_after_update
        is_user_uri_present >> rail.Label(
            'No') >> get_all_project_team_assignment_after_update >> create_assignment_date \
            >> update_project_team_members_assignment_daterange >> does_parent_project_uri_exist

        does_parent_project_uri_exist >> rail.Label(
            'Yes') >> update_allow_timeentry_against_taskonly >> get_all_tasks_of_parent_project
        does_parent_project_uri_exist >> rail.Label(
            'No') >> add_parent_project_not_present >> finish

        does_parent_project_tasks_exist >> rail.Label(
            'Yes') >> parent_project_task_collection >> get_all_tasks_of_child_project >> child_project_task_collection \
            >> query_tasks_not_present_in_child_project >> does_tasks_list_to_be_created
        does_parent_project_tasks_exist >> rail.Label(
            'No') >> get_details_for_parentproject_billing_rates

        get_all_tasks_of_parent_project >> get_all_gsap_task_for_parent >> has_any_gsap_task >> rail.Label("Yes")\
            >> get_all_gsap_task_for_child >> get_gsap_task_to_add >> has_any_gsap_task_to_add >> rail.Label("Yes")\
                >>add_gsap_task_to_child >> dummy_get_all_tasks_of_parent_project >> does_parent_project_tasks_exist
        has_any_gsap_task >> rail.Label("No") >> dummy_get_all_tasks_of_parent_project
        has_any_gsap_task_to_add >> rail.Label("No") >> dummy_get_all_tasks_of_parent_project

        does_tasks_list_to_be_created >> rail.Label(
            'Yes') >> get_all_unique_levels >> process_task_by_level >> wait_for_process_tasks_by_level >> get_details_for_parentproject_billing_rates
        does_tasks_list_to_be_created >> rail.Label(
            'No') >> get_details_for_parentproject_billing_rates

        get_details_for_parentproject_billing_rates >> does_billing_rates_exist

        does_billing_rates_exist >> rail.Label(
            'Yes') >> update_billing_rates >> check_item_category_present
        does_billing_rates_exist >> rail.Label(
            'No') >> check_item_category_present

        check_item_category_present >> rail.Label(
            'Yes') >> update_item_category >> is_parent_c1_SO
        check_item_category_present >> rail.Label(
            'No') >> is_parent_c1_SO

        is_parent_c1_SO >> rail.Label('Yes') >> update_billing_rates_on_project_team_member >> check_parent_uri_present
        is_parent_c1_SO >> rail.Label('No') >> check_parent_uri_present

        check_parent_uri_present >> rail.Label(
            'Yes') >> process_iwo_blob_update >> wait_for_process_iwo_blob_update >> finish
        check_parent_uri_present >> rail.Label(
            'No') >> finish

        finish >> get_exception_messages >> log_exception_or_success >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_iwo_details_wbs_update_child_dag)