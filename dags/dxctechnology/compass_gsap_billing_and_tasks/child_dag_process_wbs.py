from datetime import timedelta
import rail
from dxctechnology.compass_gsap_billing_and_tasks.load_attribute_parents import load_attribute_parents

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/compass_gsap_billing_and_tasks/config.py

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_gsap_billing_and_tasks_import_{config.sub_erp_name}_child_wbs',
        description=f'DXC COMPASS GSAP Billing and Tasks Child WBS - {config.sub_erp_name}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_concurrent_wbs_imports,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        load_project = rail.RepliconServiceOperator(
            task_id='load_project',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={"projects": [{"name": "{{ dag_run.conf.WBS }}"}]},
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                                          {"projectDetails": None}])[0]['projectDetails'],
        )

        does_project_exist = rail.IfOperator(
            task_id="does_project_exist",
            test="{{ result('load_project') is not none }}",
            yes_task="process_project",
            no_task="log_project_doesnt_exist",
        )

        process_project = rail.EmptyOperator(
            task_id="process_project",
        )

        log_project_doesnt_exist = rail.WriteLogOperator(
            task_id="log_project_doesnt_exist",
            message='"{{ dag_run.conf.WBS }}" is not present in Replicon',
            severity='Exception',
            properties={
                'WBS': '{{ dag_run.conf.WBS }}',
            }
        )

        load_team_members = rail.RepliconServiceOperator(
            task_id='load_team_members',
            endpoint='/services/ProjectService1.svc/GetAllProjectTeamMemberDetails',
            data={
                "projectUri": "{{ result('load_project').uri }}",
                "asOfDate": None},
            data_handler=lambda data: list(
                map(lambda assignment: assignment['resource']['uri'], data)),
        )

        update_taskrequired_oef = rail.RepliconServiceOperator(
            task_id="update_taskrequired_oef",
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data=lambda dag_run: {
                "objectUri": rail.result('load_project')['uri'],
                "value": {
                    "definition": {
                        "uri": dag_run.conf['DefinitionUri'],
                    },
                    "tag": None if not dag_run.conf['TagUri'] else {
                        'uri': dag_run.conf['TagUri']
                    }
                }
            }
        )

        load_all_tasks_from_replicon = rail.GetAllProjectTasksOperator(
            task_id='load_all_tasks_from_replicon',
            project_uri="{{ result('load_project').uri }}",
        )

        load_attr_parents_group_entry, load_attr_parents_group_exit = load_attribute_parents(
            config)

        all_data_loaded = rail.EmptyOperator(task_id='all_data_loaded')

        get_billing_keys_from_import = rail.QueryCollectionOperator(
            task_id='get_billing_keys_from_import',
            query='SELECT * FROM create_billing_keys_collection WHERE WBS = :wbs',
            query_params={
                "wbs": "{{ dag_run.conf.WBS }}"
            }
        )

        import_billing_keys = rail.TriggerDagRunForEachItemOperator(
            task_id='import_billing_keys',
            trigger_dag_id=f"dxctechnology_compass_gsap_billing_and_tasks_import_{config.sub_erp_name}_child_billingkey",
            items="{{ result('get_billing_keys_from_import') }}",
            execution_timeout=timedelta(days=14),
            conf=lambda item, dag_run: {
                'BillingKey': item,
                'BillingTasks': list(filter(lambda t: t['name'] == item['Name'], rail.result('load_all_tasks_from_replicon'))),
                'ProjectUri': rail.result('load_project')['uri'],
                'TaskTypeOptionUri': dag_run.conf['TaskTypeOptionUri'],
                "BillingKeyOptionValueUri": dag_run.conf['BillingKeyOptionValueUri'],
                'TaskTypeOptionValueUri': dag_run.conf['TaskTypeOptionValueUri'],
                'ImportTasks': rail.load_all_records(rail.result('get_tasks_from_import')),
                'AttributesParentTasks': rail.load_all_records(rail.result('merge_parent_tasks')),
                'ResourceUris': rail.result('load_team_members')
            },
            retries=0,
        )

        wait_for_import_billing_keys = rail.WaitForDagRunsSensor(
            task_id='wait_for_import_billing_keys',
            dag_runs='{{ result("import_billing_keys") }}',
            execution_timeout=timedelta(days=14),
        )

        get_tasks_from_import = rail.QueryCollectionOperator(
            task_id='get_tasks_from_import',
            query='SELECT * FROM create_task_collection WHERE WBS = :wbs',
            query_params={
                "wbs": "{{ dag_run.conf.WBS }}"
            }
        )

        has_tasks_to_import = rail.IfOperator(
            task_id="has_tasks_to_import",
            test="{{ result('get_tasks_from_import', 'length') > 0 }}",
            yes_task='load_updated_tasks_from_replicon',
            no_task='catch_and_log_errors',
        )

        load_updated_tasks_from_replicon = rail.GetAllProjectTasksOperator(
            task_id='load_updated_tasks_from_replicon',
            project_uri="{{ result('load_project').uri }}",
        )

        def is_custom_task_type(task, type_str):
            def is_matching_task_type(
                custom_field): return custom_field['customField']['name'] == 'Task Type' and custom_field['text'] == type_str
            return any(filter(is_matching_task_type, task['customFields']))

        def filter_to_gsap_billing_keys(wts_task):
            if wts_task and is_custom_task_type(wts_task, 'GSAP Billing Key'):
                return wts_task
            return None
        find_gsap_billing_key_tasks = rail.DataAdaptorOperator(
            task_id='find_gsap_billing_key_tasks',
            source="{{ result('load_updated_tasks_from_replicon') | to_json }}",
            data=filter_to_gsap_billing_keys
        )

        has_any_gsap_billing_keys = rail.IfOperator(
            task_id="has_any_gsap_billing_keys",
            test='{{ result("find_gsap_billing_key_tasks", "length") > 0 }}',
            yes_task='import_tasks',
            no_task='log_no_gsap_billing_key',
        )

        log_no_gsap_billing_key = rail.WriteLogOperator(
            task_id="log_no_gsap_billing_key",
            message='Billing keys are not available in the payload or in the WBS to be associated with the tasks.',
            severity='Exception',
            properties={
                'WBS': '{{ dag_run.conf.WBS }}',
            }
        )

        import_tasks = rail.TriggerDagRunForEachItemOperator(
            task_id='import_tasks',
            trigger_dag_id=f"dxctechnology_compass_gsap_billing_and_tasks_import_{config.sub_erp_name}_child_task",
            items="{{ result('get_tasks_from_import') }}",
            execution_timeout=timedelta(days=14),
            conf=lambda item, dag_run: {
                'Task': item,
                'ProjectUri': rail.result('load_project')['uri'],
                'TaskTypeOptionUri': dag_run.conf['TaskTypeOptionUri'],
                "BillingKeyOptionValueUri": dag_run.conf['BillingKeyOptionValueUri'],
                'TaskTypeOptionValueUri': dag_run.conf['TaskTypeOptionValueUri'],
                'AttributesParentTaskUri': rail.load_all_records(rail.result('merge_parent_tasks')),
                'ResourceUris': rail.result('load_team_members'),
                'BillingKeyTasks': rail.result("find_gsap_billing_key_tasks"),
                'ExistingTasks': list(filter(lambda t: t['name'] == item['TaskName'] and is_custom_task_type(t, 'GSAP Task'),
                                      rail.result('load_updated_tasks_from_replicon'))),
            },
            retries=0,
        )

        wait_for_import_tasks = rail.WaitForDagRunsSensor(
            task_id='wait_for_import_tasks',
            dag_runs='{{ result("import_tasks") }}',
            execution_timeout=timedelta(days=14),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties={
                'WBS': '{{ dag_run.conf.WBS }}',
            },
        )

        load_project >> does_project_exist >> rail.Label(
            "Yes") >> process_project
        process_project >> [
            load_team_members,
            load_attr_parents_group_entry,
            load_all_tasks_from_replicon,
            get_tasks_from_import,
            get_billing_keys_from_import]
        [load_team_members,
         load_attr_parents_group_exit,
         load_all_tasks_from_replicon,
         get_tasks_from_import,
         get_billing_keys_from_import] >> all_data_loaded
        all_data_loaded >> update_taskrequired_oef >> import_billing_keys >> wait_for_import_billing_keys >> has_tasks_to_import >> rail.Label("Yes") >> \
            load_updated_tasks_from_replicon >> find_gsap_billing_key_tasks >> has_any_gsap_billing_keys >> rail.Label(
                "Yes") >> import_tasks >> wait_for_import_tasks >> rail.Label("On error") >> catch_and_log_errors
        has_any_gsap_billing_keys >> rail.Label(
            "No") >> log_no_gsap_billing_key >> catch_and_log_errors
        does_project_exist >> rail.Label("No") >> log_project_doesnt_exist >> catch_and_log_errors
        has_tasks_to_import >> rail.Label("No") >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag_wbs)
