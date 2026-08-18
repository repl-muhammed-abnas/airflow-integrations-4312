from datetime import timedelta, datetime
import itertools
from airflow.models import Variable
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.initial_setup_dag_id,
        description=f'{config.company_key} Does the initial setup for ComputerEase - Replicon Integration',
        company_key=config.company_key,
        max_active_runs=config.max_active_runs,
        multi_tenant=True
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='update_last_run_initial',
            end_task='log_dagrun_details_to_table',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        update_last_run_initial = rail.PythonOperator(
            task_id='update_last_run_initial',
            python_callable=lambda dag_run: Variable.set(
                f'{config.initial_setup_last_run_var}_{dag_run.conf["company_key"]}',
                datetime.now().isoformat()
            ) or {}
        )

        get_user_and_project_oefs = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_user_and_project_oefs",
            items=['user', 'project'],
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda item: {
                "bindingContextUri": f"urn:replicon:object-type:{item}"
            },
            all_result_data_handler=lambda response: list(itertools.chain(*response))
        )

        create_oefs_list = rail.SetVariableOperator(
            task_id='create_oefs_list',
            name='oefs_list',
            append=False,
            value=[]
        )

        def filter_oefs(oefs, oef_type):
            return list(filter(lambda oef: oef['type'] == oef_type, oefs))

        foreach_text_oef_to_configure = rail.ForEachOperator(
            task_id='foreach_text_oef_to_configure',
            items=filter_oefs(config.oefs, 'text'),
            start_task='if_text_oef_not_present',
            end_task='foreach_text_oef_to_configure_end'
        )

        if_text_oef_not_present = rail.IfOperator(
            task_id='if_text_oef_not_present',
            test=lambda: not rail.find_first_by_attr_and_get_attr(rail.result(
                'get_user_and_project_oefs'), 'name', rail.result('foreach_text_oef_to_configure')['name']),
            yes_task='create_text_oef',
            no_task='add_oef_to_list'
        )

        create_text_oef = rail.RepliconServiceOperator(
            task_id='create_text_oef',
            endpoint='services/ObjectExtensionTextDefinitionService1.svc/PutObjectExtensionTextDefinition',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "objectExtensionTextDefinition": {
                    "target": {
                        "name": rail.result('foreach_text_oef_to_configure')['name'],
                        "uri": null
                    },
                    "name": rail.result('foreach_text_oef_to_configure')['name'],
                    "description": rail.result('foreach_text_oef_to_configure')['name']
                }
            }
        )

        add_oef_to_list = rail.SetVariableOperator(
            task_id='add_oef_to_list',
            name='oefs_list',
            append=True,
            value=lambda: {
                'name': rail.result('foreach_text_oef_to_configure')['name'],
                'bind': rail.result('foreach_text_oef_to_configure')['bind'],
                'uri': (rail.find_first_by_attr_and_get_attr(rail.result('get_user_and_project_oefs'), 'name', rail.result(
                    'foreach_text_oef_to_configure')['name']) or rail.result('create_text_oef')).get('uri')
            }
        )

        foreach_text_oef_to_configure_end = rail.EmptyOperator(
            task_id='foreach_text_oef_to_configure_end'
        )

        foreach_tag_oef_to_configure = rail.ForEachOperator(
            task_id='foreach_tag_oef_to_configure',
            items=filter_oefs(config.oefs, 'dropdown'),
            start_task='if_tag_oef_not_present',
            end_task='foreach_tag_oef_to_configure_end'
        )

        if_tag_oef_not_present = rail.IfOperator(
            task_id='if_tag_oef_not_present',
            test=lambda: not rail.find_first_by_attr_and_get_attr(rail.result(
                'get_user_and_project_oefs'), 'name', rail.result('foreach_tag_oef_to_configure')['name']),
            yes_task='create_tag_oef',
            no_task='if_options_present'
        )

        create_tag_oef = rail.RepliconServiceOperator(
            task_id='create_tag_oef',
            endpoint='services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTagDefinition',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "objectExtensionTagDefinition": {
                    "target": {
                        "uri": null,
                        "name": rail.result('foreach_tag_oef_to_configure')['name']
                    },
                    "name": rail.result('foreach_tag_oef_to_configure')['name'],
                    "code": rail.result('foreach_tag_oef_to_configure')['name'],
                    "description": rail.result('foreach_tag_oef_to_configure')['name'],
                    "tags": []
                }
            }
        )

        if_options_present = rail.IfOperator(
            task_id='if_options_present',
            test=lambda: rail.result('foreach_tag_oef_to_configure').get('options'),
            yes_task='trigger_tag_options_update',
            no_task='add_oef_in_list'
        )

        trigger_tag_options_update = rail.TriggerDagRunOperator(
            task_id='trigger_tag_options_update',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.tag_oef_options_update_child_dag_id,
            conf=lambda dag_run: {
                'options': rail.result('foreach_tag_oef_to_configure')['options'],
                'definition_uri': (rail.find_first_by_attr_and_get_attr(rail.result('get_user_and_project_oefs'), 'name', rail.result(
                    'foreach_tag_oef_to_configure')['name']) or rail.result('create_tag_oef')).get('uri'),
                'oef_id': rail.result('foreach_tag_oef_to_configure')['id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id'],
                'computerease_conn_id': dag_run.conf['computerease_conn_id'],
                'company_key': dag_run.conf['company_key']
            }
        )

        wait_for_tag_options_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_tag_options_update',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_tag_options_update") }}'
        )

        add_oef_in_list = rail.SetVariableOperator(
            task_id='add_oef_in_list',
            name='oefs_list',
            append=True,
            value=lambda: {
                'name': rail.result('foreach_tag_oef_to_configure')['name'],
                'bind': rail.result('foreach_tag_oef_to_configure')['bind'],
                'uri': (rail.find_first_by_attr_and_get_attr(rail.result('get_user_and_project_oefs'), 'name', rail.result(
                    'foreach_tag_oef_to_configure')['name']) or rail.result('create_tag_oef')).get('uri')
            }
        )

        foreach_tag_oef_to_configure_end = rail.EmptyOperator(
            task_id='foreach_tag_oef_to_configure_end'
        )

        foreach_number_oef_to_configure = rail.ForEachOperator(
            task_id='foreach_number_oef_to_configure',
            items=filter_oefs(config.oefs, 'number'),
            start_task='if_number_oef_not_present',
            end_task='foreach_number_oef_to_configure_end'
        )

        if_number_oef_not_present = rail.IfOperator(
            task_id='if_number_oef_not_present',
            test=lambda: not rail.find_first_by_attr_and_get_attr(rail.result(
                'get_user_and_project_oefs'), 'name', rail.result('foreach_number_oef_to_configure')['name']),
            yes_task='create_number_oef',
            no_task='add_number_oef_to_list'
        )

        create_number_oef = rail.RepliconServiceOperator(
            task_id='create_number_oef',
            endpoint='services/ObjectExtensionNumericDefinitionService1.svc/PutObjectExtensionNumericDefinition',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "objectExtensionNumericDefinition": {
                    "target": {
                        "uri": null,
                        "name": rail.result('foreach_number_oef_to_configure')['name']
                    },
                    "name": rail.result('foreach_number_oef_to_configure')['name'],
                    "description": rail.result('foreach_number_oef_to_configure')['name']
                }
            }
        )

        add_number_oef_to_list = rail.SetVariableOperator(
            task_id='add_number_oef_to_list',
            name='oefs_list',
            append=True,
            value=lambda: {
                'name': rail.result('foreach_number_oef_to_configure')['name'],
                'bind': rail.result('foreach_number_oef_to_configure')['bind'],
                'uri': (rail.find_first_by_attr_and_get_attr(rail.result('get_user_and_project_oefs'), 'name', rail.result(
                    'foreach_number_oef_to_configure')['name']) or rail.result('create_number_oef')).get('uri')
            }
        )

        foreach_number_oef_to_configure_end = rail.EmptyOperator(
            task_id='foreach_number_oef_to_configure_end'
        )

        bind_each_oef = rail.ForEachOperator(
            task_id='bind_each_oef',
            items=lambda: rail.get_dag_run_var('oefs_list'),
            start_task='if_bind_to_user',
            end_task='bind_each_oef_end'
        )

        if_bind_to_user = rail.IfOperator(
            task_id='if_bind_to_user',
            test=lambda: 'user' in rail.result('bind_each_oef')['bind'],
            yes_task='bind_oef_to_user',
            no_task='if_bind_to_project'
        )

        bind_oef_to_user = rail.RepliconServiceOperator(
            task_id='bind_oef_to_user',
            endpoint='services/ObjectExtensionService1.svc/BindObjectExtensionField',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "objectExtensionDefinitionUri": rail.result('bind_each_oef')['uri'],
                "bindingContextUri": "urn:replicon:object-type:user"
            }
        )

        if_bind_to_project = rail.IfOperator(
            task_id='if_bind_to_project',
            test=lambda: 'project' in rail.result('bind_each_oef')['bind'],
            yes_task='bind_oef_to_project',
            no_task='if_bind_to_timesheet'
        )

        bind_oef_to_project = rail.RepliconServiceOperator(
            task_id='bind_oef_to_project',
            endpoint='services/ObjectExtensionService1.svc/BindObjectExtensionField',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "objectExtensionDefinitionUri": rail.result('bind_each_oef')['uri'],
                "bindingContextUri": "urn:replicon:object-type:project"
            }
        )

        if_bind_to_timesheet = rail.IfOperator(
            task_id='if_bind_to_timesheet',
            test=lambda: 'timesheet' in rail.result('bind_each_oef')['bind'],
            yes_task='bind_oef_to_timesheet',
            no_task='bind_each_oef_end'
        )

        bind_oef_to_timesheet = rail.RepliconServiceOperator(
            task_id='bind_oef_to_timesheet',
            endpoint='services/ObjectExtensionService1.svc/BindObjectExtensionField',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "objectExtensionDefinitionUri": rail.result('bind_each_oef')['uri'],
                "bindingContextUri": "urn:replicon:object-type:timesheet"
            }
        )

        bind_oef_to_time_entry = rail.RepliconServiceOperator(
            task_id='bind_oef_to_time_entry',
            endpoint='services/ObjectExtensionService1.svc/BindObjectExtensionField',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "objectExtensionDefinitionUri": rail.result('bind_each_oef')['uri'],
                "bindingContextUri": "urn:replicon:object-type:time-entry"
            }
        )

        bind_each_oef_end = rail.EmptyOperator(
            task_id='bind_each_oef_end'
        )

        foreach_groups_to_configure = rail.ForEachOperator(
            task_id='foreach_groups_to_configure',
            items=config.groups,
            start_task='put_group_systemsettings',
            end_task='foreach_groups_to_configure_end'
        )

        put_group_systemsettings = rail.RepliconServiceOperator(
            task_id='put_group_systemsettings',
            endpoint="{{result('foreach_groups_to_configure').renameendpoint}}",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "isEnabled": "true",
                "languageSettings": [
                    {
                        "language": {
                            "cultureCode": "en-US"
                        },
                        "singularName": rail.result('foreach_groups_to_configure')['name'],
                        "pluralName": rail.result('foreach_groups_to_configure')['plural']
                    }
                ]
            }
        )

        foreach_groups_to_configure_end = rail.EmptyOperator(
            task_id='foreach_groups_to_configure_end'
        )

        trigger_union_group_sync = rail.TriggerDagRunOperator(
            task_id='trigger_union_group_sync',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.union_group_child_dag_id,
            conf=lambda dag_run: {
                **rail.find_first_by_attr_and_get_attr(config.groups, 'id', 'union'),
                'computerease_conn_id': dag_run.conf['computerease_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id']
            }
        )

        wait_for_union_group_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_union_group_sync',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_union_group_sync") }}'
        )

        trigger_file_format_creation = rail.TriggerDagRunOperator(
            task_id='trigger_file_format_creation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.file_format_creation_child_dag_id,
            conf=lambda dag_run: {
                'computerease_conn_id': dag_run.conf['computerease_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id']
            }
        )

        wait_for_file_format_creation = rail.WaitForDagRunsSensor(
            task_id='wait_for_file_format_creation',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_file_format_creation") }}'
        )

        trigger_workerclass_options_update = rail.TriggerDagRunOperator(
            task_id='trigger_workerclass_options_update',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.worker_class_child_dag_id,
            conf=lambda dag_run: {
                'computerease_conn_id': dag_run.conf['computerease_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id']
            }
        )

        wait_for_workerclass_options_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_workerclass_options_update',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_workerclass_options_update") }}'
        )

        gather_child_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_dag_errors',
            dag_runs="{{ [result('trigger_union_group_sync'), result('trigger_file_format_creation'), result('trigger_workerclass_options_update'), result('trigger_tag_options_update')] }}",
            dagrun_task_id='catch_error',
            flatten=True
        )

        is_child_dag_error = rail.IfOperator(
            task_id='is_child_dag_error',
            test="{{ (get_task_state('gather_child_dag_errors') == 'success' and result('gather_child_dag_errors') | length > 0) }}",
            yes_task='fail_child_dag_error',
            no_task='log_dagrun_details_to_table'
        )

        fail_child_dag_error = rail.FailOperator(
            task_id='fail_child_dag_error',
            message="{{ result('gather_child_dag_errors') | map_to_attr('error') | join('|') }}"
        )

        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id='log_dagrun_details_to_table',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.hmac_secret
            },
            trigger_rule='all_done',
            company_key='{{ dag_run.conf.company_key }}',
            connector_name=config.provider,
            integration_type=config.workflow
        )

        update_last_run_if_failed = rail.PythonOperator(
            task_id='update_last_run_if_failed',
            trigger_rule='one_failed',
            python_callable=lambda dag_run: Variable.set(
                f'{config.initial_setup_last_run_var}_{dag_run.conf["company_key"]}',
                '1970-01-01T00:00:00'
            ) or {}
        )
        
        batch_task >> log_dagrun_details_to_table
        batch_task >> update_last_run_initial >> get_user_and_project_oefs >> create_oefs_list >> foreach_text_oef_to_configure >> if_text_oef_not_present
        if_text_oef_not_present >> rail.Label('Yes') >> create_text_oef >> add_oef_to_list >> foreach_text_oef_to_configure_end >> foreach_tag_oef_to_configure
        if_text_oef_not_present >> rail.Label('No') >> add_oef_to_list
        foreach_text_oef_to_configure >> foreach_text_oef_to_configure_end
        foreach_tag_oef_to_configure >> if_tag_oef_not_present
        if_tag_oef_not_present >> rail.Label('Yes') >> create_tag_oef >> if_options_present
        if_tag_oef_not_present >> rail.Label('No') >> if_options_present
        if_options_present >> rail.Label('Yes') >> trigger_tag_options_update >> wait_for_tag_options_update >> add_oef_in_list
        if_options_present >> rail.Label('No') >> add_oef_in_list >> foreach_tag_oef_to_configure_end
        foreach_tag_oef_to_configure >> foreach_tag_oef_to_configure_end >> foreach_number_oef_to_configure
        foreach_number_oef_to_configure >> if_number_oef_not_present
        if_number_oef_not_present >> rail.Label('Yes') >> create_number_oef >> add_number_oef_to_list
        if_number_oef_not_present >> rail.Label('No') >> add_number_oef_to_list >> foreach_number_oef_to_configure_end
        foreach_number_oef_to_configure >> foreach_number_oef_to_configure_end >> bind_each_oef
        bind_each_oef >> if_bind_to_user
        if_bind_to_user >> rail.Label('Yes') >> bind_oef_to_user >> if_bind_to_project
        if_bind_to_user >> rail.Label('No') >> if_bind_to_project
        if_bind_to_project >> rail.Label('Yes') >> bind_oef_to_project >> if_bind_to_timesheet
        if_bind_to_project >> rail.Label('No') >> if_bind_to_timesheet
        if_bind_to_timesheet >> rail.Label('Yes') >> bind_oef_to_timesheet >> bind_oef_to_time_entry >> bind_each_oef_end
        if_bind_to_timesheet >> rail.Label('No') >> bind_each_oef_end
        bind_each_oef >> bind_each_oef_end >> foreach_groups_to_configure >> put_group_systemsettings >> foreach_groups_to_configure_end

        foreach_groups_to_configure >> foreach_groups_to_configure_end >> trigger_union_group_sync >> wait_for_union_group_sync
        wait_for_union_group_sync >> trigger_file_format_creation >> wait_for_file_format_creation
        wait_for_file_format_creation >> trigger_workerclass_options_update >> wait_for_workerclass_options_update

        wait_for_workerclass_options_update >> gather_child_dag_errors >> is_child_dag_error

        is_child_dag_error >> rail.Label('Yes') >> fail_child_dag_error >> log_dagrun_details_to_table
        is_child_dag_error >> rail.Label('No') >> log_dagrun_details_to_table

        log_dagrun_details_to_table >> update_last_run_if_failed

        return dag


rail.for_each_instance(create_dag)
