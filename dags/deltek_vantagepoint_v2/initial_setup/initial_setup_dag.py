from datetime import timedelta, datetime
import itertools
from airflow.models import Variable
import rail
from deltek_vantagepoint_v2.initial_setup.oef_mapper import get_oefs_with_required_name
from deltek_vantagepoint_v2.initial_setup.utils import get_expected_labor_code_level_count, build_combined_labor_code_options
null = None


# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.initial_setup_dag_id,
        description=f'{config.company_key} Does the initial setup for Vantagepoint - Replicon Integration',
        company_key=config.company_key,
        max_active_runs=config.max_active_runs,
        multi_tenant=True
    ) as dag:

        lc_combined_oef_name = getattr(config, 'timesheet_field_oef_name_for_lc', None)

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_and_project_oefs',
            end_task='should_log_history',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_and_project_oefs = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_user_and_project_oefs",
            items=['user', 'project', 'timesheet'],
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

        def upsert_tag_oef_data():
            item = rail.result('foreach_tag_oef_to_configure')
            replicon_oefs = rail.result('get_user_and_project_oefs')
            existing = rail.find_first_by_attr_and_get_attr(replicon_oefs, 'name', item['name'])
            if not existing and item.get('code'):
                existing = rail.find_first_by_attr_and_get_attr(replicon_oefs, 'code', item['code'])
            existing_uri = existing and existing.get('uri')
            definition = {
                "target": {"uri": existing_uri, "name": item['name']},
                "name": item['name'],
                "code": item.get('code', item['name']),
                "description": item['name']
            }
            if not existing_uri:
                definition["tags"] = []
            return {"objectExtensionTagDefinition": definition}

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
                    "code": rail.result('foreach_text_oef_to_configure').get('code', rail.result('foreach_text_oef_to_configure')['name']),
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

        get_cfg_format = rail.VantagepointAPIOperator(
            task_id='get_cfg_format',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/KeyCvt/CFGFormat',
            filters='?entity=LABCD',
            request_method='GET'
        )

        def resolve_runtime_oefs():
            dag_run = rail.get_current_context()['dag_run']
            custom_settings = dag_run.conf.get('customSettings', {})
            if 'M' in custom_settings and isinstance(custom_settings.get('M'), dict):
                custom_settings = custom_settings['M']
            labor_code_setting = custom_settings.get('laborCodeSetting', {})

            levels = []
            configure_labor_code = bool(labor_code_setting and labor_code_setting.get('configureLaborCode', False))
            cfg_format = rail.result('get_cfg_format')
            actual_level_count = cfg_format[0].get('LCLevels', 0) if cfg_format else 0

            if configure_labor_code:
                levels = labor_code_setting.get('levels', [])
                if len(levels) != actual_level_count:
                    raise RuntimeError(
                        f'Labor code level validation failed: '
                        f'Connector-UI configured {len(levels)} level(s) but '
                        f'Vantagepoint has {actual_level_count} level(s). '
                        f'Please update the labor code level settings on Connector-UI '
                        f'to match Vantagepoint configuration.'
                    )
            elif actual_level_count > 0:
                raise RuntimeError(
                    f'Labor code level validation failed: '
                    f'Connector UI labor code level count {len(levels)} '
                    f'does not match the labor code levels '
                    f'configured in Vantagepoint: {actual_level_count}. '
                    f'Please give the correct labor code configuration in Connector-UI '
                    f'to match Vantagepoint configuration.'
                )

            runtime_required = {oef['id']: oef['name'] for oef in config.oefs
                                if not oef['id'].startswith('laborcodelevel')
                                and oef['id'] != 'laborcodecombined'}
            for i, level_name in enumerate(levels):
                runtime_required[f'laborcodelevel{i + 1}'] = str(level_name)

            if lc_combined_oef_name:
                runtime_required['laborcodecombined'] = lc_combined_oef_name

            return get_oefs_with_required_name(runtime_required)

        resolve_oefs = rail.PythonOperator(
            task_id='resolve_oefs',
            python_callable=resolve_runtime_oefs
        )

        foreach_tag_oef_to_configure = rail.ForEachOperator(
            task_id='foreach_tag_oef_to_configure',
            items=lambda: filter_oefs(rail.result('resolve_oefs'), 'dropdown'),
            start_task='upsert_tag_oef',
            end_task='foreach_tag_oef_to_configure_end'
        )

        upsert_tag_oef = rail.RepliconServiceOperator(
            task_id='upsert_tag_oef',
            endpoint='services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTagDefinition',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=upsert_tag_oef_data
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
                'options': list(map(lambda option: {
                    'Description': option,
                    'Category': option
                }, rail.result('foreach_tag_oef_to_configure')['options'])),
                'definition_uri': rail.result('upsert_tag_oef').get('uri'),
                'oef_id': rail.result('foreach_tag_oef_to_configure')['id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id'],
                'vantagepoint_conn_id': dag_run.conf['vantagepoint_conn_id'],
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
                'uri': rail.result('upsert_tag_oef').get('uri')
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
                    "code": rail.result('foreach_number_oef_to_configure').get('code', rail.result('foreach_number_oef_to_configure')['name']),
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

        trigger_file_format_creation = rail.TriggerDagRunOperator(
            task_id='trigger_file_format_creation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.file_format_creation_child_dag_id,
            conf=lambda dag_run: {
                'vantagepoint_conn_id': dag_run.conf['vantagepoint_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id'],
                'company_key': dag_run.conf['company_key'],
                'customSettings': dag_run.conf.get('customSettings', {}),
                'expected_level_count': get_expected_labor_code_level_count(dag_run.conf.get('customSettings'))
            }
        )

        wait_for_file_format_creation = rail.WaitForDagRunsSensor(
            task_id='wait_for_file_format_creation',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_file_format_creation") }}'
        )

        trigger_laborcategory_options_update = rail.TriggerDagRunOperator(
            task_id='trigger_laborcategory_options_update',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.laborcategory_options_child_dag_id,
            conf=lambda dag_run: {
                'vantagepoint_conn_id': dag_run.conf['vantagepoint_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id'],
                'company_key': dag_run.conf['company_key']
            }
        )

        wait_for_laborcategory_options_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_laborcategory_options_update',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_laborcategory_options_update") }}'
        )

        trigger_laborcode_options_update = rail.TriggerDagRunOperator(
            task_id='trigger_laborcode_options_update',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.laborcode_options_child_dag_id,
            conf=lambda dag_run: {
                'vantagepoint_conn_id': dag_run.conf['vantagepoint_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id'],
                'company_key': dag_run.conf['company_key'],
                'customSettings': dag_run.conf.get('customSettings', {})
            }
        )

        wait_for_laborcode_options_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_laborcode_options_update',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_laborcode_options_update") }}'
        )

        trigger_homecompany_group_sync = rail.TriggerDagRunOperator(
            task_id='trigger_homecompany_group_sync',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.homecompany_group_child_dag_id,
            conf=lambda dag_run: {
                **rail.find_first_by_attr_and_get_attr(config.groups, 'id', 'homecompany'),
                'vantagepoint_conn_id': dag_run.conf['vantagepoint_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id'],
                'company_key': dag_run.conf['company_key']
            }
        )

        wait_for_homecompany_group_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_homecompany_group_sync',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_homecompany_group_sync") }}'
        )

        trigger_webhook_subscription_setup = rail.TriggerDagRunOperator(
            task_id='trigger_webhook_subscription_setup',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.webhook_creation_dag_id,
            conf=lambda dag_run: {
                'vantagepoint_conn_id': dag_run.conf['vantagepoint_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id'],
                'company_key': dag_run.conf['company_key'],
                'customSettings': dag_run.conf.get('customSettings', {})
            }
        )

        wait_for_webhook_subscription_setup = rail.WaitForDagRunsSensor(
            task_id='wait_for_webhook_subscription_setup',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_webhook_subscription_setup") }}'
        )

        gather_dag_runs_expr = (
            "{{ [result('trigger_file_format_creation'), result('trigger_laborcategory_options_update'), "
            "result('trigger_laborcode_options_update'), result('trigger_homecompany_group_sync'), "
            "result('trigger_webhook_subscription_setup')] "
            "+ ([result('trigger_tag_options_update')] if get_task_state('trigger_tag_options_update') == 'success' else []) "
        )
        if lc_combined_oef_name:
            gather_dag_runs_expr += (
                "+ ([result('trigger_lc_combined_options_update')] "
                "if get_task_state('trigger_lc_combined_options_update') == 'success' else []) "
            )
        gather_dag_runs_expr += "}}"

        gather_child_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_dag_errors',
            dag_runs=gather_dag_runs_expr,
            dagrun_task_id='catch_error',
            flatten=True
        )

        is_child_dag_error = rail.IfOperator(
            task_id='is_child_dag_error',
            test="{{ (get_task_state('gather_child_dag_errors') == 'success' and result('gather_child_dag_errors') | length > 0) }}",
            yes_task='fail_child_dag_error',
            no_task='should_log_history'
        )

        fail_child_dag_error = rail.FailOperator(
            task_id='fail_child_dag_error',
            message="{{ result('gather_child_dag_errors') | map_to_attr('error') | join('|') }}"
        )

        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ get_task_state('gather_child_dag_errors') == 'success' or get_task_state('batch_task') == 'failed' }}",
            trigger_rule='all_done',
            yes_task='log_dagrun_details_to_table',
            no_task='delete_this_dagrun'
        )

        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id='log_dagrun_details_to_table',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.hmac_secret
            },
            company_key='{{ dag_run.conf.company_key }}',
            connector_name=config.provider,
            integration_type=config.workflow
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        update_last_run = rail.PythonOperator(
            task_id='update_last_run',
            python_callable=lambda dag_run: Variable.set(
                f'{config.initial_setup_last_run_var}_{dag_run.conf["company_key"]}',
                datetime.now().isoformat()
            ) or {}
        )

        if lc_combined_oef_name:
            get_all_labor_codes_for_combined = rail.VantagepointAPIOperator(
                task_id='get_all_labor_codes_for_combined',
                vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
                endpoint='/accountConfiguration/laborCode',
                request_method='GET'
            )

            def build_lc_combined_options():
                return build_combined_labor_code_options(
                    rail.result('get_all_labor_codes_for_combined'))

            build_lc_combined_options_list = rail.PythonOperator(
                task_id='build_lc_combined_options',
                python_callable=build_lc_combined_options
            )

            trigger_lc_combined_options_update = rail.TriggerDagRunOperator(
                task_id='trigger_lc_combined_options_update',
                retries=0,
                execution_timeout=timedelta(days=config.execution_timeout_days),
                trigger_dag_id=config.tag_oef_options_update_child_dag_id,
                conf=lambda dag_run: {
                    'options': rail.result('build_lc_combined_options'),
                    'definition_uri': rail.find_first_by_attr_and_get_attr(
                        rail.get_dag_run_var('oefs_list'), 'name', lc_combined_oef_name, 'uri'),
                    'oef_id': 'laborcodecombined',
                    'replicon_conn_id': dag_run.conf['replicon_conn_id'],
                    'vantagepoint_conn_id': dag_run.conf['vantagepoint_conn_id'],
                    'company_key': dag_run.conf['company_key']
                }
            )

            wait_for_lc_combined_options_update = rail.WaitForDagRunsSensor(
                task_id='wait_for_lc_combined_options_update',
                execution_timeout=timedelta(days=config.execution_timeout_days),
                dag_runs='{{ result("trigger_lc_combined_options_update") }}'
            )

        batch_task >> should_log_history
        batch_task >> get_user_and_project_oefs >> create_oefs_list >> foreach_text_oef_to_configure >> if_text_oef_not_present
        if_text_oef_not_present >> rail.Label('Yes') >> create_text_oef >> add_oef_to_list >> foreach_text_oef_to_configure_end >> get_cfg_format >> resolve_oefs >> foreach_tag_oef_to_configure
        if_text_oef_not_present >> rail.Label('No') >> add_oef_to_list
        foreach_text_oef_to_configure >> foreach_text_oef_to_configure_end
        foreach_tag_oef_to_configure >> upsert_tag_oef >> if_options_present
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
        if lc_combined_oef_name:
            bind_each_oef >> bind_each_oef_end >> get_all_labor_codes_for_combined
            get_all_labor_codes_for_combined >> build_lc_combined_options_list >> trigger_lc_combined_options_update >> wait_for_lc_combined_options_update >> foreach_groups_to_configure
        else:
            bind_each_oef >> bind_each_oef_end >> foreach_groups_to_configure
        foreach_groups_to_configure >> put_group_systemsettings >> foreach_groups_to_configure_end

        foreach_groups_to_configure >> foreach_groups_to_configure_end >> trigger_file_format_creation >> wait_for_file_format_creation
        wait_for_file_format_creation >> trigger_laborcategory_options_update >> wait_for_laborcategory_options_update
        wait_for_laborcategory_options_update >> trigger_laborcode_options_update >> wait_for_laborcode_options_update
        wait_for_laborcode_options_update >> trigger_homecompany_group_sync >> wait_for_homecompany_group_sync
        wait_for_homecompany_group_sync >> trigger_webhook_subscription_setup >> wait_for_webhook_subscription_setup >> gather_child_dag_errors

        gather_child_dag_errors >> is_child_dag_error

        is_child_dag_error >> rail.Label('Yes') >> fail_child_dag_error >> should_log_history
        is_child_dag_error >> rail.Label('No') >> should_log_history

        should_log_history >> rail.Label('Yes') >> log_dagrun_details_to_table >> update_last_run
        should_log_history >> rail.Label('No') >> delete_this_dagrun

        return dag


rail.for_each_instance(create_dag)
