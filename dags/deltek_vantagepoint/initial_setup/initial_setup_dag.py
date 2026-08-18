from datetime import timedelta
import itertools
from airflow.models import Variable
import rail
from deltek_vantagepoint.initial_setup.oef_mapper import get_oefs_with_required_name
from deltek_vantagepoint.initial_setup.utils import build_combined_labor_code_options
null = None


# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_vantagepoint_initial_setup_dag_{config.instance}',
        description='Does the initial setup for Vantagepoint - Replicon Integration',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'vp_conn_id': config.deltek_vantagepoint_conn_id
        }
    ) as dag:

        lc_combined_oef_name = getattr(config, 'timesheet_field_oef_name_for_lc', None)

        oefs = [oef for oef in config.oefs if oef['id'] != 'laborcodecombined']
        if lc_combined_oef_name:
            oefs += get_oefs_with_required_name({'laborcodecombined': lc_combined_oef_name})

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_and_project_oefs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_and_project_oefs',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_and_project_oefs = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_user_and_project_oefs",
            items=['user', 'project', 'timesheet'],
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
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
            items=filter_oefs(oefs, 'text'),
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
            items=filter_oefs(oefs, 'dropdown'),
            start_task='if_tag_oef_not_present',
            end_task='foreach_tag_oef_to_configure_end'
        )

        if_tag_oef_not_present = rail.IfOperator(
            task_id='if_tag_oef_not_present',
            test=lambda: not rail.find_first_by_attr_and_get_attr(rail.result(
                'get_user_and_project_oefs'), 'name', rail.result('foreach_tag_oef_to_configure')['name']),
            yes_task='create_tag_oef',
            no_task='add_oef_in_list'
        )

        create_tag_oef = rail.RepliconServiceOperator(
            task_id='create_tag_oef',
            endpoint='services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTagDefinition',
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
            task_id = 'if_options_present',
            test = lambda: rail.result('foreach_tag_oef_to_configure').get('options'),
            yes_task='trigger_oef_options_update',
            no_task='add_oef_in_list'
        )

        trigger_oef_options_update = rail.TriggerDagRunOperator(
            task_id = 'trigger_oef_options_update',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_vantagepoint_tag_oef_options_update_child_{config.instance}',
            conf=lambda: {
                'options': list(map(lambda option: {
                    'Description': option,
                    'Category': option
                },rail.result('foreach_tag_oef_to_configure')['options'])),
                'definition': (rail.find_first_by_attr_and_get_attr(rail.result('get_user_oefs'), 'name', rail.result(
                    'foreach_tag_oef_to_configure')['name']) or rail.result('create_tag_oef')).get('uri'),
                'type': rail.result('foreach_tag_oef_to_configure')['id'],
            }
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
            items=filter_oefs(oefs, 'number'),
            start_task='if_number_oef_not_present',
            end_task='foreach_number_oef_to_configure_end'
        )

        if_number_oef_not_present = rail.IfOperator(
            task_id='if_number_oef_not_present',
            test=lambda: not rail.find_first_by_attr_and_get_attr(rail.result(
                'get_user_and_project_oefs'), 'name', rail.result('foreach_number_oef_to_configure')['name']),
            yes_task='create_number_oef',
            no_task='add_oef'
        )

        create_number_oef = rail.RepliconServiceOperator(
            task_id='create_number_oef',
            endpoint='services/ObjectExtensionNumericDefinitionService1.svc/PutObjectExtensionNumericDefinition',
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

        add_oef = rail.SetVariableOperator(
            task_id='add_oef',
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
            data=lambda: {
                "objectExtensionDefinitionUri": rail.result('bind_each_oef')['uri'],
                "bindingContextUri": "urn:replicon:object-type:timesheet"
            }
        )

        bind_oef_to_time_entry = rail.RepliconServiceOperator(
            task_id='bind_oef_to_time_entry',
            endpoint='services/ObjectExtensionService1.svc/BindObjectExtensionField',
            data=lambda: {
                "objectExtensionDefinitionUri": rail.result('bind_each_oef')['uri'],
                "bindingContextUri": "urn:replicon:object-type:time-entry"
            }
        )

        bind_each_oef_end = rail.EmptyOperator(
            task_id='bind_each_oef_end'
        )

        foreach_groups_to_configure = rail.ForEachOperator(
            task_id = 'foreach_groups_to_configure',
            items=config.groups,
            start_task='put_group_systemsettings',
            end_task='foreach_groups_to_configure_end'
        )

        put_group_systemsettings = rail.RepliconServiceOperator(
            task_id = 'put_group_systemsettings',
            endpoint="{{result('foreach_groups_to_configure').renameendpoint}}",
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
            task_id = 'foreach_groups_to_configure_end'
        )

        trigger_file_format_creation_for_time_export = rail.TriggerDagRunOperator(
            task_id='trigger_file_format_creation_for_time_export',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_vantagepoint_file_format_creation_child_{config.instance}'
        )

        trigger_laborcategory_options_update_dag = rail.TriggerDagRunOperator(
            task_id='trigger_laborcategory_options_update_dag',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_vantagepoint_laborcategory_options_update_child_{config.instance}'
        )

        trigger_laborcode_options_update_dag = rail.TriggerDagRunOperator(
            task_id='trigger_laborcode_options_update_dag',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_vantagepoint_laborcode_options_update_child_{config.instance}'
        )

        trigger_homecompany_group_sync_dag = rail.TriggerDagRunOperator(
            task_id='trigger_homecompany_group_sync_dag',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_vantagepoint_homecompany_group_sync_child_{config.instance}',
            conf=lambda: rail.find_first_by_attr_and_get_attr(config.groups, 'id', 'homecompany')
        )

        is_user_sync_filter_enabled = rail.IfOperator(
            task_id='is_user_sync_filter_enabled',
            test=lambda: Variable.get(
                config.usersync_filter_var, deserialize_json=True, default_var={}) != {},
            yes_task='trigger_user_sync_workflow_setup',
            no_task='log_to_sumo'
        )

        trigger_user_sync_workflow_setup = rail.TriggerDagRunOperator(
            task_id='trigger_user_sync_workflow_setup',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_vantagepoint_webhook_filter_workflow_setup_{config.instance}',
            conf={
                'application_name': 'EmployeeICBO',
                'filter_var': config.usersync_filter_var,
                'entity_type': 'EM'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        if lc_combined_oef_name:
            get_all_labor_codes_for_combined = rail.VantagepointAPIOperator(
                task_id='get_all_labor_codes_for_combined',
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
                trigger_dag_id=f'deltek_vantagepoint_tag_oef_options_update_child_{config.instance}',
                conf=lambda: {
                    'options': rail.result('build_lc_combined_options'),
                    'definition': rail.find_first_by_attr_and_get_attr(
                        rail.get_dag_run_var('oefs_list'), 'name', lc_combined_oef_name, 'uri'),
                    'type': 'laborcodecombined',
                }
            )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_user_and_project_oefs >> create_oefs_list >> foreach_text_oef_to_configure >> if_text_oef_not_present
        if_text_oef_not_present >> rail.Label(
            'Yes') >> create_text_oef >> add_oef_to_list >> foreach_text_oef_to_configure_end >> foreach_tag_oef_to_configure
        if_text_oef_not_present >> rail.Label('No') >> add_oef_to_list
        foreach_text_oef_to_configure >> foreach_text_oef_to_configure_end
        foreach_tag_oef_to_configure >> if_tag_oef_not_present
        if_tag_oef_not_present >> rail.Label(
            'Yes') >> create_tag_oef >> if_options_present
        if_options_present >> rail.Label('Yes') >> trigger_oef_options_update >> add_oef_in_list
        if_options_present >> rail.Label('No') >> add_oef_in_list
        if_tag_oef_not_present >> rail.Label(
            'No') >> add_oef_in_list >> foreach_tag_oef_to_configure_end >> foreach_number_oef_to_configure
        foreach_tag_oef_to_configure >> foreach_tag_oef_to_configure_end >> foreach_number_oef_to_configure
        foreach_number_oef_to_configure >> if_number_oef_not_present
        if_number_oef_not_present >> rail.Label(
            'Yes') >> create_number_oef >> add_oef
        if_number_oef_not_present >> rail.Label(
            'No') >> add_oef >> foreach_number_oef_to_configure_end
        foreach_number_oef_to_configure >> foreach_number_oef_to_configure_end >> bind_each_oef
        bind_each_oef >> if_bind_to_user
        if_bind_to_user >> rail.Label(
            'Yes') >> bind_oef_to_user >> if_bind_to_project
        if_bind_to_user >> rail.Label('No') >> if_bind_to_project
        if_bind_to_project >> rail.Label('Yes') >> bind_oef_to_project >> if_bind_to_timesheet
        if_bind_to_project >> rail.Label('No') >> if_bind_to_timesheet
        if_bind_to_timesheet >> rail.Label(
            'Yes') >> bind_oef_to_timesheet >> bind_oef_to_time_entry >> bind_each_oef_end
        if_bind_to_timesheet >> rail.Label(
            'No') >> bind_each_oef_end
        if lc_combined_oef_name:
            bind_each_oef >> bind_each_oef_end >> get_all_labor_codes_for_combined
            get_all_labor_codes_for_combined >> build_lc_combined_options_list >> trigger_lc_combined_options_update >> foreach_groups_to_configure
        else:
            bind_each_oef >> bind_each_oef_end >> foreach_groups_to_configure
        foreach_groups_to_configure >> put_group_systemsettings >> foreach_groups_to_configure_end
        foreach_groups_to_configure >> foreach_groups_to_configure_end >> trigger_file_format_creation_for_time_export
        trigger_file_format_creation_for_time_export >> trigger_laborcategory_options_update_dag >> trigger_laborcode_options_update_dag
        trigger_laborcode_options_update_dag >> trigger_homecompany_group_sync_dag >> is_user_sync_filter_enabled
        is_user_sync_filter_enabled >> rail.Label(
            'Yes') >> trigger_user_sync_workflow_setup >> log_to_sumo
        is_user_sync_filter_enabled >> rail.Label(
            'No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
