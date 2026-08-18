from datetime import timedelta
import rail
from airflow.models import Variable
from salesforce.custom_field.utils import response_handler


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_salesforce_{config.region.replace('-', '_')}_custom_field_{config.instance}",
        description=f'Salesforce {config.region} Custom Field {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_sfobject_customfields'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_sfobject_customfields',
            end_task='catch_customfield_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_sfobject_customfields = rail.SalesforceObjectGetFieldsOperator2(
            task_id='get_sfobject_customfields',
            salesforce_conn_id='{{ dag_run.conf.salesforce_conn_id }}',
            object_name='{{ dag_run.conf.sfobject }}',
            field_names=lambda dag_run: dag_run.conf['sfobject_fields']
        )

        should_add_customfield_query = rail.IfOperator(
            task_id='should_add_customfield_query',
            test="{{ dag_run.conf.should_add_customfield_query | is_truthy }}",
            yes_task='get_customfield_where_clause',
            no_task='should_process_oef_creation'
        )

        get_customfield_where_clause = rail.PythonOperator(
            task_id="get_customfield_where_clause",
            python_callable=response_handler.get_matching_customfields_values
        )

        should_process_oef_creation = rail.IfOperator(
            task_id='should_process_oef_creation',
            test="{{ dag_run.conf.should_process_oef_creation | is_truthy }}",
            yes_task='get_object_extension_tag_list',
            no_task='final_result'
        )

        get_object_extension_tag_list = rail.RepliconServicePageOperator(
            task_id='get_object_extension_tag_list',
            endpoint='/services/ObjectExtensionDefinitionListService1.svc/GetData',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                'page': 1,
                'pagesize': 10000,
                'columnUris': [
                    'urn:replicon:object-extension-tag-definition-list-column:name',
                    'urn:replicon:object-extension-tag-definition-list-column:code',
                    'urn:replicon:object-extension-tag-definition-list-column:description',
                    'urn:replicon:object-extension-tag-definition-list-column:object-extension-definition-type',
                    'urn:replicon:object-extension-tag-definition-list-column:object-extension-tag-definition'
                ]
            },
            page_handler=response_handler.page_handler,
            all_result_data_handler=response_handler.filter_object_extension_tags
        )

        get_all_object_extension_bindings = rail.RepliconServiceOperator(
            task_id='get_all_object_extension_bindings',
            endpoint='/services/ObjectExtensionService1.svc/GetPageOfObjectExtensionDefinitionsFilteredBySearch',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                'page': '1',
                'pageSize': '1000',
                'bindingContextUri': '{{ dag_run.conf.bindingUri }}'
            },
            data_handler=response_handler.filter_object_extension_bindings
        )

        declare_created_oefs_list = rail.SetVariableOperator(
            task_id='declare_created_oefs_list',
            append=False,
            name='created_oefs_list',
            value=[]
        )

        foreach_new_oef = rail.ForEachOperator(
            task_id='foreach_new_oef',
            items="{{ result('get_all_object_extension_bindings').new_oefs | to_json }}",
            start_task='get_oef_creation_params',
            end_task='foreach_new_oef_end'
        )

        get_oef_creation_params = rail.PythonOperator(
            task_id="get_oef_creation_params",
            python_callable=response_handler.get_oef_creation_params
        )

        create_new_oef = rail.RepliconServiceOperator(
            task_id='create_new_oef',
            endpoint="{{ result('get_oef_creation_params').url }}",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data="{{ result('get_oef_creation_params').params }}"
        )

        update_created_oefs_list = rail.SetVariableOperator(
            task_id='update_created_oefs_list',
            name='{{ result("declare_created_oefs_list").name }}',
            value=lambda: rail.result('create_new_oef'),
            append=True
        )

        bind_new_oef = rail.RepliconServiceOperator(
            task_id='bind_new_oef',
            endpoint='/services/ObjectExtensionService1.svc/BindObjectExtensionField',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda dag_run: {
                "objectExtensionDefinitionUri": rail.result('create_new_oef')['uri'],
                "bindingContextUri": dag_run.conf['bindingUri']
            }
        )

        is_new_oef_dropdown = rail.IfOperator(
            task_id='is_new_oef_dropdown',
            test='{{ result("foreach_new_oef").type == "picklist" }}',
            yes_task='create_dropdown_options',
            no_task='foreach_new_oef_end'
        )

        create_dropdown_options = rail.RepliconServiceOperator(
            task_id='create_dropdown_options',
            endpoint='/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                'objectExtensionTagDefinition': {'uri':  rail.result('create_new_oef')['uri']},
                'objectExtensionTags': response_handler.generate_dropdown_options(
                    rail.result('foreach_new_oef'), rail.result('create_new_oef')['uri'])
            }
        )

        foreach_new_oef_end = rail.EmptyOperator(
            task_id='foreach_new_oef_end'
        )

        foreach_existing_oef = rail.ForEachOperator(
            task_id='foreach_existing_oef',
            items="{{ result('get_all_object_extension_bindings').existing_oefs | to_json }}",
            start_task='should_bind_oef',
            end_task='foreach_existing_oef_end'
        )

        should_bind_oef = rail.IfOperator(
            task_id='should_bind_oef',
            test='{{ result("foreach_existing_oef").is_already_binded | is_truthy }}',
            yes_task='is_existing_oef_dropdown',
            no_task='bind_existing_oef'
        )

        bind_existing_oef = rail.RepliconServiceOperator(
            task_id='bind_existing_oef',
            endpoint='/services/ObjectExtensionService1.svc/BindObjectExtensionField',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda dag_run: {
                "objectExtensionDefinitionUri": rail.result('foreach_existing_oef')['uri'],
                "bindingContextUri": dag_run.conf['bindingUri']
            }
        )

        is_existing_oef_dropdown = rail.IfOperator(
            task_id='is_existing_oef_dropdown',
            test='{{ result("foreach_existing_oef").type == "picklist" }}',
            yes_task='update_dropdown_options',
            no_task='foreach_existing_oef_end'
        )

        update_dropdown_options = rail.RepliconServiceOperator(
            task_id='update_dropdown_options',
            endpoint='/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                'objectExtensionTagDefinition': {'uri':  rail.result('foreach_existing_oef')['uri']},
                'objectExtensionTags': response_handler.generate_dropdown_options(
                    rail.result('foreach_existing_oef'), rail.result('foreach_existing_oef')['uri'])
            }
        )

        foreach_existing_oef_end = rail.EmptyOperator(
            task_id='foreach_existing_oef_end'
        )

        foreach_modify_oef = rail.ForEachOperator(
            task_id='foreach_modify_oef',
            items="{{ result('get_all_object_extension_bindings').modify_oefs | to_json }}",
            start_task='get_oef_modification_params',
            end_task='foreach_modify_oef_end'
        )

        get_oef_modification_params = rail.PythonOperator(
            task_id="get_oef_modification_params",
            python_callable=response_handler.get_oef_modification_params
        )

        modify_oef_name = rail.RepliconServiceOperator(
            task_id='modify_oef_name',
            endpoint="{{ result('get_oef_modification_params').url }}",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data="{{ result('get_oef_modification_params').params }}"
        )

        foreach_modify_oef_end = rail.EmptyOperator(
            task_id='foreach_modify_oef_end'
        )

        get_created_oefs_list = rail.GetVariableOperator(
            task_id='get_created_oefs_list',
            name='created_oefs_list'
        )

        final_result = rail.PythonOperator(
            task_id='final_result',
            python_callable=response_handler.get_final_result,
            op_args=['{{ dag_run.conf.should_process_oef_creation }}']
        )

        def get_downstreamtasks_error(company_key, error_message):
            return {'error': f'Error with {company_key} - {error_message}'}

        catch_customfield_error = rail.PythonOperator(
            task_id='catch_customfield_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.company_key }}',
                     '{{ get_error_message() }}']
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> rail.Label(
                'on Error') >> catch_customfield_error
        can_run_batch_task >> rail.Label(
            'No') >> get_sfobject_customfields >> should_add_customfield_query

        should_add_customfield_query >> rail.Label(
            'Yes') >> get_customfield_where_clause >> should_process_oef_creation
        should_add_customfield_query >> rail.Label(
            'No') >> should_process_oef_creation

        should_process_oef_creation >> rail.Label(
            'Yes') >> get_object_extension_tag_list >> get_all_object_extension_bindings >> \
            declare_created_oefs_list >> foreach_new_oef
        should_process_oef_creation >> rail.Label('No') >> final_result

        foreach_new_oef >> get_oef_creation_params >> create_new_oef >> update_created_oefs_list >> bind_new_oef >> is_new_oef_dropdown
        is_new_oef_dropdown >> rail.Label(
            'Yes') >> create_dropdown_options >> foreach_new_oef_end
        is_new_oef_dropdown >> rail.Label('No') >> foreach_new_oef_end
        foreach_new_oef >> foreach_new_oef_end >> foreach_existing_oef >> should_bind_oef

        should_bind_oef >> rail.Label(
            'Yes') >> bind_existing_oef >> is_existing_oef_dropdown
        should_bind_oef >> rail.Label(
            'No') >> is_existing_oef_dropdown

        is_existing_oef_dropdown >> rail.Label(
            'yes') >> update_dropdown_options >> foreach_existing_oef_end
        is_existing_oef_dropdown >> rail.Label(
            'No') >> foreach_existing_oef_end
        foreach_existing_oef >> foreach_existing_oef_end >> foreach_modify_oef

        foreach_modify_oef >> get_oef_modification_params >> modify_oef_name >> foreach_modify_oef_end
        foreach_modify_oef >> foreach_modify_oef_end >> get_created_oefs_list

        get_created_oefs_list >> final_result >> catch_customfield_error

    return dag


rail.for_each_instance(create_main_dag)
