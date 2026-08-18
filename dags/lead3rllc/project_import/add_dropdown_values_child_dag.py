from datetime import timedelta
from airflow.models import Variable
import rail
from lead3rllc.project_import.utils.request_payload import get_oef_dropdown_option_uris, get_dropdown_success_details

null = None


def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_add_or_enable_dropdown_options_dag_id,
        description='LEAD3R LLC Project Import - Add/Enable Dropdown Values for OEF Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_dropdown_values_for_oef'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_all_dropdown_values_for_oef',
            end_task='catch_and_log_error',
        )

        get_all_dropdown_values_for_oef = rail.RepliconServiceOperator(
            task_id='get_all_dropdown_values_for_oef',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{dag_run.conf.oef_uri}}"
            },
            data_handler=lambda data: list(map(lambda x: {
                'tag_name': x['name'],
                'tag_uri': x['uri'],
                'enabled': x['isEnabled']}, data['tags'])) if data['tags'] else [{'tag_name': '', 'tag_uri': '', 'enabled': ''}]
        )

        create_netsuiteprojecttype_dropdown_values_collection = rail.CreateCollectionOperator(
            task_id='create_netsuiteprojecttype_dropdown_values_collection',
            source=lambda: rail.result(
                'get_all_dropdown_values_for_oef'),
            name='replicon_netsuiteprojecttype_values'
        )

        query_get_netsuiteprojecttype_dropdown_values_not_in_replicon = rail.QueryCollectionOperator(
            task_id='query_get_netsuiteprojecttype_dropdown_values_not_in_replicon',
            query="""SELECT DISTINCT valid_records.netsuite_project_type FROM valid_records 
                WHERE (LOWER(valid_records.netsuite_project_type) NOT IN (SELECT LOWER(replicon_netsuiteprojecttype_values.tag_name)
                FROM replicon_netsuiteprojecttype_values) AND NULLIF(valid_records.netsuite_project_type, '') IS NOT NULL)""",
            name="netsuiteprojecttype_values_to_add"
        )

        query_get_netsuiteprojecttype_dropdown_values_present_and_disabled_in_replicon = rail.QueryCollectionOperator(
            task_id='query_get_netsuiteprojecttype_dropdown_values_present_and_disabled_in_replicon',
            query="""SELECT * FROM  replicon_netsuiteprojecttype_values 
                WHERE ( LOWER(replicon_netsuiteprojecttype_values.tag_name) IN (SELECT LOWER(valid_records.netsuite_project_type) FROM valid_records) 
                AND replicon_netsuiteprojecttype_values.enabled != "1")""",
            name="netsuiteprojecttype_values_to_enable"
        )

        if_netsuiteprojecttype_dropdown_values_to_create_or_enable_present = rail.IfOperator(
            task_id='if_netsuiteprojecttype_dropdown_values_to_create_or_enable_present',
            test=lambda: rail.result(
                'query_get_netsuiteprojecttype_dropdown_values_not_in_replicon', 'length') > 0 or rail.result(
                'query_get_netsuiteprojecttype_dropdown_values_present_and_disabled_in_replicon', 'length') > 0,
            yes_task='put_or_enable_dropdown_options_netsuiteprojecttype',
            no_task='catch_and_log_error'
        )

        put_or_enable_dropdown_options_netsuiteprojecttype = rail.RepliconServiceOperator(
            task_id='put_or_enable_dropdown_options_netsuiteprojecttype',
            endpoint='/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags',
            data=lambda dag_run: {
                "objectExtensionTagDefinition": {
                    "uri": dag_run.conf['oef_uri'],
                    "name": null
                },
                "objectExtensionTags": get_oef_dropdown_option_uris()
            }
        )

        success_put_or_enable_dropdown_options_log_entry = rail.WriteLogOperator(
            task_id='success_put_or_enable_dropdown_options_log_entry',
            log="{{dag_run.conf.missing_field_value_import_logs}}",
            message='na',
            severity='Success',
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf["parentjobid"],
                "action": "Dropdown options add/enable",
                "status": "Success",
                "details": get_dropdown_success_details()
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{dag_run.conf.missing_field_value_import_logs}}",
            message='na',
            severity='Error',
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf["parentjobid"],
                "action": "Dropdown options add/enable",
                "status": "Error",
                "details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> get_all_dropdown_values_for_oef

        get_all_dropdown_values_for_oef >> create_netsuiteprojecttype_dropdown_values_collection \
            >> query_get_netsuiteprojecttype_dropdown_values_not_in_replicon >> query_get_netsuiteprojecttype_dropdown_values_present_and_disabled_in_replicon\
            >> if_netsuiteprojecttype_dropdown_values_to_create_or_enable_present

        if_netsuiteprojecttype_dropdown_values_to_create_or_enable_present >> rail.Label(
            'No') >> catch_and_log_error
        if_netsuiteprojecttype_dropdown_values_to_create_or_enable_present >> rail.Label(
            'Yes') >> put_or_enable_dropdown_options_netsuiteprojecttype >> success_put_or_enable_dropdown_options_log_entry >> catch_and_log_error

    return dag


rail.for_each_instance(create_child_dag)
