from datetime import timedelta
from airflow.models import Variable
from ge.user_sync_poland.utils import custom_methods
import rail

null = None


def create_dag(config):
    # pylnot: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_suspend_assignment_category_custom_field_dag_id,
        description=f'GE POLAND User Import Suspend Assignment Catagory Custom Field Check Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='query_inputfilerawdata_for_records'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_inputfilerawdata_for_records',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        query_inputfilerawdata_for_records = rail.QueryCollectionOperator(
            task_id='query_inputfilerawdata_for_records',
            name='records_to_process',
            query="""SELECT * FROM inputfilerawdata"""
        )

        get_all_user_custom_fields_8 = rail.RepliconServiceOperator(
            task_id='get_all_user_custom_fields_8',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda res: rail.find_first_by_attr_and_get_attr(
                res, 'displayText', 'Suspend Assignment Category', 'uri')
        )

        get_all_custom_field_drop_down_options_suspend_assignment_category_10 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_suspend_assignment_category_10',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_all_user_custom_fields_8') }}"
            }
        )

        create_collection_for_schedules_to_assign_list_13 = rail.CreateCollectionOperator(
            task_id='create_collection_for_schedules_to_assign_list_13',
            source="{{result('get_all_custom_field_drop_down_options_suspend_assignment_category_10') | to_json}}",
            name='existing_dropdown_values'
        )

        query_to_get_new_dropdown_values_to_add_14 = rail.QueryCollectionOperator(
            task_id='query_to_get_new_dropdown_values_to_add_14',
            name='new_dropdown_values_to_add',
            query="""SELECT DISTINCT SuspendAssignmentCategory FROM records_to_process WHERE 
                LOWER(SuspendAssignmentCategory) NOT IN (SELECT LOWER(displayText) FROM existing_dropdown_values)"""
        )

        if_new_dropdown_values_to_add_present_15 = rail.IfOperator(
            task_id='if_new_dropdown_values_to_add_present_15',
            test='''{{ result('query_to_get_new_dropdown_values_to_add_14')| length > 0}}''',
            yes_task="get_final_dropdown_options_to_put",
            no_task="finish",
        )

        get_final_dropdown_options_to_put = rail.PythonOperator(
            task_id='get_final_dropdown_options_to_put',
            python_callable=custom_methods.get_final_dropdown_options_list
        )

        put_drop_down_optionsfor_suspend_assignment_category_24 = rail.RepliconServiceOperator(
            task_id='put_drop_down_optionsfor_suspend_assignment_category_24',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result('get_all_user_custom_fields_8'),
                "customFieldDropDownOptionUris":  rail.result('get_final_dropdown_options_to_put')
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> query_inputfilerawdata_for_records

        query_inputfilerawdata_for_records >> get_all_user_custom_fields_8 >> get_all_custom_field_drop_down_options_suspend_assignment_category_10 \
            >> create_collection_for_schedules_to_assign_list_13 >> query_to_get_new_dropdown_values_to_add_14 >> if_new_dropdown_values_to_add_present_15

        if_new_dropdown_values_to_add_present_15 >> rail.Label('No') >> finish
        if_new_dropdown_values_to_add_present_15 >> rail.Label('Yes') >> get_final_dropdown_options_to_put \
            >> put_drop_down_optionsfor_suspend_assignment_category_24 >> finish

    return dag


rail.for_each_instance(create_dag)
