
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'horizonmedia_user_import_process_custom_fields_child_{config.instance}',
        description=f'Horizonmedia  - Process Custom fields - Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='has_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='has_data',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test="{{ dag_run.conf.customFieldvalues | is_truthy }}",
            yes_task='create_list_3',
            no_task='log_to_sumo'
        )

        create_list_3 = rail.CreateCollectionOperator(
            task_id='create_list_3',
            source="{{ dag_run.conf.customFieldvalues | to_json }}",
            name="payrolldeptvaluesfromfeedfile",
        )

        get_all_custom_field_drop_down_options_4 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_4',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ dag_run.conf.customFieldUri }}"
            }
        )

        create_list_5 = rail.CreateCollectionOperator(
            task_id='create_list_5',
            source="{{ result('get_all_custom_field_drop_down_options_4') | to_json }}",
            name="customfieldvaluesinreplicon",
        )

        query_list_checkfornewdropdownvalues_6 = rail.QueryCollectionOperator(
            task_id='query_list_checkfornewdropdownvalues_6',
            query="""SELECT  payrolldeptvaluesfromfeedfile.displayText FROM  payrolldeptvaluesfromfeedfile WHERE  payrolldeptvaluesfromfeedfile.displayText NOT IN ( SELECT DISTINCT  customfieldvaluesinreplicon.displayText FROM  customfieldvaluesinreplicon )""",
        )

        query_list_checkfornewdropdownvalues_7 = rail.QueryCollectionOperator(
            task_id='query_list_checkfornewdropdownvalues_7',
            query="""SELECT  payrolldeptvaluesfromfeedfile.displayText FROM  payrolldeptvaluesfromfeedfile WHERE LOWER( payrolldeptvaluesfromfeedfile.displayText) NOT IN ( SELECT DISTINCT LOWER( customfieldvaluesinreplicon.displayText) FROM  customfieldvaluesinreplicon )""",
        )

        if_query_list_checkfornewdropdownvalues_7_rows_greater_than_0_8 = rail.IfOperator(
            task_id='if_query_list_checkfornewdropdownvalues_7_rows_greater_than_0_8',
            test='''{{ result('query_list_checkfornewdropdownvalues_7','length') > 0 }}''',
            yes_task="declare_list_9",
            no_task="log_to_sumo",
        )

        declare_list_9 = rail.SetVariableOperator(
            task_id='declare_list_9',
            append=False,
            name='customFieldDropDownOptionUris',
            value=[]
        )

        foreach_response_10 = rail.ForEachOperator(
            task_id='foreach_response_10',
            items="{{ result('get_all_custom_field_drop_down_options_4') | to_json }}",
            start_task='insert_to_list_11',
            end_task='foreach_response_10_end'
        )

        insert_to_list_11 = rail.SetVariableOperator(
            task_id='insert_to_list_11',
            append=True,
            name='{{ result("declare_list_9").name }}',
            value={
                "target": {
                    "uri": "{{ result('foreach_response_10').uri }}",
                    "name": "{{ result('foreach_response_10').displayText }}"
                },
                "name": "{{ result('foreach_response_10').displayText }}",
                "isEnabled": "{{ result('foreach_response_10').isEnabled | lower }}"
            }
        )

        foreach_response_10_end = rail.EmptyOperator(
            task_id='foreach_response_10_end',
        )

        foreach_query_list_checkfornewdropdownvalues_7_12 = rail.ForEachOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_7_12',
            items="{{ result('query_list_checkfornewdropdownvalues_7') }}",
            start_task='insert_to_list_13',
            end_task='foreach_query_list_checkfornewdropdownvalues_7_12_end'
        )

        insert_to_list_13 = rail.SetVariableOperator(
            task_id='insert_to_list_13',
            append=True,
            name='{{ result("declare_list_9").name }}',
            value={
                "target": {
                    "uri": null,
                    "name": "{{ result('foreach_query_list_checkfornewdropdownvalues_7_12').displayText }}",
                },
                "name": "{{ result('foreach_query_list_checkfornewdropdownvalues_7_12').displayText }}",
                "isEnabled": "true"
            }
        )

        foreach_query_list_checkfornewdropdownvalues_7_12_end = rail.EmptyOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_7_12_end',
        )

        log_formattheinputto_json_14 = rail.PythonOperator(
            task_id='log_formattheinputto_json_14',
            python_callable=lambda:  rail.get_dag_run_var(
                rail.result('declare_list_9')['name'])
        )

        put_drop_down_options_15 = rail.RepliconServiceOperator(
            task_id='put_drop_down_options_15',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.get_dag_run_conf()['customFieldUri'],
                "customFieldDropDownOptionUris":  rail.result('log_formattheinputto_json_14')
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> has_data
        has_data >> rail.Label('yes') >> create_list_3
        has_data >> rail.Label('no') >> log_to_sumo
        create_list_3 >> get_all_custom_field_drop_down_options_4 >> create_list_5 >> query_list_checkfornewdropdownvalues_6 >> query_list_checkfornewdropdownvalues_7 >> if_query_list_checkfornewdropdownvalues_7_rows_greater_than_0_8
        if_query_list_checkfornewdropdownvalues_7_rows_greater_than_0_8 >> rail.Label(
            'Yes') >> declare_list_9 >> foreach_response_10 >> insert_to_list_11 >> foreach_response_10_end
        foreach_response_10 >> foreach_response_10_end >> foreach_query_list_checkfornewdropdownvalues_7_12 >> insert_to_list_13 >> foreach_query_list_checkfornewdropdownvalues_7_12_end
        foreach_query_list_checkfornewdropdownvalues_7_12 >> foreach_query_list_checkfornewdropdownvalues_7_12_end >> log_formattheinputto_json_14 >> put_drop_down_options_15 >> log_to_sumo
        if_query_list_checkfornewdropdownvalues_7_rows_greater_than_0_8 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
