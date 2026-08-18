
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'npsgeu_user_import_process_custom_field_division_child_{config.instance}',
        description=f'NPSGEU_Process Custom field (Division)- Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_custom_field_drop_down_options_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_custom_field_drop_down_options_4',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
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
            source="{{ result('get_all_custom_field_drop_down_options_4') | to_json}}",
            name="divisionfromreplicon",
        )

        query_list_checkfornewdropdownvalues_6 = rail.QueryCollectionOperator(
            task_id='query_list_checkfornewdropdownvalues_6',
            query="""SELECT  divisionfromfeed.divisionoption FROM divisionfromfeed WHERE
                LOWER( divisionfromfeed.divisionoption) NOT IN ( SELECT DISTINCT LOWER( divisionfromreplicon.displayText) FROM  divisionfromreplicon )""",
        )

        if_query_list_checkfornewdropdownvalues_6_rows_greater_than_0_7 = rail.IfOperator(
            task_id='if_query_list_checkfornewdropdownvalues_6_rows_greater_than_0_7',
            test='''{{ result('query_list_checkfornewdropdownvalues_6','length') > 0 }}''',
            yes_task="get_customfield_options_to_put",
            no_task="catch_error",
        )

        def get_final_customfield_dropdown_options():
            existing_dropdownoptions = rail.result(
                'get_all_custom_field_drop_down_options_4')
            customfielddropdownoptions = [{
                'target': {
                    'uri': option['uri'],
                    'name': option['displayText']
                },
                'name': option['displayText'],
                'isEnabled': option['isEnabled']
            } for option in existing_dropdownoptions]
            new_dropdownoptions = rail.load_all_records(
                rail.result('query_list_checkfornewdropdownvalues_6'))
            new_options = [{
                'target': {
                    'uri': null,
                    'name': null
                },
                'name': option['divisionoption'],
                'isEnabled': 'true'
            } for option in new_dropdownoptions]
            return customfielddropdownoptions + new_options

        get_customfield_options_to_put = rail.PythonOperator(
            task_id='get_customfield_options_to_put',
            python_callable=get_final_customfield_dropdown_options
        )

        put_drop_down_options_14 = rail.RepliconServiceOperator(
            task_id='put_drop_down_options_14',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda dag_run:{
                "customFieldUri": dag_run.conf['customFieldUri'],
                "customFieldDropDownOptionUris":  rail.result('get_customfield_options_to_put')
            }
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
        )
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label(
            'No') >> get_all_custom_field_drop_down_options_4 >> create_list_5 >> query_list_checkfornewdropdownvalues_6
        query_list_checkfornewdropdownvalues_6 >> if_query_list_checkfornewdropdownvalues_6_rows_greater_than_0_7
        if_query_list_checkfornewdropdownvalues_6_rows_greater_than_0_7 >> rail.Label(
            'Yes') >> get_customfield_options_to_put >> put_drop_down_options_14 >> catch_error
        if_query_list_checkfornewdropdownvalues_6_rows_greater_than_0_7 >> rail.Label(
            'No') >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
