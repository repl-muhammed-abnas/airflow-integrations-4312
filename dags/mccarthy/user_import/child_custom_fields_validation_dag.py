from datetime import timedelta
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/mccarthy/user_import/config.py


def create_customfields_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'mccarthy_user_import_custom_fields_validation_child_{config.instance}',
        description=f'LIVE | Mccarthy_UserImport_Custom_Fields_Validation - Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='query_new_dropdownvalues'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_new_dropdownvalues',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        query_new_dropdownvalues = rail.QueryCollectionOperator(
            task_id='query_new_dropdownvalues',
            query="""SELECT displayText FROM {{ dag_run.conf.feedfiletablename }} WHERE
                    LOWER(displayText) NOT IN (SELECT DISTINCT LOWER(displayText) FROM
                    {{ dag_run.conf.repliconvaluestablename }})"""
        )

        is_new_dropdowns_present = rail.IfOperator(
            task_id='is_new_dropdowns_present',
            test="{{ result('query_new_dropdownvalues', 'length') > 0 }}",
            yes_task="put_dropdown_options",
            no_task="dagrun_log_to_sumo"
        )

        def get_customfield_dropdown_option_uris(replicon_dropdowns, new_dropdowns):
            existing_dropdowns_list = rail.load_all_records(replicon_dropdowns)
            bool_dictionary = {
                '1': True,
                '0': False
            }
            final_dropdown_list = list(map(lambda x: {
                'target': {
                    'uri': x['uri']
                },
                'name': x['displayText'],
                'isEnabled': bool_dictionary[x['isEnabled']]
            }, existing_dropdowns_list)) if existing_dropdowns_list else []

            new_values_to_set = rail.load_all_records(new_dropdowns)

            final_dropdown_list.extend(map(lambda x: {
                'name': x['displayText'],
                'isEnabled': True
            }, new_values_to_set))
            return final_dropdown_list
        put_dropdown_options = rail.RepliconServiceOperator(
            task_id='put_dropdown_options',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": dag_run.conf['customFieldUri'],
                "customFieldDropDownOptionUris": get_customfield_dropdown_option_uris(
                    dag_run.conf['replicon_dropdowns'], rail.result('query_new_dropdownvalues'))
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> query_new_dropdownvalues >> is_new_dropdowns_present
        is_new_dropdowns_present >> rail.Label(
            'Yes') >> put_dropdown_options >> dagrun_log_to_sumo
        is_new_dropdowns_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_customfields_dag)
