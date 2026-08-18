from datetime import timedelta
from airflow.models import Variable
import rail
from terraconconsultants.user_import.utils.request_payload import get_jobtitle_dropdown_options_payload


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/terraconconsultants/user_import/config.py


def create_jobtitle_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'terraconconsultants_userimport_child_jobtitle_customfield_check_{config.instance}',
        description=f'TerraconConsultants Child Jobtitle Custom field check {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_jobtitle_user_customfield'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_jobtitle_user_customfield',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_jobtitle_user_customfield = rail.RepliconServiceOperator(
            task_id='get_jobtitle_user_customfield',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': "urn:replicon:object-type:user"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Job Title', 'uri', '')
        )

        get_existing_jobtitle_customfields = rail.RepliconServiceOperator(
            task_id='get_existing_jobtitle_customfields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_jobtitle_user_customfield') }}"
            }
        )

        create_existingjobtitle_collection = rail.CreateCollectionOperator(
            task_id='create_existingjobtitle_collection',
            source=lambda: rail.result('get_existing_jobtitle_customfields'),
            name="existingjobtitle"
        )

        query_jobtitle_to_create = rail.QueryCollectionOperator(
            task_id='query_jobtitle_to_create',
            query="""SELECT DISTINCT Job_Title FROM uniquejobtitles
                    WHERE Job_Title NOT IN (SELECT DISTINCT displayText FROM existingjobtitle)""",
        )

        is_jobtitle_dropdowns_to_create = rail.IfOperator(
            task_id='is_jobtitle_dropdowns_to_create',
            test="{{ result('query_jobtitle_to_create', 'length') > 0 }}",
            yes_task="put_jobtitle_dropdown_options",
            no_task="dagrun_log_to_sumo"
        )

        put_jobtitle_dropdown_options = rail.RepliconServiceOperator(
            task_id='put_jobtitle_dropdown_options',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result('get_jobtitle_user_customfield'),
                "customFieldDropDownOptionUris": get_jobtitle_dropdown_options_payload()
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_jobtitle_user_customfield
        get_jobtitle_user_customfield >> get_existing_jobtitle_customfields >> \
            create_existingjobtitle_collection >> query_jobtitle_to_create >> \
            is_jobtitle_dropdowns_to_create
        is_jobtitle_dropdowns_to_create >> rail.Label(
            'Yes') >> put_jobtitle_dropdown_options >> dagrun_log_to_sumo
        is_jobtitle_dropdowns_to_create >> rail.Label(
            'No') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_jobtitle_dag)
