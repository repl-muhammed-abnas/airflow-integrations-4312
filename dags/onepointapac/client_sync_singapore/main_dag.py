from datetime import timedelta
import rail
from onepointapac.client_sync_singapore.utils import response_filter, custom_methods


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Xero Contact to Replicon Client Sync - Singapore {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
        schedule_interval=config.schedule_interval
    ) as dag:

        get_lastsync_time = rail.PythonOperator(
            task_id='get_lastsync_time',
            python_callable=lambda: custom_methods.read_lastsync_time(config)
        )

        get_new_or_updated_contacts_in_xero = rail.XeroAPIOperator(
            task_id='get_new_or_updated_contacts_in_xero',
            xero_conn_id=config.xero_conn_id,
            endpoint='/api.xro/2.0/Contacts',
            request_method='GET',
            filters='?where=IsCustomer==true AND ContactStatus=="ACTIVE"',
            modified_since="{{result('get_lastsync_time').last_synctime}}",
            paginate=True
        )

        has_xero_contacts_data = rail.IfOperator(
            task_id='has_xero_contacts_data',
            test=lambda: rail.result('get_new_or_updated_contacts_in_xero') and rail.result(
                'get_new_or_updated_contacts_in_xero').get('Contacts'),
            yes_task='get_all_clients',
            no_task='delete_this_dagrun'
        )

        get_all_clients = rail.RepliconServiceOperator(
            task_id='get_all_clients',
            endpoint='/services/ClientService1.svc/GetAllClients'
        )

        get_all_countries = rail.RepliconServiceOperator(
            task_id='get_all_countries',
            endpoint='/services/InternationalizationService1.svc/GetAllCountries'
        )

        parse_xero_data = rail.PythonOperator(
            task_id='parse_xero_data',
            python_callable=response_filter.parse_xero_contacts
        )

        trigger_client_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_client_child_dag',
            thread_pool_size=4,
            retries=0,
            items=lambda: rail.result('parse_xero_data'),
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            trigger_dag_id=config.child_dag_id,
            conf=lambda dag_run, item: {
                **dict(item.items()),
                'currency': config.currency,
            }
        )

        wait_for_client_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_client_child_dag',
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            dag_runs='{{ result("trigger_client_child_dag") }}'
        )

        gather_client_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_client_error',
            dag_runs="{{ result('trigger_client_child_dag') }}",
            dagrun_task_id='catch_client_error',
            flatten=True
        )

        is_client_error = rail.IfOperator(
            task_id='is_client_error',
            test="{{ result('gather_client_error') | length > 0 }}",
            yes_task='fail_client_error',
            no_task='update_lastsync_time'
        )

        fail_client_error = rail.FailOperator(
            task_id='fail_client_error',
            message="{{ result('gather_client_error') | map_to_attr('error') | join('|') }}"
        )

        update_lastsync_time = rail.PythonOperator(
            task_id='update_lastsync_time',
            python_callable=lambda: custom_methods.write_lastsync_time(config)
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        finish = rail.EmptyOperator(
            task_id='finish',
            trigger_rule='all_done'
        )

        get_lastsync_time >> get_new_or_updated_contacts_in_xero >> has_xero_contacts_data
        has_xero_contacts_data >> rail.Label('Yes') >> get_all_clients >> get_all_countries >> parse_xero_data >> \
            trigger_client_child_dag >> wait_for_client_child_dag >> gather_client_error >> is_client_error
        is_client_error >> rail.Label('Yes') >> fail_client_error >> finish
        is_client_error >> rail.Label('No') >> update_lastsync_time >> finish
        has_xero_contacts_data >> rail.Label('No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
