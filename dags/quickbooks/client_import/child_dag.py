from datetime import timedelta
import uuid
import rail
from airflow.models import Variable


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_quickbooks_online_{config.region.replace('-', '_')}_client_import_child_dag_{config.instance}",
        description=f'QuickBooks Online {config.region} Client Import Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_update_client'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_update_client',
            end_task='catch_client_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def get_apply_modification_client_param(dag_run):
            null = None
            return {
                'target': {'name': dag_run.conf['client_name']} if not dag_run.conf['is_new_client'] else null,
                'modifications': {
                    'nameToApply': {
                        'value': dag_run.conf['client_name']
                    },
                    'descriptionToApply': null,
                    'statusToApply': True,
                    'clientContactToApply': {'value': dag_run.conf['client_contact']},
                    'clientAddressToApply': {
                        'address': {
                            'value': dag_run.conf['client_address']
                        },
                        'city': {'value': dag_run.conf['client_city']} if dag_run.conf['client_city'] else null,
                        'stateProvince': {'value': dag_run.conf['client_state']} if dag_run.conf['client_state'] else null,
                        'country': {'value': {'uri': dag_run.conf['client_country']}} if dag_run.conf['client_country'] else null,
                        'zipPostalCode': {'value': dag_run.conf['client_zip']} if dag_run.conf['client_zip'] else null,
                        'phoneNumber': {'value': dag_run.conf['client_phone_number']} if dag_run.conf['client_phone_number'] else null,
                        'email': {'value': dag_run.conf['client_email']} if dag_run.conf['client_email'] else null,
                        'faxNumber': {'value': dag_run.conf['client_fax']} if dag_run.conf['client_fax'] else null,
                        'website': {'value': dag_run.conf['client_website']} if dag_run.conf['client_website'] else null
                    },
                    'billingAddressToApply': {
                        'address': {
                            'value': dag_run.conf['billing_address']
                        },
                        'city': {'value': dag_run.conf['billing_city']} if dag_run.conf['billing_city'] else null,
                        'stateProvince': {'value': dag_run.conf['billing_state']} if dag_run.conf['billing_state'] else null,
                        'country': {'value': {'uri': dag_run.conf['billing_country']}} if dag_run.conf['billing_country'] else null,
                        'zipPostalCode': {'value': dag_run.conf['billing_zip']} if dag_run.conf['billing_zip'] else null
                    }
                },
                'clientModificationOptionUri': 'urn:replicon:client-modification-option:save',
                'unitOfWorkId': str(uuid.uuid4()),
            }
        create_update_client = rail.RepliconServiceOperator(
            task_id='create_update_client',
            endpoint='/services/ClientService1.svc/CreateClientOrApplyModifications',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=get_apply_modification_client_param
        )

        def get_downstreamtasks_error(client_name, error_message):
            return {
                'error': f'Error with {client_name} - {error_message}'
            }
        catch_client_error = rail.PythonOperator(
            task_id='catch_client_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.client_name }}',
                     '{{ get_error_message() }}']
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> rail.Label(
                'on Error') >> catch_client_error

        can_run_batch_task >> rail.Label(
            'No') >> create_update_client >> rail.Label(
                'On Error') >> catch_client_error

    return dag


rail.for_each_instance(create_child_dag)
