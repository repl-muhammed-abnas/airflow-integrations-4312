
from datetime import timedelta
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'locktoncompanies_client_import_enable_client_child_{config.instance}',
        description=f'Lockton_EnableClient {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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
            no_task='activate_client'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='activate_client',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        activate_client=rail.RepliconServiceOperator(
            task_id='activate_client',
            endpoint="/services/ClientService1.svc/Activate",
            data={
                "clientUri": "{{ dag_run.conf.uri }}"
            }
        )

        add_log_client_enabled=rail.WriteLogOperator(
            task_id='add_log_client_enabled',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Enabled",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "locktonmasterid": "{{ dag_run.conf.code }}",
                "clientname": "{{ dag_run.conf.name }}",
                "status": "Enabled",
                "details": "Enable Client - {{ dag_run_ecid() }} - Client activated - {{ dag_run.conf.name }}"
            }
        )

        catch_and_log_error=rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Error",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "locktonmasterid": "{{ dag_run.conf.ClientCode }}",
                "clientname": "{{ dag_run.conf.ClientName }}",
                "status": "Error",
                "details": "Enable Client - {{ dag_run_ecid() }} - {{get_error_message()}}"
            }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> activate_client
        activate_client >> add_log_client_enabled >> catch_and_log_error

    return dag

rail.for_each_instance(create_dag)
