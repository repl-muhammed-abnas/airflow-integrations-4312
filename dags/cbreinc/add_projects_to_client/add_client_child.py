from datetime import timedelta, datetime
import rail
from airflow.models import Variable

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"cbreinc_add_client_child_{config.instance}",
        description=f"CBREInc Add Client - Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        finish = rail.EmptyOperator(task_id='finish')

        can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var='').lower() == 'true',
                yes_task='batch_task',
                no_task='get_active_client_list'
            )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_active_client_list',
            end_task='finish',
        )

        # pylint: disable=line-too-long
        get_active_client_list = rail.RepliconServiceOperator(
            task_id="get_active_client_list",
            endpoint="/services/ClientService1.svc/GetActiveClients",
            response_filter=lambda response: [{"client":{"uri":client["uri"],"name":None,"code":None,"parameterCorrelationId":None},"costAllocationPercentage":0} for client in response.json()["d"] if client["uri"]]
        )

        apply_client_to_project = rail.RepliconServiceOperator(
            task_id="apply_client_to_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: {
                "target": {
                    "uri": dag_run.conf['project_uri'],
                    "name": None,
                    "code": None,
                    "parameterCorrelationId": None
                },
                "modifications": {
                    "clientBillingAllocationMethodToApply": "urn:replicon:client-billing-allocation-method:user-specified",
                    "clientAssignmentsSchedulesToApply": {"clients":rail.result('get_active_client_list')}
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": str(datetime.now())
                }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label('No') >> get_active_client_list >> apply_client_to_project >> finish >> catch_and_log_errors
    return dag

rail.for_each_instance(create_child_dag)
