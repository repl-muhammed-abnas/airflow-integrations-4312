from datetime import timedelta
import rail
from airflow.models import Variable
from guidehouse.peoplesoft_project_import_v1.utils import request_payload


def create_cost_center_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.create_division_dag_id,
        description='Guidehouse PeopleSoft Project Import - Create Divisions/Cost Centers',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name,
                default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='check_division_status'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='check_division_status',
            end_task='catch_and_log_errors',
        )

        check_division_status = rail.PythonOperator(
            task_id='check_division_status',
            python_callable=lambda dag_run: {
                'existing_division': {
                    'name': dag_run.conf.get('existing_name'),
                    'description': dag_run.conf.get('existing_description'),
                    'enabled': dag_run.conf.get('existing_enabled')
                } if dag_run.conf.get('existing_name') else None
            }
        )

        should_create_cost_center = rail.IfOperator(
            task_id='should_create_cost_center',
            test=lambda: not rail.result('check_division_status')['existing_division'],
            yes_task='create_cost_center',
            no_task='catch_and_log_errors'
        )

        create_cost_center = rail.RepliconServiceOperator(
            task_id='create_cost_center',
            endpoint="/services/DivisionService1.svc/CreateDivisionOrApplyModification",
            data=request_payload.build_create_cost_center_payload
        )

        log_cost_center_success = rail.WriteLogOperator(
            task_id="log_cost_center_success",
            log="{{ dag_run.conf.log }}",
            severity="Info",
            message="Division created successfully",
            properties=lambda dag_run: {
                "client_id": "",
                "client_name": "",
                "project_id": "",
                "project_name": "",
                "task_code": "",
                "task_name": "",
                "action": "Add (Cost Center)",
                "status": "Success",
                "details": (
                    f"Cost Center '{dag_run.conf['cost_center_name']}' created successfully"
                    + (f" (code: {dag_run.conf['cost_center_code']})"
                       if dag_run.conf.get('cost_center_code') and dag_run.conf['cost_center_code'].strip()
                       else "")
                ),
                "enforce_value": ""
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            trigger_rule='one_failed',
            log="{{ dag_run.conf.log }}",
            severity="Exception",
            message="Division operation failed",
            properties=lambda dag_run: {
                "client_id": "",
                "client_name": "",
                "project_id": "",
                "project_name": "",
                "task_code": "",
                "task_name": "",
                "action": "Cost Center Processing",
                "status": "Error",
                "details": f"Failed to process cost center '{dag_run.conf['cost_center_name']}' (code: {dag_run.conf['cost_center_code']}): {{{{ get_error_message() }}}}",
                "enforce_value": ""
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> check_division_status
        check_division_status >> should_create_cost_center
        should_create_cost_center >> rail.Label("Yes") >> create_cost_center >> log_cost_center_success >> catch_and_log_errors
        should_create_cost_center >> rail.Label("No") >> catch_and_log_errors

    return dag

rail.for_each_instance(create_cost_center_dag)
