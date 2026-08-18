from datetime import timedelta
from airflow.models import Variable
import rail
from assuredpartnersinc.user_import_v4.utils import request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.process_each_user_dag_id,
        description=f'Assured Partners User Import Process Each User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_each_user,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_user_log',
            end_task='catch_and_log_errors',
        )

        create_user_log = rail.CreateLogOperator(
            task_id="create_user_log"
        )

        get_user_data_based_on_login_name = rail.RepliconServiceOperator(
            task_id="get_user_data_based_on_login_name",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": null,
                        "loginName": "{{dag_run.conf.EmplID_Login}}",
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: [] if response == [None] else response
        )

        is_same_login_name_already_available = rail.IfOperator(
            task_id='is_same_login_name_already_available',
            test=lambda: bool(rail.result(
                'get_user_data_based_on_login_name')),
            yes_task='trigger_dag_run_assured_partners_user_update',
            no_task='trigger_dag_run_assured_partners_user_add'
        )

        trigger_dag_run_assured_partners_user_update = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_assured_partners_user_update',
            retries=0,
            trigger_dag_id=config.child_update_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: request_payload.get_add_update_dag_conf(
                dag_run, 'update', config)
        )

        wait_for_trigger_dag_run_assured_partners_user_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_dag_run_assured_partners_user_update',
            dag_runs='{{ result("trigger_dag_run_assured_partners_user_update") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        trigger_dag_run_assured_partners_user_add = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_assured_partners_user_add',
            retries=0,
            trigger_dag_id=config.child_add_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: request_payload.get_add_update_dag_conf(
                dag_run, 'add', config)
        )

        wait_for_trigger_dag_run_assured_partners_user_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_dag_run_assured_partners_user_add',
            dag_runs='{{ result("trigger_dag_run_assured_partners_user_add") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{result("create_user_log")}}',
            trigger_rule='one_failed',
            severity='Error',
            message='na',
            properties=lambda dag_run: {
                "action": "",
                "status": 'Error',
                "job_id": dag_run.conf['parentjobid'],
                "details": rail.render_template("{{ get_error_message() }}"),
                "username": rail.render_template("{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}")
            },
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_user_log

        create_user_log >> get_user_data_based_on_login_name >> is_same_login_name_already_available

        is_same_login_name_already_available >> rail.Label(
            "Yes") >> trigger_dag_run_assured_partners_user_update >> wait_for_trigger_dag_run_assured_partners_user_update >> catch_and_log_errors
        is_same_login_name_already_available >> rail.Label(
            "No") >> trigger_dag_run_assured_partners_user_add >> wait_for_trigger_dag_run_assured_partners_user_add >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)
