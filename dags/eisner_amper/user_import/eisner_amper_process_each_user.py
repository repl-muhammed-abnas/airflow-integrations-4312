import rail
from eisner_amper.user_import.utils import response_filter, request_payload
from datetime import datetime, timedelta
from airflow.models import Variable

# pylint: disable=too-many-statements


def create_child_dag(config):
    update_dags = []

    for idx in range(0, config.BATCH_COUNT):

        with rail.create_airflow_dag(
            dag_id=f"{config.process_each_user_dag_id}_batch_{idx+1}",
            description=f"Eisner Amper user sync Child {config.instance}",
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_child
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='get_user_data'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='get_user_data',
                end_task='catch_and_log_errors',
            )

            get_user_data = rail.RepliconServiceOperator(
                task_id='get_user_data',
                endpoint='/services/UserListService1.svc/GetData',
                data=request_payload.get_user_payload,
                response_filter=response_filter.check_client_data
            )

            is_uri_present = rail.IfOperator(
                task_id='is_uri_present',
                test=lambda: bool(rail.result('get_user_data')),
                yes_task='is_workagreementstatus_and_uri_present',
                no_task='is_workagreementstatus_present'
            )

            is_workagreementstatus_and_uri_present = rail.IfOperator(
                task_id='is_workagreementstatus_and_uri_present',
                test=lambda dag_run: dag_run.conf['workagreementstatus'] == "0",
                yes_task='trigger_disable_user',
                no_task='trigger_update_user'
            )

            trigger_disable_user = rail.TriggerDagRunForEachItemOperator(
                task_id="trigger_disable_user",
                items=lambda dag_run: [dag_run.conf],
                trigger_dag_id=config.disble_user_dag_id,
                conf=lambda item, dag_run: {**dict(item.items()), "uri": rail.result(
                    "get_user_data")[0]['uri'], "log": dag_run.conf['log']},
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                retries=0
            )

            wait_disable_user = rail.WaitForDagRunsSensor(
                task_id="wait_disable_user",
                dag_runs="{{result('trigger_disable_user')}}"
            )

            trigger_update_user = rail.TriggerDagRunForEachItemOperator(
                task_id="trigger_update_user",
                items=lambda dag_run: [dag_run.conf],
                trigger_dag_id=lambda dag_run: f"{config.update_user_dag_id}_batch_{dag_run.conf['batch_num']+1}",
                conf=lambda item, dag_run: {**dict(item.items()), "uri": rail.result(
                    "get_user_data")[0]['uri'], "log": dag_run.conf['log']},
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                retries=0
            )

            wait_update_user = rail.WaitForDagRunsSensor(
                task_id="wait_update_user",
                dag_runs="{{result('trigger_update_user')}}"
            )

            is_workagreementstatus_present = rail.IfOperator(
                task_id='is_workagreementstatus_present',
                test=lambda dag_run: dag_run.conf['workagreementstatus'] == "0",
                yes_task='log_skipped',
                no_task='trigger_add_user'
            )

            log_skipped = rail.WriteLogOperator(
                task_id='log_skipped',
                message="Skipped",
                log='{{dag_run.conf.log}}',
                severity='Success',
                properties={
                    'employeeid': "{{dag_run.conf.personexternalid}}",
                    'loginname': "{{dag_run.conf.name}}",
                    'action': "Validation",
                    'status': "Skipped",
                    'details': "Skipped",
                    'jobid': "{{dag_run_ecid()}}",
                    'childjobid': '',
                }
            )

            trigger_add_user = rail.TriggerDagRunForEachItemOperator(
                task_id="trigger_add_user",
                items=lambda dag_run: [dag_run.conf],
                trigger_dag_id=lambda dag_run: f"{config.add_user_dag_id}_batch_{dag_run.conf['batch_num']+1}",
                conf=lambda item, dag_run: {
                    **dict(item.items()), "log": dag_run.conf['log']},
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                retries=0
            )

            wait_add_user = rail.WaitForDagRunsSensor(
                task_id="wait_add_user",
                dag_runs="{{result('trigger_add_user')}}"
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                trigger_rule='one_failed',
                log='{{dag_run.conf.log}}',
                severity='Error',
                message='{{ get_error_message() }}',
                properties={
                    'employeeid': "{{dag_run.conf.personexternalid}}",
                    'loginname': "{{dag_run.conf.name}}",
                    'action': "Validation",
                    'status': "Error",
                    'details': '{{ get_error_message() }}',
                    'jobid': "{{dag_run_ecid()}}",
                    'childjobid': '',
                },
            )

            log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id='log_to_sumo',
                sumo_conn_id='sumologic-dagrunlogger',
                trigger_rule='all_done'
            )

            can_run_batch_task >> rail.Label(
                'Yes') >> batch_task >> catch_and_log_errors

            can_run_batch_task >> rail.Label(
                'No') >> get_user_data

            get_user_data >> is_uri_present >> rail.Label("Yes") >> is_workagreementstatus_and_uri_present >> rail.Label("Yes") >> trigger_disable_user >> wait_disable_user \
                >> catch_and_log_errors

            is_workagreementstatus_and_uri_present >> rail.Label(
                "No") >> trigger_update_user >> wait_update_user >> catch_and_log_errors

            is_uri_present >> rail.Label("No") >> is_workagreementstatus_present >> rail.Label(
                "Yes") >> log_skipped >> catch_and_log_errors

            is_workagreementstatus_present >> rail.Label(
                "No") >> trigger_add_user >> wait_add_user >> catch_and_log_errors >> log_to_sumo

    update_dags.append(dag)

    return dag


rail.for_each_instance(create_child_dag)
