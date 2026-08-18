from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.utils.response_filter import get_user_uri_reports_central_queue


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_supervisordirectreport_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_check_supervisor_direct_reports_crv2.0_{config.instance}',
        description=f'Adtalem check Supervisor direct reports Production CRV2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_direct_reports'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_direct_reports',
            end_task='dagrun_log_to_sumo',
        )

        get_direct_reports = rail.RepliconServiceOperator(
            task_id='get_direct_reports',
            endpoint="/services/UserService1.svc/GetDirectReportsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "userStatusOptionUri": "urn:replicon:user-status-option:include-only-enabled-users"
            }
        )

        get_required_supervisoruseruri = rail.RepliconServiceOperator(
            task_id='get_required_supervisoruseruri',
            endpoint='/services/UserListService1.svc/GetData',
            data={
                'page': '1',
                'pagesize': '100',
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name'
                ],
                'sort': [],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': 'central.queue'
                        }
                    }
                }
            },
            data_handler=get_user_uri_reports_central_queue
        )

        is_direct_reports = rail.IfOperator(
            task_id='is_direct_reports',
            test=lambda: len(rail.result('get_direct_reports')) > 0,
            yes_task='trigger_supervisor_directreports_child',
            no_task='dagrun_log_to_sumo'
        )

        trigger_supervisor_directreports_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_supervisor_directreports_child',
            retries=0,
            items=lambda: rail.result('get_direct_reports'),
            trigger_dag_id=f'adtalem_userimport_supervisor_direct_reports_crv2.0_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                'supervisorname': dag_run.conf['supervisorname'],
                'supervisoruri': rail.result('get_required_supervisoruseruri'),
                'useruri': item['uri'],
                'user_displaytext': item['displayText']
            }
        )

        wait_for_supervisor_directreports_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_directreports_child',
            dag_runs="{{ result('trigger_supervisor_directreports_child') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> get_direct_reports >> get_required_supervisoruseruri >> is_direct_reports

        is_direct_reports >> rail.Label(
            'Yes') >> trigger_supervisor_directreports_child >> wait_for_supervisor_directreports_child >> \
            dagrun_log_to_sumo
        is_direct_reports >> rail.Label(
            'No') >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_supervisordirectreport_child_dag)
