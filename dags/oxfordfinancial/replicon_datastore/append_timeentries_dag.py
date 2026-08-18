from datetime import timedelta
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/oxfordfinancial/replicon_datastore/config.py


def create_child_append_timeentries_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'oxfordfinancial_replicon_datastore_append_timeentries_{config.instance}',
        description=f'Oxfordfinancial Append Time Entries - Child V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_append_time_entries,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_approvalhistory_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_approvalhistory_data',
            end_task='log_dagrun_to_sumo'
        )

        def get_approvalhistory(response):
            history = response['history']
            approvalhistoryoutput = list(map(lambda item: {
                'action': item['action']['displayText'],
                'timestamp': f"{item['timestamp']['month']}/{item['timestamp']['day']}/{item['timestamp']['year']}",
                'actionby': item['authority']['displayText'],
                'loginname': item['approvalAgent']['user']['loginName'] if item['approvalAgent']['user'] else ''
            }, history)) if history else []
            submit_entries = [
                x for x in approvalhistoryoutput if x['action'] == 'Submit']
            approve_entries = [
                x for x in approvalhistoryoutput if x['action'] == 'Approve']
            return {
                'submittedon': submit_entries[-1]['timestamp'],
                'approvedby': approve_entries[-1]['actionby'],
                'approvedon': approve_entries[-1]['timestamp'],
                'loginname': submit_entries[-1]['loginname']
            }
        get_approvalhistory_data = rail.RepliconServiceOperator(
            task_id='get_approvalhistory_data',
            endpoint='/services/TimesheetApprovalService1.svc/GetTimesheetApprovalDetails2',
            data=lambda dag_run: {
                'timesheetUri': dag_run.conf['timesheeturi']
            },
            data_handler=get_approvalhistory
        )

        def get_process_append_time_entries_subchild(item, index, dag_run):
            return {
                **{k: v for k, v in item.items() if k not in ('timeentryid')},
                **{
                    'timeentryid': int(item['timeentryid']) + index + 1,
                    'log': dag_run.conf['log']
                },
                **dict(rail.result('get_approvalhistory_data').items())
            }
        process_append_time_entries_subchild = rail.TriggerDagRunForEachItemOperator(
            task_id='process_append_time_entries_subchild',
            items=lambda dag_run: dag_run.conf['timesheetdata'],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'oxfordfinancial_replicon_datastore_append_timeentries_subchild_{config.instance}',
            conf=get_process_append_time_entries_subchild
        )

        wait_for_process_append_time_entries_subchild = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_append_time_entries_subchild',
            dag_runs='{{ result("process_append_time_entries_subchild") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_dagrun_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_approvalhistory_data >> process_append_time_entries_subchild >> \
            wait_for_process_append_time_entries_subchild >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_child_append_timeentries_dag)
