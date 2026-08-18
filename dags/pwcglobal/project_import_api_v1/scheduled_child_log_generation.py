import rail
from pwcglobal.project_import_api_v1 import python_callable_method
from pwcglobal.project_import_api_v1.task.process_logs import process_logs

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/project_import_api_v1/config.py


# pylint:disable = too-many-statements
def create_child_log_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pwc_project_import_child_log_{config.instance}_v1',
        description=f'Projectimport_Logs-Scheduled-Integrationsuseast {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_scheduled_log_generation_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        def translate_row(row):
            dag_run_conf = python_callable_method.get_dag_run_conf()
            if row and (dag_run_conf['sender_id'] == row['SenderID']):
                # pylint: disable=unnecessary-comprehension
                return {
                    **{k: v for k, v in row.items()}
                }
            return None

        filter_sender_logs = rail.DataAdaptorOperator(
            task_id='filter_sender_logs',
            source="{{ dag_run.conf.project_final_logs }}",
            data=translate_row,
            columns=[
                'Date',
                'FileName',
                'Level',
                'Message',
                'EntityId',
                'EntityName',
                'EntityType',
                'Application',
                'SenderID',
                'Export',
                'Ecid'
            ]
        )

        should_process_sender_logs = rail.IfOperator(
            task_id="should_process_sender_logs",
            test="{{ result('filter_sender_logs', 'length') > 0 }}",
            yes_task="process_sender",
            no_task="finish"
        )

        process_sender = rail.EmptyOperator(
            task_id='process_sender')

        (get_application_log, send_import_complete_email) = process_logs(config)

        finish = rail.EmptyOperator(
            task_id="finish")

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.dagrun_log_conn_id,
            trigger_rule='all_done',
            extra_info={
                'Companykey': '{{ get_company_key() }}',
                'Recordcount': "{{ result('filter_sender_logs', 'length') }}",
                'SenderID': '{{ dag_run.conf.sender_id }}',
                'LogFileName': "{{ result('write_sftp_log_filename') \
                    if get_task_state('write_sftp_log_filename') == 'success' | is_truthy else 'nil' }}"
            }
        )

        filter_sender_logs >> should_process_sender_logs

        should_process_sender_logs >> rail.Label(
            "Yes") >> process_sender >> get_application_log

        send_import_complete_email >> log_dagrun_to_sumo

        should_process_sender_logs >> rail.Label(
            "No") >> finish

        return dag


rail.for_each_instance(create_child_log_airflow_dag)
