from datetime import timedelta
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/oxfordfinancial/replicon_datastore/config.py


def create_child_timeentry_dynamic_email_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'oxfordfinancial_replicon_datastore_timeentry_dynamic_email_{config.instance}',
        description=f'Oxfordfinancial Time entry Dynamic Email Child V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='format_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='format_logs',
            end_task='log_dagrun_to_sumo'
        )

        def do_format_logs():
            def load_records(log_artifact):
                try:
                    logs = rail.load_all_records(log_artifact)
                    return logs
                except:  # pylint: disable=bare-except
                    return []
            dag_run = rail.get_current_context()['dag_run']
            log_artifacts = dag_run.conf['child_logs']
            log_records = []
            if log_artifacts:
                for log in log_artifacts:
                    each_log_records = load_records(log)
                    if each_log_records:
                        log_records.extend(each_log_records)
            formatted_logs = []
            for item in log_records:
                split_timedata = item['properties']['timedata'].split('|')
                formatted_logs.append({
                    'timeentryid': split_timedata[0],
                    'clientid': split_timedata[1],
                    'firstname': split_timedata[2],
                    'lastname': split_timedata[3],
                    'middlename': split_timedata[4],
                    'initials': split_timedata[5],
                    'task': split_timedata[6].replace('<;>', '|'),
                    'servicecode': split_timedata[7],
                    'department': split_timedata[8],
                    'project': split_timedata[9],
                    'timehours': split_timedata[10],
                    'timedate': split_timedata[11],
                    'timerange': split_timedata[12],
                    'updatedate': split_timedata[13],
                    'submitteddate': split_timedata[14],
                    'updateuser': split_timedata[15],
                    'notes': split_timedata[16].replace('<;>', '|'),
                    'clienthouseholdID': split_timedata[17],
                    'personcontactID': split_timedata[18],
                    'advisorcontactID': split_timedata[19],
                    'oxfordcompany': split_timedata[20],
                    'oxfordcompanyid': split_timedata[21],
                    'timesheeturi': split_timedata[22],
                    'timeoffhours': split_timedata[23],
                    'timeofftype': split_timedata[24].replace('<;>', '|'),
                    'approvaldatetime': split_timedata[25],
                    'approvername': split_timedata[26]
                })
            return sorted(formatted_logs, key=lambda x: int(x['timeentryid']))

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            python_callable=do_format_logs
        )

        should_process_logs = rail.IfOperator(
            task_id="should_process_logs",
            test="{{ result('format_logs') | length > 0 }}",
            yes_task="render_extract_data",
            no_task="log_dagrun_to_sumo"
        )

        render_extract_data = rail.WriteCSVFileOperator(
            task_id='render_extract_data',
            source=lambda: rail.result('format_logs'),
            header=[
                'timeentryid',
                'clientid',
                'firstname',
                'lastname',
                'middlename',
                'initials',
                'task',
                'servicecode',
                'department',
                'project',
                'timehours',
                'timedate',
                'timerange',
                'updatedate',
                'submitteddate',
                'updateuser',
                'notes',
                'clienthouseholdID',
                'personcontactID',
                'advisorcontactID',
                'oxfordcompany',
                'oxfordcompanyid',
                'timesheeturi',
                'timeoffhours',
                'timeofftype',
                'approvaldatetime',
                'approvername'
            ],
            row=[
                '{{ item.timeentryid }}',
                '{{ item.clientid }}',
                '{{ item.firstname }}',
                '{{ item.lastname }}',
                '{{ item.middlename }}',
                '{{ item.initials }}',
                '{{ item.task }}',
                '{{ item.servicecode }}',
                '{{ item.department }}',
                '{{ item.project }}',
                '{{ item.timehours }}',
                '{{ item.timedate }}',
                '{{ item.timerange }}',
                '{{ item.updatedate }}',
                '{{ item.submitteddate }}',
                '{{ item.updateuser }}',
                '{{ item.notes }}',
                '{{ item.clienthouseholdID }}',
                '{{ item.personcontactID }}',
                '{{ item.advisorcontactID }}',
                '{{ item.oxfordcompany }}',
                '{{ item.oxfordcompanyid }}',
                '{{ item.timesheeturi }}',
                '{{ item.timeoffhours }}',
                '{{ item.timeofftype }}',
                '{{ item.approvaldatetime }}',
                '{{ item.approvername }}'
            ]
        )

        update_extract_data = rail.SFTPUploadFileOperator(
            task_id='update_extract_data',
            content="{{ result('render_extract_data') }}",
            remote_filepath=config.extract_filepath
        )

        send_export_complete_email = rail.EmailOperator(
            task_id='send_export_complete_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} Replicon to Data Store Time Data Export - Success - {{ dag_run.conf.date }}',
            html_content="/email_template/export_complete.html"
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_dagrun_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> format_logs >> should_process_logs

        should_process_logs >> rail.Label(
            'Yes') >> render_extract_data >> update_extract_data >> send_export_complete_email >> log_dagrun_to_sumo

        should_process_logs >> rail.Label(
            'No') >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_child_timeentry_dynamic_email_dag)
