from datetime import timedelta, datetime, timezone
from airflow.models import Variable, DagRun
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_schedule_data_import_sync_logs_{config.instance}',
        description=f'Technicolor Schedule data sync logs - V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.log_generation_dag_interval,
        max_active_runs=1,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_log_dagruns_to_process'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_log_dagruns_to_process',
            end_task='log_to_sumo',
        )

        def get_dagruns_to_process():
            current_time = datetime.now(timezone.utc)
            lookup_timestamp_value = Variable.get(
                config.lookup_log_timestamp_var, default_var=None)

            query_execution_start_date = datetime.fromisoformat(lookup_timestamp_value) if lookup_timestamp_value else (
                current_time - timedelta(hours=3))

            Variable.set(config.lookup_log_timestamp_var,
                         current_time.isoformat())

            dag_runs = []
            for run in DagRun.find(dag_id=f'technicolorg3_schedule_data_import_child_{config.instance}', state='success', execution_start_date=query_execution_start_date):
                dag_runs.append(run.id)

            return dag_runs

        get_log_dagruns_to_process = rail.PythonOperator(
            task_id='get_log_dagruns_to_process',
            python_callable=get_dagruns_to_process
        )

        is_log_dagruns_present = rail.IfOperator(
            task_id='is_log_dagruns_present',
            test="{{ result('get_log_dagruns_to_process') | length > 0 }}",
            yes_task='get_import_logs',
        )

        get_import_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='get_import_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_log_dagruns_to_process') }}",
            dagrun_task_id='format_logs',
            flatten=True
        )

        has_any_data = rail.IfOperator(
            task_id="has_any_data",
            test="{{ result('get_import_logs') | length > 0 }}",
            yes_task='get_log_info',
        )

        get_log_info = rail.PythonOperator(
            task_id='get_log_info',
            python_callable=lambda:  {
                    "time": datetime.now(timezone.utc).strftime("%m/%d/%YT%H:%M:%S"),
                    "timeinHHMMSS":  datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"),
                    "inputfilename":  datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_Scheduledata_sync_logs.csv",
                    "checkerror": bool(list(filter(lambda x: x['properties']['status'] == 'Error', rail.result('get_import_logs')))),
                    "checkexpection": bool(list(filter(lambda x: x['properties']['status'] == 'Exception', rail.result('get_import_logs')))),
                    "subjectline": "completed with errors" if bool(list(filter(lambda x: x['properties']['status'] == 'Error', rail.result('get_import_logs')))) else
                                   "completed with exceptions" if bool(list(filter(lambda x: x['properties']['status'] == 'Exception', rail.result('get_import_logs')))) else
                                   "completed successfully",
                    "body": "<br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>"
                if bool(list(filter(lambda x: x['properties']['status'] == 'Error', rail.result('get_import_logs')))) else
                "<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>"
            }
        )

        # pylint: disable=too-many-return-statements
        def get_attr_value(data, attr_path, default_value=None):
            if not data or not attr_path:
                return default_value

            cur_attr_path = attr_path.split(
                '.')[0] if '.' in attr_path else attr_path
            child_attr_path = '.'.join(attr_path.split('.')[1:]) if '.' in attr_path and len(
                attr_path.split('.')) > 1 else None
            if not cur_attr_path:
                return default_value

            if isinstance(data, dict) and cur_attr_path in data:
                if not child_attr_path:
                    return data[cur_attr_path]
                return get_attr_value(data[cur_attr_path], child_attr_path, default_value)

            if cur_attr_path.isnumeric() and isinstance(data, list) and int(cur_attr_path) < len(data):
                if not child_attr_path:
                    return data[int(cur_attr_path)]
                return get_attr_value(data[int(cur_attr_path)], child_attr_path, default_value)
            return default_value

        create_csv_file = rail.WriteCSVFileOperator(
            task_id='create_csv_file',
            source="{{ result('get_import_logs') | to_json() }}",
            header=['Mill_MPC',
                    'Project Number',
                    'Title',
                    'Role',
                    'Service',
                    'Description',
                    'Starttime',
                    'Endtime',
                    'Duration',
                    'Resourcename',
                    'Resourcescheduleservice ID',
                    'fd_status',
                    'referenceid',
                    'Status',
                    'details'],
            row=lambda item: [
                item['properties']['mill_mpc'],
                get_attr_value(item['properties']
                               ['projectnumber|title'].split("|"), '0'),
                get_attr_value(item['properties']
                               ['projectnumber|title'].split("|"), '1'),
                get_attr_value(item['properties']
                               ['role|service'].split("|"), '0'),
                get_attr_value(item['properties']
                               ['role|service'].split("|"), '1'),
                item['properties']['description'],
                get_attr_value(item['properties']
                               ['starttime|endtime'].split("|"), '0'),
                get_attr_value(item['properties']
                               ['starttime|endtime'].split("|"), '1'),
                get_attr_value(
                    item['properties']['duration|resourcename|resourcescheduleserviceid'].split("|"), '0'),
                get_attr_value(
                    item['properties']['duration|resourcename|resourcescheduleserviceid'].split("|"), '1'),
                get_attr_value(
                    item['properties']['duration|resourcename|resourcescheduleserviceid'].split("|"), '2'),
                item['properties'].get('fd_status'),
                item['ecid'],
                item['properties']['status'],
                item['properties']['message'],
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_file')}}",
            output_file_name='{{ result("get_log_info").inputfilename }}',
            expires_in_seconds=7*24*60*60,
        )

        send_mail = rail.EmailOperator(
            task_id='send_mail',
            to=config.tenant_email,
            bcc="{%- if not result('get_log_info').checkerror  -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Schedule Data Sync  - " + result("get_log_info").subjectline + " - " + result("get_log_info").time }}',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
            Please find the below link to download the Schedule Data sync logs for reference. <br /> <br /><a href="{{result('generate_download_link')}}">Download log file</a> </p>
            <p><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p> {{result('get_log_info').body}} ''',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> get_log_dagruns_to_process

        get_log_dagruns_to_process >> is_log_dagruns_present >> rail.Label('yes') >>\
            get_import_logs >> has_any_data >> rail.Label('yes') >> get_log_info >> \
            create_csv_file >> generate_download_link >> send_mail >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
