from datetime import datetime

import airflow
from airflow.utils.edgemodifier import Label

import rail
from system.sftp_monitoring_alerts import config

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/sftp_monitoring_alerts/config.py

with airflow.DAG(
    dag_id='system_sftp_monitoring_alerts_child',
    description='System SFTP Monitoring alerts child v0.1',
    schedule=None,
    max_active_runs=config.max_active_runs_child_dag,
    tags=['system'],
    start_date=datetime(2022, 1, 1),
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
    default_args={
        'owner': 'system',
    },
) as dag:

    rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

    def get_conf():
        return rail.get_current_context()['dag_run'].conf

    def get_email_content(sftp_files):
        total_files_count = 0
        html_content = '''<table style="border:1px solid black" border="1">
                <tr>
                    <th>Path</th>
                    <th>File Name</th>
                    <th>Last Modified</th>
                    <th>Size(Bytes)</th>
                </tr>'''
        for item in sftp_files:
            files = item['files']
            total_files_count = total_files_count + len(files)
            for file in files:
                # file = {'size': 5819, 'type': 'file', 'modify': '20211028113048', 'name': 'ReplicontoC110282021T0412.xml'}
                html_content = html_content + \
                    f'''<tr>
                        <td> {item['path']} </td>
                        <td> {file['name']} </td>
                        <td> {datetime.strptime(file['modify'], '%Y%m%d%H%M%S')} </td>
                        <td> {file['size']} </td>
                    </tr>'''
        html_content = html_content + '</table>'
        return {'count': total_files_count, 'html_content': html_content}

    list_sftp_files = rail.SFTPListFilesOperator(
        task_id='list_sftp_files',
        paths=lambda: get_conf()['paths'],
        sftp_conn_id='{{ dag_run.conf.sftp_conn_id}}'
    )

    has_files = rail.IfOperator(
        task_id='has_files',
        test='{{ result("list_sftp_files") | length > 0 }}',
        yes_task='filter_count_threshold_files',
        no_task='finish',
    )

    def filter_files_based_on_count():
        paths = list(filter(lambda x: len(rail.result('list_sftp_files')[x]) >
                            get_conf()['sftp_file_count_threshold'], rail.result('list_sftp_files')))
        return list(map(lambda path: {'path': path, 'files': rail.result(
            'list_sftp_files')[path]}, paths))

    filter_count_threshold_files = rail.PythonOperator(
        task_id='filter_count_threshold_files',
        python_callable=filter_files_based_on_count
    )

    can_trigger_count_threshold_email = rail.IfOperator(
        task_id='can_trigger_count_threshold_email',
        test='{{ result("filter_count_threshold_files") | length > 0 }}',
        yes_task='get_count_threshold_email_content',
        no_task='filter_time_threshold_files',
    )

    get_count_threshold_email_content = rail.PythonOperator(
        task_id='get_count_threshold_email_content',
        python_callable=lambda: get_email_content(
            rail.result('filter_count_threshold_files'))
    )

    send_count_threshold_email = rail.EmailOperator(
        task_id='send_count_threshold_email',
        to='{{ dag_run.conf.alert_email }}',
        # pylint: disable=line-too-long
        subject='{{ dag_run.conf.company_key }} - {{ dag_run.conf.sftp_conn_id }} | {{ result("get_count_threshold_email_content").count }} Files are queued at SFTP - {{ current_time() }}',
        html_content='{{ result("get_count_threshold_email_content").html_content}}'
    )

    def filter_files_based_on_time():
        sftp_files = rail.result('list_sftp_files')
        old_files = []
        for path in sftp_files:
            files = sftp_files[path]
            files = list(filter(lambda x: (datetime.utcnow() - datetime.strptime(
                x['modify'], '%Y%m%d%H%M%S')).total_seconds() > get_conf()['sftp_file_hours_threshold'] * 3600, files))
            if len(files) > 0:
                old_files.append({'path': path, 'files': files})
        return old_files

    filter_time_threshold_files = rail.PythonOperator(
        task_id='filter_time_threshold_files',
        python_callable=filter_files_based_on_time
    )

    can_trigger_time_threshold_email = rail.IfOperator(
        task_id='can_trigger_time_threshold_email',
        test='{{ result("filter_time_threshold_files") | length > 0 }}',
        yes_task='get_time_threshold_email_content',
        no_task='finish',
    )

    get_time_threshold_email_content = rail.PythonOperator(
        task_id='get_time_threshold_email_content',
        python_callable=lambda: get_email_content(
            rail.result('filter_time_threshold_files'))
    )

    send_time_threshold_email = rail.EmailOperator(
        task_id='send_time_threshold_email',
        to='{{ dag_run.conf.alert_email }}',
        # pylint: disable=line-too-long
        subject='{{ dag_run.conf.company_key }} - {{ dag_run.conf.sftp_conn_id }} | {{ result("get_time_threshold_email_content").count }} Files are queued for > {{ dag_run.conf.sftp_file_hours_threshold }} hrs at SFTP - {{ current_time() }}',
        html_content='{{ result("get_time_threshold_email_content").html_content}}'
    )

    finish = rail.EmptyOperator(
        task_id='finish'
    )

    list_sftp_files >> has_files
    has_files >> Label("No") >> finish
    has_files >> Label(
        'Yes') >> filter_count_threshold_files >> can_trigger_count_threshold_email
    can_trigger_count_threshold_email >> Label(
        "No") >> filter_time_threshold_files >> can_trigger_time_threshold_email
    can_trigger_count_threshold_email >> Label(
        'Yes') >> get_count_threshold_email_content >> send_count_threshold_email >> \
        filter_time_threshold_files >> can_trigger_time_threshold_email
    can_trigger_time_threshold_email >> Label("No") >> finish
    can_trigger_time_threshold_email >> Label(
        "Yes") >> get_time_threshold_email_content >> send_time_threshold_email >> finish
