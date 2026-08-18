
from datetime import timedelta, datetime
import itertools
import json
import pytz
from pendulum import datetime as pendulum_datetime
import rail
from airflow.models import Variable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'kla_user_import_usa_master_{config.instance}',
        description=f'USA_KLA_User_Import_Master_V2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum_datetime(2022, 10, 10, tz=config.schedule_time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=1,
    ) as dag:

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        log_message_yesterdaysdate = rail.PythonOperator(
            task_id='log_message_yesterdaysdate',
            python_callable=lambda: (datetime.now(
                tz=pytz.UTC) - timedelta(days=1)).strftime("%d-%m-%y")
        )

        can_use_conf_payload = rail.IfOperator(
            task_id='can_use_conf_payload',
            test=lambda: Variable.get(
                config.can_use_conf_payload_var_name, default_var='').lower() == 'true',
            yes_task='get_conf_payload',
            no_task='get_http_payload'
        )

        get_conf_payload = rail.PythonOperator(
            task_id='get_conf_payload',
            python_callable=lambda: json.dumps(
                rail.get_current_context()['dag_run'].conf)
        )

        get_http_payload = rail.SimpleHttpOperator(
            task_id='get_http_payload',
            method='GET',
            http_conn_id=config.http_conn_id,
            data={
                "UPDATE_DATE": "{{ result('log_message_yesterdaysdate') }}",
                "REQUESTOR": "REPLICON"
            },
            headers={
                "Content-Type": 'application/json; charset=utf-8'
            },
            extra_options={
                'verify': False
            }
        )

        get_empviews = rail.PythonOperator(
            task_id='get_empviews',
            python_callable=lambda: rail.result(
                'get_conf_payload') or rail.result('get_http_payload')
        )

        log_message_checkifnodataisfound = rail.PythonOperator(
            task_id='log_message_checkifnodataisfound',
            python_callable=lambda: json.loads(rail.result(
                'get_empviews'))['Employee_ViewXSD_response']['Statement1_response']
        )

        has_no_userdata = rail.IfOperator(
            task_id='has_no_userdata',
            test=lambda: 'NoRecordsFound' in rail.result(
                'log_message_checkifnodataisfound'),
            yes_task="send_mail_nodata",
            no_task="log_message_checkiftherowhaslistdata",
        )

        send_mail_nodata = rail.EmailOperator(
            task_id='send_mail_nodata',
            to=config.tenant_email,
            subject='''{{ get_company_key() }} | Production User import completed, no new/updated records found - {{ current_time() }} ''',
            html_content='''<strong>This is an automated mail, please don't reply.</strong><br />
            <br />
            Hello, <br />
            <br />
            The User Import job is completed and there were no new/updated records found on -  {{ current_time() }} .  <br />
            <br />
            For any queries, please contact our support team at https://support.deltek.com <br />
            <br />
            Regards, <br />
            Deltek Inc. ''',
            params=None,
        )

        log_message_checkiftherowhaslistdata = rail.PythonOperator(
            task_id='log_message_checkiftherowhaslistdata',
            python_callable=lambda: isinstance(json.loads(rail.result('get_empviews')).get(
                'Employee_ViewXSD_response', {}).get('Statement1_response', {}).get('row', {}), list)
        )

        log_message_todaysdate = rail.PythonOperator(
            task_id='log_message_todaysdate',
            python_callable=lambda: (datetime.now(
                tz=pytz.UTC)).strftime("%m_%d_%Y")
        )

        has_no_listdata = rail.IfOperator(
            task_id='has_no_listdata',
            test=lambda: not rail.result(
                'log_message_checkiftherowhaslistdata'),
            yes_task="parse_row_to_json",
            no_task="has_first_name2",
        )

        parse_row_to_json = rail.PythonOperator(
            task_id='parse_row_to_json',
            python_callable=lambda: json.loads(rail.result('get_empviews'))[
                'Employee_ViewXSD_response']['Statement1_response']['row']
        )

        has_first_name = rail.IfOperator(
            task_id='has_first_name',
            test=lambda: bool(rail.result(
                'parse_row_to_json')['FIRSTNAME']),
            yes_task="process_cost_center_department_check",
            no_task="has_first_name2",
        )

        process_cost_center_department_check = rail.TriggerDagRunForEachItemOperator(
            task_id='process_cost_center_department_check',
            retries=0,
            items=['process'],
            trigger_dag_id=f'kla_user_import_usa_cost_center_department_check_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "date": rail.result('log_message_yesterdaysdate'),
                "emp_view": rail.result('parse_row_to_json')
            }
        )

        wait_for_process_cost_center_department_check = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_cost_center_department_check',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("process_cost_center_department_check") }}'
        )

        create_user_collection = rail.CreateCollectionOperator(
            task_id='create_user_collection',
            source=lambda: [rail.result('parse_row_to_json')],
            name='users'
        )

        query_users = rail.QueryCollectionOperator(
            task_id='query_users',
            query='''SELECT * FROM users WHERE (users.CAMPUSCOUNTRY= "USA") OR
                    (users.HASUSADIRECTREPORTS= "Y" AND NOT users.CAMPUSCOUNTRY= "USA" AND NOT users.CAMPUSCOUNTRY= "JPN")''',
        )

        has_employeeid = rail.IfOperator(
            task_id='has_employeeid',
            test=lambda: rail.result('query_users', 'length') > 0 and
            bool(rail.load_all_records(rail.result(
                    'query_users'))[0]['EMPLOYEEID']),
            yes_task="process_users",
            no_task="has_first_name2",
        )

        process_users = rail.TriggerDagRunForEachItemOperator(
            task_id='process_users',
            retries=0,
            items="{{ result('query_users') }}",
            trigger_dag_id=f'kla_user_import_usa_process_users_{config.instance}',
            conf=lambda item: {
                **{k.lower().replace(' ', '_'): v for k, v in item.items()},
            },
            execution_timeout=timedelta(days=14)
        )

        wait_for_process_users = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_users',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("process_users") }}'
        )

        log_message_listsize1 = rail.PythonOperator(
            task_id='log_message_listsize1',
            python_callable=lambda: rail.result('query_users', 'length') * 3
        )

        has_first_name2 = rail.IfOperator(
            task_id='has_first_name2',
            test=lambda: isinstance(json.loads(rail.result('get_empviews'))[
                'Employee_ViewXSD_response']['Statement1_response']['row'], list) and
            json.loads(rail.result('get_empviews'))[
                'Employee_ViewXSD_response']['Statement1_response']['row'][0]['FIRSTNAME'],
            yes_task="create_user_list2",
            no_task="log_message_listsizeofdataprocessed",
        )

        create_user_list2 = rail.CreateCollectionOperator(
            task_id='create_user_list2',
            source=lambda: json.loads(rail.result('get_empviews'))[
                'Employee_ViewXSD_response']['Statement1_response']['row'],
            name='users'
        )

        query_user_list2 = rail.QueryCollectionOperator(
            task_id='query_user_list2',
            query='''SELECT * FROM users WHERE (users.CAMPUSCOUNTRY= "USA") OR
                    (users.HASUSADIRECTREPORTS= "Y" AND NOT users.CAMPUSCOUNTRY= "USA" AND NOT users.CAMPUSCOUNTRY= "JPN")''',
        )

        has_rows2 = rail.IfOperator(
            task_id='has_rows2',
            test=lambda: rail.result('query_user_list2', 'length') > 0,
            yes_task="process_cost_center_department_check2",
            no_task="log_message_listsizeofdataprocessed",
        )

        process_cost_center_department_check2 = rail.TriggerDagRunForEachItemOperator(
            task_id='process_cost_center_department_check2',
            retries=0,
            items=['process'],
            trigger_dag_id=f'kla_user_import_usa_cost_center_department_check_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "date": rail.result('log_message_yesterdaysdate'),
                "emp_view": json.loads(rail.result('get_empviews'))[
                    'Employee_ViewXSD_response']['Statement1_response']['row']
            }
        )

        wait_for_process_cost_center_department_check2 = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_cost_center_department_check2',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("process_cost_center_department_check2") }}'
        )

        process_users2 = rail.TriggerDagRunForEachItemOperator(
            task_id='process_users2',
            retries=0,
            items="{{ result('query_user_list2') }}",
            trigger_dag_id=f'kla_user_import_usa_process_users_{config.instance}',
            conf=lambda item: {
                **{k.lower().replace(' ', '_'): v for k, v in item.items()},
            },
            execution_timeout=timedelta(days=14)
        )

        wait_for_process_users2 = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_users2',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("process_users2") }}'
        )

        log_message_listsize_2 = rail.PythonOperator(
            task_id='log_message_listsize_2',
            python_callable=lambda: rail.result(
                'query_user_list2', 'length') * 3
        )

        log_message_listsizeofdataprocessed = rail.PythonOperator(
            task_id='log_message_listsizeofdataprocessed',
            python_callable=lambda: rail.result(
                'log_message_listsize_2') or rail.result('log_message_listsize1')
        )

        has_processed_data = rail.IfOperator(
            task_id='has_processed_data',
            test=lambda: rail.result(
                'log_message_listsizeofdataprocessed') and rail.result(
                'log_message_listsizeofdataprocessed') > 0,
            yes_task="log_message_listsize_final",
            no_task="finish",
        )

        log_message_listsize_final = rail.PythonOperator(
            task_id='log_message_listsize_final',
            python_callable=lambda: rail.result(
                'log_message_listsizeofdataprocessed')
        )

        gather_supervisor_assignment = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_supervisor_assignment',
            dag_runs="{{ result('process_users') or result('process_users2') }}",
            dagrun_task_id='get_supervisor_assignment',
            flatten=True,
        )

        process_supervisor_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supervisor_assignment',
            retries=0,
            items="{{ result('gather_supervisor_assignment') | to_json }}",
            trigger_dag_id=f'kla_user_import_usa_supervisor_assignment_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                'log': rail.result('create_log'),
                "supervisorid": item['supervisorid'],
                "useruri": item['useruri'],
                "loginname": item['loginname'],
            }
        )

        wait_for_process_supervisor_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_supervisor_assignment',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("process_supervisor_assignment") }}'
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs="{{ result('process_users') or result('process_users2') }}",
            dagrun_task_id='create_log',
            flatten=True,
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=lambda: list(list(itertools.chain(
                *list(map(rail.load_all_records, rail.result('gather_child_logs'))))))
                + rail.load_all_records(rail.result(create_log.task_id))
        )

        log_message_checkifthereiserror = rail.PythonOperator(
            task_id='log_message_checkifthereiserror',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('format_logs'), 'properties.status', 'Error')
        )

        create_csv_log = rail.WriteCSVFileOperator(
            task_id='create_csv_log',
            source="{{ result('format_logs') | to_json }}",
            header=['jobid',
                    'username',
                    'EmployeeID',
                    'action',
                    'status',
                    'details'],
            row=lambda item: {
                "jobid": item['ecid'],
                "username": item['properties']['loginname'],
                "EmployeeID": item['properties']['action'].split('|')[-1],
                "action": item['properties']['action'].split('|')[0],
                "status": item['properties']['status'],
                "details": item['properties']['message'],
            }.values(),
        )

        log_message_filenamefullpath = rail.PythonOperator(
            task_id='log_message_filenamefullpath',
            python_callable=lambda:  f"{rail.result('log_message_todaysdate')}.csv"
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_log')}}",
            output_file_name='{{ result("log_message_filenamefullpath") }}',
            expires_in_seconds=7*24*60*60,
        )

        upload_logfile_s3 = rail.S3UploadFileOperator(
            task_id='upload_logfile_s3',
            aws_conn_id=config.aws_conn_id,
            key_name=config.s3_key_name + '/{{ result("log_message_filenamefullpath") }}',
            source="{{ result('create_csv_log') }}",
        )

        has_error_message = rail.IfOperator(
            task_id='has_error_message',
            test=lambda: bool(rail.result('log_message_checkifthereiserror')),
            yes_task="send_error_mail",
            no_task="send_success_mail",
        )

        send_error_mail = rail.EmailOperator(
            task_id='send_error_mail',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='''{{ get_company_key() }} | User import completed with errors - {{ current_time() }}''',
            html_content='''<p><strong>This is an automated mail, please don't reply</strong><br /> <br />Hello, <br /> <br />
            The User Import job is completed with errors on - {{ current_time() }}.
            Please click the below link to download the user import logs.<br />
            <a href="{{ result('generate_download_link') }}">Download log file</a>
            <br /><span style="font-size: small;">The download link is valid for 7 days.</span><br /><br />For any queries, please contact our support team at https://support.deltek.com <br />
            <br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        send_success_mail = rail.EmailOperator(
            task_id='send_success_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | User import completed successfully - {{ current_time() }}''',
            html_content='''<p><strong>This is an automated mail, please don't reply</strong><br /> <br />Hello, <br /> <br />The User Import job is completed successfully on - {{ current_time() }}.
            Please click the below link to download the user import logs. <br /><br /><a href="{{ result('generate_download_link') }}">Download log file</a>
            <br /><span style="font-size: small;">The download link is valid for 7 days.</span><br /><br />For any queries, please contact our support team at https://support.deltek.com <br />
            <br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        create_log >> log_message_yesterdaysdate >> can_use_conf_payload
        can_use_conf_payload >> rail.Label(
            'yes') >> get_conf_payload >> get_empviews
        can_use_conf_payload >> rail.Label(
            'no') >> get_http_payload >> get_empviews
        get_empviews >> log_message_todaysdate >> log_message_checkifnodataisfound >> has_no_userdata
        has_no_userdata >> rail.Label(
            'No') >> send_mail_nodata >> finish
        has_no_userdata >> rail.Label(
            'Yes') >> log_message_checkiftherowhaslistdata >> has_no_listdata
        has_no_listdata >> rail.Label(
            'Yes') >> parse_row_to_json >> has_first_name >> has_first_name2
        has_no_listdata >> rail.Label('No') >> has_first_name2
        has_first_name >> rail.Label(
            'Yes') >> process_cost_center_department_check >> wait_for_process_cost_center_department_check >> create_user_collection >> query_users >> has_employeeid
        has_employeeid >> rail.Label(
            'Yes') >> process_users >> wait_for_process_users >> log_message_listsize1 >> has_first_name2
        has_employeeid >> rail.Label(
            'No') >> has_first_name2
        has_first_name2 >> rail.Label(
            'Yes') >> create_user_list2 >> query_user_list2 >> has_rows2
        has_first_name2 >> rail.Label(
            'No') >> log_message_listsizeofdataprocessed
        has_rows2 >> rail.Label(
            'Yes') >> process_cost_center_department_check2 >> wait_for_process_cost_center_department_check2 >> process_users2 >> wait_for_process_users2 >> log_message_listsize_2 >> log_message_listsizeofdataprocessed
        has_rows2 >> rail.Label('No') >> log_message_listsizeofdataprocessed
        log_message_listsizeofdataprocessed >> has_processed_data
        has_processed_data >> rail.Label(
            'Yes') >> log_message_listsize_final >> gather_supervisor_assignment >> process_supervisor_assignment >> wait_for_process_supervisor_assignment >> gather_child_logs >> format_logs >> log_message_checkifthereiserror >> create_csv_log >> log_message_filenamefullpath >> generate_download_link >> upload_logfile_s3 >> has_error_message
        has_processed_data >> rail.Label(
            'No') >> finish
        has_error_message >> rail.Label(
            'Yes') >> send_error_mail >> finish
        has_error_message >> rail.Label(
            'no') >> send_success_mail >> finish

    return dag


rail.for_each_instance(create_dag)
