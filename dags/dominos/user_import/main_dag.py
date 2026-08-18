from datetime import datetime, timedelta, timezone
from os import path
import pendulum
import rail
from rail.filters import split
from dominos.user_import.utils.request_payload import do_has_file_content


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/dominos/user_import/config.py


def create_maindag(config):
    with rail.create_airflow_dag(
        dag_id=f'dominos_userimport_master_{config.instance}',
        description=f'Dominos User Import Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.master_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        get_jobstarttime = rail.PythonOperator(
            task_id='get_jobstarttime',
            python_callable=lambda: pendulum.now(
                config.pacific_timezone).isoformat()
        )

        has_file_content = rail.IfOperator(
            task_id='has_file_content',
            test=do_has_file_content,
            yes_task='load_csv_contents',
            no_task='process_complete_maindag'
        )

        load_csv_contents = rail.LoadCSVFileOperator(
            task_id='load_csv_contents',
            document="{{ result('download_file') }}"
        )

        create_rawdata_collection = rail.CreateCollectionOperator(
            task_id='create_rawdata_collection',
            source="{{ result('load_csv_contents') }}",
            name='rawdata',
            columns={
                'login_id': 'loginname',
                'name': 'name',
                'first_name': 'firstname',
                'last_name': 'lastname',
                'email_id': 'email',
                'employee_id': 'employeeid',
                'manager': 'supervisorname',
                'manager.login_id': 'supervisorid',
                'terminated_date': 'enddate',
                'status': 'loginstatus',
                'department': 'department',
                'department_code': 'departmentcode',
                'employee_type': 'employeetype'
            }
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test="{{ result('create_rawdata_collection', 'length') > 0 }}",
            yes_task='get_repliconusers',
            no_task='process_complete_maindag'
        )

        get_repliconusers = rail.RepliconServiceOperator(
            task_id='get_repliconusers',
            endpoint="/services/UserService1.svc/GetAllUsers"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test="{{ get_task_state('new_file_sensor') == 'success' and \
                result('has_file_content') == 'load_csv_contents' }}",
            yes_task='upload_to_secondary_sftp',
            no_task='delete_this_dagrun'
        )

        upload_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_to_secondary_sftp',
            sftp_conn_id=config.secondary_sftp_conn_id,
            content="{{ result('download_file') }}",
            remote_filepath=config.secondary_filepath +
            "/{{ result('new_file_sensor') | file_name }}"
        )

        remove_input_file = rail.SFTPDeleteFileOperator(
            task_id='remove_input_file',
            existing_filename="{{ result('new_file_sensor') }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        get_userimport_reference_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_userimport_reference_report_details',
            report_name=config.user_import_reference_report
        )

        get_report_filteruri_userimport_reference = rail.PythonOperator(
            task_id='get_report_filteruri_userimport_reference',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_userimport_reference_report_details')[
                    'filterConfiguration']['enabledFilters'], 'displayText', 'UserFilter', 'uri', '')
        )

        get_supervisor_user_permissionset = rail.RepliconServiceOperator(
            task_id='get_supervisor_user_permissionset',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Supervisor - Supervisor', 'uri', '')
        )

        trigger_user_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_user_child_dag',
            retries=0,
            items="{{ result('create_rawdata_collection') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'dominos_userimport_child_process_user_{config.instance}',
            conf=lambda item: {
                **{k.lower(): v.strip() if v else '' for k, v in item.items()},
                'useruri': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_repliconusers'), 'loginName', item['loginname'].lower(), 'uri', ''),
                'reporturi': rail.result('get_userimport_reference_report_details')['uri'],
                'reportfilteruri': rail.result('get_report_filteruri_userimport_reference'),
                'supervisorpermissionuri': rail.result(
                    'get_supervisor_user_permissionset')
            }
        )

        wait_for_user_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_user_child_dag',
            dag_runs="{{ result('trigger_user_child_dag') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs="{{ result('trigger_user_child_dag') }}",
            dagrun_task_id='create_userlog',
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'dominos_userimport_child_log_{config.instance}',
            conf=lambda: {
                'child_log': rail.result('gather_child_logs'),
                'filename': split(string=path.split(rail.result('new_file_sensor'))[1], separator='.')[0],
                'log_filename': f"Logs_userimport_{datetime.now(timezone.utc).strftime('%m%d%Y')}.csv",
                'jobstarttime': rail.result('get_jobstarttime')
            }
        )

        process_complete_maindag = rail.EmptyOperator(
            task_id='process_complete_maindag',
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            trigger_rule='all_done',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_dag',
            no_task='process_logtosumo'
        )

        fail_dag = rail.FailOperator(
            task_id='fail_dag',
            message="{{ get_error_message() }}"
        )

        process_logtosumo = rail.EmptyOperator(
            task_id='process_logtosumo'
        )

        check_if_new_file_found = rail.IfOperator(
            task_id='check_if_new_file_found',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='dagrun_log_to_sumo'
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id
        )

        new_file_sensor >> download_file >> get_jobstarttime >> \
            has_file_content

        has_file_content >> rail.Label(
            'Yes') >> load_csv_contents >> create_rawdata_collection >> has_data

        has_data >> rail.Label(
            'Yes') >> get_repliconusers >> rail.Label(
                'Always') >> was_new_file_found

        was_new_file_found >> rail.Label(
            'Yes') >> upload_to_secondary_sftp >> remove_input_file

        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun

        get_repliconusers >> get_userimport_reference_report_details >> \
            get_report_filteruri_userimport_reference >> get_supervisor_user_permissionset >> \
            trigger_user_child_dag >> wait_for_user_child_dag >> gather_child_logs >> \
            process_log_generation >> process_complete_maindag

        has_data >> rail.Label(
            'No') >> process_complete_maindag

        has_file_content >> rail.Label(
            'No') >> process_complete_maindag

        process_complete_maindag >> rail.Label(
            'Always') >> should_fail_dag

        should_fail_dag >> rail.Label(
            'Yes') >> fail_dag

        should_fail_dag >> rail.Label(
            'No') >> process_logtosumo >> check_if_new_file_found >> rail.Label(
                'Yes') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_maindag)
