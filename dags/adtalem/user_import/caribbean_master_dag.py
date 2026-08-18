from datetime import datetime, timedelta, timezone
import rail
from adtalem.user_import.utils import request_payload

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


# pylint: disable=too-many-statements
def create_caribbean_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_caribbean_master_{config.instance}',
        description=f'Adtalem User Import Caribbean Master_Prod {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        query_raw_data = rail.QueryCollectionOperator(
            task_id='query_raw_data',
            query="""SELECT lastname, firstname, employeenumber, dnumber,
                    jobcode, jobtitle, jobfunctionname, managerindicator,
                    hiredate, rehiredate, servicedate, paygroup, division,
                    worklocationname, salaryhourly, regulartemp, fullparttime,
                    employeestatus, departmentnumber, filenumber, managerdnumber,
                    businessemailaddress, state, standardhours, flsastatus, terminationdate,
                    effectivedate, encoded FROM rawinputfile""",
            name='rawdatacollectioncaribbean'
        )

        create_rawdata_csv = rail.WriteCSVFileOperator(
            task_id='create_rawdata_csv',
            source="{{ result('query_raw_data') }}",
            header=['lastname', 'firstname', 'employeenumber', 'dnumber', 'jobcode', 'jobtitle', 'jobfunctionname',
                    'managerindicator', 'hiredate', 'rehiredate', 'servicedate', 'paygroup', 'division', 'worklocationname',
                    'salaryhourly', 'regulartemp', 'fullparttime', 'employeestatus', 'departmentnumber', 'filenumber',
                    'managerdnumber', 'businessemailaddress', 'state', 'standardhours', 'flsastatus', 'terminationdate',
                    'effectivedate', 'encoded'],
            row=request_payload.get_row_data
        )

        create_inputdatafilerefreshed_collection = rail.CreateCollectionOperator(
            task_id='create_inputdatafilerefreshed_collection',
            source="{{ result('create_rawdata_csv') }}",
            name='inputdatafilerefreshedcaribbean'
        )

        list_caribbean_reference_files = rail.SFTPListFilesOperator(
            task_id='list_caribbean_reference_files',
            paths=[config.caribbean_reference_filepath]
        )

        should_use_caribbean_referencefile = rail.IfOperator(
            task_id='should_use_caribbean_referencefile',
            test=lambda: bool(rail.result('list_caribbean_reference_files').get(
                config.caribbean_reference_filepath)),
            yes_task='trigger_caribbean_referencefile_download_child',
            no_task='query_unique_supervisors'
        )

        trigger_caribbean_referencefile_download_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_caribbean_referencefile_download_child',
            retries=0,
            items=lambda: rail.result('list_caribbean_reference_files')[
                config.caribbean_reference_filepath],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'adtalem_userimport_child_referencefile_{config.instance}',
            conf=lambda item: {
                'reference_file': f"{config.caribbean_reference_filepath}/{item['name']}",
                'action': 'download'
            }
        )

        wait_for_caribbean_referencefile_download_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_caribbean_referencefile_download_child',
            dag_runs="{{ result('trigger_caribbean_referencefile_download_child') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_caribbean_userreference_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_caribbean_userreference_data',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('trigger_caribbean_referencefile_download_child') }}",
            dagrun_task_id='create_userreference_data',
            flatten=True
        )

        create_caribbean_userreference_data_collection = rail.CreateCollectionOperator(
            task_id='create_caribbean_userreference_data_collection',
            name='caribbeanreferencefile',
            source=lambda: rail.result('gather_caribbean_userreference_data')
        )

        query_caribbean_changed_users = rail.QueryCollectionOperator(
            task_id='query_caribbean_changed_users',
            query="""SELECT * FROM inputdatafilerefreshedcaribbean WHERE
                    encoded NOT IN (SELECT DISTINCT encoded FROM caribbeanreferencefile)""",
        )

        query_caribbean_unchanged_users = rail.QueryCollectionOperator(
            task_id='query_caribbean_unchanged_users',
            query="""SELECT * FROM inputdatafilerefreshedcaribbean WHERE
                    encoded IN (SELECT DISTINCT encoded FROM caribbeanreferencefile)""",
        )

        query_unique_supervisors = rail.QueryCollectionOperator(
            task_id='query_unique_supervisors',
            query="""SELECT DISTINCT managerdnumber FROM rawdatacollectioncaribbean"""
        )

        is_unique_supervisor_present = rail.IfOperator(
            task_id='is_unique_supervisor_present',
            test="{{ result('query_unique_supervisors', 'length') > 0 }}",
            yes_task='trigger_caribbean_supervisor_child',
            no_task='create_caribbean_supervisorlog'
        )

        trigger_caribbean_supervisor_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_caribbean_supervisor_child',
            retries=0,
            items="{{ result('query_unique_supervisors') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'adtalem_userimport_caribbean_supervisor_{config.instance}',
            conf={
                'loginname': '{{ item.managerdnumber }}',
                'supervisorpermissionuri': '{{ dag_run.conf.supervisorpermissionuri }}',
                'enduserpermissionuri': '{{ dag_run.conf.enduserpermissionuri }}'
            }
        )

        wait_for_caribbean_supervisor_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_caribbean_supervisor_child',
            dag_runs="{{ result('trigger_caribbean_supervisor_child') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_caribbean_supervisor_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_caribbean_supervisor_child_logs',
            dag_runs="{{ result('trigger_caribbean_supervisor_child') }}",
            dagrun_task_id='create_caribbean_userlog',
            flatten=True
        )

        create_caribbean_supervisorlog = rail.CreateLogOperator(
            task_id='create_caribbean_supervisorlog'
        )

        is_caribbean_changed_users_present = rail.IfOperator(
            task_id='is_caribbean_changed_users_present',
            test=lambda: (rail.result('query_caribbean_changed_users', 'length') and rail.result(
                'query_caribbean_changed_users', 'length') > 0) or
            int(rail.result('create_inputdatafilerefreshed_collection', 'length')) > 0,
            yes_task='trigger_caribbean_user_child_dag',
            no_task='process_complete_caribbean_maindag'
        )

        trigger_caribbean_user_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_caribbean_user_child_dag',
            retries=0,
            items=lambda: rail.result('query_caribbean_changed_users') or rail.result(
                'create_inputdatafilerefreshed_collection'),
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'adtalem_userimport_caribbean_process_user_{config.instance}',
            conf=lambda item, dag_run: {
                **dict(item.items()),
                'supervisor_log': rail.result('create_caribbean_supervisorlog'),
                'supervisorpermissionuri': dag_run.conf['supervisorpermissionuri'],
                'enduserpermissionuri': dag_run.conf['enduserpermissionuri']
            }
        )

        wait_for_caribbean_user_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_caribbean_user_child',
            dag_runs="{{ result('trigger_caribbean_user_child_dag') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_caribbean_user_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_caribbean_user_child_logs',
            dag_runs="{{ result('trigger_caribbean_user_child_dag') }}",
            dagrun_task_id='create_caribbean_userlog',
            flatten=True
        )

        get_time_for_file = rail.PythonOperator(
            task_id='get_time_for_file',
            python_callable=lambda: datetime.now(
                timezone.utc).strftime('%m_%d_%Y_T%H_%M_%S')
        )

        process_caribbean_logs = rail.TriggerDagRunOperator(
            task_id='process_caribbean_logs',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'adtalem_userimport_child_log_{config.instance}',
            conf=lambda dag_run: {
                'import_type': 'Caribbean User import',
                'log_filename': f"Carribean_Processed_Logs_{rail.result('get_time_for_file')}",
                'user_logs': rail.result('gather_caribbean_user_child_logs'),
                'time': datetime.now(timezone.utc).strftime('%m%d%Y'),
                'filename': dag_run.conf['filename']
            }
        )

        has_caribbean_reference_files_archive = rail.IfOperator(
            task_id='has_caribbean_reference_files_archive',
            test=lambda: bool(rail.result('list_caribbean_reference_files').get(
                config.caribbean_reference_filepath)),
            yes_task='trigger_caribbean_referencefile_archive_child',
            no_task='upload_caribbean_referencefile_to_sftp'
        )

        trigger_caribbean_referencefile_archive_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_caribbean_referencefile_archive_child',
            retries=0,
            items=lambda: rail.result('list_caribbean_reference_files')[
                config.caribbean_reference_filepath],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'adtalem_userimport_child_referencefile_{config.instance}',
            conf=lambda dag_run, item: {
                'reference_file': f"{config.caribbean_reference_filepath}/{item['name']}",
                'time': rail.result('get_time_for_file'),
                'filename': dag_run.conf['filename'],
                'archive_filepath': config.caribbean_archive_filepath,
                'action': 'archive'
            }
        )

        wait_for_caribbean_referencefile_archive_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_caribbean_referencefile_archive_child',
            dag_runs="{{ result('trigger_caribbean_referencefile_archive_child') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        upload_caribbean_referencefile_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_caribbean_referencefile_to_sftp',
            content="{{ result('create_rawdata_csv') }}",
            remote_filepath=config.caribbean_reference_filepath +
            "/New_Reference_{{ dag_run.conf.filename }}_{{ result('get_time_for_file') }}.csv"
        )

        process_complete_caribbean_maindag = rail.EmptyOperator(
            task_id='process_complete_caribbean_maindag',
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        query_raw_data >> create_rawdata_csv >> create_inputdatafilerefreshed_collection >> \
            list_caribbean_reference_files >> should_use_caribbean_referencefile

        should_use_caribbean_referencefile >> rail.Label(
            'Yes') >> trigger_caribbean_referencefile_download_child >> wait_for_caribbean_referencefile_download_child >> \
            gather_caribbean_userreference_data >> create_caribbean_userreference_data_collection >> \
            query_caribbean_changed_users >> query_caribbean_unchanged_users >> query_unique_supervisors

        should_use_caribbean_referencefile >> rail.Label(
            'No') >> query_unique_supervisors

        query_unique_supervisors >> is_unique_supervisor_present

        is_unique_supervisor_present >> rail.Label(
            'Yes') >> trigger_caribbean_supervisor_child >> wait_for_caribbean_supervisor_child >> gather_caribbean_supervisor_child_logs >> \
            create_caribbean_supervisorlog

        is_unique_supervisor_present >> rail.Label(
            'No') >> create_caribbean_supervisorlog

        create_caribbean_supervisorlog >> is_caribbean_changed_users_present

        is_caribbean_changed_users_present >> rail.Label(
            'Yes') >> trigger_caribbean_user_child_dag >> wait_for_caribbean_user_child >> \
            gather_caribbean_user_child_logs >> get_time_for_file >> process_caribbean_logs >> has_caribbean_reference_files_archive

        has_caribbean_reference_files_archive >> rail.Label(
            'Yes') >> trigger_caribbean_referencefile_archive_child >> wait_for_caribbean_referencefile_archive_child >> \
            upload_caribbean_referencefile_to_sftp

        has_caribbean_reference_files_archive >> rail.Label(
            'No') >> upload_caribbean_referencefile_to_sftp

        upload_caribbean_referencefile_to_sftp >> process_complete_caribbean_maindag

        is_caribbean_changed_users_present >> rail.Label(
            'No') >> process_complete_caribbean_maindag

        process_complete_caribbean_maindag >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_caribbean_main_dag)
