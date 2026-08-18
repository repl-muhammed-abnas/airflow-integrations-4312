from datetime import timedelta
import itertools
import rail
from pwcglobal.project_import_ury_and_arg.utils import python_callable
from pwcglobal.project_import_ury_and_arg.utils import request_payload
from pwcglobal.project_import_ury_and_arg.task.get_project_prereqs import get_project_prereqs_task_group


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/project_import_ury_and_arg/config.py


# pylint:disable = too-many-statements
def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master,
        description=f'Project Import data sync_Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_incorrect_fileformat_mail'
        )

        send_incorrect_fileformat_mail = rail.EmailOperator(
            task_id='send_incorrect_fileformat_mail',
            to=config.tenant_email,
            subject="{{ get_company_key() }} | LAN AC Project import - incorrect file format recieved - {{current_time_in_specified_tz()}}",
            html_content="templates/emails/incorrect_fileformat_mail.html",
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        log_current_date = rail.PythonOperator(
            task_id='log_current_date',
            python_callable=python_callable.get_current_date_time
        )

        create_exception_log = rail.CreateLogOperator(
            task_id = "create_exception_log"
        )

        parse_input_file_csv = rail.LoadCSVFileOperator(
            task_id="parse_input_file_csv",
            document='{{result("download_file")}}',
            encoding='utf-8-sig',
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('parse_input_file_csv') }}",
            columns={
                'Client Name': 'clientname',
                'Client Code': 'clientcode',
                'Client PRID': 'clientprid',
                'Project Name': 'projectname',
                'Project Code': 'projectcode',
                'Start Date': 'startdate',
                'End Date': 'enddate',
                'Status': 'status',
                'Time & Expense Entry': 'time_expenseentry',
                'Task Name': 'taskname',
                'Task Code': 'taskcode',
                'Allow Time Entry': 'allowtimeentry',
                'Type': 'type',
                'LAN AC Project Type': 'lanacprojecttype',
                'LAN AC LOS': 'lanaclos',
                'Project Manager_Party ID': 'projectmanager_partyid',
                'Project Manager_Legal entity ID': 'projectmanager_legalentityid',
                'Engagement Partner_PartyID': 'engagementpartner_partyid',
                'Engagement Partner_Legal Entity': 'engagementpartner_legalentity'
            },
            name="inputdata"
        )

        has_data_in_input_file = rail.IfOperator(
            task_id = "has_data_in_input_file",
            test = "{{result('create_input_data_collection', 'length') > 0 }}",
            yes_task = "query_client_records",
            no_task = "send_no_data_to_import_mail"
        )

        send_no_data_to_import_mail = rail.EmailOperator(
            task_id='send_no_data_to_import_mail',
            to=config.tenant_email,
            subject="{{ get_company_key() }} | LAN AC Project import has been skipped - {{current_time_in_specified_tz()}}",
            html_content="templates/emails/no_records_in_file_mail.html",
        )

        query_client_records = rail.QueryCollectionOperator(
            task_id="query_client_records",
            query="""SELECT DISTINCT clientcode from inputdata WHERE (NULLIF(clientcode, '')
                IS NOT NULL and NULLIF(clientname, '')IS NOT NULL)""",
            name="clientrecords"
        )

        dummy_get_project_prereqs, get_project_prereqs = get_project_prereqs_task_group()

        dummy_process_clients = rail.EmptyOperator(
            task_id='dummy_process_clients'
        )

        process_clients = rail.trigger_parallel_dagrun(
            task_id='process_clients',
            items="{{ result('query_client_records') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_clients,
            trigger_dag_id=config.process_clients,
            conf=request_payload.get_process_client_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_invalid_projects = rail.QueryCollectionOperator(
            task_id='query_invalid_projects',
            name='invalidprojects',
            query="""SELECT * FROM inputdata WHERE NULLIF(projectcode, '') IS NULL"""
        )

        is_invalid_records_exists = rail.IfOperator(
            task_id='is_invalid_records_exists',
            test='{{ result("query_invalid_projects", "length") > 0 }}',
            yes_task='log_project_skipped',
            no_task='query_distict_projects'
        )

        log_project_skipped = rail.WriteLogOperator(
            task_id="log_project_skipped",
            log="{{result('create_exception_log')}}",
            severity="Exception",
            message="Skipped",
            items='{{ result("query_invalid_projects") }}',
            properties={
                "projectcode": '{{ item.projectcode }}',
                "projectname": '{{ item.projectname }}',
                "clientcode": '{{ item.clientcode }}',
                "taskcode": '{{ item.taskcode }}',
                "taskname": '{{ item.taskname }}',
                'action': 'Validation',
                "status": 'Exception',
                "details": 'Project unique identifier: Project Code is NULL',
            }
        )

        query_distict_projects = rail.QueryCollectionOperator(
            task_id='query_distict_projects',
            name='distinctprojects',
            query="""SELECT DISTINCT projectcode,clientname,clientcode,clientprid,projectname,startdate,enddate,status,\
                time_expenseentry,type,lanacprojecttype,lanaclos,projectmanager_partyid,projectmanager_legalentityid,engagementpartner_partyid,\
                engagementpartner_legalentity FROM inputdata WHERE NULLIF(projectcode, '') IS NOT NULL"""
        )

        dummy_process_projects = rail.EmptyOperator(
            task_id='dummy_process_projects'
        )

        process_projects = rail.trigger_parallel_dagrun(
            task_id='process_projects',
            items="{{ result('query_distict_projects') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_projects,
            trigger_dag_id=config.process_projects,
            conf=lambda item: request_payload.get_process_project_conf(item, config.project_belongs_to),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_process_projects_dag_ids =rail.PythonOperator(
            task_id= 'get_process_projects_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_projects_{x+1}'), range(config.trigger_parallel_dagrun_count_process_projects))))),
            show_return_value_in_logs= False
        )

        gather_project_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_project_logs',
            dag_runs='{{ result("get_process_projects_dag_ids") }}',
            dagrun_task_id='create_project_log',
            execution_timeout=timedelta(
                hours=config.gather_project_logs_timeout_hours),
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation,
            conf={
                'projectlogs': "{{result('gather_project_logs')}}",
                'otherlogs': "{{result('create_exception_log')}}",
                'log_filename': '{{ result("new_file_sensor") | file_name | replace(".csv", "") }}_logs.csv'
            }
        )

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule="all_done",
            test=lambda: request_payload.get_task_state('delete_this_dagrun') != "success" and
                request_payload.get_task_state('download_file') == "success",
            yes_task="log_to_sumo",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "File Name": "{{ result('new_file_sensor') | file_name }}",
                "Number of Records": "{{ result('create_input_data_collection', 'length') }}",
                "Uniq Count of Projects": "{{result('query_distict_projects', 'length')}}",
                "Uniq Count of Clients": "{{result('query_client_records', 'length')}}",
            }
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        new_file_sensor >> is_csv

        is_csv >> rail.Label(
            'Yes') >> download_file

        is_csv >> rail.Label(
            'No') >> send_incorrect_fileformat_mail

        download_file >> was_new_file_found

        was_new_file_found >> rail.Label('No') >> delete_this_dagrun

        download_file >> log_current_date >> create_exception_log >> parse_input_file_csv >> create_input_data_collection >> \
        has_data_in_input_file >> rail.Label(
            'Yes') >> query_client_records
        has_data_in_input_file >> rail.Label(
            'No') >> send_no_data_to_import_mail

        query_client_records >> dummy_get_project_prereqs
        get_project_prereqs >> dummy_process_clients >> process_clients >> query_invalid_projects >> is_invalid_records_exists
        
        is_invalid_records_exists >> rail.Label("Yes") >> log_project_skipped >> query_distict_projects
        is_invalid_records_exists >> rail.Label("No") >> query_distict_projects

        query_distict_projects >> dummy_process_projects >> process_projects >> get_process_projects_dag_ids >> \
        gather_project_logs >> process_log_generation >> can_log_to_sumo

        can_log_to_sumo >> rail.Label('Yes') >> log_to_sumo >> can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag

rail.for_each_instance(create_main_airflow_dag)
