from datetime import timedelta
from hashlib import md5
import itertools
import pendulum
from conduent.user_import.utils import custom_methods
from conduent.user_import.task.process_user_group_data import create_prerequisite_data
from conduent.user_import.task.user_report import user_data_report
import rail


def create_airflow_master(config):
    with rail.create_airflow_dag(
        dag_id=config.conduent_user_import_master,
        description="Conduent user import master",
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_run_master,
        start_date=pendulum.datetime(
            year=2024, month=8, day=1, tz=config.time_zone),
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id="new_file_sensor",
            path=config.sftp_input_path,
            soft_fail_timeout=timedelta(minutes=10)
        )

        is_import_file_csv = rail.IfOperator(
            task_id="is_import_file_csv",
            test="{{ result('new_file_sensor') |lower| ends_with('csv') }}",
            yes_task="create_conduent_user_import_log",
            no_task="send_incorrect_file_format_mail"
        )

        send_incorrect_file_format_mail = rail.EmailOperator(
            task_id="send_incorrect_file_format_mail",
            to=config.tenant_mail,
            subject='{{get_company_key()}} | User import - File processing is skipped - {{current_time_in_specified_tz(fmt="%d%m%Y_%H%M%S", tz="US/Eastern")}}',
            html_content="templates/incorrect_file_format.html"
        )

        create_conduent_user_import_log = rail.CreateLogOperator(
            task_id="create_conduent_user_import_log"
        )

        download_import_file = rail.SFTPDownloadFileOperator(
            task_id="download_import_file",
            remote_filepath='{{result("new_file_sensor")}}'
        )

        was_new_file_found = rail.IfOperator(
            task_id="was_new_file_found",
            trigger_rule="all_done",
            test='{{get_task_state("new_file_sensor") == "success"}}',
            yes_task="archive_file",
            no_task="delete_dagrun"
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id="archive_file",
            existing_filename='{{result("new_file_sensor")}}',
            new_filename=config.sftp_archive_path + "/" +
            'archive_{{dag_run_ecid()}}_{{result("new_file_sensor")|file_name}}',
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dagrun"
        )

        parse_import_csv = rail.LoadCSVFileOperator(
            task_id="parse_import_csv",
            document='{{result("download_import_file")}}',
            headers=['win_id', 'first_name', 'last_name', 'preferred_name', 'email',
                     'assignment_status', 'date_active', 'date_termed',
                     'manager_win', 'work_schedule_name', 'location_code',
                     'cost_center', 'job_title', 'effective_date', 'business_group']
        )

        user_data = rail.CreateCollectionOperator(
            task_id="user_data",
            source='{{result("parse_import_csv")}}'
        )

        def get_md5(item):
            if not item:
                return []
            return {
                **item,
                "md5": md5((item["win_id"] + item["email"]).encode()).hexdigest()
            }

        create_user_import_md5 = rail.DataAdaptorOperator(
            task_id="create_user_import_md5",
            source='{{result("user_data")}}',
            columns=['win_id', 'first_name', 'last_name', 'preferred_name', 'email',
                     'assignment_status', 'date_active', 'date_termed',
                     'manager_win', 'work_schedule_name', 'location_code',
                     'cost_center', 'job_title', 'effective_date', 'business_group', "md5"],
            data=get_md5
        )

        create_user_import_collection = rail.CreateCollectionOperator(
            task_id="create_user_import_collection",
            source='{{result("create_user_import_md5")}}',
            name="userdeltarecords"
        )

        if_user_data = rail.IfOperator(
            task_id="if_user_data",
            test='{{result("create_user_import_collection", "length") > 0}}',
            yes_task="query_valid_users",
            no_task="send_no_records_found_mail"
        )

        send_no_records_found_mail = rail.EmailOperator(
            task_id="send_no_records_found_mail",
            to=config.tenant_mail,
            subject='{{get_company_key()}} | User import - no records in file {{ current_time_in_specified_tz(fmt="%d%m%Y_%H%M%S", tz="US/Eastern")}}',
            html_content="templates/no_records_template.html",
        )

        query_valid_users = rail.QueryCollectionOperator(
            task_id="query_valid_users",
            query="""SELECT * FROM userdeltarecords WHERE NULLIF(win_id,"") IS NOT NULL AND
            NULLIF(first_name,"") IS NOT NULL AND NULLIF(last_name,"") IS NOT NULL AND NULLIF(email,"") IS NOT NULL AND
            NULLIF(assignment_status,"") IS NOT NULL AND NULLIF(date_active,"") IS NOT NULL""",
            name="valid_import_user_records"
        )

        query_invalid_users = rail.QueryCollectionOperator(
            task_id="query_invalid_users",
            query="""SELECT * FROM userdeltarecords WHERE NULLIF(win_id,"") IS NULL OR
            NULLIF(first_name,"") IS NULL OR NULLIF(last_name,"") IS NULL OR NULLIF(email,"") IS NULL OR
            NULLIF(assignment_status,"") IS NULL OR NULLIF(date_active,"") IS NULL"""
        )

        if_invalid_users = rail.IfOperator(
            task_id="if_invalid_users",
            test='{{result("query_invalid_users", "length") > 0}}',
            yes_task="write_invalid_users_exception_log",
            no_task="groups_prerequisite_data"
        )

        write_invalid_users_exception_log = rail.WriteLogOperator(
            task_id="write_invalid_users_exception_log",
            log='{{result("create_conduent_user_import_log")}}',
            items='{{result("query_invalid_users")}}',
            message="Mandatory fields missing",
            properties=lambda item: {
                "win_id": item["win_id"],
                "first_name": item["first_name"],
                "last_name": item["last_name"],
                "email": item["email"],
                "assignment_status": item["assignment_status"],
                "date_active": item["date_active"],
                "details": custom_methods.get_exception_logs(item),
                "status": "Exception"
            }
        )

        groups_prerequisite_data = rail.EmptyOperator(
            task_id="groups_prerequisite_data")
        groups_data_start, groups_data_end = create_prerequisite_data()

        report_start = rail.EmptyOperator(task_id="report_start")
        existing_user_report = user_data_report(config)

        query_update_users = rail.QueryCollectionOperator(
            task_id="query_update_users",
            query="""SELECT vimur.*, eur.useruri FROM valid_import_user_records vimur
            LEFT JOIN existing_user_records eur WHERE vimur.md5=eur.md5""",
            name="update_users"
        )

        query_new_hire_or_rehire_users = rail.QueryCollectionOperator(
            task_id="query_new_hire_or_rehire_users",
            query="""SELECT * FROM valid_import_user_records WHERE md5 NOT IN (SELECT DISTINCT md5 FROM existing_user_records)""",
            name="new_hire_or_rehire_users_records"
        )

        query_rehire_users = rail.QueryCollectionOperator(
            task_id="query_rehire_users",
            query="""SELECT nhorur.*, eur.useruri FROM new_hire_or_rehire_users_records nhorur , existing_user_records eur
                    ON (nhorur.win_id = eur.win_id or nhorur.email =eur.login_name) AND
                    eur.login_name NOT LIKE '%/_old' ESCAPE '/' AND
                    nhorur.md5 NOT IN (SELECT DISTINCT md5 FROM update_users)""",
            name="rehire_user_records"
        )

        query_new_hire_users = rail.QueryCollectionOperator(
            task_id="query_new_hire_users",
            query="""SELECT * FROM new_hire_or_rehire_users_records WHERE win_id NOT IN (SELECT DISTINCT win_id FROM rehire_user_records)"""
        )

        if_new_hires = rail.IfOperator(
            task_id="if_new_hires",
            test='{{result("query_new_hire_users", "length") > 0}}',
            yes_task="process_create_users_start",
            no_task="if_rehire_users"
        )

        process_create_users_start = rail.EmptyOperator(
            task_id="process_create_users_start")

        process_create_users = rail.trigger_parallel_dagrun(
            task_id="process_create_users",
            items='{{result("query_new_hire_users")}}',
            parallel_count=config.trigger_parallel_dagrun_count,
            trigger_dag_id=config.conduent_user_import_create_users_child,
            execution_timeout=timedelta(config.execution_timeout),
            conf=lambda item: {
                **item,
                **custom_methods.get_user_config(item, config),
                "user_type": "new_user"
            }
        )

        get_new_user_dagrun_ids = rail.PythonOperator(
            task_id='get_new_user_dagrun_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'process_create_users_{x+1}') if rail.result(
                    f'process_create_users_{x+1}') else []), range(config.trigger_parallel_dagrun_count))))),
            show_return_value_in_logs=False
        )

        gather_new_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_new_user_logs',
            dag_runs='{{ result("get_new_user_dagrun_ids") }}',
            dagrun_task_id='create_user_log',
            execution_timeout=timedelta(days=config.execution_timeout),
            flatten=True
        )

        if_rehire_users = rail.IfOperator(
            task_id="if_rehire_users",
            test='{{result("query_rehire_users", "length") > 0}}',
            yes_task="process_rehire_users_start",
            no_task="if_update_users"
        )

        process_rehire_users_start = rail.EmptyOperator(
            task_id="process_rehire_users_start")

        process_rehire_users = rail.trigger_parallel_dagrun(
            task_id="process_rehire_users",
            items='{{result("query_rehire_users")}}',
            parallel_count=config.trigger_parallel_dagrun_count,
            trigger_dag_id=config.conduent_user_import_create_users_child,
            execution_timeout=timedelta(config.execution_timeout),
            conf=lambda item: {
                **item,
                **custom_methods.get_user_config(item, config),
                "user_type": "rehire_user"
            }
        )

        get_rehire_user_dagrun_ids = rail.PythonOperator(
            task_id='get_rehire_user_dagrun_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'process_rehire_users_{x+1}') if rail.result(
                    f'process_rehire_users_{x+1}') else []), range(config.trigger_parallel_dagrun_count))))),
            show_return_value_in_logs=False
        )

        gather_rehire_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_rehire_user_logs',
            dag_runs='{{ result("get_rehire_user_dagrun_ids") }}',
            dagrun_task_id='create_user_log',
            execution_timeout=timedelta(days=config.execution_timeout),
            flatten=True
        )

        if_update_users = rail.IfOperator(
            task_id="if_update_users",
            test='{{result("query_update_users", "length") > 0}}',
            yes_task="process_update_users_start",
            no_task="process_logs"
        )

        process_update_users_start = rail.EmptyOperator(
            task_id="process_update_users_start")

        process_update_users = rail.trigger_parallel_dagrun(
            task_id="process_update_users",
            items='{{result("query_update_users")}}',
            parallel_count=config.trigger_parallel_dagrun_count,
            trigger_dag_id=config.conduent_user_import_update_users_child,
            execution_timeout=timedelta(config.execution_timeout),
            conf=lambda item: {
                **item,
                **custom_methods.get_user_config(item, config),
                "user_type": "update_user"
            }
        )

        get_update_user_dagrun_ids = rail.PythonOperator(
            task_id='get_update_user_dagrun_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'process_update_users_{x+1}') if rail.result(
                    f'process_update_users_{x+1}') else []), range(config.trigger_parallel_dagrun_count))))),
            show_return_value_in_logs=False
        )

        gather_update_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_update_user_logs',
            dag_runs='{{ result("get_update_user_dagrun_ids") }}',
            dagrun_task_id='create_user_log',
            execution_timeout=timedelta(days=config.execution_timeout),
            flatten=True
        )

        def get_log_artifacts():
            log_artifacts = []
            log_artifacts.append(rail.result("create_conduent_user_import_log"))
            log_artifacts.extend(rail.result("gather_new_user_logs") or [])
            log_artifacts.extend(rail.result("gather_rehire_user_logs") or [])
            log_artifacts.extend(rail.result("gather_update_user_logs") or [])
            return log_artifacts

        process_logs = rail.TriggerDagRunOperator(
            task_id="process_logs",
            trigger_dag_id=config.conduent_user_import_process_logs_child,
            wait_for_completion=True,
            execution_timeout=timedelta(config.execution_timeout),
            conf=lambda dag_run: {
                "parent_run_id": dag_run.id,
                "file_name": rail.render_template("{{ result('new_file_sensor') | file_name }}"),
                "log_artifacts": get_log_artifacts(),
                "total_import_records": int(rail.render_template('{{result("query_valid_users",key="length")}}')) + \
                   int(rail.render_template('{{result("query_invalid_users",key="length")}}'))
            }
        )

        user_import_fin = rail.EmptyOperator(task_id="user_import_fin")



        new_file_sensor >>\
            is_import_file_csv >> rail.Label("Yes") >> create_conduent_user_import_log >> download_import_file >>\
            was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_dagrun
        download_import_file >> parse_import_csv >> user_data >> create_user_import_md5 >> create_user_import_collection >>\
            if_user_data >> rail.Label("Yes") >> query_valid_users >>\
            query_invalid_users >> if_invalid_users >> rail.Label(
                "Yes") >> write_invalid_users_exception_log >> groups_prerequisite_data
        if_invalid_users >> rail.Label("No") >>\
            groups_prerequisite_data >> groups_data_start >> groups_data_end >> report_start >> existing_user_report >>\
            query_update_users >> \
            query_new_hire_or_rehire_users >> query_rehire_users >>\
            query_new_hire_users >>\
            if_new_hires >> rail.Label("Yes") >>\
            process_create_users_start >>\
            process_create_users >> get_new_user_dagrun_ids >> gather_new_user_logs >>\
            if_rehire_users
        if_new_hires >> rail.Label("No") >>\
            if_rehire_users >> rail.Label("Yes") >>\
            process_rehire_users_start >>\
            process_rehire_users >> get_rehire_user_dagrun_ids >> gather_rehire_user_logs >> if_update_users
        if_rehire_users >> rail.Label("No") >>\
            if_update_users >> rail.Label("Yes") >>\
            process_update_users_start >>\
            process_update_users >> get_update_user_dagrun_ids >> gather_update_user_logs >> process_logs
        if_update_users >> rail.Label("No") >> process_logs >> user_import_fin
        if_user_data >> rail.Label(
            "No") >> send_no_records_found_mail >> user_import_fin
        is_import_file_csv >> rail.Label("No") >> send_incorrect_file_format_mail >>\
        user_import_fin

        return dag


rail.for_each_instance(create_airflow_master)
