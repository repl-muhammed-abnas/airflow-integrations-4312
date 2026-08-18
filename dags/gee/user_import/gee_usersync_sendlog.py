import rail
from gee.user_import.utils.python_callable import get_dag_trigger_time

def create_child_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.gee_usersync_sendlog,
        description=f'GEE usersync sendlog child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        create_csv_lines=rail.WriteCSVFileOperator(
            task_id='create_csv_lines',
            source="{{ dag_run.conf.user_logs }}",
            header=['loginname',
                    'employeeid',
                    'action',
                    'status',
                    'details',
                    'jobid'],
            row= [
                "{{ item.properties.loginname }}",
                "{{ item.properties.empid }}",
                "{{ item.properties.action }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.properties.jobid }}|{{ item.properties.childjobid }}"
            ],
        )

        def log_status(dag_run):
            entries = rail.load_all_records(dag_run.conf['user_logs'])
            return rail.find_first_by_attr_and_get_attr(entries, 'properties.status', 'Failed', 'properties', False)

        get_log_status = rail.PythonOperator(
            task_id = "get_log_status",
            python_callable=log_status
        )

        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{result('create_csv_lines')}}",
            output_file_name="{{ dag_run.conf.filename }}",
            expires_in_seconds=7*24*60*60
        )

        if_failed_entry_present = rail.IfOperator(
            task_id='if_failed_entry_present',
            test="{{result('get_log_status') | is_truthy}}",
            yes_task='send_completed_with_error_email',
            no_task='send_completed_successfully_email'
        )

        send_completed_with_error_email = rail.EmailOperator(
            task_id='send_completed_with_error_email',
            to="{{ dag_run.conf['emailid'] }}",
            # bcc=config.bcc_email,
            subject=f"{config.company_key} | Replicon user import completed with failed records - {get_dag_trigger_time()['dag_trigger_time']}",
            html_content="templates/emails/send_completed_with_error_email.html",
        )

        send_completed_successfully_email = rail.EmailOperator(
            task_id='send_completed_successfully_email',
            to="{{ dag_run.conf['emailid'] }}",
            # bcc=config.bcc_email,
            subject=f"{config.company_key} | Replicon user import completed successfully - {get_dag_trigger_time()['dag_trigger_time']}",
            html_content="templates/emails/send_completed_successfully_email.html",
        )

        create_csv_lines >> get_log_status >> generate_downloadable_link >> if_failed_entry_present >> rail.Label(
            "Yes") >> send_completed_with_error_email
        if_failed_entry_present >> rail.Label(
            "No") >> send_completed_successfully_email

        return dag


rail.for_each_instance(create_child_dag)
