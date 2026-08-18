import rail


def create_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    location = f'{config.location}_' if config.location else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_process_email_chunk_{location}child{dag_id_postfix}'.lower(),
        description=f"{dag_id_prefix}_send_email_to_supervisor_child{dag_id_postfix}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_child_run
    ) as dag:

        def get_user_formatted_data(dag_run):
            return {"email": dag_run.conf['item']['id'], "value": dag_run.conf['item']["data"]}

        get_email = rail.PythonOperator(
            task_id='get_email',
            python_callable=get_user_formatted_data
        )

        check_for_useremail = rail.IfOperator(
            task_id='check_for_useremail',
            test='''{{ result('get_email')['email'] | is_truthy and '@' in result('get_email')['email'] }}''',
            yes_task="send_email_to_supervisor",
            no_task="finish",
        )

        send_email_to_supervisor = rail.EmailOperator(
            task_id='send_email_to_supervisor',
            to='{{ result("get_email").get("email") }}',
            subject='Approved Timesheets',
            html_content='templates/email/template_for_supervisor_email.html'
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        get_email >> check_for_useremail >> rail.Label("Yes") >> send_email_to_supervisor >> finish >> log_to_sumo
        check_for_useremail >> rail.Label("No") >> finish

    return dag

rail.for_each_instance(create_dag)
