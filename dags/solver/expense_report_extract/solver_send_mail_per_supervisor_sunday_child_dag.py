from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'solver_expense_report_extract_send_mail_per_supervisor_sunday_child_{config.instance}',
        description=f'SolverInc_Send_Mail_Per_Supervisor_Sunday_run_Child  {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='query_entries_outside_the_range_per_supervisor'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_entries_outside_the_range_per_supervisor',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        query_entries_outside_the_range_per_supervisor=rail.QueryCollectionOperator(
            task_id='query_entries_outside_the_range_per_supervisor',
            query="""SELECT expensedata.username, expensedata.trackingnumber, expensedata.amountcurrency, expensedata.amountamount, expensedata.incurreddate,
                expensedata.savedon, expensedata.weekstartdatesavedon, expensedata.weekenddatesavedon, expensedata.usersupervisor,
                expensedata.usersupervisoremailaddress, expensedata.daydiff FROM expensedata WHERE
                expensedata.usersupervisoremailaddress="{{dag_run.conf.usersupervisoremailaddress}}" AND
                ( CAST(expensedata.daydiff AS INTEGER) >= 7 OR  CAST(expensedata.daydiff AS INTEGER) <0)""",
        )

        if_entries_outside_range_present=rail.IfOperator(
            task_id='if_entries_outside_range_present',
            test='''{{ result('query_entries_outside_the_range_per_supervisor','length') > 0 }}''',
            yes_task="compose_csv_for_entries",
            no_task="finish",
        )

        compose_csv_for_entries=rail.WriteCSVFileOperator(
            task_id='compose_csv_for_entries',
            source="{{ result('query_entries_outside_the_range_per_supervisor') }}",
            header=['User Name',
                    'Tracking Number',
                    'Amount - Currency',
                    'Amount - Amount',
                    'Incurred Date',
                    'Saved On',
                    'User Supervisor',
                    'User Supervisor Email address'],
            row= [
                "{{ item.username }}",
                "{{ item.trackingnumber }}",
                "{{ item.amountcurrency }}",
                "{{ item.amountamount }}",
                "{{ item.incurreddate }}",
                "{{ item.savedon }}",
                "{{ item.usersupervisor }}",
                "{{ item.usersupervisoremailaddress }}"
            ],
        )

        get_week_start_and_end_date=rail.PythonOperator(
            task_id='get_week_start_and_end_date',
            python_callable=lambda:  {
                'startdate': (datetime.now()-timedelta(days=7)).strftime("%b %d, %Y"),
                'enddate': (datetime.now()-timedelta(days=1)).strftime("%b %d, %Y")
            }
        )

        def get_name_and_email():
            entries = rail.load_all_records(rail.result('query_entries_outside_the_range_per_supervisor'))
            supervisorname = (entries[0]['usersupervisor'].split(','))[-1].strip() if entries and entries[0]['usersupervisor'] else 'Name'
            supervisoremail = entries[0]['usersupervisoremailaddress'] if entries else ''
            return{
                'supervisorname': supervisorname,
                'supervisoremail': supervisoremail,
                'filename': "Incorrectexpenses_thisweek" + (('_' + supervisorname) if supervisoremail else '') + ".csv"
            }

        log_supervisorname_and_email_address = rail.PythonOperator(
            task_id = 'log_supervisorname_and_email_address',
            python_callable= get_name_and_email
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_csv_for_entries')}}",
            output_file_name="{{result('log_supervisorname_and_email_address').filename}}",
            expires_in_seconds=7*24*60*60,
        )

        if_supervisor_email_present=rail.IfOperator(
            task_id='if_supervisor_email_present',
            test=lambda: bool( rail.result('log_supervisorname_and_email_address')['supervisoremail'] ),
            yes_task="send_mail_to_supervisor",
            no_task="if_supervisor_email_not_present",
        )

        send_mail_to_supervisor=rail.EmailOperator(
            task_id='send_mail_to_supervisor',
            to="{{result('log_supervisorname_and_email_address').supervisoremail}}",
            cc=config.tenant_email,
            #pylint: disable = line-too-long
            subject='''Solver Inc - Incorrect expenses in Replicon for this week {{ result('get_week_start_and_end_date').startdate }} to {{ result('get_week_start_and_end_date').enddate }}''',
            html_content= '''templates/mail_to_supervisor.html''',
        )

        if_supervisor_email_not_present=rail.IfOperator(
            task_id='if_supervisor_email_not_present',
            test=lambda: not bool( rail.result('log_supervisorname_and_email_address')['supervisoremail'] ),
            yes_task="send_mail_to_team",
            no_task="finish",
        )

        send_mail_to_team=rail.EmailOperator(
            task_id='send_mail_to_team',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            #pylint: disable = line-too-long
            subject='''Solver Inc - Incorrect expenses in Replicon for this week {{ result('get_week_start_and_end_date').startdate }} to {{ result('get_week_start_and_end_date').enddate }} ''',
            html_content= '''templates/mail_to_team.html''',
        )

        finish = rail.EmptyOperator(
            task_id = 'finish'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> query_entries_outside_the_range_per_supervisor >> if_entries_outside_range_present
        if_entries_outside_range_present >> rail.Label('Yes') >> compose_csv_for_entries >> get_week_start_and_end_date >> log_supervisorname_and_email_address
        log_supervisorname_and_email_address >> generate_download_link >> if_supervisor_email_present
        if_supervisor_email_present >> rail.Label('Yes') >> send_mail_to_supervisor >> if_supervisor_email_not_present
        if_supervisor_email_present >> rail.Label('No') >> if_supervisor_email_not_present
        if_supervisor_email_not_present >> rail.Label('Yes')  >> send_mail_to_team >> finish
        if_supervisor_email_not_present >> rail.Label('No') >> finish
        if_entries_outside_range_present >> rail.Label('No') >> finish

    return dag

rail.for_each_instance(create_dag)
