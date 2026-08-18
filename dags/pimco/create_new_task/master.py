from datetime import timedelta
from pendulum import datetime
import rail
from rail.lib.ecid import get_dagrun_ecid
from pimco.create_new_task.utils import custom_methods
from pimco.create_new_task.utils import python_callable

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pimco_add_new_task_to_project_master_dag_{config.instance}',
        description=f'PIMCO Add new task to project master dag {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 12, 5, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs
    ) as dag:

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=custom_methods.logging_details,
            op_args=[config.time_zone]
        )

        get_task_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_task_report_details',
            report_name=config.extract_task_report_name,
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='model_project_task_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_task_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        task_report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('model_project_task_report.get_report_result', 'has_data') }}",
            yes_task='load_task_report_data',
            no_task='fail_no_task_report_data',
        )

        fail_no_task_report_data = rail.FailOperator(
            task_id="fail_no_task_report_data",
            message="Report \"Model project - task details\" execution failed",
        )

        load_task_report_data = rail.LoadCSVFileOperator(
            task_id='load_task_report_data',
            document="{{ result('model_project_task_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        task_report_data_collection = rail.CreateCollectionOperator(
            task_id='task_report_data_collection',
            source="{{ result('load_task_report_data') }}",
            name='taskreportdata',
            columns={
                'Billing Activity-Task Name (Full Path)': 'tasknamefullpath',
                'Billing Activity-Task Description': 'taskdescription',
                'Billing Activity-Task Status': 'taskstatus',
                'Created': 'created',
                'Billing Activity-Task Code': 'taskcode',
                'Billing Activity-Task Market Rate': 'marketrate',
                'Org Costs': 'orgcosts',
                'Lux Flag': 'luxflag',
                'taskUri': 'taskuri',
                'Billing Activity-Task Start Date': 'taskstartdate',
                'Billing Activity-Task End Date': 'taskenddate',
                'Billing Activity-Task Name': 'taskname',
                'Billing Activity-Task Time & Expense Entry Type': 'entrytype',
                'Billing Activity-Task Cost Type': 'costtype',
                'Billing Activity-Task Estimated Hours': 'esthrs',
            }
        )

        query_creatable_tasks = rail.QueryCollectionOperator(
            task_id='query_creatable_tasks',
            query="SELECT * FROM taskreportdata WHERE created = :created",
            query_params={
                "created": custom_methods.get_prev_day(config.time_zone)
            },
            name='query_creatable_tasks'
        )

        is_creatable_tasks_exists = rail.IfOperator(
            task_id='is_creatable_tasks_exists',
            test='{{ result("query_creatable_tasks", "length") > 0}}',
            yes_task='add_task_level',
            no_task='send_no_tasks_email'
        )

        send_no_tasks_email = rail.EmailOperator(
            task_id='send_no_tasks_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }}' + " | Adding new tasks from base project to all in-progress projects completed \
                    successfully without any tasks to be created at"+'{{ " " + result("get_logging_details")["dag_run_start_time"] }}',
            html_content="templates/emails/no_updation_mail.html"
        )

        add_task_level = rail.DataAdaptorOperator(
            task_id='add_task_level',
            source='{{ result("query_creatable_tasks") }}',
            columns=['tasknamefullpath', 'taskdescription', 'taskstatus', 'created', 'taskcode', 'marketrate', 'orgcosts',
                     'luxflag', 'taskuri', 'taskstartdate', 'taskenddate', 'taskname', 'entrytype', 'costtype', 'esthrs', 'tasklevel'],
            data=custom_methods.append_task_level
        )

        task_data_and_task_level = rail.CreateCollectionOperator(
            task_id='task_data_and_task_level',
            source='{{ result("add_task_level") }}',
            name='task_data_and_task_level'
        )

        query_max_level = rail.QueryCollectionOperator(
            task_id='query_max_level',
            query="SELECT MAX(tasklevel) FROM task_data_and_task_level"
        )

        process_task_levels = rail.TriggerDagRunForEachItemOperator(
            task_id='process_task_levels',
            retries=0,
            items=python_callable.make_list,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'pimco_process_task_levels_child_dag_{config.instance}',
            conf=lambda item, dag_run: {
                "dag_run_ecid": get_dagrun_ecid(dag_run),
                "task_level": item,
                "creatable_tasks": rail.result("query_creatable_tasks")
            }
        )

        wait_for_process_task_levels = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_task_levels',
            dag_runs='{{ result("process_task_levels") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        filter_master_log = rail.FilterLogEntriesOperator(
            task_id='filter_master_log',
            severity='Error',
        )

        is_any_child_failed = rail.IfOperator(
            task_id='is_any_child_failed',
            test="{{ result('filter_master_log', 'length') > 0 }}",
            yes_task='render_logs_csv',
            no_task='send_completion_mail'
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=['Projectname', 'Dagrunid', 'Status', 'Details', 'Ecid', '{{ current_time("%d/%m/%YT%H:%M:%S") }}'],
            row=['{{ item.properties.projectname }}', '{{ item.properties.runid }}',
                 '{{ item.properties.status }}', '{{ item.message }}', '{{ item.ecid }}'],
        )

        send_completion_mail = rail.EmailOperator(
            task_id='send_completion_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Adding new tasks from base project to all in-progress projects completed successfully at - {{ result("get_logging_details")["dag_run_start_time"] }}',
            html_content="templates/emails/completion_mail.html"
        )

        open_brackets = "{{"
        close_brackets = "}}"

        send_child_dags_failure_mail = rail.EmailOperator(
            task_id='send_child_dags_failure_mail',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Adding new task to all in-progress projects Failed at - {{ result("get_logging_details")["dag_run_start_time"] }}',
            html_content="templates/emails/child_failure_mail.html",
            files=[(f'{open_brackets}get_company_key(){close_brackets}_create_task_log_file_{open_brackets}current_time(){close_brackets}.csv',
                    f"{open_brackets}result('render_logs_csv'){close_brackets}")],
            params={
                'emails': config.tenant_email
            }
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        send_failure_mail = rail.EmailOperator(
            task_id='send_failure_mail',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Adding new task to all in-progress projects Failed at - {{ result("get_logging_details")["dag_run_start_time"] }}',
            html_content="templates/emails/failure_mail.html",
            params={
                'dag_id': f'pimco_add_new_task_to_project_master_dag_{config.instance}'
            }
        )

        get_logging_details >> get_task_report_details >> report_group_entry
        report_group_exit >> task_report_has_data

        task_report_has_data >> rail.Label("Yes") >> load_task_report_data >> task_report_data_collection \
            >> query_creatable_tasks >> is_creatable_tasks_exists
        task_report_has_data >> rail.Label("No") >> fail_no_task_report_data
        is_creatable_tasks_exists >> rail.Label("No") >> send_no_tasks_email
        is_creatable_tasks_exists >> rail.Label("Yes") >> add_task_level >> task_data_and_task_level \
            >> query_max_level >> process_task_levels >> wait_for_process_task_levels \
                >> filter_master_log >> is_any_child_failed
        is_any_child_failed >> rail.Label("Yes") >> render_logs_csv >> send_child_dags_failure_mail >> on_error
        is_any_child_failed >> rail.Label("No") >> send_completion_mail >> on_error >> send_failure_mail

    return dag


rail.for_each_instance(create_dag)
