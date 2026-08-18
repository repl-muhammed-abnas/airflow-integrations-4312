# pylint: disable=line-too-long wildcard-import redefined-outer-name unused-wildcard-import
from datetime import timedelta
import pendulum
from cie_gen3_products.banking_utility.utils.python_callable import *
import rail


def create_main_airflow_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'cie_{config.company_key}_Gen3_Products_BankingUtillity{dag_id_postfix}'.lower(
        ),
        description=f'cie_Gen3_Products_{config.company_key}_BankingUtillity',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2022, 10, 10,  tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={

        },
    ) as dag:

        report_name = config.base_report_name

        get_timedata_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_timedata_report_details',
            report_name=report_name,
        )

        get_report_filter = rail.PythonOperator(
            task_id='get_report_filter',
            python_callable=get_filter_fields,
            op_args=[config]
        )

        run_report_1_group_entry, run_report_1_group_exit = rail.run_report(
            group_id='generate_base_report_in_batch',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_timedata_report_details').get('uri'),
                        "filterValues": rail.result('get_report_filter'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
        )

        is_report_data = rail.IfOperator(
            task_id='is_report_data',
            test="{{ result('generate_base_report_in_batch.get_report_result', 'has_data') }}",
            yes_task='get_all_time_off_types',
            no_task='final_status',
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
        )

        get_report_data = rail.PythonOperator(
            task_id='get_report_data',
            python_callable=generate_csv_data_with_mapped_timeoff_uri,
        )
        get_grouped_report_data = rail.PythonOperator(
            task_id='get_grouped_report_data',
            python_callable=group_data
        )
        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )
        process_entry_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_entry_child',
            items=lambda: rail.result('get_grouped_report_data'),
            trigger_dag_id=f'cie_{config.company_key}_process_each_user_timeoff_child{dag_id_postfix}'.lower(
            ),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
            conf=lambda item: {
                'item': item,
                'logid': rail.result('create_log'),
            }
        )

        wait_for_process_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_child',
            dag_runs='{{ result("process_entry_child") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_logs = rail.PythonOperator(
            task_id='get_logs',
            python_callable=get_error_logs
        )

        create_log_csv = rail.WriteCSVFileOperator(
            task_id='create_log_csv',
            source=lambda: rail.result('get_logs'),
            header=['User Name',
                    'Accrual Date',
                    'Accrual Hours',
                    'Time Off Type',
                    'Status',
                    'Job Details'],
            row=lambda item: [
                item['user_name'],
                item['accrual_date'],
                item['hour_to_accrue'],
                item['time_off_type'],
                item['status'],
                item['childjobid'],
            ],
        )

        get_subject = rail.PythonOperator(
            task_id='get_subject',
            python_callable=get_subject_details
        )

        send_task_completion_email = rail.EmailOperator(
            task_id='send_task_completion_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Comp Time Accrual details - {{ result("get_subject") }} - {{ current_time_in_specified_tz() }}',
            html_content="templates/email/email_for_success_format.html",
            files=[('replicon_logs_{{ current_time_in_specified_tz("America/New_York","%m%d%Y") }}.csv',
                    '{{result("create_log_csv")}}')]
        )
        send_task_failure_email = rail.EmailOperator(
            task_id='send_task_failure_email',
            trigger_rule='one_failed',
            to=config.alert_email,
            subject="{{ get_company_key() }} | Comp Time Accrual - failed to Accrue - {{ current_time_in_specified_tz() }}",
            html_content="templates/email/failure_email.html",
            params={
                'dag_id': f'{config.company_key}_timesheet_approval_master{dag_id_postfix}'.lower()
            }
        )

        def final_status(**kwargs):
            for task_instance in kwargs['dag_run'].get_task_instances():
                if task_instance.current_state() == "failed" and \
                        task_instance.task_id != kwargs['task_instance'].task_id:
                    raise Exception(
                        f"Task {task_instance.task_id} failed. Failing this DAG run")

        final_status = rail.PythonOperator(
            task_id='final_status',
            python_callable=final_status,
        )

        get_timedata_report_details >> get_report_filter >> run_report_1_group_entry >> run_report_1_group_exit >> is_report_data
        is_report_data >> rail.Label(
            'Yes') >> get_all_time_off_types >> get_report_data >> get_grouped_report_data >> create_log >> process_entry_child >> wait_for_process_child >> get_logs >> create_log_csv >> get_subject >> send_task_completion_email >> send_task_failure_email >> final_status

        is_report_data >> rail.Label('No') >> final_status

    return dag


rail.for_each_instance(create_main_airflow_dag)
