"""
Main DAG for VP UKG Pro Journal Sync.
Fetches payroll data from UKG Pro and triggers cash disbursement sync.
"""
import json
import logging
from datetime import datetime, timedelta
from airflow.models import Variable
import rail
from vp_ukgpro_integration_v2.journal_sync.utils.python_callable_method import (
    aggregate_artifacts_method,
    aggregate_resolved_records_method,
    capture_main_error,
    format_cash_disbursement_data,
    format_payroll_data_method,
    get_wbs_mapping_data
)
from vp_ukgpro_integration_v2.journal_sync.utils.config_helper import (
    extract_dynamic_config_from_dag_run
)


# pylint:disable=too-many-statements,line-too-long,pointless-statement,
# pylint:disable=expression-not-assigned,too-many-locals,import-error
def create_dag(config):
    """
    Create main DAG for fetching payroll data from UKG Pro.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'vp_ukgpro_journal_sync_v2_main_{config.instance}',
        description=(
            'Fetch payroll data from UKG Pro and '
            'trigger cash disbursement sync'
        ),
        integration_type='generic',
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=['vantagepoint_ukgpro', 'journal_sync', 'main'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        extract_dynamic_config = rail.PythonOperator(
            task_id='extract_dynamic_config',
            python_callable=lambda dag_run: extract_dynamic_config_from_dag_run(dag_run, config)
        )

        def prepare_sync_timestamps():
            """
            Capture current timestamp and get last sync time.
            Returns dict with both timestamps to prevent race conditions.
            """
            customer_id = (
                rail.get_current_context()['dag_run'].conf
                .get('customerId')
            )
            variable_key = (
                f'vp_ukgpro_journal_sync_v2_last_run_'
                f'{config.instance}_{customer_id}'
            )
            current_time = (
                datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            )
            try:
                last_sync_time = Variable.get(variable_key)
                logging.info(
                    "Retrieved last sync time from Variable: %s",
                    last_sync_time
                )
            except KeyError:
                last_sync_time = config.initial_sync_time
                logging.info(
                    "Variable %s not found, using initial sync time: %s",
                    variable_key, last_sync_time
                )
            return {
                'last_sync_time': last_sync_time,
                'current_sync_time': current_time
            }

        prepare_timestamps = rail.PythonOperator(
            task_id='prepare_sync_timestamps',
            python_callable=prepare_sync_timestamps
        )

        def build_query_params():
            """Build query params using the captured timestamps"""
            timestamps = rail.result('prepare_sync_timestamps')
            last_sync_time = timestamps['last_sync_time']
            current_time = timestamps['current_sync_time']
            return {
                '$expand': 'Earnings,Deductions',
                '$filter': (
                    f"PayDate ge datetime'{last_sync_time}'"
                    f" and PayDate le datetime'{current_time}'"
                )
            }

        fetch_ukgpro_payroll = rail.UKGProThirdPartyPayOperator(
            task_id='fetch_ukgpro_payroll',
            ukgpro_conn_id="{{ dag_run.conf.connections.ukgpro }}",
            query_params=build_query_params
        )

        has_payroll_data = rail.IfOperator(
            task_id='has_payroll_data',
            test='{{ result("fetch_ukgpro_payroll") | length > 0 }}',
            yes_task='clear_processed_records_variable',
            no_task='update_last_sync_time'
        )

        clear_processed_records_variable = rail.SetVariableOperator(
            task_id='clear_processed_records_variable',
            append=False,
            name='processed_record_artifacts',
            value=lambda: []
        )

        clear_invalid_records_variable = rail.SetVariableOperator(
            task_id='clear_invalid_records_variable',
            append=False,
            name='invalid_record_artifacts',
            value=lambda: []
        )

        clear_resolved_records_variable = rail.SetVariableOperator(
            task_id='clear_resolved_records_variable',
            append=False,
            name='resolved_payroll_records',
            value=lambda: []
        )

        fetch_mapping_data = rail.PythonOperator(
            task_id='fetch_mapping_data',
            python_callable=get_wbs_mapping_data,
            op_kwargs={'instance': config.instance}
        )

        fetch_employee_data = rail.UKGProGenericOperator(
            task_id='fetch_employee_data',
            ukgpro_conn_id="{{ dag_run.conf.connections.ukgpro }}",
            endpoint=(
                "/personnel/v1/employment-details"
                "?employeeNumber="
                "{{result('resolve_each_record')['EmployeeNumber']}}"
            ),
            method='GET',
            required_fields=[
                'companyID', 'companyCode', 'companyName',
                'employeeID', 'orgLevel1Code', 'orgLevel2Code',
                'orgLevel3Code', 'orgLevel4Code', 'employeeNumber'
            ],
            extract_from_array=True
        )

        def build_resolved_record():
            """Merge only the needed fields from payroll + employee data."""
            payroll = rail.result('resolve_each_record')
            employee = rail.result('fetch_employee_data')
            return {
                'Id': payroll.get('Id'),
                'CheckAddModeCode': payroll.get('CheckAddModeCode'),
                'CheckNumber': payroll.get('CheckNumber'),
                'PayDate': payroll.get('PayDate'),
                'NameFirst': payroll.get('NameFirst'),
                'NameLast': payroll.get('NameLast'),
                'EmployeeNumber': payroll.get('EmployeeNumber'),
                'companyCode': employee.get('companyCode'),
                'orgLevel2Code': employee.get('orgLevel2Code'),
                'orgLevel3Code': employee.get('orgLevel3Code'),
                'orgLevel4Code': employee.get('orgLevel4Code'),
                'Earnings': [
                    {
                        'EarningCode': e.get('EarningCode'),
                        'EarningCurrentAmount': e.get('EarningCurrentAmount'),
                        'EarningDescription': e.get('EarningDescription')
                    }
                    for e in (payroll.get('Earnings') or [])
                ],
                'Deductions': [
                    {
                        'DeductionCode': d.get('DeductionCode'),
                        'EmployeeDeductionAmount': d.get(
                            'EmployeeDeductionAmount'
                        ),
                        'DeductionDescription': d.get('DeductionDescription')
                    }
                    for d in (payroll.get('Deductions') or [])
                ]
            }

        build_resolved_record_task = rail.PythonOperator(
            task_id='build_resolved_record',
            python_callable=build_resolved_record
        )

        store_resolved_record = rail.SetVariableOperator(
            task_id='store_resolved_record',
            append=True,
            name='resolved_payroll_records',
            value=lambda: rail.result('build_resolved_record')
        )

        resolve_each_record_end = rail.EmptyOperator(
            task_id='resolve_each_record_end'
        )

        resolve_each_record = rail.ForEachOperator(
            task_id='resolve_each_record',
            items="{{ result('fetch_ukgpro_payroll') | to_json }}",
            start_task='fetch_employee_data',
            end_task='resolve_each_record_end'
        )

        fetch_resolved_records = rail.PythonOperator(
            task_id='fetch_resolved_records',
            python_callable=lambda: rail.get_dag_run_var(
                'resolved_payroll_records'
            )
        )

        aggregate_resolved_records = rail.PythonOperator(
            task_id='aggregate_resolved_records',
            python_callable=aggregate_resolved_records_method
        )

        format_payroll_data = rail.PythonOperator(
            task_id='format_payroll_data',
            python_callable=format_payroll_data_method
        )

        write_record_artifact = rail.PythonOperator(
            task_id='write_record_artifact',
            python_callable=lambda: rail.write_json_artifact(
                format_cash_disbursement_data(
                    notification_email=config.notification_email
                )
            )
        )

        store_artifact_reference = rail.SetVariableOperator(
            task_id='store_artifact_reference',
            append=True,
            name='processed_record_artifacts',
            value=lambda: rail.result('write_record_artifact')
        )

        write_invalid_artifact = rail.PythonOperator(
            task_id='write_invalid_artifact',
            python_callable=lambda: rail.write_json_artifact(
                rail.result('format_payroll_data').get('invalid_records') or []
            )
        )

        store_invalid_artifact_reference = rail.SetVariableOperator(
            task_id='store_invalid_artifact_reference',
            append=True,
            name='invalid_record_artifacts',
            value=lambda: rail.result('write_invalid_artifact')
        )

        process_each_record_end = rail.EmptyOperator(
            task_id='process_each_record_end'
        )

        process_each_record = rail.ForEachOperator(
            task_id='process_each_record',
            items="{{ result('aggregate_resolved_records') | to_json }}",
            start_task='format_payroll_data',
            end_task='process_each_record_end'
        )

        fetch_artifact_references = rail.PythonOperator(
            task_id='fetch_artifact_references',
            python_callable=lambda: rail.get_dag_run_var(
                'processed_record_artifacts'
            )
        )

        aggregate_artifacts = rail.PythonOperator(
            task_id='aggregate_artifacts',
            python_callable=aggregate_artifacts_method
        )

        trigger_cash_disbursement = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_cash_disbursement',
            items=lambda: rail.result('aggregate_artifacts'),
            trigger_dag_id=(
                f'vp_ukgpro_journal_sync_v2_cash_disbursement_{config.instance}'
            ),
            conf=lambda item: {
                'artifact_name': item,
                'connections': (
                    rail.get_current_context()['dag_run'].conf
                    .get('connections')
                )
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_cd_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_cd_dag_runs',
            dag_runs="{{ result('trigger_cash_disbursement') }}",
            allowed_states=['success', 'failed'],
            failed_states=[],
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_cd_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_cd_errors',
            dag_runs="{{ result('trigger_cash_disbursement') }}",
            dagrun_task_id='catch_cd_dag_error',
            flatten=True
        )

        catch_main_dag_error = rail.PythonOperator(
            task_id='catch_main_dag_error',
            trigger_rule='all_done',
            python_callable=capture_main_error
        )

        def fetch_and_merge_invalid_records():
            artifact_names = (
                rail.get_dag_run_var('invalid_record_artifacts') or []
            )
            all_records = []
            for artifact_name in artifact_names:
                records = json.loads(rail.read_artifact(artifact_name))
                all_records.extend(records)
            return all_records

        fetch_invalid_records = rail.PythonOperator(
            task_id='fetch_invalid_records',
            python_callable=fetch_and_merge_invalid_records
        )

        has_invalid_records = rail.IfOperator(
            task_id='has_invalid_records',
            test='{{ result("fetch_invalid_records") | length > 0 }}',
            yes_task='generate_invalid_records_csv',
            no_task='update_last_sync_time'
        )

        generate_invalid_records_csv = rail.WriteCSVFileOperator(
            task_id='generate_invalid_records_csv',
            source="{{ result('fetch_invalid_records') | to_json }}",
            header=[
                'payrollCode', 'companyCode', 'orglevel2',
                'orglevel3', 'orglevel4',
                'employeeNumber', 'employeeName', 'BankCode',
                'Payee', 'CheckNo',
                'TransDate', 'WBS1', 'WBS2', 'WBS3', 'Account', 'Amount',
                'DetailDescription', 'Batch', 'wbs_error'
            ],
            row=[
                '{{ item.payrollCode }}', '{{ item.companyCode }}',
                '{{ item.orglevel2 }}', '{{ item.orglevel3 }}',
                '{{ item.orglevel4 }}', '{{ item.employeeNumber }}',
                '{{ item.employeeName }}', '{{ item.BankCode }}',
                '{{ item.Payee }}', '{{ item.CheckNo }}',
                '{{ item.TransDate }}', '{{ item.WBS1 }}',
                '{{ item.WBS2 }}', '{{ item.WBS3 }}',
                '{{ item.Account }}', '{{ item.Amount }}',
                '{{ item.DetailDescription }}', '{{ item.Batch }}',
                '{{ item.wbs_error }}'
            ]
        )

        send_invalid_records_email = rail.EmailOperator(
            task_id='send_invalid_records_email',
            to=config.notification_email,
            subject='Info | Vantagepoint Journal Sync | Invalid Records',
            html_content='templates/invalid_records_mail.html',
            files=[(
                'invalid_records.csv',
                "{{ result('generate_invalid_records_csv') }}"
            )]
        )

        def update_last_sync_time():
            """
            Update the last sync time Variable with the captured timestamp.
            Uses the SAME timestamp that was used for the query to prevent
            race conditions.
            """
            customer_id = (
                rail.get_current_context()['dag_run'].conf
                .get('customerId')
            )
            variable_key = (
                f'vp_ukgpro_journal_sync_v2_last_run_'
                f'{config.instance}_{customer_id}'
            )
            timestamps = rail.result('prepare_sync_timestamps')
            current_time = timestamps['current_sync_time']
            Variable.set(variable_key, current_time)
            logging.info(
                "Updated last sync time Variable '%s' to: %s",
                variable_key, current_time
            )
            return current_time

        update_sync_time = rail.PythonOperator(
            task_id='update_last_sync_time',
            trigger_rule='all_done',
            python_callable=update_last_sync_time
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url=(
                "{{ var.value.get('middleware_api_base_url', '') }}"
            ),
            trigger_rule='all_done'
        )

        extract_dynamic_config >> prepare_timestamps >> fetch_ukgpro_payroll >> has_payroll_data
        (
            has_payroll_data >> rail.Label('Yes') >>
            clear_processed_records_variable
        )
        has_payroll_data >> rail.Label('No') >> update_sync_time

        clear_processed_records_variable >> clear_invalid_records_variable
        clear_invalid_records_variable >> clear_resolved_records_variable
        clear_resolved_records_variable >> fetch_mapping_data
        fetch_mapping_data >> resolve_each_record
        (
            resolve_each_record >> fetch_employee_data >>
            build_resolved_record_task
        )
        (
            build_resolved_record_task >> store_resolved_record >>
            resolve_each_record_end
        )
        resolve_each_record >> resolve_each_record_end

        resolve_each_record_end >> fetch_resolved_records
        fetch_resolved_records >> aggregate_resolved_records
        aggregate_resolved_records >> process_each_record
        process_each_record >> format_payroll_data
        (
            format_payroll_data >> write_record_artifact >>
            store_artifact_reference
        )
        (
            store_artifact_reference >> write_invalid_artifact >>
            store_invalid_artifact_reference
        )
        store_invalid_artifact_reference >> process_each_record_end
        process_each_record >> process_each_record_end

        process_each_record_end >> fetch_artifact_references
        fetch_artifact_references >> aggregate_artifacts
        aggregate_artifacts >> trigger_cash_disbursement
        trigger_cash_disbursement >> wait_for_cd_dag_runs
        wait_for_cd_dag_runs >> gather_cd_errors
        gather_cd_errors >> catch_main_dag_error
        catch_main_dag_error >> fetch_invalid_records
        fetch_invalid_records >> has_invalid_records
        (
            has_invalid_records >> rail.Label('Yes') >>
            generate_invalid_records_csv
        )
        has_invalid_records >> rail.Label('No') >> update_sync_time
        (
            generate_invalid_records_csv >> send_invalid_records_email >>
            update_sync_time
        )

        update_sync_time >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
