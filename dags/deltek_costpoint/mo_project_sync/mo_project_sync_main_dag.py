from datetime import datetime, timedelta
import itertools
import pendulum
from pytz import timezone
from airflow.models import Variable
import rail

# pylint:disable = too-many-statements, line-too-long
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_mo_project_sync_main_{config.instance}',
        description=f'deltek_costpoint_mo_project_sync_main{config.instance}',
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_last_run_date'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_last_run_date',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def do_get_last_run_date():
            current_time = pendulum.now(
                config.cp_timezone) - timedelta(seconds=2)
            lookup_timestamp_value = Variable.get(
                config.last_run_date_var_name, default_var=None)
            last_run_date = datetime.strptime(
                lookup_timestamp_value, "%Y-%m-%d %H:%M:%S") if lookup_timestamp_value else current_time
            formated_last_run = last_run_date.strftime("%Y-%m-%d %H:%M:%S")
            rail.set_result(current_time.strftime(
                "%Y-%m-%d %H:%M:%S"), 'current_time')
            return formated_last_run

        get_last_run_date = rail.PythonOperator(
            task_id='get_last_run_date',
            python_callable=do_get_last_run_date
        )

        get_modified_mos = rail.DeltekCostPointODBCOperator(
            task_id='get_modified_mos',
            deltek_costpoint_odbc_conn_id=config.odbc_conn_id,
            query=config.mo_query,
            query_params=["{{result('get_last_run_date')}}"]
        )

        has_project_data = rail.IfOperator(
            task_id='has_project_data',
            test=lambda: bool(rail.result('get_modified_mos')),
            yes_task='group_data_by_root_project',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        group_data_by_root_project = rail.PythonOperator(
            task_id='group_data_by_root_project',
            python_callable=lambda: [{'root_project_id': k, 'data': list(g)} for k, g in itertools.groupby(
                (rail.result('get_modified_mos')), lambda x: x['MO_ID'])]
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
        )

        get_billing_rates_from_replicon = rail.RepliconServiceOperator(
            task_id='get_billing_rates_from_replicon',
            endpoint='/services/BillingRateListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "999999",
                "columnUris": [
                    "urn:replicon:billing-rate-list-column:name",
                    "urn:replicon:billing-rate-list-column:description"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: list(map(lambda x: {'name': x['cells'][0].get(
                'textValue'), 'code': x['cells'][1].get('textValue')}, data['rows']))
        )

        get_costpoint_plcs = rail.DeltekCostPointServiceOperator(
            task_id='get_costpoint_plcs',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data={
                "filter": {
                    "id": "replicon_exp_plcs",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "ADMUDT07_HDR",
                                "conditions": [
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            },
            data_handler=lambda data: list(map(lambda x: {
                                           'name': x['row']['data']['UDT07_NAME'][0:50], 'code': x['row']['data']['UDT07_ID']}, data['document']['rows'])),
        )

        process_new_billingrates = rail.RepliconServiceCallForEachItemOperator(
            task_id='process_new_billingrates',
            endpoint="/services/BillingRateService1.svc/PutCompanyBillingRate2",
            items=lambda: list(filter(lambda x: not rail.find_first_by_attr_and_get_attr(rail.result(
                'get_billing_rates_from_replicon'), 'code', x['code']), rail.result('get_costpoint_plcs'))),
            data={
                "billingRate": {
                    "target": {
                        "uri": null,
                        "name": "{{ item.name}}"
                    },
                    "name": "{{ item.name}}",
                    "description": "{{ item.code}}",
                    "isEnabled": "true",
                    "defaultRates": []
                }
            }
        )

        get_updated_billing_rates_from_replicon = rail.RepliconServiceOperator(
            task_id='get_updated_billing_rates_from_replicon',
            endpoint='/services/BillingRateListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "999999",
                "columnUris": [
                    "urn:replicon:billing-rate-list-column:name",
                    "urn:replicon:billing-rate-list-column:description"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: list(map(lambda x: {'name': x['cells'][0].get(
                'textValue'), 'code': x['cells'][1].get('textValue'), 'uri': x['cells'][0]['uri']}, data['rows']))
        )

        get_replicon_divisions = rail.RepliconServiceOperator(
            task_id='get_replicon_divisions',
            endpoint='/services/DivisionListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "999999",
                "columnUris": [
                    "urn:replicon:division-list-column:division",
                    "urn:replicon:division-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: list(map(lambda x: {'name': x['cells'][0].get(
                'textValue'), 'code': x['cells'][1].get('textValue'), 'uri': x['cells'][0]['uri']}, data['rows']))
        )

        get_project_udfs = rail.RepliconServiceOperator(
            task_id='get_project_udfs',
            endpoint="/services/ProjectCustomFieldListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "999999",
                "columnUris": [
                    "urn:replicon:project-custom-field-list-column:project-custom-field",
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: list(
                map(lambda x: x['cells'][0], data['rows']))
        )

        process_each_root_project = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_root_project',
            retries=0,
            items=lambda: rail.result('group_data_by_root_project'),
            trigger_dag_id=f'deltek_costpoint_mo_project_sync_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {'item': {**item},
                               'divisions': rail.result('get_replicon_divisions'),
                               'permission_sets': rail.result('get_all_permission_sets'),
                               'project_udfs': rail.result('get_project_udfs')
                               }
        )

        wait_for_process_each_root_project = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_root_project',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("process_each_root_project") }}'
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_root_project") }}',
            dagrun_task_id='create_log',
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=lambda: list(list(itertools.chain(
                *list(map(rail.load_all_records, rail.result('gather_child_logs'))))))
        )

        get_logged_errors = rail.PythonOperator(
            task_id='get_logged_errors',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('format_logs'), 'properties.status', 'Error')
        )

        has_error_logs = rail.IfOperator(
            task_id='has_error_logs',
            test=lambda: bool(rail.result('get_logged_errors')),
            yes_task='create_csv_lines',
            no_task='update_last_run_date'
        )

        create_csv_lines = rail.WriteCSVFileOperator(
            task_id='create_csv_lines',
            source="{{ result('format_logs') | to_json }}",
            header=['Parent Job ID',
                    'Project ID',
                    'Project Name',
                    'Status',
                    'Details',
                    'Job ID'],
            row=[
                "{{ dag_run_ecid() }}",
                "{{ item.properties.proj_id }}",
                "{{ item.properties.proj_name }}",
                "{{ item.properties.status }}",
                "{{ item.properties.get('details','') }}",
                "{{ item.ecid }}",
            ]
        )

        log_filename = rail.PythonOperator(
            task_id='log_filename',
            python_callable=lambda:  rail.render_template(
                "Log_{{ dag_run_ecid() }}_project_sync.csv")
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines')}}",
            output_file_name='{{ result("log_filename") }}',
            expires_in_seconds=7*24*60*60,
        )

        send_mail_error = rail.EmailOperator(
            task_id='send_mail_error',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Deltek Costpoint MO sync Completed with Errors - {{ current_time() }}''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Deltek Costpoint MO sync is completed with failures based on the file - '{{ result('log_filename') }}'. Please find the  link below to download the logs.
            <br /> <br /> <a href="{{ result('generate_download_link') }}">Download log file</a><br /> <br /><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
            <br />
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Replicon Inc.</p> ''',
            params=None,
        )

        update_last_run_date = rail.PythonOperator(
            task_id='update_last_run_date',
            python_callable=lambda: Variable.set(config.last_run_date_var_name,
                                                 rail.result('get_last_run_date', 'current_time'))
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> get_last_run_date
        get_last_run_date >> get_modified_mos >> has_project_data >> rail.Label(
            'yes') >> group_data_by_root_project
        has_project_data >> rail.Label(
            'no') >> delete_this_dagrun >> update_last_run_date >> finish
        group_data_by_root_project >> get_costpoint_plcs >> get_project_udfs >> get_all_permission_sets >> \
            get_billing_rates_from_replicon >> process_new_billingrates >> \
            get_updated_billing_rates_from_replicon >> get_replicon_divisions >> \
            process_each_root_project >> wait_for_process_each_root_project >> \
            gather_child_logs >> format_logs >> get_logged_errors >> has_error_logs
        has_error_logs >> rail.Label(
            'yes') >> create_csv_lines >> log_filename >> generate_download_link >> \
            send_mail_error >> update_last_run_date >> finish
        has_error_logs >> rail.Label('no') >> update_last_run_date >> finish

        return dag


rail.for_each_instance(create_dag)
