
from datetime import timedelta
import json
from airflow.models import Variable
from isuzu.invoice_sync.utils import python_callable, request_payload
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'{config.company_key}_invoice_sync_create_invoices_in_replicon_based_on_sftp_file_child_{config.instance}',
        description=f'Create invoices in Replicon based on SFTP file {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='load_csv_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='load_csv_data',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        load_csv_data = rail.PythonOperator(
            task_id='load_csv_data',
            python_callable=lambda dag_run: rail.load_all_records(
                dag_run.conf["reportdata"])
        )

        get_request_id_details = rail.PythonOperator(
            task_id='get_request_id_details',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('load_csv_data'), 'Internal Notes', dag_run.conf["Request_ID"])
        )

        get_project_uri = rail.RepliconServiceOperator(
            task_id='get_project_uri',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=lambda dag_run: {
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:project-list-column:project",
                    "urn:replicon:project-list-column:code"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:project-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": "Non CB Expense",
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            response_filter=lambda response: response.json()['d']['rows']
        )
        log_invoice_date_details = rail.PythonOperator(
            task_id='log_invoice_date_details',
            python_callable=python_callable.get_dates
        )

        log_invoice_line_amount = rail.PythonOperator(
            task_id='log_invoice_line_amount',
            python_callable=lambda dag_run:   float(
                dag_run.conf['Line_Item_Quantity']) * float(dag_run.conf['Line_Item_Unit_Price'])
        )
        log_invoice_description = rail.PythonOperator(
            task_id='log_invoice_description',
            python_callable=lambda dag_run:  f"Description:{dag_run.conf['Line_Item_Description']} \\ Vendor: {dag_run.conf['Vendor_Name']} \\ Expense code: {dag_run.conf['Line_Item_Expense_Type_Name']} \\ Profit Center: {dag_run.conf['Line_Item_Custom_07_Profit_Center']}"
        )
        if_request_line_item_custom_10_client_blank_8 = rail.IfOperator(
            task_id='if_request_line_item_custom_10_client_blank_8',
            test='''{{ dag_run.conf.Line_Item_Custom_10_Client | is_falsy }}''',
            yes_task="if_log_4_equals_to_noncbexpense_9",
            no_task="log_31",
        )

        if_log_4_equals_to_noncbexpense_9 = rail.IfOperator(
            task_id='if_log_4_equals_to_noncbexpense_9',
            test='''{{ result('get_request_id_details') == 'Non CB Expense' }}''',
            yes_task="log_existinginvoiceuri_10",
            no_task="get_invoice_payload_24",
        )

        log_existinginvoiceuri_10 = rail.PythonOperator(
            task_id='log_existinginvoiceuri_10',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('load_csv_data'), 'Internal Notes', dag_run.conf["Request_ID"], "Invoiceuri")
        )

        change_invoice_status_v2_52_52_11 = rail.SimpleHttpOperator(
            task_id='change_invoice_status_v2_52_52_11',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/MarkAsBilled',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=json.dumps({
                "invoiceUri": "{{ result('log_existinginvoiceuri_10') }}"
            }, indent=4),
            extra_options={
                'verify': False
            },
        )

        change_invoice_status_v2_52_52_12 = rail.SimpleHttpOperator(
            task_id='change_invoice_status_v2_52_52_12',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/MarkAsOpen',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=json.dumps({
                "invoiceUri": "{{ result('log_existinginvoiceuri_10') }}"
            }, indent=4),
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        get_invoice_payload_15 = rail.PythonOperator(
            task_id='get_invoice_payload_15',
            python_callable=request_payload.get_create_invoice_payload_15

        )
        addinvoicelineitems_15 = rail.SimpleHttpOperator(
            task_id='addinvoicelineitems_15',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/PutInvoiceItem2',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data="{{ result('get_invoice_payload_15') | to_json }}",
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        change_invoice_status_v2_52_52_16 = rail.SimpleHttpOperator(
            task_id='change_invoice_status_v2_52_52_16',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/MarkAsBilled',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=json.dumps({
                "invoiceUri": "{{ result('log_existinginvoiceuri_10') }}"
            }, indent=4),
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        change_invoice_status_v2_52_52_17 = rail.SimpleHttpOperator(
            task_id='change_invoice_status_v2_52_52_17',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/MarkAsPaid',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=json.dumps({
                "invoiceUri": "{{ result('log_existinginvoiceuri_10') }}"
            }, indent=4),
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        get_invoice_details_v2_54_54_18 = rail.SimpleHttpOperator(
            task_id='get_invoice_details_v2_54_54_18',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/GetInvoiceDetails',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=json.dumps({
                "invoiceUri": "{{ result('log_existinginvoiceuri_10') }}"
            }, indent=4),
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        get_invoice_payload_24 = rail.PythonOperator(
            task_id='get_invoice_payload_24',
            python_callable=request_payload.get_create_invoice_payload_24

        )

        createinvoice_24 = rail.SimpleHttpOperator(
            task_id='createinvoice_24',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/PutInvoice3',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data="{{ result('get_invoice_payload_24') | to_json }}",
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        get_invoice_payload_26 = rail.PythonOperator(
            task_id='get_invoice_payload_26',
            python_callable=request_payload.get_create_invoice_payload_26

        )
        addinvoicelineitems_26 = rail.SimpleHttpOperator(
            task_id='addinvoicelineitems_26',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/PutInvoiceItem2',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data="{{ result('get_invoice_payload_26') | to_json }}",
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        change_invoice_status_v2_52_52_27 = rail.SimpleHttpOperator(
            task_id='change_invoice_status_v2_52_52_27',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/MarkAsBilled',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=json.dumps({
                "invoiceUri": "{{ result('createinvoice_24').invoiceReference.uri }}"
            }, indent=4),
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        change_invoice_status_v2_52_52_28 = rail.SimpleHttpOperator(
            task_id='change_invoice_status_v2_52_52_28',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/MarkAsPaid',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=json.dumps({
                "invoiceUri": "{{ result('createinvoice_24').invoiceReference.uri }}"
            }, indent=4),
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        get_invoice_details_v2_54_54_29 = rail.SimpleHttpOperator(
            task_id='get_invoice_details_v2_54_54_29',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/GetInvoiceDetails',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=json.dumps({
                "invoiceUri": "{{ result('createinvoice_24').invoiceReference.uri }}"
            }, indent=4),
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        log_31 = rail.PythonOperator(
            task_id='log_31',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(list(filter(lambda name: name['Internal Notes'] == dag_run.conf["Request_ID"]
                                                                                             and name['Client Name'] == dag_run.conf["Line_Item_Custom_10_Client"], rail.result('load_csv_data'))), 'Internal Notes', dag_run.conf["Request_ID"], "Client Name")
        )

        if_log_23_equals_to_dataworkato_servicereceive_requestrequestline_item_custom_10_client_32 = rail.IfOperator(
            task_id='if_log_23_equals_to_dataworkato_servicereceive_requestrequestline_item_custom_10_client_32',
            test='''{{ result('log_31') == dag_run.conf.Line_Item_Custom_10_Client }}''',
            yes_task="log_existinginvoiceuri_33",
            no_task="get_invoice_payload_48",
        )

        log_existinginvoiceuri_33 = rail.PythonOperator(
            task_id='log_existinginvoiceuri_33',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(list(filter(lambda name: name['Internal Notes'] == dag_run.conf["Request_ID"]
                                                                                             and name['Client Name'] == dag_run.conf["Line_Item_Custom_10_Client"], rail.result('load_csv_data'))), 'Internal Notes', dag_run.conf["Request_ID"], "Invoiceuri")
        )

        change_invoice_status_v2_52_52_34 = rail.SimpleHttpOperator(
            task_id='change_invoice_status_v2_52_52_34',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/MarkAsBilled',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=json.dumps({
                "invoiceUri": "{{ result('log_existinginvoiceuri_33') }}"
            }, indent=4),
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        change_invoice_status_v2_52_52_35 = rail.SimpleHttpOperator(
            task_id='change_invoice_status_v2_52_52_35',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/MarkAsOpen',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=json.dumps({
                "invoiceUri": "{{ result('log_existinginvoiceuri_33') }}"
            }, indent=4),
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        search_projects_37_37_37 = rail.RepliconServiceOperator(
            task_id='search_projects_37_37_37',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:project-list-column:project",
                    "urn:replicon:project-list-column:code"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:project-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": dag_run.conf['Line_Item_Custom_11_Project'],
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            response_filter=lambda response: response.json()['d']['rows']
        )

        get_invoice_payload_39 = rail.PythonOperator(
            task_id='get_invoice_payload_39',
            python_callable=request_payload.get_create_invoice_payload_39

        )
        addinvoicelineitems_39 = rail.SimpleHttpOperator(
            task_id='addinvoicelineitems_39',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/PutInvoiceItem2',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data="{{ result('get_invoice_payload_39') | to_json }}",
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']

        )

        change_invoice_status_v2_52_52_40 = rail.SimpleHttpOperator(
            task_id='change_invoice_status_v2_52_52_40',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/MarkAsBilled',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=json.dumps({
                "invoiceUri": "{{ result('log_existinginvoiceuri_33') }}"
            }, indent=4),
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        change_invoice_status_v2_52_52_41 = rail.SimpleHttpOperator(
            task_id='change_invoice_status_v2_52_52_41',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/MarkAsPaid',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=json.dumps({
                "invoiceUri": "{{ result('log_existinginvoiceuri_33') }}"
            }, indent=4),
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        get_invoice_details_v2_54_54_42 = rail.SimpleHttpOperator(
            task_id='get_invoice_details_v2_54_54_42',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/GetInvoiceDetails',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=json.dumps({
                "invoiceUri": "{{ result('log_existinginvoiceuri_33') }}"
            }, indent=4),
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        get_invoice_payload_48 = rail.PythonOperator(
            task_id='get_invoice_payload_48',
            python_callable=request_payload.get_create_invoice_payload_48

        )
        createinvoice_48 = rail.SimpleHttpOperator(
            task_id='createinvoice_48',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/PutInvoice3',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data="{{ result('get_invoice_payload_48') | to_json }}",
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        search_projects_37_37_49 = rail.RepliconServiceOperator(
            task_id='search_projects_37_37_49',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:project-list-column:project",
                    "urn:replicon:project-list-column:code"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:project-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": dag_run.conf['Line_Item_Custom_11_Project'],
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            response_filter=lambda response: response.json()['d']['rows']
        )

        get_invoice_payload_51 = rail.PythonOperator(
            task_id='get_invoice_payload_51',
            python_callable=request_payload.get_create_invoice_payload_51

        )
        addinvoicelineitems_51 = rail.SimpleHttpOperator(
            task_id='addinvoicelineitems_51',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/PutInvoiceItem2',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data="{{ result('get_invoice_payload_51') | to_json }}",
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        change_invoice_status_v2_52_52_52 = rail.SimpleHttpOperator(
            task_id='change_invoice_status_v2_52_52_52',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/MarkAsBilled',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=json.dumps({
                "invoiceUri": "{{ result('createinvoice_48').invoiceReference.uri }}"
            }, indent=4),
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        change_invoice_status_v2_52_52_53 = rail.SimpleHttpOperator(
            task_id='change_invoice_status_v2_52_52_53',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/MarkAsPaid',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=json.dumps({
                "invoiceUri": "{{ result('createinvoice_48').invoiceReference.uri }}"
            }, indent=4),
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )

        get_invoice_details_v2_54_54_54 = rail.SimpleHttpOperator(
            task_id='get_invoice_details_v2_54_54_54',
            method='POST',
            endpoint='/services/billing/InvoiceService2.svc/GetInvoiceDetails',
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json;',
                "Authorization": "Bearer {{ var.value." + config.token_var + " }}"
            },
            data=json.dumps({
                "invoiceUri": "{{ result('createinvoice_48').invoiceReference.uri }}"
            }, indent=4),
            extra_options={
                'verify': False
            },
            response_filter=lambda response: response.json()['d']
        )
        get_invoice_details_tolog = rail.PythonOperator(
            task_id='get_invoice_details_tolog',
            python_callable=python_callable.get_invoice_details_tolog,
        )

        invoice_import_logs_add_entry_55 = rail.WriteLogOperator(
            task_id='invoice_import_logs_add_entry_55',
            log="{{ dag_run.conf.logid }}",
            message="na",
            severity="Success",
            properties={
                "request_id": "{{ dag_run.conf.Request_ID }}",
                "invoice_number": "{{ result('get_invoice_details_tolog').invoiceNumberText }}",
                "status": "Success",
                "child_job_id": "{{ dag_run_ecid() }}  | Created"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )
        failure_reason = rail.PythonOperator(
            task_id='failure_reason',
            trigger_rule='one_failed',
            python_callable=python_callable.failure_reason,
        )
        invoice_import_logs_add_entry_57 = rail.WriteLogOperator(
            task_id='invoice_import_logs_add_entry_57',
            log="{{ dag_run.conf.logid }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error ",
            properties={
                "request_id": "{{ dag_run.conf.Request_ID }}",
                "invoice_number": "{{ result('get_invoice_details_tolog').invoiceNumberText }}",
                "status": "Error ",
                "child_job_id": "{{ dag_run_ecid() }} | {{ result('failure_reason') }}"
            }
        )
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> load_csv_data
        load_csv_data >> get_request_id_details >> get_project_uri >> log_invoice_date_details >> log_invoice_line_amount >> log_invoice_description >> if_request_line_item_custom_10_client_blank_8
        if_request_line_item_custom_10_client_blank_8 >> rail.Label(
            'Yes') >> if_log_4_equals_to_noncbexpense_9
        if_log_4_equals_to_noncbexpense_9 >> rail.Label(
            'Yes') >> log_existinginvoiceuri_10 >> change_invoice_status_v2_52_52_11 >> change_invoice_status_v2_52_52_12 >> get_invoice_payload_15 >> addinvoicelineitems_15 >> change_invoice_status_v2_52_52_16 >> change_invoice_status_v2_52_52_17 >> get_invoice_details_v2_54_54_18 >> get_invoice_details_tolog
        if_log_4_equals_to_noncbexpense_9 >> rail.Label(
            'No') >> get_invoice_payload_24
        get_invoice_payload_24 >> createinvoice_24 >> get_invoice_payload_26 >> addinvoicelineitems_26 >> change_invoice_status_v2_52_52_27 >> change_invoice_status_v2_52_52_28 >> get_invoice_details_v2_54_54_29 >> get_invoice_details_tolog
        if_request_line_item_custom_10_client_blank_8 >> rail.Label(
            'No') >> log_31
        log_31 >> if_log_23_equals_to_dataworkato_servicereceive_requestrequestline_item_custom_10_client_32
        if_log_23_equals_to_dataworkato_servicereceive_requestrequestline_item_custom_10_client_32 >> rail.Label(
            'Yes') >> log_existinginvoiceuri_33 >> change_invoice_status_v2_52_52_34 >> change_invoice_status_v2_52_52_35 >> search_projects_37_37_37 >> get_invoice_payload_39 >> addinvoicelineitems_39 >> change_invoice_status_v2_52_52_40 >> change_invoice_status_v2_52_52_41 >> get_invoice_details_v2_54_54_42 >> get_invoice_details_tolog
        if_log_23_equals_to_dataworkato_servicereceive_requestrequestline_item_custom_10_client_32 >> rail.Label(
            'No') >> get_invoice_payload_48
        get_invoice_payload_48 >> createinvoice_48 >> search_projects_37_37_49 >> get_invoice_payload_51 >> addinvoicelineitems_51 >> change_invoice_status_v2_52_52_52 >> change_invoice_status_v2_52_52_53 >> get_invoice_details_v2_54_54_54 >> get_invoice_details_tolog

        get_invoice_details_tolog >> invoice_import_logs_add_entry_55 >> finish >> log_to_sumo >> failure_reason >> invoice_import_logs_add_entry_57

    return dag


rail.for_each_instance(create_dag)
