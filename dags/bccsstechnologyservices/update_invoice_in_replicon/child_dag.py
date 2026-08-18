from datetime import datetime, timedelta
import uuid
import rail
from airflow.models import Variable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'bccsstechnologyservices_update_invoice_in_replicon_child_{config.instance}',
        description=f'Bccsstechnologyservices_update_invoice_in_replicon_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_oldinvoicenumber_is_blank'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_oldinvoicenumber_is_blank',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_oldinvoicenumber_is_blank = rail.IfOperator(
            task_id='if_oldinvoicenumber_is_blank',
            test="{{ dag_run.conf.oldinvoicenumber | is_falsy }}",
            yes_task="log_error_entries",
            no_task="log_ponumber",
        )

        log_error_entries = rail.WriteLogOperator(
            task_id='log_error_entries',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                "old_invoice_number": dag_run.conf['newinvoicenumber'],
                "new_invoice_number": dag_run.conf['newinvoicenumber'],
                "status": "Error",
                "reason": "Old invoice number not provided in the feed file" + " | " + dag_run.conf['job_id'],
                "jobid": dag_run.conf['job_id'],
            }
        )

        stop_job = rail.EmptyOperator(
            task_id='stop_job',

        )

        log_ponumber = rail.PythonOperator(
            task_id='log_ponumber',
            python_callable=lambda dag_run: dag_run.conf['ponumber'] if dag_run.conf['ponumber'] else null
        )

        get_invoice_uri = rail.RepliconServiceOperator(
            task_id='get_invoice_uri',
            endpoint="/services/InvoiceService2.svc/GetUriFromInvoiceNumber1",
            retries=0,
            data=lambda dag_run: {
                "invoiceNumberText": dag_run.conf['oldinvoicenumber']
            }
        )

        get_invoice_details = rail.RepliconServiceOperator(
            task_id='get_invoice_details',
            endpoint="/services/InvoiceService2.svc/GetInvoiceDetails",
            retries=0,
            data={
                "invoiceUri": "{{ result('get_invoice_uri') }}"
            }
        )

        log_invoice_status = rail.PythonOperator(
            task_id='log_invoice_status',
            python_callable=lambda:  rail.result('get_invoice_details')[
                'invoiceStatus'].split(":")[-1]
        )

        if_invoice_status_equals_to_open = rail.IfOperator(
            task_id='if_invoice_status_equals_to_open',
            test="{{ result('log_invoice_status') == 'open'  }}",
            yes_task="log_date_to_be_passed",
            no_task="add_error_entries",
        )

        log_date_to_be_passed = rail.PythonOperator(
            task_id='log_date_to_be_passed',
            python_callable=lambda dag_run: {
                "keyUri": "urn:replicon:invoice-metadata-key:invoice-date",
                "value": {
                    "uri": null,
                    "slug": null,
                    "bool": null,
                    "date": {
                        "year": int(dag_run.conf['dateofissue'].replace('-', '"/"').split("/")[2]),
                        "month": int(dag_run.conf['dateofissue'].replace('-', '"/"').split("/")[0]),
                        "day": int(dag_run.conf['dateofissue'].replace('-', '"/"').split("/")[1])
                    }
                }
            }
        )

        log_payment_terms = rail.PythonOperator(
            task_id='log_payment_terms',
            python_callable=lambda: int(rail.find_first_by_attr_and_get_attr(rail.result('get_invoice_details')[
                'customMetadata'], 'keyUri', 'urn:replicon:invoice-metadata-key:payment-terms', 'value.number', null))

        )

        log_payment_duedate = rail.PythonOperator(
            task_id='log_payment_duedate',
            python_callable=lambda dag_run: (datetime.strptime(dag_run.conf['dateofissue'].replace(
                '-', '"/"'), '%m/%d/%Y') + timedelta(days=rail.result('log_payment_terms'))).strftime('%m/%d/%Y')
        )

        def get_billing_address():
            data = rail.find_first_by_attr_and_get_attr(rail.result('get_invoice_details')[
                                                        'customMetadata'], 'keyUri', 'urn:replicon:invoice-metadata-key:billing-address', 'value.text', null)
            address = rail.smartjoin_by_delim(data.split(
                " "), " ").replace('\\', '\\\\') if data else None
            return address

        log_billingaddress = rail.PythonOperator(
            task_id='log_billingaddress',
            python_callable=get_billing_address
        )

        log_billingaddress_date = rail.PythonOperator(
            task_id='log_billingaddress_date',
            python_callable=lambda: rail.smartjoin_by_delim(rail.result(
                'log_billingaddress').split(" "), "").replace('"', '\"') if rail.result('log_billingaddress') else None
        )

        log_invoice_template_uri = rail.PythonOperator(
            task_id='log_invoice_template_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_invoice_details')['customMetadata'], 'keyUri', 'urn:replicon:invoice-metadata-key:invoice-template', 'value.uri', null)
        )

        log_collection = rail.PythonOperator(
            task_id='log_collection',
            python_callable=lambda: [(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_invoice_details')['customMetadata'], 'keyUri', 'urn:replicon:invoice-metadata-key:summarize-columns', 'value.collection', []))]
        )

        def get_formatted_log_list():
            derived_collections = []
            invoice_collection = rail.result('log_collection')
            for invoices in invoice_collection:
                if invoices:
                    for invoice in invoices:
                        derived_collections.append({
                            "uri": None,
                            "slug": None,
                            "bool": None,
                            "date": None,
                            "number": None,
                            "text": invoice['text'],
                            "time": None,
                            "calendarDayDurationValue": None,
                            "workdayDurationValue": None,
                            "dateRange": None
                        })
            return derived_collections

        put_updated_invoice = rail.RepliconServiceOperator(
            task_id='put_updated_invoice',
            endpoint="/services/InvoiceService2.svc/PutInvoice3",
            data=lambda dag_run: {
                "invoice": {
                    "target": {
                        "uri": rail.result('get_invoice_uri'),
                        "invoiceNumberText": null,
                        "parameterCorrelationId": null
                    },
                    "client": {
                        "uri": rail.result('get_invoice_details')['client']['uri'],
                        "name": null
                    },
                    "invoiceNumberText": dag_run.conf['newinvoicenumber'],
                    "invoiceCurrency": {
                        "uri": rail.result('get_invoice_details')['invoiceCurrency']['uri'],
                        "name": null,
                        "symbol": null
                    },
                    "customMetadata": [
                        {
                            "keyUri": "urn:replicon:invoice-metadata-key:internal-notes",
                            "value": {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": null,
                                "text": dag_run.conf['notesforcustomer'],
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            }
                        },
                        {
                            "keyUri": "urn:replicon:invoice-metadata-key:po-number",
                            "value": {
                                "text": rail.result('log_ponumber')
                            }
                        },
                        {
                            "keyUri": "urn:replicon:invoice-metadata-key:invoice-date",
                            "value": {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": {
                                    "year": int(dag_run.conf['dateofissue'].replace('-', '"/"').split("/")[-1]),
                                    "month": int(dag_run.conf['dateofissue'].replace('-', '"/"').split("/")[0]),
                                    "day": int(dag_run.conf['dateofissue'].replace('-', '"/"').split("/")[1])
                                },
                                "number": null,
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            }
                        },
                        {
                            "keyUri": "urn:replicon:invoice-metadata-key:payment-terms",
                            "value": {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": rail.result('log_payment_terms'),
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            }
                        },
                        {
                            "keyUri": "urn:replicon:invoice-metadata-key:payment-due-date",
                            "value": {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": {
                                    "year": int(rail.result('log_payment_duedate').split("/")[-1]),
                                    "month":int(rail.result('log_payment_duedate').split("/")[0]),
                                    "day": int(rail.result('log_payment_duedate').split("/")[1]),
                                },
                                "number": null,
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            }
                        },
                        {
                            "keyUri": "urn:replicon:invoice-metadata-key:billing-address",
                            "value": {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": null,
                                "text": rail.result('log_billingaddress_date'),
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            }
                        },
                        {
                            "keyUri": "urn:replicon:invoice-metadata-key:description",
                            "value": {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": null,
                                "text": dag_run.conf['description'],
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            }
                        },
                        {
                            "keyUri": "urn:replicon:invoice-metadata-key:summarize-columns",
                            "value": {
                                "uri": null,
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": null,
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": get_formatted_log_list()
                            }
                        },
                        {
                            "keyUri": "urn:replicon:invoice-metadata-key:invoice-template",
                            "value": {
                                "uri": rail.result('log_invoice_template_uri'),
                                "slug": null,
                                "bool": null,
                                "date": null,
                                "number": null,
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "collection": []
                            }
                        }
                    ],
                    "extensionFieldValues": []
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        mark_as_issued = rail.RepliconServiceOperator(
            task_id='mark_as_issued',
            endpoint="/services/InvoiceService2.svc/MarkAsBilled",
            data={
                "invoiceUri": "{{ result('get_invoice_uri') }}"
            }
        )

        log_success_entries = rail.WriteLogOperator(
            task_id='log_success_entries',
            log="{{dag_run.conf.lookuptable}}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "old_invoice_number": dag_run.conf['oldinvoicenumber'],
                "new_invoice_number": dag_run.conf['newinvoicenumber'],
                "status": "Success",
                "reason": "Invoice Updated " + "|" + dag_run.conf['job_id'],
                "jobid": dag_run.conf['job_id'],
            }
        )

        add_error_entries = rail.WriteLogOperator(
            task_id='add_error_entries',
            log="{{dag_run.conf.lookuptable}}",
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                "old_invoice_number": dag_run.conf['oldinvoicenumber'],
                "new_invoice_number": dag_run.conf['newinvoicenumber'],
                "status": "Error",
                "reason": "Invoice not updated as the Invoice is not in Draft Status" + "|" + dag_run.conf['job_id'],
                "jobid": dag_run.conf['job_id'],
            }
        )

        def get_error_message(dag_run):
            error_message = rail.render_template("{{get_error_message()}}")
            message = error_message + dag_run.conf['job_id']
            if rail.get_current_context()['dag_run'].get_task_instance('get_invoice_uri').current_state() == 'failed':
                if '400 Bad Request' in error_message or 'InvoiceNumberTextNotFoundError1' in error_message or 'Identifier not found' in error_message:
                    message = "Invoice not present in Replicon|" + \
                        dag_run.conf['job_id']
                elif '500 Internal Server' in error_message or 'InternalServiceFault' in error_message or 'internal error' in error_message:
                    message = 'Multiple invoices present in Replicon for the same invoice number|' + \
                        dag_run.conf['job_id']
            return message

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{dag_run.conf.lookuptable}}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "old_invoice_number": dag_run.conf['oldinvoicenumber'],
                "new_invoice_number": dag_run.conf['newinvoicenumber'],
                "status": "Error",
                "reason": get_error_message(dag_run),
                "jobid": dag_run.conf['job_id'],

            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> if_oldinvoicenumber_is_blank
        if_oldinvoicenumber_is_blank >> rail.Label(
            'Yes') >> log_error_entries >> stop_job >> log_ponumber
        if_oldinvoicenumber_is_blank >> rail.Label(
            'No') >> log_ponumber >> get_invoice_uri >> get_invoice_details >> log_invoice_status
        log_invoice_status >> if_invoice_status_equals_to_open >> rail.Label(
            'Yes') >> log_date_to_be_passed >> log_payment_terms >> log_payment_duedate >> log_billingaddress
        log_billingaddress >> log_billingaddress_date >> log_invoice_template_uri >> log_collection
        log_collection >> put_updated_invoice >> mark_as_issued >> log_success_entries >> catch_and_log_error
        log_invoice_status >> if_invoice_status_equals_to_open >> rail.Label(
            'No') >> add_error_entries >> catch_and_log_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
