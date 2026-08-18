import rail
import itertools
null = None
from larochellegroupeconseil.invoice_sync.utils import python_callable
from larochellegroupeconseil.invoice_sync.utils import request_payload
from datetime import datetime, timedelta, timezone


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.child_dag_id,
        description= 'LarochelleGroupeConseil_Invoicesync_child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:
        
        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')
        
        invoice_items = rail.RepliconServiceOperator(
            task_id= 'invoice_items',
            endpoint="/services/InvoiceService2.svc/GetPageOfInvoiceItemsForInvoice3",
            data= {
                "page": 1,
                "pageSize": 10000,
                "invoice": {
                    "uri": "{{ dag_run.conf.uri }}"    #invoice uri
                },
                "invoiceItemColumnOptions": [
                "urn:replicon:invoice-item-column-option:project",
                ]}
        )
        
        check_status_and_projecturi = rail.IfOperator(
            task_id = 'check_status_and_projecturi',
            test = "{{ ((result('invoice_items') | default([]) | first | default({})).project | default({})).uri | default('') | is_truthy }}",
            yes_task= 'bulk_get_project_details',
            no_task= 'empty_task'
        )

        bulk_get_project_details = rail.RepliconServiceOperator(
            task_id= 'bulk_get_project_details',
            endpoint= '/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=lambda: {
                "projects": [{
                    "uri": rail.result("invoice_items")[0]['project']['uri'],
                    "name": null,
                    "code": null,
                    "parameterCorrelationId": null
                    }]
                    }
        )

        custom_metadata_list = rail.PythonOperator(
            task_id= 'custom_metadata_list',
            python_callable= python_callable.custom_metadata_list,
        )

        update_invoice_details = rail.RepliconServiceOperator(
            task_id= 'update_invoice_details',
            endpoint= '/services/InvoiceService2.svc/PutInvoice3',
            data= request_payload.update_invoice_details
        )

        update_invoice_sync_status_success = rail.RepliconServiceOperator(
            task_id='update_invoice_sync_status_success',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data= request_payload.update_invoice_sync_status_success
        )

        empty_task = rail.EmptyOperator(
            task_id= 'empty_task'
        )

        one_failed = rail.EmptyOperator(
            task_id='one_failed',
            trigger_rule='one_failed'  
        )

        update_invoice_sync_status_fail = rail.RepliconServiceOperator(
            task_id='update_invoice_sync_status_fail',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_update_invoice_sync_status_fail
        )

        fail_dagrun = rail.FailOperator(
            task_id = 'fail_dagrun',
            message= "{{get_error_message()}}"
        )

        


        invoice_items >> check_status_and_projecturi >> rail.Label("yes") >> bulk_get_project_details >>\
        custom_metadata_list >> update_invoice_details >> update_invoice_sync_status_success >> one_failed >>\
        update_invoice_sync_status_fail >> fail_dagrun

        check_status_and_projecturi >> rail.Label("No") >> empty_task


    return dag

rail.for_each_instance(create_child_dag)