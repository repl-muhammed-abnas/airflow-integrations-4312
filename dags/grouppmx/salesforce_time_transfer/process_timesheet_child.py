from datetime import timedelta
import rail
from grouppmx.salesforce_time_transfer.utils import custom_methods,request_payload

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.timesheet_dag_id,
        description=f'Grouppmx Time Transfer To Salesforce Timesheet Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = 'view_dagrun_conf')

        get_required_details = rail.PythonOperator(
            task_id = 'get_required_details',
            python_callable= custom_methods.get_required_timesheet_details
        )

        search_contacts_in_salesforce = rail.SalesforceQueryOperator2(
            task_id = 'search_contacts_in_salesforce',
            query= '''SELECT FIELDS(ALL) FROM Contact WHERE Replicon_ID__c LIKE '{{ result("get_required_details").user_uri }}' LIMIT 150''',
            salesforce_conn_id= config.salesforce_conn_id
        )

        if_contact_is_not_present = rail.IfOperator(
            task_id = 'if_contact_is_not_present',
            test= '{{ result("search_contacts_in_salesforce").records | is_falsy }}',
            yes_task= 'write_exception_log',
            no_task= 'search_timesheets_in_salesforce'
        )

        write_exception_log = rail.WriteLogOperator(
            task_id = 'write_exception_log',
            log= '{{ dag_run.conf.log }}',
            message="na",
            severity="Exception",
            properties=lambda: {
                "contact": rail.result("get_required_details")['user_name'],
                "project": "",
                "account": "",
                "entrydate": "",
                "hoursworked": "",
                "timeoffhours": "",
                "status": "Exception",
                "details": f'Contact with Replicon ID - {rail.result("get_required_details")["user_uri"]} not found in Salesforce',
            }
        )

        search_timesheets_in_salesforce = rail.SalesforceQueryOperator2(
            task_id = 'search_timesheets_in_salesforce',
            query= """SELECT FIELDS(ALL) FROM Timesheet__c WHERE Replicon_ID__c LIKE '{{ dag_run.conf.timesheet_uri }}' LIMIT 150""",
            salesforce_conn_id= config.salesforce_conn_id
        )

        if_timesheet_available = rail.IfOperator(
            task_id = 'if_timesheet_available',
            test= '{{ result("search_timesheets_in_salesforce").records | is_truthy }}',
            yes_task= 'process_delete_all_timesheet_entries',
            no_task= 'create_timesheet_in_salesforce'
        )

        process_delete_all_timesheet_entries = rail.TriggerDagRunOperator(
            task_id='process_delete_all_timesheet_entries',
            trigger_dag_id= config.delete_time_entry_dag_id,
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                'timesheet_id': rail.result("search_timesheets_in_salesforce")['records'][0]['Id'],
                'user_name': rail.result("get_required_details")['user_name'],
                'start_date': rail.result("get_required_details")['start_date'],
                'end_date': rail.result("get_required_details")['end_date'],
                'user_uri': rail.result("get_required_details")['user_uri'],
                'timesheet_uri': dag_run.conf['timesheet_uri'],
                'log': dag_run.conf['log']
            }
        )

        wait_for_process_delete_all_timesheet_entries = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_delete_all_timesheet_entries',
            dag_runs='{{ result("process_delete_all_timesheet_entries") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        update_timesheet_in_salesforce = rail.SalesforceUpdateObjectOperator2(
            task_id = 'update_timesheet_in_salesforce',
            operation= 'update',
            object_name= 'Timesheet__c',
            salesforce_conn_id = config.salesforce_conn_id,
            payload = request_payload.update_timesheet_payload
        )

        create_timesheet_in_salesforce = rail.SalesforceUpdateObjectOperator2(
            task_id = 'create_timesheet_in_salesforce',
            operation= 'insert',
            object_name= 'Timesheet__c',
            salesforce_conn_id = config.salesforce_conn_id,
            payload = request_payload.create_timesheet_payload
        )

        get_timesheet_details = rail.SalesforceQueryOperator2(
            task_id = 'get_timesheet_details',
            query= """SELECT FIELDS(ALL) FROM Timesheet__c WHERE Replicon_ID__c LIKE '{{ dag_run.conf.timesheet_uri }}' LIMIT 150""",
            salesforce_conn_id= config.salesforce_conn_id
        )

        get_timesheet_resource_association = rail.SalesforceQueryOperator2(
            task_id = 'get_timesheet_resource_association',
            query="SELECT FIELDS(ALL) FROM Resource_Timesheet_Association__c WHERE Timesheet__c = '{{ result('create_timesheet_in_salesforce')[0].id }}' LIMIT 150",
            salesforce_conn_id= config.salesforce_conn_id
        )

        for_each_timesheet_resorce = rail.ForEachOperator(
            task_id = 'for_each_timesheet_resorce',
            items=lambda: rail.result("get_timesheet_resource_association")['records'],
            start_task= 'delete_resource_timesheet_association',
            end_task= 'for_each_end'
        )

        delete_resource_timesheet_association = rail.SalesforceUpdateObjectOperator2(
            task_id = 'delete_resource_timesheet_association',
            operation= 'delete',
            object_name= 'Resource_Timesheet_Association__c',
            salesforce_conn_id = config.salesforce_conn_id,
            payload = [
                {
                    "Id": '{{ result("for_each_timesheet_resorce").Id }}'
                }
            ]
        )

        for_each_end = rail.EmptyOperator(
            task_id = 'for_each_end'
        )

        query_users_per_timesheet = rail.QueryCollectionOperator(
            task_id = 'query_users_per_timesheet',
            query= '''SELECT * FROM timeentryreportcollection WHERE TimesheetPeriodUri = '{{ dag_run.conf.timesheet_uri }}' and Approver_Name != "< System >" '''
        )

        write_users_timesheet_csv = rail.WriteCSVFileOperator(
            task_id = 'write_users_timesheet_csv',
            source= '{{ result("query_users_per_timesheet") }}',
            header= ['timesheet_uri', 'user_uri', 'project_uri', 'entry_date', 'supervisor_name', 'activity_name', 'billingrate_name',
                     'hours_worked', 'timeoff_hours', 'contact', 'project', 'account', 'billingrate_amount', 'timesheetid', 'accountid',
                     'projectid', 'contactid', 'billing_hours', 'nonbillable_hours', 'timeoff_type', 'approval_status', 'submitted_on',
                     'approver_name', 'approval_date'],
            row=custom_methods.get_csv_lines
        )

        process_each_timeentry = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_timeentry',
            trigger_dag_id= config.timeentry_dag_id,
            retries=0,
            items= "{{ result('write_users_timesheet_csv') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item,dag_run: {
                **item,
                'log': dag_run.conf['log']
            }
        )

        wait_for_process_each_timeentry = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_timeentry',
            dag_runs='{{ result("process_each_timeentry") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        log_success = rail.WriteLogOperator(
            task_id = 'log_success',
            log= '{{ dag_run.conf.log }}',
            message="Timesheet entry successful",
            severity="Success",
            properties={
                "contact": "{{ result('get_required_details').user_name }}",
                "project": "",
                "account": "",
                "entrydate": "",
                "hoursworked": "",
                "timeoffhours": "",
                "status": "Success",
                "details": '''{{ dag_run_ecid() }} - Timesheet completely synced - {{ result("get_required_details").start_date }} - {{ result("get_required_details").end_date }} - {{ result("search_timesheets_in_salesforce").records[0].Name if result("search_timesheets_in_salesforce").records | is_truthy else result("get_timesheet_details").records[0].Name }} - {{ dag_run.conf.timesheet_uri }}''',
            }
        )

        finish = rail.EmptyOperator(
            task_id = 'finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log = "{{ dag_run.conf.log}}",
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "contact": "{{ result('get_required_details').user_name }}",
                "project": "",
                "account": "",
                "entrydate": "",
                "hoursworked": "",
                "timeoffhours": "",
                "status": "Error",
                "details": '{{ dag_run_ecid() }} - {{ get_error_message() }}',
            }
        )

        get_required_details >> search_contacts_in_salesforce >> if_contact_is_not_present

        if_contact_is_not_present >> rail.Label(
            "Yes") >> write_exception_log >> finish

        if_contact_is_not_present >> rail.Label(
            "No") >> search_timesheets_in_salesforce >> if_timesheet_available

        if_timesheet_available >> rail.Label(
            "Yes") >> process_delete_all_timesheet_entries >> wait_for_process_delete_all_timesheet_entries >> update_timesheet_in_salesforce >>\
                query_users_per_timesheet

        if_timesheet_available >> rail.Label(
            "No") >> create_timesheet_in_salesforce >> get_timesheet_details >> get_timesheet_resource_association >> for_each_timesheet_resorce

        for_each_timesheet_resorce >> delete_resource_timesheet_association >> for_each_end

        for_each_timesheet_resorce >> for_each_end >> query_users_per_timesheet >> write_users_timesheet_csv >> \
            process_each_timeentry >> wait_for_process_each_timeentry >> log_success >> finish >> catch_and_log_errors

    return dag

rail.for_each_instance(create_dag)
