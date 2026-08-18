import rail
from grouppmx.salesforce_time_transfer.utils import request_payload
from grouppmx.salesforce_time_transfer.utils.request_payload import get_required_resource_details,get_required_pto_resource_details
from grouppmx.salesforce_time_transfer.tasks.create_resource import process_project_resource_task_group

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.timeentry_dag_id,
        description=f'Grouppmx Time Transfer To Salesforce Delete Time Entry Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = 'view_dagrun_conf')

        get_timesheet_details_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='get_timesheet_details_in_salesforce',
            salesforce_conn_id= config.salesforce_conn_id,
            query="SELECT FIELDS(ALL) FROM Timesheet__c WHERE Id = '{{ dag_run.conf.timesheetid }}' LIMIT 150"
        )

        if_approval_status_is_approved = rail.IfOperator(
            task_id = 'if_approval_status_is_approved',
            test= '{{ dag_run.conf.approval_status == "Approved" }}',
            yes_task= 'is_project_uri_present',
            no_task= 'log_success'
        )

        is_project_uri_present = rail.IfOperator(
            task_id = 'is_project_uri_present',
            test= '{{ dag_run.conf.project_uri | is_truthy }}',
            yes_task= 'if_hours_worked_greater_than_zero',
            no_task= 'if_timeoff_type_is_not_present'
        )

        if_hours_worked_greater_than_zero = rail.IfOperator(
            task_id = 'if_hours_worked_greater_than_zero',
            test=lambda dag_run: float(dag_run.conf['hours_worked']) > 0,
            yes_task= 'is_project_id_present',
            no_task= 'log_success'
        )

        is_project_id_present = rail.IfOperator(
            task_id = 'is_project_id_present',
            test= '{{ dag_run.conf.projectid | is_truthy }}',
            yes_task= 'get_project_resources_in_salesforce',
            no_task= 'log_project_id_exception'
        )

        get_project_resources_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='get_project_resources_in_salesforce',
            salesforce_conn_id= config.salesforce_conn_id,
            query="SELECT FIELDS(ALL) FROM Project_Resource__c WHERE Project__c = '{{ dag_run.conf.projectid }}' LIMIT 150"
        )

        has_any_project_resources = rail.IfOperator(
            task_id = 'has_any_project_resources',
            test= '{{ result("get_project_resources_in_salesforce").records | is_truthy }}',
            yes_task= 'get_required_resource',
            no_task= 'start_create_resource_task'
        )

        get_required_resource = rail.IfOperator(
            task_id = 'get_required_resource',
            test=lambda dag_run: bool(get_required_resource_details(dag_run.conf['contactid'])['project_resource']),
            yes_task= 'get_resource_timesheet_association',
            no_task= 'start_create_resource_task'
        )

        get_resource_timesheet_association = rail.SalesforceQueryOperator2(
            task_id='get_resource_timesheet_association',
            salesforce_conn_id= config.salesforce_conn_id,
            query="SELECT FIELDS(ALL) FROM Resource_Timesheet_Association__c WHERE Timesheet__c = '{{ dag_run.conf.timesheetid }}' LIMIT 150"
        )

        is_resource_available = rail.IfOperator(
            task_id = 'is_resource_available',
            test=lambda dag_run:bool(get_required_resource_details(dag_run.conf['contactid'], get_required_resource_details(dag_run.conf['contactid'])['project_resource'])['resource_ts_association']),
            yes_task= 'create_project_time_entry',
            no_task= 'create_timesheet_resource_association'
        )

        create_timesheet_resource_association = rail.SalesforceUpdateObjectOperator2(
            task_id = 'create_timesheet_resource_association',
            operation= 'insert',
            object_name= 'Resource_Timesheet_Association__c',
            salesforce_conn_id = config.salesforce_conn_id,
            payload =lambda dag_run: [{
                'Timesheet__c': dag_run.conf['timesheetid'],
                'Project_Resource__c': get_required_resource_details(dag_run.conf['contactid'])['project_resource']
            }]
        )

        create_project_time_entry = rail.SalesforceUpdateObjectOperator2(
            task_id = 'create_project_time_entry',
            operation= 'insert',
            object_name= 'Time_Entry__c',
            salesforce_conn_id = config.salesforce_conn_id,
            payload=lambda dag_run: request_payload.get_create_time_entry_payload(dag_run)
        )

        start_create_resource_task = rail.EmptyOperator(task_id = 'start_create_resource_task')

        process_project_resource = process_project_resource_task_group(config.salesforce_conn_id,type = 'regular')

        end_create_resource_task = rail.EmptyOperator(task_id = 'end_create_resource_task')

        if_timeoff_type_is_not_present = rail.IfOperator(
            task_id = 'if_timeoff_type_is_not_present',
            test= '{{ not dag_run.conf.timeoff_type | is_truthy }}',
            yes_task= 'log_timeoff_exception',
            no_task= 'get_pto_project_resources'
        )

        log_timeoff_exception = rail.WriteLogOperator(
            task_id = 'log_timeoff_exception',
            log= '{{ dag_run.conf.log }}',
            message="na",
            severity="Exception",
            properties= {
                "contact": "{{ dag_run.conf.contact }}",
                "project": "{{ dag_run.conf.project }}",
                "account": "{{ dag_run.conf.account }}",
                "entrydate": "{{ dag_run.conf.entry_date }}",
                "hoursworked": "{{ dag_run.conf.hours_worked }}",
                "timeoffhours": "",
                "status": "Exception",
                "details": '{{ dag_run_ecid() }} No Project selected',
            }
        )

        get_pto_project_resources = rail.SalesforceQueryOperator2(
            task_id='get_pto_project_resources',
            salesforce_conn_id= config.salesforce_conn_id,
            query="""SELECT FIELDS(ALL) FROM Project_Resource__c WHERE Project__c = 'a021C00000Tgx8KQAR' LIMIT 150"""
        )

        has_any_pto_project_resources = rail.IfOperator(
            task_id = 'has_any_pto_project_resources',
            test= '{{ result("get_pto_project_resources").records | is_truthy }}',
            yes_task= 'check_required_pto_resource_available',
            no_task= 'start_create_pto_resource_task'
        )

        check_required_pto_resource_available = rail.IfOperator(
            task_id = 'check_required_pto_resource_available',
            test= lambda dag_run: bool(get_required_pto_resource_details(dag_run.conf['contactid'])['project_resource']),
            yes_task= 'get_pto_resource_timesheet_association',
            no_task= 'start_create_pto_resource_task'
        )

        get_pto_resource_timesheet_association = rail.SalesforceQueryOperator2(
            task_id='get_pto_resource_timesheet_association',
            salesforce_conn_id= config.salesforce_conn_id,
            query="SELECT FIELDS(ALL) FROM Resource_Timesheet_Association__c WHERE Timesheet__c = '{{ dag_run.conf.timesheetid }}' LIMIT 150"
        )

        is_pto_resource_available = rail.IfOperator(
            task_id = 'is_pto_resource_available',
            test= lambda dag_run:bool(get_required_pto_resource_details(dag_run.conf['contactid'], get_required_pto_resource_details(dag_run.conf['contactid'])['project_resource'])['resource_ts_association']),
            yes_task= 'create_pto_time_entry',
            no_task= 'create_timesheet_pto_resource_association'
        )

        create_timesheet_pto_resource_association = rail.SalesforceUpdateObjectOperator2(
            task_id = 'create_timesheet_pto_resource_association',
            operation= 'insert',
            object_name= 'Resource_Timesheet_Association__c',
            salesforce_conn_id = config.salesforce_conn_id,
            payload = lambda dag_run: [{
                'Timesheet__c': dag_run.conf['timesheetid'],
                'Project_Resource__c': get_required_pto_resource_details(dag_run.conf['contactid'])['project_resource']
            }]
        )

        create_pto_time_entry = rail.SalesforceUpdateObjectOperator2(
            task_id = 'create_pto_time_entry',
            operation= 'insert',
            object_name= 'Time_Entry__c',
            salesforce_conn_id = config.salesforce_conn_id,
            payload= lambda dag_run: request_payload.get_create_time_off_entry_payload(dag_run)
        )

        start_create_pto_resource_task = rail.EmptyOperator(task_id = 'start_create_pto_resource_task')

        process_pto_project_resource = process_project_resource_task_group(config.salesforce_conn_id, type = 'timeoff')

        end_pto_create_resource_task = rail.EmptyOperator(task_id = 'end_pto_create_resource_task')

        log_project_id_exception = rail.WriteLogOperator(
            task_id = 'log_project_id_exception',
            log= '{{ dag_run.conf.log }}',
            message="Project not found in Salesforce",
            severity="Exception",
            properties=lambda: {
                "contact": "{{ dag_run.conf.contact }}",
                "project": "{{ dag_run.conf.project }}",
                "account": "{{ dag_run.conf.account }}",
                "entrydate": "{{ dag_run.conf.entry_date }}",
                "hoursworked": "{{ dag_run.conf.hours_worked }}",
                "timeoffhours": "{{dag_run.conf.timeoff_hours }}",
                "status": "Exception",
                "details": '{{ dag_run_ecid() }} - Project not found in Salesforce',
            }
        )

        log_success = rail.WriteLogOperator(
            task_id = 'log_success',
            log= '{{ dag_run.conf.log }}',
            message="Timesheet entry successful",
            severity="Success",
            properties={
                "contact": "{{ dag_run.conf.contact }}",
                "project": "{{ dag_run.conf.project if dag_run.conf.timeoff_type  == '' else 'PMX PTO' }}",
                "account": "{{ dag_run.conf.account if dag_run.conf.timeoff_type  == '' else '' }}",
                "entrydate": "{{ dag_run.conf.entry_date }}",
                "hoursworked": "{{dag_run.conf.hours_worked if dag_run.conf.timeoff_type  == '' else '' }}",
                "timeoffhours": "{{ dag_run.conf.timeoff_hours if not dag_run.conf.timeoff_type  == '' else '' }}",
                "status": "Success",
                "details": '''{{ dag_run_ecid() }} - {{ "Time Off entry successful" if not  dag_run.conf.timeoff_type == ''  else  "Timesheet entry successful" }}''',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log = "{{ dag_run.conf.log}}",
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "contact": "{{ dag_run.conf.contact }}",
                "project": "{{ dag_run.conf.project }}",
                "account": "{{ dag_run.conf.account }}",
                "entrydate": "{{ dag_run.conf.entry_date }}",
                "hoursworked": "{{dag_run.conf.hours_worked }}",
                "timeoffhours": "{{dag_run.conf.timeoff_hours }}",
                "status": "Success",
                "details": '{{ dag_run_ecid() }} - {{ get_error_message() }}',
            }
        )

        get_timesheet_details_in_salesforce >> if_approval_status_is_approved

        if_approval_status_is_approved >> rail.Label(
            "Yes") >> is_project_uri_present

        if_approval_status_is_approved >> rail.Label(
            "No") >> log_success

        is_project_uri_present >> rail.Label(
            "Yes") >> if_hours_worked_greater_than_zero

        is_project_uri_present >> rail.Label(
            "No") >> if_timeoff_type_is_not_present

        if_hours_worked_greater_than_zero >> rail.Label(
            "No") >> log_success

        if_hours_worked_greater_than_zero >> rail.Label(
            "Yes") >> is_project_id_present

        is_project_id_present >> rail.Label(
            "No") >> log_project_id_exception >> catch_and_log_errors

        is_project_id_present >> rail.Label(
            "Yes") >> get_project_resources_in_salesforce >> has_any_project_resources

        has_any_project_resources >> rail.Label(
            "Yes") >> get_required_resource

        get_required_resource >> rail.Label(
            "Yes") >> get_resource_timesheet_association >> is_resource_available

        get_required_resource >> rail.Label(
            "No") >> start_create_resource_task

        start_create_resource_task >> process_project_resource >> end_create_resource_task >> log_success

        has_any_project_resources >> rail.Label(
            "No") >> start_create_resource_task

        is_resource_available >> rail.Label(
            "Yes") >> create_project_time_entry >> log_success

        is_resource_available >> rail.Label(
            "No") >> create_timesheet_resource_association >> create_project_time_entry

        if_timeoff_type_is_not_present >> rail.Label(
            "Yes") >> log_timeoff_exception >> catch_and_log_errors

        if_timeoff_type_is_not_present >> rail.Label(
            "No") >> get_pto_project_resources

        get_pto_project_resources >> has_any_pto_project_resources >> rail.Label(
            "Yes") >> check_required_pto_resource_available

        has_any_pto_project_resources >> rail.Label(
            "No") >> start_create_pto_resource_task

        check_required_pto_resource_available >> rail.Label(
            "Yes") >> get_pto_resource_timesheet_association >> is_pto_resource_available

        check_required_pto_resource_available >> rail.Label(
            "No") >> start_create_pto_resource_task

        is_pto_resource_available >> rail.Label(
            "Yes") >> create_pto_time_entry >> log_success

        is_pto_resource_available >> rail.Label(
            "No") >> create_timesheet_pto_resource_association >> create_pto_time_entry

        start_create_pto_resource_task >> process_pto_project_resource >> end_pto_create_resource_task >> log_success >> catch_and_log_errors


    return dag

rail.for_each_instance(create_dag)
