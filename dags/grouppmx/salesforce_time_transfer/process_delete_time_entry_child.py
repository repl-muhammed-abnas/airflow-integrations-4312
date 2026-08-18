import rail

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.delete_time_entry_dag_id,
        description=f'Grouppmx Time Transfer To Salesforce Delete Time Entry Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = 'view_dagrun_conf')

        get_list_of_time_entries_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='get_list_of_time_entries_in_salesforce',
            salesforce_conn_id= config.salesforce_conn_id,
            query='''SELECT FIELDS(ALL) FROM Time_Entry__c WHERE Timesheet__c = '{{ dag_run.conf.timesheet_id }}' LIMIT 150 '''
        )

        has_any_time_entries = rail.IfOperator(
            task_id = 'has_any_time_entries',
            test= '{{ result("get_list_of_time_entries_in_salesforce").records | is_truthy }}',
            yes_task= 'for_each_time_entry',
            no_task= 'get_list_of_resources_in_salesforce'
        )

        for_each_time_entry = rail.ForEachOperator(
            task_id = 'for_each_time_entry',
            items= '{{ result("get_list_of_time_entries_in_salesforce").records | to_json }}',
            start_task= 'delete_time_entry',
            end_task= 'for_each_time_entry_end'
        )

        delete_time_entry = rail.SalesforceUpdateObjectOperator2(
            task_id = 'delete_time_entry',
            salesforce_conn_id= config.salesforce_conn_id,
            operation= 'delete',
            object_name= 'Time_Entry__c',
            payload= [{
                'Id': '{{ result("for_each_time_entry").Id }}'
            }]
        )

        for_each_time_entry_end = rail.EmptyOperator(
            task_id = 'for_each_time_entry_end'
        )

        get_list_of_resources_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='get_list_of_resources_in_salesforce',
            salesforce_conn_id= config.salesforce_conn_id,
            query=''' SELECT FIELDS(ALL) FROM Resource_Timesheet_Association__c WHERE Timesheet__c = '{{ dag_run.conf.timesheet_id }}' LIMIT 150 ''',
        )

        has_any_resource_entries = rail.IfOperator(
            task_id = 'has_any_resource_entries',
            test= '{{ result("get_list_of_resources_in_salesforce").records | is_truthy }}',
            yes_task= 'for_each_resource_entry',
            no_task= 'finish'
        )

        for_each_resource_entry = rail.ForEachOperator(
            task_id = 'for_each_resource_entry',
            items= '{{ result("get_list_of_resources_in_salesforce").records | to_json }}',
            start_task= 'delete_resource_entry',
            end_task= 'for_each_resource_end'
        )

        delete_resource_entry = rail.SalesforceUpdateObjectOperator2(
            task_id = 'delete_resource_entry',
            salesforce_conn_id= config.salesforce_conn_id,
            operation= 'delete',
            object_name= 'Resource_Timesheet_Association__c',
            payload= [{
                'Id': '{{ result("for_each_resource_entry").Id }}'
            }]
        )

        for_each_resource_end = rail.EmptyOperator(
            task_id = 'for_each_resource_end'
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
                "contact": "",
                "project": "",
                "account": "",
                "entrydate": "",
                "hoursworked": "",
                "timeoffhours": "",
                "status": "Success",
                "details": '{{ dag_run_ecid() }} - {{ get_error_message() }}',
            }
        )

        get_list_of_time_entries_in_salesforce >> has_any_time_entries

        has_any_time_entries >> rail.Label(
            "Yes") >> for_each_time_entry >> delete_time_entry >> for_each_time_entry_end

        for_each_time_entry >> for_each_time_entry_end >> get_list_of_resources_in_salesforce

        has_any_time_entries >> rail.Label(
            "No") >> get_list_of_resources_in_salesforce >> has_any_resource_entries

        has_any_resource_entries >> rail.Label(
            "Yes") >> for_each_resource_entry >> delete_resource_entry >> for_each_resource_end

        for_each_resource_entry >> for_each_resource_end >> finish

        has_any_resource_entries >> rail.Label(
            "No") >> finish >> catch_and_log_errors

    return dag

rail.for_each_instance(create_dag)
