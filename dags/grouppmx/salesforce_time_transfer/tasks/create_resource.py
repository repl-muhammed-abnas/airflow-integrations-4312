from datetime import timedelta, datetime
from dateutil.parser import parse as date_parser
import rail
from grouppmx.salesforce_time_transfer.utils import request_payload

null = None
DATE_FORMAT = "%m/%d/%Y"

def process_project_resource_task_group(salesforce_conn_id, type):
    with rail.TaskGroup(group_id=f'process_resources_task_{type}', prefix_group_id=False) as process_task:

        create_project_resource = rail.SalesforceUpdateObjectOperator2(
            task_id = f'create_project_resource_{type}',
            operation= 'insert',
            object_name= 'Project_Resource__c',
            salesforce_conn_id = salesforce_conn_id,
            payload =lambda dag_run: [{
                'Resource_Name__c': dag_run.conf['contactid'],
                'Project__c': dag_run.conf['projectid'] if type != 'timeoff' else 'a021C00000Tgx8KQAR'
            }]
        )

        create_timesheet_association = rail.SalesforceUpdateObjectOperator2(
            task_id = f'create_timesheet_association_{type}',
            operation= 'insert',
            object_name= 'Resource_Timesheet_Association__c',
            salesforce_conn_id = salesforce_conn_id,
            payload =lambda dag_run: [{
                'Project_Resource__c': rail.result(create_project_resource.task_id)[0]['id'],
                'Timesheet__c': dag_run.conf['timesheetid']
            }]
        )

        create_time_entry = rail.SalesforceUpdateObjectOperator2(
            task_id = f'create_time_entry_{type}',
            operation= 'insert',
            object_name= 'Time_Entry__c',
            salesforce_conn_id = salesforce_conn_id,
            payload = lambda dag_run: request_payload.get_create_time_entry_payload(
                dag_run, f'create_timesheet_association_{type}',type) if type == 'regular' else request_payload.get_create_time_off_entry_payload(
                    dag_run,f'create_timesheet_association_{type}', type)
        )

        create_project_resource >> create_timesheet_association >> create_time_entry

    return process_task
