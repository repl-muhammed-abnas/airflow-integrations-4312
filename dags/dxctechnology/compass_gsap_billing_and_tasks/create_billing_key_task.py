from datetime import timedelta
import itertools
import rail


def get_create_billingkey_task():
    with rail.TaskGroup(group_id='create_billingkey_task', prefix_group_id=False):

        def get_put_task_data(item, dag_run):
            def convert_date(d):
                return {
                    "year": d[0:4], "month": d[4:6], "day": d[6:8]
                }
            return {
                "project": {"uri": dag_run.conf['ProjectUri']},
                "task": {
                    "target": {
                        "name": dag_run.conf['BillingKey']['Name'],
                        "parent": {"uri": item['TaskUri']} if item else None,
                    },
                    "name": dag_run.conf['BillingKey']['Name'],
                    "code": dag_run.conf['BillingKey']['Description'],
                    "timeEntryDateRange": {
                        "startDate": convert_date(dag_run.conf['BillingKey']['StartDate']),
                        "endDate": convert_date(dag_run.conf['BillingKey']['EndDate']),
                    },
                    "customFieldValues": [
                        {
                            "customField": {"uri": dag_run.conf['TaskTypeOptionUri']},
                            "dropDownOption": {"uri": dag_run.conf['BillingKeyOptionValueUri']},
                        }
                    ],
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
                    "isTimeEntryAllowed": True,
                }
            }
        put_billingkey_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id='put_billingkey_tasks',
            endpoint='/services/ProjectService1.svc/PutTask',
            items=lambda dag_run: [
                None] if not dag_run.conf['AttributesParentTasks'] else dag_run.conf['AttributesParentTasks'],
            data=get_put_task_data,
            execution_timeout=timedelta(days=14),
            data_handler=lambda data, item: {"task": data, "parent": item},
        )

        has_any_resources_for_billingkeytasks = rail.IfOperator(
            task_id="has_any_resources_for_billingkeytasks",
            test='{{ dag_run.conf.ResourceUris | length > 0 }}',
            yes_task='put_billingkeytask_resources',
            no_task='log_successful_billingkey_task_creation',
        )

        put_billingkeytask_resources = rail.RepliconServiceCallForEachItemOperator(
            task_id='put_billingkeytask_resources',
            endpoint='/services/TaskService1.svc/BulkUpdateResourceAssignments',
            items='{{ result("put_billingkey_tasks") | to_json }}',
            execution_timeout=timedelta(days=14),
            data=lambda item, dag_run: {
                'taskUri': item['task']['uri'],
                'resourceUris': dag_run.conf['ResourceUris'],
                'isAssigned': True
            }
        )

        log_successful_billingkey_task_creation = rail.WriteLogOperator(
            task_id="log_successful_billingkey_task_creation",
            log="{{ result('create_log') }}",
            items='{{ result("put_billingkey_tasks") | to_json }}',
            severity="Success",
            message="""
                {%- if item.parent -%}
                    {{ item.task.name }} is added as a subtask of {{ item.parent.TaskName }} successfully
                {%- else -%}
                    {{ item.task.name }} created under {{ dag_run.conf.BillingKey.WBS }} successfully
                {%- endif -%}""",
        )

        log_any_tasks_missing_dates = rail.WriteLogOperator(
            task_id="log_any_tasks_missing_dates",
            log="{{ result('create_log') }}",
            items=lambda dag_run: list(
                filter(
                    lambda task: not task.get(
                        'StartDate') or not task.get('EndDate'),
                    dag_run.conf['ImportTasks'])),
            severity="Exception",
            message="""
                {%- set msgs = [] -%}
                {%- do msgs.append(item.TaskName + ' has no start date') if not item.StartDate -%}
                {%- do msgs.append(item.TaskName + ' has no end date') if not item.EndDate -%}
                {{- msgs | join(', ') -}}
            """,
        )

        def get_importtask_billingkeytask_combinations(dag_run):
            billingkey_tasks = rail.result("put_billingkey_tasks")
            import_tasks = filter(
                lambda task: task.get('StartDate') and task.get('EndDate'),
                dag_run.conf['ImportTasks'])
            return [{"billingkey_task": bk_task['task'], "import_task": imp_task}
                    for bk_task, imp_task in itertools.product(billingkey_tasks, import_tasks)]

        def get_put_import_task_data(item, dag_run):
            def convert_date(d):
                return {
                    "year": d[0:4], "month": d[4:6], "day": d[6:8]
                }
            return {
                "project": {"uri": dag_run.conf['ProjectUri']},
                "task": {
                    "target": {
                        "name": item['import_task']['TaskName'],
                        "parent": {"uri": item['billingkey_task']['uri']},
                    },
                    "name": item['import_task']['TaskName'],
                    "code": item['import_task']['Description'],
                    "timeEntryDateRange": {
                        "startDate": convert_date(item['import_task']['StartDate']),
                        "endDate": convert_date(item['import_task']['EndDate']),
                    },
                    "customFieldValues": [
                        {
                            "customField": {"uri": dag_run.conf['TaskTypeOptionUri']},
                            "dropDownOption": {"uri": dag_run.conf['TaskTypeOptionValueUri']},
                        }
                    ],
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
                    "isTimeEntryAllowed": True,
                }
            }
        put_import_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id='put_import_tasks',
            endpoint='/services/ProjectService1.svc/PutTask',
            items=get_importtask_billingkeytask_combinations,
            data=get_put_import_task_data,
            execution_timeout=timedelta(days=14),
            data_handler=lambda data, item: {
                "task": data, "parent": item['billingkey_task']},
        )

        has_any_resources_for_importtasks = rail.IfOperator(
            task_id="has_any_resources_for_importtasks",
            test='{{ dag_run.conf.ResourceUris | length > 0 }}',
            yes_task='put_importtask_resources',
            no_task='log_successful_import_task_creation',
        )

        put_importtask_resources = rail.RepliconServiceCallForEachItemOperator(
            task_id='put_importtask_resources',
            endpoint='/services/TaskService1.svc/BulkUpdateResourceAssignments',
            items='{{ result("put_import_tasks") | to_json }}',
            execution_timeout=timedelta(days=14),
            data=lambda item, dag_run: {
                'taskUri': item['task']['uri'],
                'resourceUris': dag_run.conf['ResourceUris'],
                'isAssigned': True
            }
        )

        log_successful_import_task_creation = rail.WriteLogOperator(
            task_id="log_successful_import_task_creation",
            log="{{ result('create_log') }}",
            items='{{ result("put_import_tasks") | to_json }}',
            severity="Success",
            message="{{ item.task.name }} added as subtask of {{ item.parent.name }} successfully",
        )

        creation_complete = rail.EmptyOperator(task_id='creation_complete')

        put_billingkey_tasks >> has_any_resources_for_billingkeytasks >> rail.Label(
            "Yes") >> put_billingkeytask_resources >> log_successful_billingkey_task_creation
        has_any_resources_for_billingkeytasks >> rail.Label(
            "No") >> log_successful_billingkey_task_creation
        put_billingkey_tasks >> put_import_tasks >> has_any_resources_for_importtasks >> rail.Label(
            "Yes") >> put_importtask_resources >> log_successful_import_task_creation
        has_any_resources_for_importtasks >> rail.Label(
            "No") >> log_successful_import_task_creation
        put_billingkey_tasks >> log_any_tasks_missing_dates
        [log_any_tasks_missing_dates, log_successful_import_task_creation,
            log_successful_billingkey_task_creation] >> creation_complete

        return put_billingkey_tasks, creation_complete
