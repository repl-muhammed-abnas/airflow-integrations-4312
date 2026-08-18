from datetime import timedelta
import itertools
import rail


def get_update_billingkey_task():
    with rail.TaskGroup(group_id='update_billingkey_task', prefix_group_id=False):
        updating_billingkey_task = rail.EmptyOperator(
            task_id="updating_billingkey_task")

        get_billingkeytasks_to_update_code = rail.PythonOperator(
            task_id='get_billingkeytasks_to_update_code',
            python_callable=lambda dag_run: [
                task['uri'] for task in dag_run.conf['BillingTasks'] if task['code'] != dag_run.conf['BillingKey']['Description']],
        )

        def convert_date_dxc_to_str(
            d): return f'{d[0:4]}-{d[4:6]}-{d[6:8]}' if d else None

        def convert_date_dxc_to_contract(
            d): return {"year": d[0:4], "month": d[4:6], "day": d[6:8]} if d else None

        def convert_date_contract_to_str(
            d): return f"{d['year']:02}-{d['month']:02}-{d['day']:02}" if d else None

        get_billingkeytasks_to_update_daterange = rail.PythonOperator(
            task_id='get_billingkeytasks_to_update_daterange',
            python_callable=lambda dag_run: [task['uri'] for task in dag_run.conf['BillingTasks'] if
                                             task['timeEntryDateRange']['startDate'] != convert_date_dxc_to_str(dag_run.conf['BillingKey']['StartDate']) or
                                             task['timeEntryDateRange']['endDate'] != convert_date_dxc_to_str(
                                                 dag_run.conf['BillingKey']['EndDate'])
                                             ],
        )

        update_billingkeytask_codes = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_billingkeytask_codes',
            endpoint='/services/TaskService1.svc/UpdateCode',
            items='{{ result("get_billingkeytasks_to_update_code") | to_json }}',
            execution_timeout=timedelta(days=14),
            data={
                'taskUri': '{{ item }}',
                'code': '{{ dag_run.conf.BillingKey.Description }}',
            },
        )

        log_updated_billingkeytask_codes = rail.WriteLogOperator(
            task_id="log_updated_billingkeytask_codes",
            log="{{ result('create_log') }}",
            items='{{ result("get_billingkeytasks_to_update_code") | to_json }}',
            severity='Success',
            message="The Billing Key Description is updated to {{ dag_run.conf.BillingKey.Description }} on {{ item }}",
        )

        update_billingkeytask_dateranges = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_billingkeytask_dateranges',
            endpoint='/services/TaskService1.svc/UpdateTimeEntryDateRange',
            items='{{ result("get_billingkeytasks_to_update_daterange") | to_json }}',
            execution_timeout=timedelta(days=14),
            data=lambda item, dag_run: {
                'taskUri': item,
                'dateRange': {
                    'startDate': convert_date_dxc_to_contract(dag_run.conf['BillingKey']['StartDate']),
                    'endDate': convert_date_dxc_to_contract(dag_run.conf['BillingKey']['EndDate']),
                },
            },
        )

        log_updated_billingkeytask_dateranges = rail.WriteLogOperator(
            task_id="log_updated_billingkeytask_dateranges",
            log="{{ result('create_log') }}",
            items='{{ result("get_billingkeytasks_to_update_code") | to_json }}',
            severity="Success",
            message="The time entry date range is updated for Billing Key {{ dag_run.conf.BillingKey.Name }} on {{ item }}",
        )

        get_existing_billingkey_task_descendants = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_existing_billingkey_task_descendants',
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            execution_timeout=timedelta(days=14),
            items=lambda dag_run: dag_run.conf['BillingTasks'],
            data={'parentUri': '{{ item.uri }}'},
        )

        def get_billingkeytask_combinations(dag_run):
            billingkey_tasks = dag_run.conf['BillingTasks']
            import_tasks = dag_run.conf['ImportTasks']
            return [{"billingkey_task": bk_task, "import_task": imp_task}
                    for bk_task, imp_task in itertools.product(billingkey_tasks, import_tasks)]

        def get_existing_gsap_tasks(import_task):
            existing_tasks = rail.result(
                "get_existing_billingkey_task_descendants")
            result = []
            for bk_descendanttasks in existing_tasks:
                result.extend(
                    [et['task'] for et in bk_descendanttasks if et['task']['name'] == import_task['TaskName']])
            return result

        def find_existing_task(combo):
            existing_tasks = get_existing_gsap_tasks(combo['import_task'])
            existing_task = [et for et in existing_tasks if et['parent']
                             ['task']['uri'] == combo['billingkey_task']['uri']]
            return existing_task[0] if existing_task else None

        get_importtasks_to_be_created = rail.DataAdaptorOperator(
            task_id='get_importtasks_to_be_created',
            source=get_billingkeytask_combinations,
            data=lambda item: None if item and find_existing_task(
                item) else item,
        )

        def do_get_importtasks_to_update_code(item):
            existing_tasks = get_existing_gsap_tasks(item) if item else []
            return [{
                'task_uri': et['uri'],
                'task_name': item['TaskName'],
                'new_code': item['Description']
            } for et in existing_tasks if et['code'] != item['Description']]
        get_importtasks_to_update_code = rail.DataAdaptorOperator(
            task_id='get_importtasks_to_update_code',
            source='{{ dag_run.conf.ImportTasks | to_json }}',
            data=do_get_importtasks_to_update_code,
            columns=['task_uri', 'task_name', 'new_code'],
        )

        def do_get_importtasks_to_update_daterange(item):
            if item and item['StartDate'] and item['EndDate']:
                existing_tasks = get_existing_gsap_tasks(item)
                return [{
                    'task_uri': et['uri'],
                    'task_name': item['TaskName'],
                    'new_daterange': {
                        'startDate': convert_date_dxc_to_contract(item['StartDate']),
                        'endDate': convert_date_dxc_to_contract(item['EndDate']),
                    }
                } for et in existing_tasks if
                    convert_date_contract_to_str(et['timeEntryDateRange']['startDate']) != convert_date_dxc_to_str(item['StartDate']) or
                    convert_date_contract_to_str(et['timeEntryDateRange']['endDate']) != convert_date_dxc_to_str(item['EndDate'])]
            return []
        get_importtasks_to_update_daterange = rail.DataAdaptorOperator(
            task_id='get_importtasks_to_update_daterange',
            source='{{ dag_run.conf.ImportTasks | to_json }}',
            data=do_get_importtasks_to_update_daterange,
            columns=['task_uri', 'task_name', 'new_daterange'],
        )

        get_importtasks_missing_dates = rail.DataAdaptorOperator(
            task_id='get_importtasks_missing_dates',
            source='{{ dag_run.conf.ImportTasks | to_json }}',
            data=lambda it: None if it and it['StartDate'] and it['EndDate'] else it,
        )

        log_importtasks_missing_dates = rail.WriteLogOperator(
            task_id="log_importtasks_missing_dates",
            log="{{ result('create_log') }}",
            items="{{ result('get_importtasks_missing_dates') }}",
            severity="Exception",
            message="""
                {%- set msgs = [] -%}
                {%- do msgs.append(item.TaskName + ' has no start date') if not item.StartDate -%}
                {%- do msgs.append(item.TaskName + ' has no end date') if not item.EndDate -%}
                {{- msgs | join(', ') -}}
            """,
        )

        def get_create_missing_importtasks_data(item, dag_run):
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
                        "startDate": convert_date_dxc_to_contract(item['import_task']['StartDate']),
                        "endDate": convert_date_dxc_to_contract(item['import_task']['EndDate']),
                    },
                    "customFieldValues": [
                        {
                            "customField": {"uri": dag_run.conf['TaskTypeOptionUri']},
                            "dropDownOption": {"uri": dag_run.conf['TaskTypeOptionValueUri']},
                        }
                    ],
                    "assignedResources" : [{'uri': resource_uri} for resource_uri in dag_run.conf['ResourceUris']],
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
                    "isTimeEntryAllowed": True,
                }
            }
        create_missing_importtasks = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_missing_importtasks',
            endpoint='/services/ProjectService1.svc/PutTask',
            items='{{ result("get_importtasks_to_be_created") }}',
            execution_timeout=timedelta(days=14),
            data=get_create_missing_importtasks_data,
        )

        log_successful_importtask_creation = rail.WriteLogOperator(
            task_id="log_successful_importtask_creation",
            log="{{ result('create_log') }}",
            items='{{ result("create_missing_importtasks") | to_json }}',
            severity="Success",
            message="{{ item.name }} was added successfully",
        )

        update_importtasks_codes = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_importtasks_codes',
            endpoint='/services/TaskService1.svc/UpdateCode',
            items='{{ result("get_importtasks_to_update_code") }}',
            execution_timeout=timedelta(days=14),
            data={
                'taskUri': '{{ item.task_uri }}',
                'code': '{{ item.new_code }}',
            }
        )
        log_successful_code_updates = rail.WriteLogOperator(
            task_id="log_successful_code_updates",
            log="{{ result('create_log') }}",
            items='{{ result("get_importtasks_to_update_code") }}',
            severity="Success",
            message="The Task Description for {{ item.task_name }} is updated to {{ item.new_code }}",
        )

        update_importtasks_dateranges = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_importtasks_dateranges',
            endpoint='/services/TaskService1.svc/UpdateTimeEntryDateRange',
            items='{{ result("get_importtasks_to_update_daterange") }}',
            execution_timeout=timedelta(days=14),
            data=lambda item: {
                'taskUri': item['task_uri'],
                'dateRange': item['new_daterange'],
            }
        )
        log_successful_daterange_updates = rail.WriteLogOperator(
            task_id="log_successful_daterange_updates",
            log="{{ result('create_log') }}",
            items='{{ result("get_importtasks_to_update_daterange") }}',
            severity="Success",
            message="The time entry date range is updated for {{ item.task_name }}",
        )

        update_complete = rail.EmptyOperator(task_id='update_complete')

        updating_billingkey_task >> [
            get_billingkeytasks_to_update_code,
            get_billingkeytasks_to_update_daterange,
            get_existing_billingkey_task_descendants]
        get_billingkeytasks_to_update_code >> update_billingkeytask_codes >> log_updated_billingkeytask_codes >> update_complete
        get_billingkeytasks_to_update_daterange >> update_billingkeytask_dateranges >> log_updated_billingkeytask_dateranges >> update_complete
        get_existing_billingkey_task_descendants >> [
            get_importtasks_to_be_created,
            get_importtasks_to_update_code,
            get_importtasks_to_update_daterange,
            get_importtasks_missing_dates]
        get_importtasks_missing_dates >> log_importtasks_missing_dates >> update_complete
        get_importtasks_to_be_created >> create_missing_importtasks >> log_successful_importtask_creation >> update_complete
        get_importtasks_to_update_code >> update_importtasks_codes >> log_successful_code_updates >> update_complete
        get_importtasks_to_update_daterange >> update_importtasks_dateranges >> log_successful_daterange_updates >> update_complete
        return updating_billingkey_task, update_complete
