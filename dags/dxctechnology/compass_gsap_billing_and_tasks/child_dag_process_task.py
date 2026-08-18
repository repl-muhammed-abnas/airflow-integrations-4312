from datetime import timedelta
import uuid
import rail

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/compass_gsap_billing_and_tasks/config.py

def create_child_dag_task(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_gsap_billing_and_tasks_import_{config.sub_erp_name}_child_task',
        description=f'DXC COMPASS GSAP Billing and Tasks Child Task - {config.sub_erp_name}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_concurrent_gsap_task_imports,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        has_all_required_fields = rail.IfOperator(
            task_id='has_all_required_fields',
            test='{{ dag_run.conf.Task | attr_or_default("StartDate", "") | length > 0 and \
                dag_run.conf.Task | attr_or_default("EndDate", "") | length > 0 and \
                dag_run.conf.Task | attr_or_default("TaskName", "") | length > 0 }}',
            yes_task='create_log',
            no_task='log_missing_required_fields',
        )

        def get_log_missing_required_fields_msg(dag_run):
            msg = []
            msg.append(
                "Task name is not present" if not dag_run.conf['Task']['TaskName'] else None)
            msg.append(
                "Task Start Date is not present" if not dag_run.conf['Task']['StartDate'] else None)
            msg.append(
                "Task End Date is not present" if not dag_run.conf['Task']['EndDate'] else None)
            return ", ".join([m for m in msg if m is not None])
        log_missing_required_fields = rail.WriteLogOperator(
            task_id="log_missing_required_fields",
            severity='Error',
            message=get_log_missing_required_fields_msg,
            properties={
                'WBS': '{{ dag_run.conf.Task.WBS }}',
                'Task': '{{ dag_run.conf.Task.TaskName }}',
            }
        )

        create_log = rail.CreateLogOperator(task_id='create_log')

        does_this_task_already_exist = rail.IfOperator(
            task_id="does_this_task_already_exist",
            test="{{ dag_run.conf.ExistingTasks | length > 0 }}",
            yes_task='get_gsaptasks_to_update',
            no_task='create_gsap_tasks',
        )

        def get_put_task_data(item, dag_run):
            def convert_date(d):
                return {
                    "year": d[0:4], "month": d[4:6], "day": d[6:8]
                }
            return {
                "project": {"uri": dag_run.conf['ProjectUri']},
                "task": {
                    "target": {
                        "name": dag_run.conf['Task']['TaskName'],
                        "parent": {"uri": item['uri']},
                    },
                    "name": dag_run.conf['Task']['TaskName'],
                    "code": dag_run.conf['Task']['Description'],
                    "timeEntryDateRange": {
                        "startDate": convert_date(dag_run.conf['Task']['StartDate']),
                        "endDate": convert_date(dag_run.conf['Task']['EndDate']),
                    },
                    "customFieldValues": [
                        {
                            "customField": {"uri": dag_run.conf['TaskTypeOptionUri']},
                            "dropDownOption": {"uri": dag_run.conf['TaskTypeOptionValueUri']},
                        }
                    ],
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
                    "isTimeEntryAllowed": True,
                    "assignedResources": [{'uri': r} for r in dag_run.conf['ResourceUris']],
                }
            }
        create_gsap_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_gsap_tasks',
            endpoint='/services/ProjectService1.svc/PutTask',
            items=lambda dag_run: dag_run.conf['BillingKeyTasks'],
            data=get_put_task_data,
            execution_timeout=timedelta(days=14),
            data_handler=lambda data, item: {"task": data, "parent": item},
        )

        log_gsap_task_creations = rail.WriteLogOperator(
            task_id="log_gsap_task_creations",
            log="{{ result('create_log') }}",
            items='{{ result("create_gsap_tasks") | to_json }}',
            severity="Success",
            message="Task added successfully under {{ item.parent.name }}",
        )

        def do_get_gsaptasks_to_update(item, dag_run):
            def convert_date_to_str(d):
                return f'{d[0:4]}-{d[4:6]}-{d[6:8]}'

            if item:
                if item['timeEntryDateRange']['startDate'] != convert_date_to_str(dag_run.conf['Task']['StartDate']) or \
                        item['timeEntryDateRange']['endDate'] != convert_date_to_str(dag_run.conf['Task']['EndDate']) or \
                        item['code'] != dag_run.conf['Task']['Description']:
                    return item
            return None
        get_gsaptasks_to_update = rail.DataAdaptorOperator(
            task_id='get_gsaptasks_to_update',
            source=lambda dag_run: dag_run.conf['ExistingTasks'],
            data=do_get_gsaptasks_to_update,
        )

        def get_update_task_data(item, dag_run):
            def convert_date(d):
                return {
                    "year": d[0:4], "month": d[4:6], "day": d[6:8]
                }
            return {
                "target": {"uri": item['uri']},
                "project": {"uri": dag_run.conf['ProjectUri']},
                "modifications": {
                    "codeToApply": {"value": dag_run.conf['Task']['Description']},
                    "timeEntryStartDateToApply": {"date": convert_date(dag_run.conf['Task']['StartDate'])},
                    "timeEntryEndDateToApply": {"date": convert_date(dag_run.conf['Task']['EndDate'])},
                },
                "unitOfWorkId": str(uuid.uuid4()),
            }
        update_existing_gsap_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_existing_gsap_tasks',
            execution_timeout=timedelta(days=14),
            endpoint='/services/TaskService1.svc/CreateTaskOrApplyModifications',
            items=lambda: rail.result('get_gsaptasks_to_update'),
            data=get_update_task_data,
        )

        log_gsap_task_updates = rail.WriteLogOperator(
            task_id="log_gsap_task_updates",
            log="{{ result('create_log') }}",
            items=lambda: rail.result('get_gsaptasks_to_update'),
            severity="Success",
            message="Updated successfully under {{ item | attr_or_default('parent.task.name', 'Unknown') }}",
        )

        def get_completion_log_severity():
            logs = rail.load_all_records(rail.result('create_log'))
            if any(filter(lambda e: e['severity'] == 'Error', logs)):
                return 'Error'
            if any(filter(lambda e: e['severity'] == 'Exception', logs)):
                return 'Exception'
            return 'Success'
        log_successful_completion = rail.WriteLogOperator(
            task_id='log_successful_completion',
            message='{{ result("create_log") | load_all_records | map_to_attr("message") | join(" | ") | default("Task processed successfully", True)}}',
            severity=get_completion_log_severity,
            properties={
                'WBS': '{{ dag_run.conf.Task.WBS }}',
                'Task': '{{ dag_run.conf.Task.TaskName }}',
            },
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'WBS': '{{ dag_run.conf.Task.WBS }}',
                'Task': '{{ dag_run.conf.Task.TaskName }}',
            },
        )

        has_all_required_fields >> rail.Label("Yes") >> create_log >> does_this_task_already_exist >> rail.Label("Yes") >> get_gsaptasks_to_update >> \
            update_existing_gsap_tasks >> log_gsap_task_updates >> log_successful_completion
        does_this_task_already_exist >> rail.Label(
            "No") >> create_gsap_tasks >> log_gsap_task_creations >> log_successful_completion
        has_all_required_fields >> rail.Label(
            "No") >> log_missing_required_fields >> catch_and_log_errors
        log_successful_completion >> rail.Label(
            "On error") >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag_task)
