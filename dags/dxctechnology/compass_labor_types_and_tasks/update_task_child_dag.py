
from datetime import datetime, timedelta
from uuid import uuid4
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_labor_types_and_tasks_update_task_child_{config.sub_erp_name}_{config.instance}',
        description=f'DXC_COMPASS_Labour Types and Task- child update task {config.sub_erp_name}_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_dag_run_child_process,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config
        )

        def conf():
            return rail.get_current_context()['dag_run'].conf

        def get_replicon_date(val):
            if not val:
                return null
            date = datetime.strptime(val, '%Y%m%d')
            return {
                "year": date.year,
                "month": date.month,
                "day": date.day
            }

        create_task_or_apply_modifications = rail.RepliconServiceOperator(
            task_id='create_task_or_apply_modifications',
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=lambda: {
                "target": {
                    "uri": conf()['taskuri'],
                    "name": null,
                    "parent": null,
                    "parameterCorrelationId": null
                },
                "project": {
                    "uri": conf()['projecturi'],
                    "name": null,
                    "code": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": null,
                    "codeToApply": {
                        "value": conf()['description']
                    },
                    "descriptionToApply": null,
                    "isClosed": "false",
                    "timeEntryStartDateToApply": {'date': get_replicon_date(conf()['startdate'])},
                    "timeEntryEndDateToApply": {'date': get_replicon_date(conf()['enddate'])},
                    "timeAndExpenseEntryTypeToApply": null,
                    "isTimeEntryAllowed": "true",
                    "costTypeToApply": null,
                    "estimatedHoursToApply": null,
                    "estimatedCostToApply": null,
                    "resourceAssignmentModifications": null,
                    "customFieldsToApply": [],
                    "keyValuesToApply": [],
                    "objectExtensionFieldsToApply": []
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        get_user_basedon_employee_id = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_user_basedon_employee_id',
            execution_timeout=timedelta(days=14),
            endpoint="/services/UserListService1.svc/GetData",
            items=lambda: list(filter(lambda x: bool(x['personnelnumber']),
                                      rail.get_current_context()['dag_run'].conf['resourceandtaskassignment'])),
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
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
                            "text": "{{item.personnelnumber}}",
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=lambda data: list(map(lambda row: {
                "name": row['cells'][0].get('textValue'),
                "uri": row['cells'][0].get('uri'),
                "employeeid": row['cells'][1].get('textValue'),
                "status": row['cells'][2].get('textValue'),
            }, data['rows'])),
            flatten=True,
        )

        putresourcetaskallocationsfortask = rail.RepliconServiceCallForEachItemOperator(
            task_id='putresourcetaskallocationsfortask',
            execution_timeout=timedelta(days=14),
            endpoint="/services/ResourceService1.svc/PutResourceTaskAllocationsForTask",
            items=lambda: list(filter(lambda x: x['personnelnumber'] and x['taskassignmentstartdate'] and x['taskassignmentenddate'] and
                                      rail.find_first_by_attr_and_get_attr(
                rail.result('get_user_basedon_employee_id'),
                'employeeid',
                x['personnelnumber'], 'uri'
            ),
                rail.get_current_context()['dag_run'].conf['resourceandtaskassignment'])),
            data=lambda item: {
                "taskUri": rail.result('create_task_or_apply_modifications')['uri'],
                "taskAllocations": [
                    {
                        "resourceUri": rail.find_first_by_attr_and_get_attr(
                            rail.result('get_user_basedon_employee_id'),
                            'employeeid',
                            item['personnelnumber'], 'uri'
                        ),
                        "dateRange": {
                            "startDate": get_replicon_date(item['taskassignmentstartdate']),
                            "endDate": get_replicon_date(item['taskassignmentenddate'])
                        }
                    }
                ]
            }
        )

        log_entry = rail.WriteLogOperator(
            task_id='log_entry',
            log="{{dag_run.conf.log}}",
            message='Updated successfully',
            severity='Success',
            properties={
                'wbs': '{{dag_run.conf.projectname}}',
                'task': '{{ dag_run.conf.name }}',
                'billingrate': '',
                'message': 'Updated successfully',
                'status': 'Success',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message()}}',
            properties={
                'wbs': '{{dag_run.conf.projectname}}',
                'task': '{{ dag_run.conf.name }}',
                'billingrate': '',
                'message': '{{ get_error_message()}}',
                'status': 'Error',
            }
        )

        create_task_or_apply_modifications >> get_user_basedon_employee_id >> putresourcetaskallocationsfortask >> log_entry >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)
