import uuid
from datetime import timedelta
from pendulum import now
from airflow.models import Variable
from pwcglobal.ord_department_hierarchy_sync.utils import response_filter
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'pwcglobal_ord_department_hierarchy_sync_new_pwc_ord_department_group_hierarchy_sync_enable_v1_0_{config.instance}',
        description=f'PwC | ORD Department Group Hierarchy Sync Enable V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_level_6_and_existing_uri'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_level_6_and_existing_uri',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_level_6_and_existing_uri = rail.IfOperator(
            task_id='if_level_6_and_existing_uri',
            test=lambda dag_run: dag_run.conf.get('level', '') == '6' and dag_run.conf.get('existing_dep_uri'),
            yes_task='todays_date',
            no_task='enable_department_group'
        )

        todays_date = rail.PythonOperator(
            task_id='todays_date',
            python_callable=lambda: now('Etc/UTC').strftime('%Y/%m/%d')
        )

        get_all_users_with_old_dept = rail.RepliconServiceOperator(
            task_id='get_all_users_with_old_dept',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:enabled",
                    "urn:replicon:user-list-column:department-group"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
                        },
                        "operatorUri": "urn:replicon:filter-operator:equal",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": null,
                                "uris": [],
                                "bool": "true",
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": null,
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
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:user-list-filter:department-group"
                        },
                        "operatorUri": "urn:replicon:filter-operator:equal",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": "{{ dag_run.conf.existing_dep_uri }}",
                                "uris": [],
                                "bool": null,
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": null,
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
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            response_filter=response_filter.get_userlist
        )

        update_users_with_new_dept = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_users_with_new_dept',
            items="{{ result('get_all_users_with_old_dept') | to_json }}",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run, item: {
                "user": {
                    "uri": item['uri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "departmentGroupScheduleToApply": {
                        "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementDepartmentGroupSchedule": [],
                        "updateDepartmentGroupScheduleOverDateRange": {
                            "replacementDepartmentGroupScheduleEntries": [
                                {
                                    "departmentGroup": {
                                        "uri": dag_run.conf['uri'],
                                        "parent": null,
                                        "name": null,
                                        "parameterCorrelationId": null
                                    },
                                    "effectiveDate": {
                                        "year": rail.result('todays_date').split('/')[0],
                                        "month": rail.result('todays_date').split('/')[1],
                                        "day":  rail.result('todays_date').split('/')[2]
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        get_all_projects_with_old_dept = rail.RepliconServiceOperator(
            task_id='get_all_projects_with_old_dept',
            endpoint="/services/ProjectListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:project-list-column:project",
                    "urn:replicon:project-list-column:status"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:project-list-filter:department-group"
                        },
                        "operatorUri": "urn:replicon:filter-operator:equal",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": "{{ dag_run.conf.existing_dep_uri }}",
                                "uris": [],
                                "bool": null,
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": null,
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
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:project-list-filter:status"
                        },
                        "operatorUri": "urn:replicon:filter-operator:equal",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": "urn:replicon:project-status-type:in-progress",
                                "uris": [],
                                "bool": null,
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": null,
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
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            response_filter=response_filter.get_projectlist
        )

        update_projects_with_new_dept = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_projects_with_new_dept',
            items="{{ result('get_all_projects_with_old_dept') | to_json }}",
            endpoint="/services/ProjectService1.svc/UpdateDepartmentGroup2",
            data={
                "projectUri": "{{ item.uri }}",
                "departmentGroup": {
                    "uri": "{{ dag_run.conf.uri }}",
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                }
            }
        )

        update_code_disable_department = rail.RepliconServiceOperator(
            task_id='update_code_disable_department',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "uri": dag_run.conf['existing_dep_uri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": null,
                    "codeToApply": {"value": null},
                    "descriptionToApply": null,
                    "isEnabled": "false"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        update_code_enable_department = rail.RepliconServiceOperator(
            task_id='update_code_enable_department',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "uri": dag_run.conf['uri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": dag_run.conf['name'],
                    "codeToApply": {"value": dag_run.conf['code']} if dag_run.conf['code'] else null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        enable_department_group = rail.RepliconServiceOperator(
            task_id='enable_department_group',
            endpoint="/services/DepartmentGroupService1.svc/Enable",
            data={
                "departmentGroupUri": "{{ dag_run.conf.uri }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> if_level_6_and_existing_uri
        if_level_6_and_existing_uri >> rail.Label('Yes') >> todays_date >> get_all_users_with_old_dept >> \
        update_users_with_new_dept >> get_all_projects_with_old_dept >> update_projects_with_new_dept >> \
        update_code_disable_department >> update_code_enable_department >> log_to_sumo
        if_level_6_and_existing_uri >> rail.Label('No') >> enable_department_group >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
