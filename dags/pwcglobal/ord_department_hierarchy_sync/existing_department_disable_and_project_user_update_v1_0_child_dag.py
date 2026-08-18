from datetime import timedelta
from pendulum import now
from airflow.models import Variable
from pwcglobal.ord_department_hierarchy_sync.utils import response_filter
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'pwc_ord_department_group_hierarchy_sync_existingdepartment_disable_and_project_userupdate_v10_{config.instance}',
        description=f'PwC | ORD Department Group Hierarchy Sync Existing Department disable and project-user update V1.0 {config.instance}',
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
            no_task='date_split_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='date_split_3',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        date_split_3 = rail.PythonOperator(
            task_id='date_split_3',
            python_callable=lambda: now('Etc/UTC').strftime('%Y/%m/%d')
        )

        getallusersassociatedwitholdgroup_4 = rail.RepliconServiceOperator(
            task_id='getallusersassociatedwitholdgroup_4',
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
                                "uri": "{{ dag_run.conf.existinguri }}",
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

        updateusersdepartmentgroup_7 = rail.RepliconServiceCallForEachItemOperator(
            task_id='updateusersdepartmentgroup_7',
            items="{{ result('getallusersassociatedwitholdgroup_4') | to_json }}",
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
                                        "uri": dag_run.conf['newuri'],
                                        "parent": null,
                                        "name": null,
                                        "parameterCorrelationId": null
                                    },
                                    "effectiveDate": {
                                        "year": rail.result('date_split_3').split('/')[0],
                                        "month": rail.result('date_split_3').split('/')[1],
                                        "day":  rail.result('date_split_3').split('/')[2]
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

        getallprojectsassociatedwitholdgroup_8 = rail.RepliconServiceOperator(
            task_id='getallprojectsassociatedwitholdgroup_8',
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
                                "uri": "{{ dag_run.conf.existinguri }}",
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

        update_department_groupforproject_11 = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_department_groupforproject_11',
            items="{{ result('getallprojectsassociatedwitholdgroup_8') | to_json }}",
            endpoint="/services/ProjectService1.svc/UpdateDepartmentGroup2",
            data={
                "projectUri": "{{ item.uri }}",
                "departmentGroup": {
                    "uri": "{{ dag_run.conf.newuri }}",
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                }
            }
        )

        disableolddepartment_12 = rail.RepliconServiceOperator(
            task_id='disableolddepartment_12',
            endpoint="/services/DepartmentGroupService1.svc/Disable",
            data={
                "departmentGroupUri": "{{ dag_run.conf.existinguri }}"
            }
        )

        renmove_codeforolddepartment_13 = rail.RepliconServiceOperator(
            task_id='renmove_codeforolddepartment_13',
            endpoint="/services/DepartmentGroupService1.svc/UpdateCode",
            data={
                "departmentGroupUri": "{{ dag_run.conf.existinguri }}",
                "code": null
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> date_split_3
        date_split_3 >> getallusersassociatedwitholdgroup_4 >> updateusersdepartmentgroup_7 >> getallprojectsassociatedwitholdgroup_8 \
            >> update_department_groupforproject_11 >> disableolddepartment_12 >> renmove_codeforolddepartment_13 >> finish
        finish >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
