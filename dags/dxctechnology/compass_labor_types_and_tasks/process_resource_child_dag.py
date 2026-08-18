
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_labor_types_and_tasks_process_resource_child_{config.sub_erp_name}_{config.instance}',
        description=f'DXC_Compass_Labour_Type_and_Task_Automation- Process resource child {config.sub_erp_name}_{config.instance}',
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

        has_personnelnumber = rail.IfOperator(
            task_id='has_personnelnumber',
            test="{{ dag_run.conf.personnelnumber | is_truthy }}",
            yes_task="get_user_basedon_employee_id",
        )

        get_user_basedon_employee_id = rail.RepliconServiceOperator(
            task_id='get_user_basedon_employee_id',
            endpoint="/services/UserListService1.svc/GetData",
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
                            "text": "{{ dag_run.conf.personnelnumber }}",
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
            data_handler=lambda data: rail.find_first_by_attr_and_get_attr(
                list(map(lambda item: {
                    "name": item['cells'][0]['textValue'],
                    "uri": item['cells'][0]['uri'],
                    "employeeid": item['cells'][1].get('textValue'),
                    "status": item['cells'][2].get('textValue')
                }, data['rows'])), 'employeeid', rail.get_current_context()['dag_run'].conf['personnelnumber'], 'uri')

        )

        has_invalid_user = rail.IfOperator(
            task_id='has_invalid_user',
            test=lambda: not rail.result('get_user_basedon_employee_id'),
            yes_task="add_invalid_log",
            no_task='has_no_assignment',
        )

        add_invalid_log = rail.WriteLogOperator(
            task_id='add_invalid_log',
            log="{{ dag_run.conf.log }}",
            message='Required user with employee id {{ dag_run.conf.personnelnumber }} not available in Replicon',
            severity='Exception',
            properties={
                'wbs': '{{dag_run.conf.wbs}}',
                'task': '',
                'billingrate': '',
                'message': 'Required user with employee id {{ dag_run.conf.personnelnumber }} not available in Replicon',
                'status': 'Exception',
            }
        )

        def do_has_no_assignment():
            return len(list(filter(lambda x: x['resource'] and x['resource']['uri'] ==
                                   rail.result(
                'get_user_basedon_employee_id'),
                rail.get_current_context(
            )['dag_run'].conf['project_info']['team'])
            )) == 0

        has_no_assignment = rail.IfOperator(
            task_id='has_no_assignment',
            test=do_has_no_assignment,
            yes_task="assign_user_to_project",
            no_task="get_task_assignments_for_resource",
        )

        assign_user_to_project = rail.RepliconServiceOperator(
            task_id='assign_user_to_project',
            endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
            data={
                "projectUri": "{{ dag_run.conf.project_info.project.uri }}",
                "resourceUri": "{{ result('get_user_basedon_employee_id') }}",
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            }
        )

        get_task_assignments_for_resource = rail.RepliconServiceOperator(
            task_id='get_task_assignments_for_resource',
            endpoint="/services/ProjectService1.svc/GetTaskAssignmentsForResource2",
            data={
                "projectUri": "{{ dag_run.conf.project_info.project.uri }}",
                "teamMember": {
                    "user": {
                        "uri": "{{ result('get_user_basedon_employee_id') }}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "department": null,
                    "location": null,
                    "division": null,
                    "departmentGroup": null,
                    "employeeTypeGroup": null,
                    "serviceCenter": null,
                    "costCenter": null
                },
                "asOfDate": null
            },

            data_handler=lambda data: list(
                map(lambda x: {'taskuri': x['taskUri']}, data))
        )

        has_task_assignments_for_resource = rail.IfOperator(
            task_id='has_task_assignments_for_resource',
            test="{{ result('get_task_assignments_for_resource') | length > 0 and dag_run.conf.project_tasks | length > 0}}",
            yes_task='create_task_collection',
            no_task='log_to_sumo'
        )

        create_task_collection = rail.CreateCollectionOperator(
            task_id='create_task_collection',
            name='taskassigned',
            source="{{ result('get_task_assignments_for_resource') | to_json }}"
        )

        query_list_task = rail.QueryCollectionOperator(
            task_id='query_list_task',
            query='''SELECT * FROM project_task WHERE taskuri NOT IN
                    (SELECT taskuri FROM  taskassigned )
                    AND tasktype = 'C1 IWO Task' OR
                    taskname= ':wbs' ''',
            query_params={'wbs': '{{dag_run.conf.wbs}}'}
        )

        has_tasks = rail.IfOperator(
            task_id='has_tasks',
            test="{{ result('query_list_task','length') > 0 }}",
            yes_task="bulk_update_resource_assignments",
            no_task="log_to_sumo"
        )

        bulk_update_resource_assignments = rail.RepliconServiceCallForEachItemOperator(
            task_id='bulk_update_resource_assignments',
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            items="{{ result('query_list_task') }}",
            data={
                "taskUri": "{{item.taskuri}}",
                "resourceUris": [
                    "{{ result('get_user_basedon_employee_id') }}"
                ],
                "isAssigned": "true"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        has_personnelnumber >> rail.Label(
            'yes') >> get_user_basedon_employee_id
        get_user_basedon_employee_id >> has_invalid_user
        has_invalid_user >> rail.Label('yes') >> add_invalid_log >> log_to_sumo
        has_invalid_user >> rail.Label('No') >> has_no_assignment
        has_no_assignment >> rail.Label(
            'yes') >> assign_user_to_project >> get_task_assignments_for_resource
        has_no_assignment >> rail.Label(
            'no') >> get_task_assignments_for_resource
        get_task_assignments_for_resource >> has_task_assignments_for_resource
        has_task_assignments_for_resource >> rail.Label(
            'yes') >> create_task_collection
        has_task_assignments_for_resource >> rail.Label('no') >> log_to_sumo
        create_task_collection >> query_list_task >> has_tasks
        has_tasks >> rail.Label(
            'yes') >> bulk_update_resource_assignments >> log_to_sumo
        has_tasks >> rail.Label('no') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
