
from datetime import timedelta, datetime
from vjtechnologies.projectsync_v1.mappers.vjtechnologies_projectstatus_mapper import projectstatus_mapper
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.process_each_project_record_child_dagid,
        description=f'VJTechnologies_{config.entity_name}_Process_each_project_record_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_projectstatus_mapper_entries'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_projectstatus_mapper_entries',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_projectstatus_mapper_entries=rail.PythonOperator(
            task_id='get_projectstatus_mapper_entries',
            python_callable= lambda:  list(filter(lambda entry: entry["identifier"] == "yes" ,projectstatus_mapper))
        )

        get_project_status=rail.PythonOperator(
            task_id='get_project_status',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_projectstatus_mapper_entries'),'status',(dag_run.conf['projectstatus']).strip(),'value','Not Defined in Mapper')
        )

        create_logs_list_variable=rail.SetVariableOperator(
            task_id='create_logs_list_variable',
            append=False,
            name='logs',
            value=[]
        )

        if_projectstatus_not_defined_in_mapper=rail.IfOperator(
            task_id='if_projectstatus_not_defined_in_mapper',
            test='''{{ result('get_project_status') == 'Not Defined in Mapper' }}''',
            yes_task="insert_log_status_not_defined_in_mapper",
            no_task="if_projectname_and_projectcode_present",
        )

        insert_log_status_not_defined_in_mapper=rail.SetVariableOperator(
            task_id='insert_log_status_not_defined_in_mapper',
            append=True,
            name='{{ result("create_logs_list_variable").name }}',
            value={
                "status": "Exception",
                "details": "Project not added as the project status is not defined in mapper/not present in input file"
            }
        )

        add_log_status_not_defined_in_mapper=rail.WriteLogOperator(
            task_id='add_log_status_not_defined_in_mapper',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Exception",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "type": "project",
                "client": "{{ dag_run.conf.clientname }}",
                "project": "{{ dag_run.conf.projectname }}",
                "code": "{{ dag_run.conf.projectcode }}",
                "task": null,
                "status": "Exception",
                "reason": "Project not added as the project status is not defined in mapper/not present in input file",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_projectname_and_projectcode_present=rail.IfOperator(
            task_id='if_projectname_and_projectcode_present',
            test='''{{ dag_run.conf.projectname | is_truthy  and dag_run.conf.projectcode | is_truthy }}''',
            yes_task="get_expense_codes",
            no_task="add_log_project_name_or_code_not_present",
        )

        def get_expensecodes(data):
            return [{
                'name': expensecode['cells'][0]['dataType'],
                'uri': expensecode['cells'][0]['uri'],
                'status': expensecode['cells'][1]['textValue']
            } for expensecode in data['rows'] if expensecode['cells'][1]['textValue'] == 'True']

        get_expense_codes=rail.RepliconServiceOperator(
            task_id='get_expense_codes',
            endpoint="/services/ExpenseCodeListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:expense-code-list-column:name",
                    "urn:replicon:expense-code-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_expensecodes
        )

        create_variable_projectmanager=rail.SetVariableOperator(
            task_id='create_variable_projectmanager',
            append=False,
            name='projectmanager',
            value=None
        )

        if_projectname_present=rail.IfOperator(
            task_id='if_projectname_present',
            test='''{{ dag_run.conf.projectname | is_truthy }}''',
            yes_task="search_projectmanager_user",
            no_task="if_project_start_present",
        )

        def get_user_details(response,dag_run):
            projectmanager = []
            for user in response['rows']:
                if user['cells'][1]['textValue'] == dag_run.conf['projectmanager'] and user['cells'][2]['textValue'] == 'True':
                    projectmanager.append(user)
            return projectmanager

        search_projectmanager_user=rail.RepliconServiceOperator(
            task_id='search_projectmanager_user',
            endpoint='/services/UserListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{ dag_run.conf.projectmanager }}"
                        }
                    }
                }
            },
            data_handler=get_user_details
        )

        if_projectmanager_present_and_enabled=rail.IfOperator(
            task_id='if_projectmanager_present_and_enabled',
            test=lambda: rail.result('search_projectmanager_user') and rail.result('search_projectmanager_user')[0]['cells'][2]['textValue'] == 'True',
            yes_task="get_assigned_permissions",
            no_task="if_project_start_present",
        )

        get_assigned_permissions=rail.RepliconServiceOperator(
            task_id='get_assigned_permissions',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data=lambda:{
                "userUri": rail.result('search_projectmanager_user')[0]['cells'][0]['uri']
            }
        )

        check_project_management_permission = rail.PythonOperator(
            task_id = 'check_project_management_permission',
            python_callable=lambda:rail.find_first_by_attr_and_get_attr(rail.result(
                'get_assigned_permissions'),'policyUri','urn:replicon:policy:project-management','user.uri','')
        )

        if_project_management_permission_assigned=rail.IfOperator(
            task_id='if_project_management_permission_assigned',
            test=lambda: bool(rail.result('check_project_management_permission')),
            yes_task="update_projectmanager_variable",
            no_task="if_project_start_present",
        )

        update_projectmanager_variable=rail.SetVariableOperator(
            task_id='update_projectmanager_variable',
            append=False,
            name='{{ result("create_variable_projectmanager").name }}',
            value="{{ result('check_project_management_permission') }}"
        )

        if_project_start_present=rail.IfOperator(
            task_id='if_project_start_present',
            test='''{{ dag_run.conf.projectstart | is_truthy }}''',
            yes_task="get_projectstart_date_object",
            no_task="if_project_end_present",
        )

        def get_date_object(datestring):
            dateobj = datetime.strptime(datestring,'%Y/%m/%d')
            return{
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year
            }

        get_projectstart_date_object=rail.PythonOperator(
            task_id='get_projectstart_date_object',
            python_callable=lambda dag_run: get_date_object(dag_run.conf['projectstart'])
        )

        if_project_end_present=rail.IfOperator(
            task_id='if_project_end_present',
            test='''{{ dag_run.conf.projectend | is_truthy }}''',
            yes_task="get_projectend_date_object",
            no_task="if_clientcode_present",
        )

        get_projectend_date_object=rail.PythonOperator(
            task_id='get_projectend_date_object',
            python_callable=lambda dag_run: get_date_object(dag_run.conf['projectend'])
        )

        if_clientcode_present=rail.IfOperator(
            task_id='if_clientcode_present',
            test='''{{ dag_run.conf.clientcode | is_truthy }}''',
            yes_task="search_client",
            no_task="bulk_get_project_details",
        )

        def get_required_client(response,dag_run):
            required_client = {}
            for client in response['rows']:
                if client['cells'][0]['textValue'] == dag_run.conf['clientcode']:
                    required_client = client
                    break
            return required_client

        search_client=rail.RepliconServiceOperator(
            task_id='search_client',
            endpoint="/services/ClientListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:client-list-column:code",
                    "urn:replicon:client-list-column:client"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:client-list-filter:code"
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
                        "text": "{{ dag_run.conf.clientcode }}",
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
            data_handler=get_required_client
        )

        bulk_get_project_details=rail.RepliconServiceOperator(
            task_id='bulk_get_project_details',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data={
                "projects": [
                    {
                    "uri": null,
                    "name": null,
                    "code": "{{ dag_run.conf.projectcode }}",
                    "parameterCorrelationId": null
                    }
                ]
            }
        )

        def get_current_project_start_end_date():
            projectdetails = rail.result('bulk_get_project_details')[0]['projectDetails']['timeEntryDateRange'] if rail.result(
                'bulk_get_project_details')[0]['projectDetails'] and rail.result(
                'bulk_get_project_details')[0]['projectDetails']['timeEntryDateRange'] else null
            startdate = projectdetails['startDate'] if projectdetails else ''
            enddate = projectdetails['endDate'] if projectdetails else ''
            return {
                'start_date': str(startdate['year']) + '/' + str(startdate['month']) + '/' + str(startdate['day']) if startdate else '',
                'end_date': str(enddate['year']) + '/' + str(enddate['month']) + '/' + str(enddate['day']) if enddate else '',
            } if projectdetails else null

        get_project_current_start_end_date = rail.PythonOperator(
            task_id = 'get_project_current_start_end_date',
            python_callable=get_current_project_start_end_date
        )

        create_variable_projecturi=rail.SetVariableOperator(
            task_id='create_variable_projecturi',
            append=False,
            name='projecturi',
            value=lambda: rail.result('bulk_get_project_details')[0]['projectDetails']['uri'] if rail.result('bulk_get_project_details') and
                rail.result('bulk_get_project_details')[0]['projectDetails'] and rail.result('bulk_get_project_details')[0]['projectDetails']['uri'] else null
        )

        if_projecturi_not_present=rail.IfOperator(
            task_id='if_projecturi_not_present',
            test=lambda: not bool(rail.result('bulk_get_project_details')[0]['projectDetails'] and
                    rail.result('bulk_get_project_details')[0]['projectDetails']['uri']),
            yes_task="create_project",
            no_task="if_billingtype_equals_fixedbid",
        )

        create_project=rail.RepliconServiceOperator(
            task_id='create_project',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run:{
                "target": null,
                "modifications": {
                    "nameToApply": {
                        "value": dag_run.conf['projectname']
                    },
                    "codeToApply": {
                        "value": dag_run.conf['projectcode']
                    },
                    "clientAssignmentsSchedulesToApply": {
                        "clients": [
                            {
                                "client": {
                                    "uri": rail.result('search_client')['cells'][1]['uri']
                                },
                                "costAllocationPercentage": "100"
                            }
                        ]
                    } if rail.result('search_client') and rail.result('search_client')['cells'][1]['uri'] else null,
                    "startDateToApply": {
                        "date": {
                            "year": rail.result('get_projectstart_date_object')['year'],
                            "month": rail.result('get_projectstart_date_object')['month'],
                            "day": rail.result('get_projectstart_date_object')['day']
                        }
                    } if rail.result('get_projectstart_date_object') else null,
                    "endDateToApply": {
                        "date": {
                            "year": rail.result('get_projectend_date_object')['year'],
                            "month": rail.result('get_projectend_date_object')['month'],
                            "day": rail.result('get_projectend_date_object')['day']
                        }
                    } if rail.result('get_projectend_date_object') else null,
                    "isTimeEntryAllowed": "0",
                    "billingTypeToApply": {
                        "value": "urn:replicon:billing-type:non-billable"
                    }
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        update_projecturi_variable=rail.SetVariableOperator(
            task_id='update_projecturi_variable',
            append=False,
            name='{{ result("create_variable_projecturi").name }}',
            value="{{result('create_project').uri}}"
        )

        if_projectmanager_present=rail.IfOperator(
            task_id='if_projectmanager_present',
            test=lambda: bool(rail.get_dag_run_var('projectmanager')),
            yes_task="update_manager",
            no_task="create_project_or_apply_modifications",
        )

        update_manager=rail.RepliconServiceOperator(
            task_id='update_manager',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda:{
                "target": {
                    "uri": rail.get_dag_run_var('projecturi'),
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "nameToApply": null,
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "percentCompletedToApply": null,
                    "startDateToApply": null,
                    "endDateToApply": null,
                    "billingTypeToApply": null,
                    "clientBillingAllocationMethodToApply": null,
                    "clientAssignmentsSchedulesToApply": null,
                    "statusToApply": null,
                    "clientRepresentativeToApply": null,
                    "healthStateToApply": null,
                    "programToApply": null,
                    "projectLeaderToApply": {
                    "user": {
                        "uri": rail.get_dag_run_var('projectmanager'),
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                    },
                    "isProjectLeaderApprovalRequired": "true",
                    "costTypeToApply": null,
                    "isTimeEntryAllowed": null,
                    "estimatedHoursToApply": null,
                    "estimatedCostToApply": null,
                    "defaultBillingCurrencyToApply": null,
                    "timeAndMaterials": null,
                    "fixedBid": null,
                    "customFieldsToApply": [],
                    "resourceAssignmentModifications": null,
                    "keyValuesToApply": [],
                    "objectExtensionFieldsToApply": []
                },
                "unitOfWorkId": rail.render_template("{{ dag_run_ecid() }}PM")
            }
        )

        create_project_or_apply_modifications=rail.RepliconServiceOperator(
            task_id='create_project_or_apply_modifications',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data={
                "target": {
                    "uri": "{{ result('create_project').uri}}",
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "nameToApply": null,
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "percentCompletedToApply": "0",
                    "startDateToApply": null,
                    "billingTypeToApply": null,
                    "clientBillingAllocationMethodToApply": null,
                    "clientAssignmentsSchedulesToApply": null,
                    "statusToApply": {
                    "uri": null,
                    "name": "{{ result('get_project_status') }}"
                    },
                    "clientRepresentativeToApply": null,
                    "healthStateToApply": null,
                    "programToApply": null,
                    "projectLeaderToApply": null,
                    "isProjectLeaderApprovalRequired": "true",
                    "costTypeToApply": null,
                    "isTimeEntryAllowed": "false",
                    "estimatedHoursToApply": null,
                    "estimatedCostToApply": null,
                    "defaultBillingCurrencyToApply": null,
                    "timeAndMaterials": null,
                    "fixedBid": null,
                    "customFieldsToApply": [],
                    "resourceAssignmentModifications": null,
                    "keyValuesToApply": [],
                    "objectExtensionFieldsToApply": []
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}status"
            }
        )

        if_expensecodes_present=rail.IfOperator(
            task_id='if_expensecodes_present',
            test=lambda: len(rail.result('get_expense_codes')) > 0,
            yes_task="foreach_expense_code",
            no_task="insert_log_project_created",
        )

        foreach_expense_code=rail.ForEachOperator(
            task_id='foreach_expense_code',
            items=lambda: rail.result('get_expense_codes'),
            start_task = 'update_expense_code_allowing_expense_entry',
            end_task = 'foreach_expense_code_end'
        )

        update_expense_code_allowing_expense_entry=rail.RepliconServiceOperator(
            task_id='update_expense_code_allowing_expense_entry',
            endpoint="/services/ProjectService1.svc/UpdateExpenseCodeAllowingExpenseEntry",
            data={
                "projectUri": "{{ result('create_project').uri }}",
                "expenseCodeUri": "{{ result('foreach_expense_code').uri }}",
                "allowed": "true"
            }
        )

        foreach_expense_code_end=rail.EmptyOperator(
            task_id='foreach_expense_code_end',
        )

        insert_log_project_created=rail.SetVariableOperator(
            task_id='insert_log_project_created',
            append=True,
            name='{{ result("create_logs_list_variable").name }}',
            value={
                "status": "Success",
                "details": "Project created"
            }
        )

        if_billingtype_equals_fixedbid=rail.IfOperator(
            task_id='if_billingtype_equals_fixedbid',
            test='''{{ result('bulk_get_project_details')[0].projectDetails.billingType.displayText == 'Fixed Bid' }}''',
            yes_task="add_log_this_is_fixedbid_project",
            no_task="if_projectstatus_unequal_current",
        )

        add_log_this_is_fixedbid_project=rail.WriteLogOperator(
            task_id='add_log_this_is_fixedbid_project',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Exception",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "type": "project",
                "client": "{{ dag_run.conf.clientname }}",
                "project": "{{ dag_run.conf.projectname }}",
                "code": "{{ dag_run.conf.projectcode }}",
                "task": "{{ dag_run.conf.taskname }}",
                "status": "Exception",
                "reason": "This is a Fixed Bid Project",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_projectstatus_unequal_current=rail.IfOperator(
            task_id='if_projectstatus_unequal_current',
            test='''{{ result('bulk_get_project_details')[0].projectDetails.status.name != result('get_project_status') }}''',
            yes_task="update_status",
            no_task="if_projectmanager_present_but_unequal_current",
        )

        update_status=rail.RepliconServiceOperator(
            task_id='update_status',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda:{
                "target": {
                    "uri": rail.get_dag_run_var('projecturi'),
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "nameToApply": null,
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "percentCompletedToApply": null,
                    "startDateToApply": null,
                    "endDateToApply": null,
                    "billingTypeToApply": null,
                    "clientBillingAllocationMethodToApply": null,
                    "clientAssignmentsSchedulesToApply": null,
                    "statusToApply": {
                    "uri": null,
                    "name": rail.result('get_project_status')
                    },
                    "clientRepresentativeToApply": null,
                    "healthStateToApply": null,
                    "programToApply": null,
                    "projectLeaderToApply": null,
                    "isProjectLeaderApprovalRequired": "true",
                    "costTypeToApply": null,
                    "isTimeEntryAllowed": "false",
                    "estimatedHoursToApply": null,
                    "estimatedCostToApply": null,
                    "defaultBillingCurrencyToApply": null,
                    "timeAndMaterials": null,
                    "fixedBid": null,
                    "customFieldsToApply": [],
                    "resourceAssignmentModifications": null,
                    "keyValuesToApply": [],
                    "objectExtensionFieldsToApply": []
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}_status"
            }
        )

        if_projectmanager_present_but_unequal_current=rail.IfOperator(
            task_id='if_projectmanager_present_but_unequal_current',
            test=lambda: rail.get_dag_run_var('projectmanager') and 'urn' in rail.get_dag_run_var('projectmanager') and
                    (not(rail.result('bulk_get_project_details')[0]['projectDetails']['projectLeader']) or
                    rail.result('bulk_get_project_details')[0]['projectDetails']['projectLeader']['uri'] != rail.get_dag_run_var('projectmanager')),
            yes_task="update_project_manager",
            no_task="if_projectstart_present_but_unequal_current",
        )

        update_project_manager=rail.RepliconServiceOperator(
            task_id='update_project_manager',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda:{
                "target": {
                    "uri": rail.get_dag_run_var('projecturi'),
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "nameToApply": null,
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "percentCompletedToApply": null,
                    "startDateToApply": null,
                    "endDateToApply": null,
                    "billingTypeToApply": null,
                    "clientBillingAllocationMethodToApply": null,
                    "clientAssignmentsSchedulesToApply": null,
                    "statusToApply": null,
                    "clientRepresentativeToApply": null,
                    "healthStateToApply": null,
                    "programToApply": null,
                    "projectLeaderToApply": {
                    "user": {
                        "uri": rail.get_dag_run_var('projectmanager'),
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                    },
                    "isProjectLeaderApprovalRequired": "true",
                    "costTypeToApply": null,
                    "isTimeEntryAllowed": null,
                    "estimatedHoursToApply": null,
                    "estimatedCostToApply": null,
                    "defaultBillingCurrencyToApply": null,
                    "timeAndMaterials": null,
                    "fixedBid": null,
                    "customFieldsToApply": [],
                    "resourceAssignmentModifications": null,
                    "keyValuesToApply": [],
                    "objectExtensionFieldsToApply": []
                },
                "unitOfWorkId": rail.render_template("{{ dag_run_ecid() }}PM")
            }
        )

        if_projectstart_present_but_unequal_current=rail.IfOperator(
            task_id='if_projectstart_present_but_unequal_current',
            test=lambda dag_run: dag_run.conf['projectstart'] and rail.result('get_project_current_start_end_date')['start_date'] and
                    datetime.strptime(dag_run.conf['projectstart'],'%Y/%m/%d') != datetime.strptime(
                    rail.result('get_project_current_start_end_date')['start_date'],"%Y/%m/%d"),
            yes_task="update_start_date",
            no_task="if_projectend_present_but_unequal_current",
        )

        update_start_date=rail.RepliconServiceOperator(
            task_id='update_start_date',
            endpoint="/services/ProjectService1.svc/UpdateTimeEntryDateRange",
            data=lambda:{
                "projectUri": rail.get_dag_run_var('projecturi'),
                "dateRange": {
                    "startDate": {
                        "year": rail.result('get_projectstart_date_object')['year'],
                        "month": rail.result('get_projectstart_date_object')['month'],
                        "day": rail.result('get_projectstart_date_object')['day']
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_projectend_present_but_unequal_current=rail.IfOperator(
            task_id='if_projectend_present_but_unequal_current',
            test=lambda dag_run: dag_run.conf['projectend'] and rail.result('get_project_current_start_end_date')['end_date'] and
                    datetime.strptime(dag_run.conf['projectend'],'%Y/%m/%d') != datetime.strptime(rail.result(
                    'get_project_current_start_end_date')['end_date'],"%Y/%m/%d"),
            yes_task="update_start_and_end_date",
            no_task="if_projectname_present_and_unequal_current",
        )

        update_start_and_end_date=rail.RepliconServiceOperator(
            task_id='update_start_and_end_date',
            endpoint="/services/ProjectService1.svc/UpdateTimeEntryDateRange",
            data=lambda:{
                "projectUri": rail.get_dag_run_var('projecturi'),
                "dateRange": {
                    "startDate": {
                        "year": rail.result('get_projectstart_date_object')['year'],
                        "month": rail.result('get_projectstart_date_object')['month'],
                        "day": rail.result('get_projectstart_date_object')['day']
                    },
                    "endDate": {
                        "year": rail.result('get_projectend_date_object')['year'],
                        "month": rail.result('get_projectend_date_object')['month'],
                        "day": rail.result('get_projectend_date_object')['day']
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_projectname_present_and_unequal_current=rail.IfOperator(
            task_id='if_projectname_present_and_unequal_current',
            test='''{{ dag_run.conf.projectname | is_truthy  and dag_run.conf.projectname != result('bulk_get_project_details')[0].projectDetails.name }}''',
            yes_task="update_project_name",
            no_task="if_clientname_present_and_unequal_current",
        )

        update_project_name=rail.RepliconServiceOperator(
            task_id='update_project_name',
            endpoint="/services/ProjectService1.svc/UpdateName",
            data=lambda dag_run:{
                "projectUri": rail.get_dag_run_var('projecturi'),
                "name": dag_run.conf['projectname']
            }
        )

        if_clientname_present_and_unequal_current=rail.IfOperator(
            task_id='if_clientname_present_and_unequal_current',
            test=lambda dag_run: bool(dag_run.conf['clientname'] and rail.result('bulk_get_project_details')[0] and
                    rail.result('bulk_get_project_details')[0]['projectDetails']['clients'] and
                    rail.result('bulk_get_project_details')[0]['projectDetails']['clients'][0]['client']['name'] and
                    dag_run.conf['clientname'] != rail.result('bulk_get_project_details')[0]['projectDetails']['clients'][0]['client']['name'] and
                    rail.result('search_client')),
            yes_task="update_project_client",
            no_task="if_expense_codes_present",
        )

        update_project_client=rail.RepliconServiceOperator(
            task_id='update_project_client',
            endpoint="/services/ProjectService1.svc/UpdateClients",
            data=lambda :{
                "projectUri": rail.get_dag_run_var('projecturi'),
                "clients": [
                    {
                    "client": {
                        "uri": rail.result('search_client')['cells'][1]['uri'],
                        "name": null,
                        "code": null,
                        "parameterCorrelationId": null
                    },
                    "costAllocationPercentage": "100"
                    }
                ]
            }
        )

        if_expense_codes_present=rail.IfOperator(
            task_id='if_expense_codes_present',
            test=lambda: len(rail.result('get_expense_codes')) > 0,
            yes_task="foreach_expensecode",
            no_task="insert_project_update_log",
        )

        foreach_expensecode=rail.ForEachOperator(
            task_id='foreach_expensecode',
            items=lambda:rail.result('get_expense_codes'),
            start_task = 'update_expensecode_allowing_expense_entry',
            end_task = 'foreach_expensecode_end'
        )

        update_expensecode_allowing_expense_entry=rail.RepliconServiceOperator(
            task_id='update_expensecode_allowing_expense_entry',
            endpoint="/services/ProjectService1.svc/UpdateExpenseCodeAllowingExpenseEntry",
            data=lambda: {
                "projectUri": rail.get_dag_run_var('projecturi'),
                "expenseCodeUri": rail.result('foreach_expensecode')['uri'],
                "allowed": "true"
            }
        )

        foreach_expensecode_end=rail.EmptyOperator(
            task_id='foreach_expensecode_end',
        )

        insert_project_update_log=rail.SetVariableOperator(
            task_id='insert_project_update_log',
            append=True,
            name='{{ result("create_logs_list_variable").name }}',
            value=lambda dag_run:{
                "status": ( "Exception" if len(rail.result('search_projectmanager_user')) < 1 else ("Exception" if
                    len(rail.result('search_projectmanager_user')) > 1 else "Success") ) if dag_run.conf['projectmanager'] else 'Success',
                "details": ( "Project updated without project manager as multiple users were found" if
                    len(rail.result('search_projectmanager_user')) > 1 else ("Project updated without project manager as no user found" if
                    len(rail.result('search_projectmanager_user')) < 1 else "Project updated successfully") ) if
                    dag_run.conf['projectmanager'] else 'Project updated successfully'
            }
        )

        def get_companycode_list(response,dag_run):
            print(response)
            companycode = dag_run.conf['companycode'].lower() if dag_run.conf['companycode'] else ''
            companycodelist = [{
                'name': code['cells'][0]['textValue'],
                'code': code['cells'][1]['textValue'].lower() if code['cells'][1]['textValue'] else code['cells'][1]['textValue']
            } for code in response['rows']]
            return list(filter(lambda code: code['code'] == companycode,companycodelist))

        get_divisions=rail.RepliconServiceOperator(
            task_id='get_divisions',
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:division-list-column:name",
                    "urn:replicon:division-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_companycode_list
        )

        if_companycode_present=rail.IfOperator(
            task_id='if_companycode_present',
            test='''{{ (result('get_divisions') and result('get_divisions')[0] and result('get_divisions')[0].name) | is_truthy }}''',
            yes_task="update_company_code",
            no_task="if_userid_present",
        )

        update_company_code=rail.RepliconServiceOperator(
            task_id='update_company_code',
            endpoint="/services/ProjectService1.svc/UpdateDivision2",
            data=lambda:{
                "projectUri": rail.get_dag_run_var('projecturi'),
                "division": {
                    "uri": null,
                    "parentUri": null,
                    "name": rail.result('get_divisions')[0]['name']
                }
            }
        )

        if_userid_present=rail.IfOperator(
            task_id='if_userid_present',
            test='''{{ dag_run.conf.userid | is_truthy }}''',
            yes_task="create_resources_to_assign_list",
            no_task="if_taskcode_present",
        )

        create_resources_to_assign_list=rail.SetVariableOperator(
            task_id='create_resources_to_assign_list',
            append=False,
            name='resourcestoassign',
            value=[]
        )

        foreach_user_in_list=rail.ForEachOperator(
            task_id='foreach_user_in_list',
            items=lambda dag_run: dag_run.conf['userid'].split(',') if dag_run.conf['userid'] else [],
            start_task = 'search_resource_user',
            end_task = 'foreach_user_in_list_end'
        )

        def get_user_by_employeeid(response):
            resourceuser = {}
            for user in response['rows']:
                if user['cells'][3]['textValue'] == rail.result('foreach_user_in_list').strip() and user['cells'][2]['textValue'] == 'True':
                    resourceuser = user
                    break
            return resourceuser if resourceuser else False

        search_resource_user=rail.RepliconServiceOperator(
            task_id='search_resource_user',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda:{
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled",
                    "urn:replicon:user-list-column:employee-id"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": rail.result('foreach_user_in_list').strip()
                        }
                    }
                }
            },
            data_handler=get_user_by_employeeid
        )

        if_resource_user_found_and_enabled=rail.IfOperator(
            task_id='if_resource_user_found_and_enabled',
            test=lambda: bool(rail.result('search_resource_user')),
            yes_task="insert_to_resourcetoassign_list",
            no_task="insert_log_user_not_present_or_disabled",
        )

        insert_to_resourcetoassign_list=rail.SetVariableOperator(
            task_id='insert_to_resourcetoassign_list',
            append=True,
            name='{{ result("create_resources_to_assign_list").name }}',
            value=lambda:{
                "uri": rail.result('search_resource_user')['cells'][0]['uri']
            }
        )

        insert_log_user_not_present_or_disabled=rail.SetVariableOperator(
            task_id='insert_log_user_not_present_or_disabled',
            append=True,
            name='{{ result("create_logs_list_variable").name }}',
            value=lambda: {
                "status": "Exception",
                "details": rail.result('foreach_user_in_list').strip() + ' is either disabled or not present'
            }
        )

        foreach_user_in_list_end=rail.EmptyOperator(
            task_id='foreach_user_in_list_end',
        )

        log_resources_to_assign=rail.PythonOperator(
            task_id='log_resources_to_assign',
            python_callable= lambda: [resource['uri'] for resource in rail.get_dag_run_var('resourcestoassign')]
        )

        bulk_update_project_team_members_assignment=rail.RepliconServiceOperator(
            task_id='bulk_update_project_team_members_assignment',
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment2",
            data=lambda:{
                "projectUri": rail.get_dag_run_var('projecturi'),
                "userUris": rail.result('log_resources_to_assign'),
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            }
        )

        if_taskcode_present=rail.IfOperator(
            task_id='if_taskcode_present',
            test='''{{ dag_run.conf.taskcode | is_truthy }}''',
            yes_task="get_all_project_tasks",
            no_task="add_final_logs_for_the_project",
        )

        get_all_project_tasks=rail.RepliconServiceOperator(
            task_id='get_all_project_tasks',
            endpoint='/services/ProjectService1.svc/BulkGetTaskDetails',
            data=lambda:{
                "pageIndex": "1",
                "pageSize": "10000",
                "projectUris": [
                    rail.get_dag_run_var('projecturi')
                ]
            }
        )

        check_task_present_by_code=rail.PythonOperator(
            task_id='check_task_present_by_code',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_project_tasks'),'code',dag_run.conf['taskcode'],'uri','')
        )

        if_task_present=rail.IfOperator(
            task_id='if_task_present',
            test='''{{ result('check_task_present_by_code') | is_truthy }}''',
            yes_task="trigger_child_update_task",
            no_task="if_task_name_not_present",
        )

        trigger_child_update_task=rail.TriggerDagRunOperator(
            task_id='trigger_child_update_task',
            retries=0,
            trigger_dag_id=config.update_task_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run:{
                "taskname": dag_run.conf['taskname'],
                "taskcode": dag_run.conf['taskcode'],
                "taskdescription": dag_run.conf['taskdescription'],
                "taskstartdate": dag_run.conf['taskstartdate'],
                "taskenddate": dag_run.conf['taskenddate'],
                "estimatedefforthours": dag_run.conf['estimatedefforthours'],
                "companycode": dag_run.conf['companycode'],
                "projectname": dag_run.conf['projectname'],
                "projecturi": rail.get_dag_run_var('projecturi'),
                "resourceuris": rail.result('log_resources_to_assign'),
                "taskuri": rail.result('check_task_present_by_code'),
                "parentjob": dag_run.conf['callerjobid']
            }
        )

        wait_for_child_update_task = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_update_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_update_task") }}'
        )

        if_task_name_not_present=rail.IfOperator(
            task_id='if_task_name_not_present',
            test=lambda dag_run: not bool(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_project_tasks'),'name',(dag_run.conf['taskname'] + '-' + dag_run.conf['taskcode'],'uri',''))),
            yes_task="trigger_child_add_task",
            no_task="insert_log_task_with_same_name_already_present",
        )

        trigger_child_add_task=rail.TriggerDagRunOperator(
            task_id='trigger_child_add_task',
            trigger_dag_id=config.add_task_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run:{
                "taskname": dag_run.conf['taskname'],
                "taskcode": dag_run.conf['taskcode'],
                "taskdescription": dag_run.conf['taskdescription'],
                "taskstartdate": dag_run.conf['taskstartdate'],
                "taskenddate": dag_run.conf['taskenddate'],
                "estimatedefforthours": dag_run.conf['estimatedefforthours'],
                "companycode": dag_run.conf['companycode'],
                "projectname": dag_run.conf['projectname'],
                "resourceuris": rail.result('log_resources_to_assign'),
                "projecturi": rail.get_dag_run_var('projecturi'),
                "parentjob": dag_run.conf['callerjobid']
            }
        )

        wait_for_child_add_task = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_add_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_add_task") }}'
        )

        insert_log_task_with_same_name_already_present=rail.SetVariableOperator(
            task_id='insert_log_task_with_same_name_already_present',
            append=True,
            name='{{ result("create_logs_list_variable").name }}',
            value={
                "status": "Exception",
                "details": "The task with same name is already present."
            }
        )

        add_final_logs_for_the_project=rail.WriteLogOperator(
            task_id='add_final_logs_for_the_project',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity=lambda: "Exception" if rail.find_first_by_attr_and_get_attr(
                rail.get_dag_run_var('logs'),'status','Exception','details','') else "Success",
            properties=lambda dag_run:{
                "jobid": dag_run.conf['callerjobid'],
                "type": "project",
                "client": dag_run.conf['clientname'],
                "project": dag_run.conf['projectname'],
                "code": dag_run.conf['projectcode'],
                "task": dag_run.conf['taskname'],
                "status": "Exception" if rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var('logs'),'status','Exception','details','') else "Success",
                "reason": ','.join([log['details'] for log in rail.get_dag_run_var('logs')]) if rail.get_dag_run_var('logs') else null,
                "childjobid": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        add_log_project_name_or_code_not_present=rail.WriteLogOperator(
            task_id='add_log_project_name_or_code_not_present',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Exception",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "type": "project",
                "client": "{{ dag_run.conf.clientname }}",
                "project": "{{ dag_run.conf.projectname }}",
                "code": "{{ dag_run.conf.projectcode }}",
                "task": "{{ dag_run.conf.taskname }}",
                "status": "Exception",
                "reason": "Project Name/Code is not present",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        catch_and_log_error=rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.logslookuptable}}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "type": "project",
                "client": "{{ dag_run.conf.clientname }}",
                "project": "{{ dag_run.conf.projectname }}",
                "code": "{{ dag_run.conf.projectcode }}",
                "task": "{{ dag_run.conf.taskname }}",
                "status": "Error",
                "reason": "{{get_error_message()}}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> get_projectstatus_mapper_entries
        get_projectstatus_mapper_entries >> get_project_status >> create_logs_list_variable >> if_projectstatus_not_defined_in_mapper
        if_projectstatus_not_defined_in_mapper >> rail.Label(
            'Yes')  >> insert_log_status_not_defined_in_mapper >> add_log_status_not_defined_in_mapper >> log_to_sumo
        if_projectstatus_not_defined_in_mapper >> rail.Label('No') >> if_projectname_and_projectcode_present
        if_projectname_and_projectcode_present >> rail.Label('Yes')  >> get_expense_codes >> create_variable_projectmanager >> if_projectname_present
        if_projectname_present >> rail.Label('Yes')  >> search_projectmanager_user >> if_projectmanager_present_and_enabled
        if_projectmanager_present_and_enabled >> rail.Label(
            'Yes') >> get_assigned_permissions >> check_project_management_permission >> if_project_management_permission_assigned
        if_project_management_permission_assigned >> rail.Label('Yes') >> update_projectmanager_variable >> if_project_start_present
        if_project_management_permission_assigned >> rail.Label('No') >> if_project_start_present
        if_projectmanager_present_and_enabled >> rail.Label('No') >> if_project_start_present
        if_projectname_present >> rail.Label('No') >> if_project_start_present
        if_project_start_present >> rail.Label('Yes')  >> get_projectstart_date_object >> if_project_end_present
        if_project_start_present >> rail.Label('No') >> if_project_end_present
        if_project_end_present >> rail.Label('Yes')  >> get_projectend_date_object >> if_clientcode_present
        if_project_end_present >> rail.Label('No') >> if_clientcode_present
        if_clientcode_present >> rail.Label('Yes')  >> search_client >> bulk_get_project_details
        if_clientcode_present >> rail.Label(
            'No') >> bulk_get_project_details >> get_project_current_start_end_date >> create_variable_projecturi >> if_projecturi_not_present
        if_projecturi_not_present >> rail.Label('Yes')  >> create_project >> update_projecturi_variable >> if_projectmanager_present
        if_projectmanager_present >> rail.Label('Yes')  >> update_manager >> create_project_or_apply_modifications
        if_projectmanager_present >> rail.Label('No') >> create_project_or_apply_modifications >> if_expensecodes_present
        if_expensecodes_present >> rail.Label('Yes')  >> foreach_expense_code >> update_expense_code_allowing_expense_entry >> foreach_expense_code_end
        foreach_expense_code >> foreach_expense_code_end >> insert_log_project_created
        if_expensecodes_present >> rail.Label('No') >> insert_log_project_created >> get_divisions
        if_projecturi_not_present >> rail.Label('No') >> if_billingtype_equals_fixedbid
        if_billingtype_equals_fixedbid >> rail.Label('Yes')  >> add_log_this_is_fixedbid_project >> log_to_sumo
        if_billingtype_equals_fixedbid >> rail.Label('No') >> if_projectstatus_unequal_current
        if_projectstatus_unequal_current >> rail.Label('Yes')  >> update_status >> if_projectmanager_present_but_unequal_current
        if_projectstatus_unequal_current >> rail.Label('No') >> if_projectmanager_present_but_unequal_current
        if_projectmanager_present_but_unequal_current >> rail.Label('Yes')  >> update_project_manager >> if_projectstart_present_but_unequal_current
        if_projectmanager_present_but_unequal_current >> rail.Label('No') >> if_projectstart_present_but_unequal_current
        if_projectstart_present_but_unequal_current >> rail.Label('Yes')  >> update_start_date >> if_projectend_present_but_unequal_current
        if_projectstart_present_but_unequal_current >> rail.Label('No') >> if_projectend_present_but_unequal_current
        if_projectend_present_but_unequal_current >> rail.Label('Yes')  >> update_start_and_end_date >> if_projectname_present_and_unequal_current
        if_projectend_present_but_unequal_current >> rail.Label('No') >> if_projectname_present_and_unequal_current
        if_projectname_present_and_unequal_current >> rail.Label('Yes')  >> update_project_name >> if_clientname_present_and_unequal_current
        if_projectname_present_and_unequal_current >> rail.Label('No') >> if_clientname_present_and_unequal_current
        if_clientname_present_and_unequal_current >> rail.Label('Yes')  >> update_project_client >> if_expense_codes_present
        if_clientname_present_and_unequal_current >> rail.Label('No') >> if_expense_codes_present
        if_expense_codes_present >> rail.Label('Yes')  >> foreach_expensecode >> update_expensecode_allowing_expense_entry >> foreach_expensecode_end
        foreach_expensecode >> foreach_expensecode_end >> insert_project_update_log
        if_expense_codes_present >> rail.Label('No') >> insert_project_update_log >> get_divisions >> if_companycode_present
        if_companycode_present >> rail.Label('Yes')  >> update_company_code >> if_userid_present
        if_companycode_present >> rail.Label('No') >> if_userid_present
        if_userid_present >> rail.Label(
            'Yes') >> create_resources_to_assign_list >> foreach_user_in_list >> search_resource_user >> if_resource_user_found_and_enabled
        if_resource_user_found_and_enabled >> rail.Label('Yes')  >> insert_to_resourcetoassign_list >> foreach_user_in_list_end
        if_resource_user_found_and_enabled >> rail.Label('No') >> insert_log_user_not_present_or_disabled >> foreach_user_in_list_end
        foreach_user_in_list >> foreach_user_in_list_end >> log_resources_to_assign >> bulk_update_project_team_members_assignment >> if_taskcode_present
        if_userid_present >> rail.Label('No') >> if_taskcode_present
        if_taskcode_present >> rail.Label('Yes')  >> get_all_project_tasks >> check_task_present_by_code >> if_task_present
        if_task_present >> rail.Label('Yes')  >> trigger_child_update_task >> wait_for_child_update_task >> add_final_logs_for_the_project
        if_task_present >> rail.Label('No') >> if_task_name_not_present
        if_task_name_not_present >> rail.Label('Yes')  >> trigger_child_add_task >> wait_for_child_add_task >> add_final_logs_for_the_project
        if_task_name_not_present >> rail.Label('No') >> insert_log_task_with_same_name_already_present >> add_final_logs_for_the_project
        if_taskcode_present >> rail.Label('No') >> add_final_logs_for_the_project >> catch_and_log_error
        if_projectname_and_projectcode_present >> rail.Label('No') >> add_log_project_name_or_code_not_present >> catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
