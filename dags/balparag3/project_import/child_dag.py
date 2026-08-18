from datetime import datetime, timedelta
import re
import uuid
import itertools
from airflow.models import Variable
import pendulum
import rail
from rail import get_current_context


null = None


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/balparag3/project_import/config.py


# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'balparag3_projectimport_child_{config.instance}',
        description=f'Balparag3 projectimport Child V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_project_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_exception_skipped_records'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_exception_skipped_records',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def is_alphabet(string):
            return re.search('[a-zA-Z]', string)

        def get_exception_skipped_records_message():
            dag_run_conf = get_current_context()['dag_run'].conf

            def get_exception_records(dag_run_conf):
                exception_records = []

                def get_number(val):
                    number = null
                    if val.isdigit():
                        return int(val)
                    try:
                        number = float(val)
                    except ValueError:
                        number = null
                    return number
                estimatedhours = dag_run_conf['estimatedhours'].strip().replace(
                    ",", "").replace(".", "")
                estimatedcost = dag_run_conf['estimatedcost'].strip().replace(
                    ",", "").replace(".", "")
                if estimatedhours and not get_number(estimatedhours):
                    exception_records.append(
                        'Estimated hours contains non numerical values')
                if estimatedcost and not get_number(estimatedcost):
                    exception_records.append(
                        'Estimated cost contains non numerical values')
                if dag_run_conf['projectname'] and not is_alphabet(dag_run_conf['projectname']):
                    exception_records.append('Project name has no alphabets')
                if dag_run_conf['clientname'] and not is_alphabet(dag_run_conf['clientname']):
                    exception_records.append('Client name has no alphabets')
                if dag_run_conf['clientcontact'] and not is_alphabet(dag_run_conf['clientcontact']):
                    exception_records.append('Client contact has no alphabets')
                if dag_run_conf['projectmanager'] and not is_alphabet(dag_run_conf['projectmanager']):
                    exception_records.append(
                        'Project manager (Default) has no alphabets')
                if dag_run_conf['projectmanagers'] and not is_alphabet(dag_run_conf['projectmanagers']):
                    exception_records.append(
                        'Project managers (UDF) has no alphabets')
                if dag_run_conf['invoiceclientcontact'] and not is_alphabet(dag_run_conf['invoiceclientcontact']):
                    exception_records.append(
                        'Invoice client contact has no alphabets')
                if dag_run_conf['invoicebalparacontact'] and not is_alphabet(dag_run_conf['invoicebalparacontact']):
                    exception_records.append(
                        'Invoice balpara contact has no alphabets')
                if dag_run_conf['location'] and not is_alphabet(dag_run_conf['location']):
                    exception_records.append('Location has no alphabets')
                return exception_records
            exception_records = get_exception_records(dag_run_conf)

            def get_skipped_records(dag_run_conf):
                skipped_records = []
                department = dag_run_conf['department']
                users = dag_run_conf['users']
                billingrates = dag_run_conf['billingrates']
                for each_dept in department.split(';'):
                    if each_dept and not is_alphabet(each_dept):
                        skipped_records.append({
                            'skip': 'Yes',
                            'type': 'Department',
                            'value': each_dept,
                            'reason': 'Department has no alphabets'
                        })
                for each_user in users.split(';'):
                    if each_user and not is_alphabet(each_user):
                        skipped_records.append({
                            'skip': 'Yes',
                            'type': 'User',
                            'value': each_user,
                            'reason': 'Users has no alphabets'
                        })
                for each_billingrate in billingrates.split(';'):
                    if each_billingrate and not is_alphabet(each_billingrate):
                        skipped_records.append({
                            'skip': 'Yes',
                            'type': 'Billing rate',
                            'value': each_billingrate,
                            'reason': 'Billing rates has no alphabets'
                        })
                return skipped_records
            skipped_records = get_skipped_records(dag_run_conf)
            return {
                'exception_message': ','.join(exception_records) if exception_records else '',
                'skipped_message': ','.join([set(x['reason'] for x in skipped_records)]) if skipped_records else '',
            }
        get_exception_skipped_records = rail.PythonOperator(
            task_id='get_exception_skipped_records',
            python_callable=get_exception_skipped_records_message
        )

        is_exception_message = rail.IfOperator(
            task_id='is_exception_message',
            test="{{ result('get_exception_skipped_records').exception_message | is_truthy }}",
            yes_task='write_exception_log',
            no_task='is_skipped_message'
        )

        write_exception_log = rail.WriteLogOperator(
            task_id='write_exception_log',
            log='{{ dag_run.conf.log }}',
            message='Exception Message',
            severity='Exception',
            properties={
                'project_code': '{{ dag_run.conf.projectcode }}',
                'project_name': '{{ dag_run.conf.projectname }}',
                'status': 'Exception',
                'details': "{{ result('get_exception_skipped_records').exception_message }}",
                'type': 'project'
            }
        )

        is_skipped_message = rail.IfOperator(
            task_id='is_skipped_message',
            test="{{ result('get_exception_skipped_records').skipped_message | is_truthy }}",
            yes_task='write_skipped_log',
            no_task='bulk_get_project_from_name'
        )

        write_skipped_log = rail.WriteLogOperator(
            task_id='write_skipped_log',
            log='{{ dag_run.conf.log }}',
            message='Skipped Message',
            severity='Exception',
            properties={
                'project_code': '{{ dag_run.conf.projectcode }}',
                'project_name': '{{ dag_run.conf.projectname }}',
                'status': 'Exception',
                'details': "{{ result('get_exception_skipped_records').skipped_message }}",
                'type': 'project'
            }
        )

        bulk_get_project_from_name = rail.RepliconServiceOperator(
            task_id='bulk_get_project_from_name',
            endpoint="/services/ProjectService1.svc/BulkGetProjects2",
            data={
                "projects": [
                    {
                        "name": "{{ dag_run.conf.projectname }}"
                    }
                ]
            },
            data_handler=lambda response, dag_run: {
                'uri': rail.find_first_by_attr_and_get_attr(
                    response, 'name', dag_run.conf['projectname'], 'uri', '') if response else '',
                'name': response[0]['name'] if response else '',
                'code': response[0]['code'] if response else ''
            }
        )

        is_projecturi_not_present = rail.IfOperator(
            task_id='is_projecturi_not_present',
            test="{{ result('bulk_get_project_from_name').uri | is_falsy }}",
            yes_task="bulk_get_project_from_code",
            no_task="project_add_name_exception"
        )

        bulk_get_project_from_code = rail.RepliconServiceOperator(
            task_id='bulk_get_project_from_code',
            endpoint="/services/ProjectService1.svc/BulkGetProjects2",
            data={
                "projects": [
                    {
                        "code": "{{ dag_run.conf.projectcode }}"
                    }
                ]
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'code', dag_run.conf['projectcode'], 'uri', '') if response else ''
        )

        is_projecturi_not_present2 = rail.IfOperator(
            task_id='is_projecturi_not_present2',
            test="{{ result('bulk_get_project_from_code') | is_falsy }}",
            yes_task="create_project_or_apply_modifications",
            no_task="project_add_code_exception"
        )

        def create_project_payload(dag_run):
            estimated_hours = float(
                dag_run.conf['estimatedhours'].strip().replace(",", ""))
            estimated_cost = float(
                dag_run.conf['estimatedcost'].strip().replace(",", ""))
            return {
                "modifications": {
                    "nameToApply": {
                        "value": dag_run.conf['projectname']
                    },
                    "codeToApply": {
                        "value": dag_run.conf['projectcode']
                    },
                    "billingTypeToApply": {
                        "value": "urn:replicon:billing-type:time-and-material"
                    },
                    "clientAssignmentsSchedulesToApply": {
                        "clients": [
                            {
                                "client": {
                                    "name": dag_run.conf['clientname']
                                },
                                "costAllocationPercentage": "100"
                            }
                        ]
                    },
                    "statusToApply": {
                        "name": "In Progress"
                    },
                    "estimatedHoursToApply": {
                        "duration": {
                            "hours": int(estimated_hours),
                            "minutes": int((estimated_hours*60) % 60),
                            "seconds": int((estimated_hours*60*60) % 60)
                        }
                    },
                    "estimatedCostToApply": {
                        "value": {
                            "amount": estimated_cost,
                            "currency": {
                                "symbol":  dag_run.conf['defaultcurrencysymbol']
                            }
                        }
                    },
                    "timeAndMaterials": {
                        "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable"
                    }
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        create_project_or_apply_modifications = rail.RepliconServiceOperator(
            task_id='create_project_or_apply_modifications',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=create_project_payload
        )

        remove_all_user_assignments = rail.RepliconServiceOperator(
            task_id='remove_all_user_assignments',
            endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
            data={
                "projectUri": "{{ result('create_project_or_apply_modifications').uri }}",
                "resourceUri": "urn:replicon-tenant:{{ get_tenant_slug() }}:department:1",
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:unassign"
            }
        )

        remove_start_and_end_date = rail.RepliconServiceOperator(
            task_id='remove_start_and_end_date',
            endpoint="/services/ProjectService1.svc/UpdateTimeEntryDateRange",
            data=lambda: {
                "projectUri": rail.result('create_project_or_apply_modifications')['uri'],
                "dateRange": {
                    "startDate": null,
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        project_add_code_exception = rail.WriteLogOperator(
            task_id='project_add_code_exception',
            log="{{ dag_run.conf.log }}",
            message="na",
            severity="Exception",
            properties={
                "project_code": "{{ dag_run.conf.projectcode }}",
                "project_name": "{{ dag_run.conf.projectname }}",
                "status": "Exception",
                # pylint: disable=line-too-long
                "details": "Project already available with code - {{ dag_run.conf.projectcode }} and different name \"{{ result('bulk_get_project_from_name').name }}\"",
                "type": "project"
            }
        )

        project_add_name_exception = rail.WriteLogOperator(
            task_id='project_add_name_exception',
            log="{{ dag_run.conf.log }}",
            message="na",
            severity="Skipped",
            properties={
                "project_code": "{{ dag_run.conf.projectcode }}",
                "project_name": "{{ dag_run.conf.projectname }}",
                "status": "Skipped",
                "details": "Project already available with name - {{ dag_run.conf.projectname }} and code- {{ result('bulk_get_project_from_name').code }} ",
                "type": "project"
            }
        )

        is_project_created = rail.IfOperator(
            task_id='is_project_created',
            test="{{ result('create_project_or_apply_modifications').uri | is_truthy }}",
            yes_task="is_invoicebalparacontact_present",
            no_task="catch_and_log_errors"
        )

        is_invoicebalparacontact_present = rail.IfOperator(
            task_id='is_invoicebalparacontact_present',
            test="{{ dag_run.conf.invoicebalparacontact | is_truthy }}",
            yes_task="is_invoicebalparacontacturi_present",
            no_task="search_project_manager"
        )

        is_invoicebalparacontacturi_present = rail.IfOperator(
            task_id='is_invoicebalparacontacturi_present',
            test="{{ dag_run.conf.invoicebalparacontact_oef_uri | is_truthy }}",
            yes_task="update_invoice_balpara_contact_oef",
            no_task="search_project_manager"
        )

        update_invoice_balpara_contact_oef = rail.RepliconServiceOperator(
            task_id='update_invoice_balpara_contact_oef',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data={
                "objectUri": "{{ result('create_project_or_apply_modifications').uri }}",
                "value": {
                    "definition": {
                        "uri": "{{ dag_run.conf.invoicebalparacontact_oef_uri }}"
                    },
                    "textValue": "{{ dag_run.conf.invoicebalparacontact }}"
                }
            }
        )

        def get_useruri_status(response, dag_run):
            user_uris = [item['cells'][0]['uri'] for item in response['rows']
                         if item['cells'][0].get('textValue') == dag_run.conf[
                'projectmanager']] if response['rows'] else []
            statuses = [item['cells'][1]['textValue'] for item in response['rows']
                        if item['cells'][0].get('textValue') == dag_run.conf[
                'projectmanager']] if response['rows'] else []
            return {
                'uri': rail.smartjoin_by_delim(user_uris) if user_uris else '',
                'status': rail.smartjoin_by_delim(statuses) if statuses else ''
            }
        search_project_manager = rail.RepliconServiceOperator(
            task_id='search_project_manager',
            endpoint='/services/UserListService1.svc/GetData',
            data={
                'page': '1',
                'pagesize': '100',
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:enabled'
                ],
                'sort': [],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': "{{ dag_run.conf.projectmanager | replace(',', ' ') }}"
                        }
                    }
                }
            },
            data_handler=get_useruri_status
        )

        is_projectmanager_present = rail.IfOperator(
            task_id='is_projectmanager_present',
            test="{{ result('search_project_manager').uri | is_truthy }}",
            yes_task="is_projectmanager_enabled",
            no_task="get_custom_fields_to_assign"
        )

        is_projectmanager_enabled = rail.IfOperator(
            task_id='is_projectmanager_enabled',
            test="{{ result('search_project_manager').status == 'True' }}",
            yes_task="get_assigned_projectmanager_permission",
            no_task="get_custom_fields_to_assign"
        )

        def is_projectmanager_permission(response):
            project_manager_permission = False
            if response:
                if rail.find_first_by_attr_and_get_attr(
                        response, 'policyUri', 'urn:replicon:policy:project-management', 'permissionSet'):
                    project_manager_permission = True
            return project_manager_permission
        get_assigned_projectmanager_permission = rail.RepliconServiceOperator(
            task_id='get_assigned_projectmanager_permission',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_project_manager').uri }}"
            },
            data_handler=is_projectmanager_permission
        )

        is_update_projectleader = rail.IfOperator(
            task_id='is_update_projectleader',
            test="{{ result('get_assigned_projectmanager_permission') | is_truthy }}",
            yes_task="project_leader_to_apply",
            no_task="get_custom_fields_to_assign"
        )

        project_leader_to_apply = rail.PythonOperator(
            task_id='project_leader_to_apply',
            python_callable=lambda: {
                "user": {
                    "uri": rail.result('search_project_manager')['uri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                }
            }
        )

        # pylint: disable=too-many-branches
        def get_customfields_assign():
            dag_run_conf = get_current_context()['dag_run'].conf
            custom_fields = []

            def get_replicon_datetime_obj(date_str, fmt='%d/%m/%Y'):
                datetime_obj = datetime.strptime(date_str, fmt)
                return {
                    'year': datetime_obj.year,
                    'month': datetime_obj.month,
                    'day': datetime_obj.day
                }
            if dag_run_conf['project_manager_udf_uri']:
                custom_fields.append({
                    'customField': {
                        'uri': dag_run_conf['project_manager_udf_uri']
                    },
                    'dropDownOption': {
                        'name': dag_run_conf['projectmanagers']
                    }
                })
            if dag_run_conf['actual_projectstartdate_udf_uri']:
                custom_fields.append({
                    'customField': {
                        'uri': dag_run_conf['actual_projectstartdate_udf_uri']
                    },
                    'date': get_replicon_datetime_obj(dag_run_conf['actualprojectstartdate'])
                })
            if dag_run_conf['invoiceclientcontact_udf_uri']:
                custom_fields.append({
                    'customField': {
                        'uri': dag_run_conf['invoiceclientcontact_udf_uri']
                    },
                    'text': dag_run_conf['invoiceclientcontact']
                })
            if dag_run_conf['clientproject'] and dag_run_conf['client_project_udf_uri']:
                custom_fields.append({
                    'customField': {
                        'uri': dag_run_conf['client_project_udf_uri']
                    },
                    'text': dag_run_conf['clientproject']
                })
            if dag_run_conf['comments'] and dag_run_conf['comments_udf_uri']:
                custom_fields.append({
                    'customField': {
                        'uri': dag_run_conf['comments_udf_uri']
                    },
                    'text': dag_run_conf['comments']
                })
            if dag_run_conf['workorder'] and dag_run_conf['workorder_udf_uri']:
                custom_fields.append({
                    'customField': {
                        'uri': dag_run_conf['workorder_udf_uri']
                    },
                    'text': dag_run_conf['workorder']
                })
            if dag_run_conf['po'] and dag_run_conf['po_udf_uri']:
                custom_fields.append({
                    'customField': {
                        'uri': dag_run_conf['po_udf_uri']
                    },
                    'text': dag_run_conf['po']
                })
            if dag_run_conf['pmo'] and dag_run_conf['pmo_udf_uri']:
                custom_fields.append({
                    'customField': {
                        'uri': dag_run_conf['pmo_udf_uri']
                    },
                    'text': dag_run_conf['pmo']
                })
            if dag_run_conf['invoiceclientcode'] and dag_run_conf['invoiceclientcode_udf_uri']:
                custom_fields.append({
                    'customField': {
                        'uri': dag_run_conf['invoiceclientcode_udf_uri']
                    },
                    'text': dag_run_conf['invoiceclientcode']
                })
            return custom_fields
        get_custom_fields_to_assign = rail.PythonOperator(
            task_id='get_custom_fields_to_assign',
            python_callable=get_customfields_assign
        )

        create_project_or_apply_modifications2 = rail.RepliconServiceOperator(
            task_id='create_project_or_apply_modifications2',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: {
                "target": {
                    "uri": rail.result('create_project_or_apply_modifications')['uri']
                },
                "modifications": {
                    "percentCompletedToApply": "0",
                    "projectLeaderToApply": rail.result('project_leader_to_apply'),
                    "isProjectLeaderApprovalRequired": "false",
                    "isTimeEntryAllowed": "true",
                    "defaultBillingCurrencyToApply": {
                        "currency": {
                            "symbol": dag_run.conf['defaultcurrencysymbol']
                        }
                    },
                    "customFieldsToApply": rail.result('get_custom_fields_to_assign')
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        put_task = rail.RepliconServiceOperator(
            task_id='put_task',
            endpoint="/services/ProjectService1.svc/PutTask",
            data={
                "project": {
                    "uri": "{{ result('create_project_or_apply_modifications').uri }}"
                },
                "task": {
                    "target": {
                        "name": "Dummy Task"
                    },
                    "name": "Dummy Task",
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "false",
                    "isClosed": "true"
                }
            }
        )

        is_location_present = rail.IfOperator(
            task_id='is_location_present',
            test="{{ dag_run.conf.location | is_truthy }}",
            yes_task="is_locationuri_present",
            no_task="get_departmentgroup_uri"
        )

        is_locationuri_present = rail.IfOperator(
            task_id='is_locationuri_present',
            test="{{ dag_run.conf.location_uri | is_truthy }}",
            yes_task="update_project_location",
            no_task="get_departmentgroup_uri"
        )

        update_project_location = rail.RepliconServiceOperator(
            task_id='update_project_location',
            endpoint="/services/ProjectService1.svc/UpdateLocation",
            data={
                "projectUri": "{{ result('create_project_or_apply_modifications').uri }}",
                "location": {
                    "uri": "{{ dag_run.conf.location_uri }}"
                }
            }
        )

        def get_departmentgroup_data_from_list(response, dag_run):
            department_data = []
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            department_list = dag_run.conf['department'].split(';')
            for item in department_list:
                for x in flatten_rows:
                    if x['cells'][0]['textValue'] == item:
                        department_data.append({
                            'uri': x['cells'][0]['uri'],
                            'name': item
                        })
            return department_data if department_data else ''
        get_departmentgroup_uri = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_departmentgroup_uri',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            items=lambda dag_run: dag_run.conf['department'].split(';'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True,
            data={
                'page': '1',
                'pagesize': '10',
                'columnUris': [
                    'urn:replicon:department-group-list-column:department-group'
                ],
                'sort': [],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:department-group-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': '{{ item }}'
                        }
                    }
                }
            },
            all_result_data_handler=get_departmentgroup_data_from_list
        )

        def get_user_data_from_list(response, dag_run):
            user_data = []
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            user_list = dag_run.conf['users'].split(';')
            for item in user_list:
                for x in flatten_rows:
                    if x['cells'][0]['textValue'] == item:
                        user_data.append({
                            'uri': x['cells'][0]['uri'],
                            'name': item
                        })
            return user_data if user_data else ''
        get_user_uri = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_user_uri',
            endpoint='/services/UserListService1.svc/GetData',
            items=lambda dag_run: dag_run.conf['users'].split(';'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True,
            data={
                'page': '1',
                'pagesize': '100',
                'columnUris': [
                    'urn:replicon:user-list-column:user'
                ],
                'sort': [],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': "{{ item | replace(',', ' ') }}"
                        }
                    }
                }
            },
            all_result_data_handler=get_user_data_from_list
        )

        def get_required_resources_to_assign():
            resource_list = []
            department_resource_uris = rail.result('get_departmentgroup_uri')
            user_uris = rail.result('get_user_uri')
            for department_resource_uri in department_resource_uris:
                if department_resource_uri:
                    resource_list.append({
                        'uri': department_resource_uri['uri'],
                        'name': department_resource_uri['name'],
                        'type': 'department'
                    })
            for user_uri in user_uris:
                if user_uri:
                    resource_list.append({
                        'uri': user_uri['uri'],
                        'name': user_uri['name'],
                        'type': 'user'
                    })
            return resource_list
        get_required_resources = rail.PythonOperator(
            task_id='get_required_resources',
            python_callable=get_required_resources_to_assign
        )

        is_resources_to_assign = rail.IfOperator(
            task_id='is_resources_to_assign',
            test="{{ result('get_required_resources') | length > 0 }}",
            yes_task="bulk_update_project_team_members",
            no_task="get_enabled_company_billingrates"
        )

        bulk_update_project_team_members = rail.RepliconServiceOperator(
            task_id='bulk_update_project_team_members',
            endpoint='/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
            data=lambda: {
                'projectUri': rail.result('create_project_or_apply_modifications')['uri'],
                'resourceUri': [x['uri'] for x in rail.result('get_required_resources')] if rail.result(
                    'get_required_resources') else [],
                'projectTeamMemberAssignmentOptionUri': 'urn:replicon:project-team-member-assignment-option:assign'
            }
        )

        get_enabled_company_billingrates = rail.RepliconServiceOperator(
            task_id='get_enabled_company_billingrates',
            endpoint="/services/BillingRateService1.svc/GetEnabledCompanyBillingRates"
        )

        def get_required_billingrates_to_assign(resource_type):
            dag_run_conf = get_current_context()['dag_run'].conf
            resource_billing_rates = dag_run_conf[f'{resource_type}billingrates']
            billing_rate_assignment_team_members = []
            billing_rate_exception = []
            accumulated_billing_rates = []
            for resource_billing_rate in resource_billing_rates:
                if resource_billing_rate['billing_rates']:
                    for each_billing_rate in resource_billing_rate['billing_rates']:
                        split_billing_rate = each_billing_rate.split('|')
                        for billing_rate in split_billing_rate:
                            billing_rate = billing_rate.strip()
                            billingrates_to_consider = rail.result(
                                'get_required_billing_rates_user', 'accumulated_billing_rates') if rail.result(
                                    'get_required_billing_rates_user', 'accumulated_billing_rates') else rail.result(
                                        'get_enabled_company_billingrates')
                            billing_rate_uri = rail.find_first_by_attr_and_get_attr(
                                billingrates_to_consider, 'name', billing_rate, 'uri', '')
                            resource_uri = [x['uri'] for x in rail.result(
                                'get_required_resources') if x['name'] == resource_billing_rate[f'{resource_type}_name'] and
                                x['type'] == resource_type]
                            if billing_rate_uri:
                                billing_rate_assignment_team_members.append({
                                    f'{resource_type}uri': resource_uri[0] if resource_uri else '',
                                    'billingrateuri': billing_rate_uri
                                })
                            else:
                                billing_rate_exception.append(
                                    f'Billing rate not added as {billing_rate} is not available in Replicon')
                            accumulated_billing_rates.append(
                                {
                                    'name': billing_rate,
                                    'uri': billing_rate_uri
                                }
                            )
            if billing_rate_exception:
                rail.set_result(billing_rate_exception,
                                'billing_rate_exception')
            if accumulated_billing_rates:
                rail.set_result(accumulated_billing_rates,
                                'accumulated_billing_rates')
            return billing_rate_assignment_team_members
        get_required_billing_rates_user = rail.PythonOperator(
            task_id='get_required_billing_rates_user',
            python_callable=get_required_billingrates_to_assign,
            op_args=['user']
        )

        is_required_billing_rates_user = rail.IfOperator(
            task_id='is_required_billing_rates_user',
            test="{{ result('get_required_billing_rates_user') | length > 0 }}",
            yes_task='update_billingrate_assignment_team_members_user',
            no_task='get_required_billing_rates_department'
        )

        update_billingrate_assignment_team_members_user = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_billingrate_assignment_team_members_user',
            items=lambda: rail.result('get_required_billing_rates_user'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            endpoint="/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers",
            data={
                "projectUri": "{{ result('create_project_or_apply_modifications').uri }}",
                "billingRateUri": "{{ item.billingrateuri }}",
                "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:available"
            }
        )

        is_useruri_present = rail.IfOperator(
            task_id='is_useruri_present',
            test="{{ result('get_required_billing_rates_user') | map_to_attr('useruri') | \
                remove_empty | length > 0 }}",
            yes_task='put_project_teammember_billing_rates_user',
            no_task='get_required_billing_rates_department'
        )

        put_project_teammember_billing_rates_user = rail.RepliconServiceCallForEachItemOperator(
            task_id='put_project_teammember_billing_rates_user',
            items=lambda: list(set(x['useruri'] for x in rail.result(
                'get_required_billing_rates_user'))),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime",
            data=lambda item: {
                "projectUri": rail.result('create_project_or_apply_modifications')['uri'],
                "resourceUri": item,
                "billingRateUris": [x['billingrateuri'] for x in rail.result('get_required_billing_rates_user') if x['useruri'] == item]
            }
        )

        get_required_billing_rates_department = rail.PythonOperator(
            task_id='get_required_billing_rates_department',
            python_callable=get_required_billingrates_to_assign,
            op_args=['department']
        )

        is_required_billing_rates_department = rail.IfOperator(
            task_id='is_required_billing_rates_department',
            test="{{ result('get_required_billing_rates_department') | length > 0 }}",
            yes_task='update_billingrate_assignment_team_members_department',
            no_task='is_projectcreateddate_uri_present'
        )

        update_billingrate_assignment_team_members_department = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_billingrate_assignment_team_members_department',
            items=lambda: rail.result('get_required_billing_rates_department'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            endpoint="/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers",
            data={
                "projectUri": "{{ result('create_project_or_apply_modifications').uri }}",
                "billingRateUri": "{{ item.billingrateuri }}",
                "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:available"
            }
        )

        is_departmenturi_present = rail.IfOperator(
            task_id='is_departmenturi_present',
            test="{{ result('get_required_billing_rates_department') | map_to_attr('departmenturi') | \
                remove_empty | length > 0 }}",
            yes_task='put_project_teammember_billing_rates_department',
            no_task='is_projectcreateddate_uri_present'
        )

        put_project_teammember_billing_rates_department = rail.RepliconServiceCallForEachItemOperator(
            task_id='put_project_teammember_billing_rates_department',
            items=lambda: list(set(x['departmenturi'] for x in rail.result(
                'get_required_billing_rates_department'))),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime",
            data=lambda item: {
                "projectUri": rail.result('create_project_or_apply_modifications')['uri'],
                "resourceUri": item,
                "billingRateUris": [x['billingrateuri'] for x in rail.result('get_required_billing_rates_department') if x['departmenturi'] == item]
            }
        )

        is_projectcreateddate_uri_present = rail.IfOperator(
            task_id='is_projectcreateddate_uri_present',
            test="{{ dag_run.conf.projectcreatedate_oef_uri | is_truthy }}",
            yes_task="update_project_created_date_oef",
            no_task="is_clientcontacturi_present"
        )

        def get_update_project_createddate_oef(dag_run):
            time_stamp = pendulum.now(config.time_zone).strftime('%d/%m/%Y') + ' ' + pendulum.now(
                config.time_zone).strftime('%H:%M')
            return {
                "objectUri": rail.result('create_project_or_apply_modifications')['uri'],
                "value": {
                    "definition": {
                        "uri": dag_run.conf['projectcreatedate_oef_uri']
                    },
                    "textValue": f"{dag_run.conf['requester']} - {time_stamp}"
                }
            }
        update_project_created_date_oef = rail.RepliconServiceOperator(
            task_id='update_project_created_date_oef',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data=get_update_project_createddate_oef
        )

        is_clientcontacturi_present = rail.IfOperator(
            task_id='is_clientcontacturi_present',
            test="{{ dag_run.conf.clientcontact_oef_uri | is_truthy }}",
            yes_task="update_project_contact_oef",
            no_task="get_exception_message"
        )

        update_project_contact_oef = rail.RepliconServiceOperator(
            task_id='update_project_contact_oef',
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data={
                "objectUri": "{{ result('create_project_or_apply_modifications').uri }}",
                "value": {
                    "definition": {
                        "uri": "{{ dag_run.conf.clientcontact_oef_uri }}"
                    },
                    "textValue": "{{ dag_run.conf.clientcontact }}"
                }
            }
        )

        def get_exception_message_string():
            dag_run_conf = get_current_context()['dag_run'].conf

            def get_task_state(task_id):
                return get_current_context()['dag_run'].get_task_instance(task_id).current_state()
            exception_logs = []
            if get_task_state('is_invoicebalparacontact_present') == 'success' and get_task_state(
                'is_invoicebalparacontacturi_present') == 'success' and rail.result(
                    'is_invoicebalparacontacturi_present') == 'search_project_manager':
                exception_logs.append(
                    'Invoice Balpara Contact dynamic udf is not available')
            if get_task_state('is_projectmanager_present') == 'success' and rail.result(
                    'is_projectmanager_present') == 'get_custom_fields_to_assign':
                exception_logs.append(
                    'Project manager not assigned as user is not available in Replicon')
            if get_task_state('is_projectmanager_enabled') == 'success' and rail.result(
                    'is_projectmanager_enabled') == 'get_custom_fields_to_assign':
                exception_logs.append(
                    'Project manager not assigned as user is disabled in Replicon')
            if get_task_state('is_update_projectleader') == 'success' and rail.result(
                    'is_update_projectleader') == 'get_custom_fields_to_assign':
                exception_logs.append(
                    'Project manager not assigned as user do not have project manager permission in Replicon')

            if not dag_run_conf['project_manager_udf_uri']:
                exception_logs.append('Project Managers udf is not available')
            if not dag_run_conf['actual_projectstartdate_udf_uri']:
                exception_logs.append(
                    'Actual Project Start Date udf is not available')
            if not dag_run_conf['invoiceclientcontact_udf_uri']:
                exception_logs.append(
                    'Invoice Client Contact udf is not available')
            if dag_run_conf['clientproject'] and not dag_run_conf['client_project_udf_uri']:
                exception_logs.append('Client Project # udf is not available')
            if dag_run_conf['comments'] and not dag_run_conf['comments_udf_uri']:
                exception_logs.append('Comments udf is not available')
            if dag_run_conf['workorder'] and not dag_run_conf['workorder_udf_uri']:
                exception_logs.append('Work Order # udf is not available')
            if dag_run_conf['po'] and not dag_run_conf['po_udf_uri']:
                exception_logs.append('PO # udf is not available')
            if dag_run_conf['pmo'] and not dag_run_conf['pmo_udf_uri']:
                exception_logs.append('PMO # udf is not available')
            if dag_run_conf['invoiceclientcode'] and not dag_run_conf[
                    'invoiceclientcode_udf_uri']:
                exception_logs.append(
                    'Invoice Client Code udf is not available')
            if dag_run_conf['location'] and not dag_run_conf[
                    'location_uri']:
                exception_logs.append('Location is not available')

            if get_task_state('get_departmentgroup_uri') == 'success':
                for x in rail.result('get_departmentgroup_uri'):
                    if not x['uri']:
                        exception_logs.append(
                            f"Department group not available for {x['name']}")
            if get_task_state('get_user_uri') == 'success':
                for x in rail.result('get_user_uri'):
                    if not x['uri']:
                        exception_logs.append(
                            f"User not available for {x['name']}")
            if get_task_state('get_required_billing_rates_user') == 'success' and rail.result(
                    'get_required_billing_rates_user', 'billing_rate_exception'):
                exception_logs.extend(rail.result(
                    'get_required_billing_rates_user', 'billing_rate_exception'))
            if get_task_state('get_required_billing_rates_department') == 'success' and rail.result(
                    'get_required_billing_rates_department', 'billing_rate_exception'):
                exception_logs.extend(rail.result(
                    'get_required_billing_rates_department', 'billing_rate_exception'))
            if get_task_state('is_projectcreateddate_uri_present') == 'success' and rail.result(
                    'is_projectcreateddate_uri_present') == 'is_clientcontacturi_present':
                exception_logs.append(
                    'Project Created Date dynamic udf is not available')
            if get_task_state('is_clientcontacturi_present') == 'success' and rail.result(
                    'is_clientcontacturi_present') == 'get_exception_message':
                exception_logs.append(
                    'Project client Contact oef is not available')
            return ','.join(exception_logs) if exception_logs else ''

        get_exception_message = rail.PythonOperator(
            task_id='get_exception_message',
            python_callable=get_exception_message_string
        )

        write_project_imported_log = rail.WriteLogOperator(
            task_id='write_project_imported_log',
            log="{{ dag_run.conf.log }}",
            message="na",
            severity="\
                {%- if result('get_exception_message') -%} \
                    Exception\
                {%- else -%}\
                    Success\
                {%- endif -%}",
            properties={
                "project_code": "{{ dag_run.conf.projectcode }}",
                "project_name": "{{ dag_run.conf.projectname }}",
                "status": "\
                    {%- if result('get_exception_message') -%} \
                        Exception\
                    {%- else -%}\
                        Success\
                    {%- endif -%}",
                "details": "\
                    {%- if result('get_exception_message') -%} \
                        Project created with exceptions as {{ result('get_exception_message') }} \
                    {%- else -%}\
                        Created Successfully\
                    {%- endif -%}",
                "type": "project"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log }}",
            message="na",
            trigger_rule='one_failed',
            severity="Error",
            properties={
                "project_code": "{{ dag_run.conf.projectcode }}",
                "project_name": "{{ dag_run.conf.projectname }}",
                "status": "Error",
                "details": "\
                    {%- if result('create_project_or_apply_modifications') | is_truthy -%} \
                        Project created with error - {{ get_error_message() }} \
                    {%- else -%}\
                        {{ get_error_message() }}\
                    {%- endif -%}",
                "type": "project"
            }
        )

        dag_run_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dag_run_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.dagrun_log_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> get_exception_skipped_records >> is_exception_message
        is_exception_message >> rail.Label(
            'Yes') >> write_exception_log >> rail.Label(
            'On Error') >> catch_and_log_errors
        is_exception_message >> rail.Label(
            'No') >> is_skipped_message
        is_skipped_message >> rail.Label(
            'Yes') >> write_skipped_log >> rail.Label(
            'On Error') >> catch_and_log_errors
        is_skipped_message >> rail.Label(
            'No') >> bulk_get_project_from_name

        bulk_get_project_from_name >> is_projecturi_not_present
        is_projecturi_not_present >> rail.Label(
            'Yes') >> bulk_get_project_from_code >> is_projecturi_not_present2
        is_projecturi_not_present2 >> rail.Label(
            'Yes') >> create_project_or_apply_modifications >> remove_all_user_assignments >> remove_start_and_end_date >> \
            is_project_created
        is_projecturi_not_present2 >> rail.Label(
            'No') >> project_add_code_exception >> rail.Label(
            'On Error') >> catch_and_log_errors
        is_projecturi_not_present >> rail.Label(
            'No') >> project_add_name_exception >> rail.Label(
            'On Error') >> catch_and_log_errors
        is_project_created >> rail.Label(
            'Yes') >> is_invoicebalparacontact_present
        is_invoicebalparacontact_present >> rail.Label(
            'Yes') >> is_invoicebalparacontacturi_present
        is_invoicebalparacontacturi_present >> rail.Label(
            'Yes') >> update_invoice_balpara_contact_oef >> search_project_manager
        is_invoicebalparacontacturi_present >> rail.Label(
            'No') >> search_project_manager
        is_invoicebalparacontact_present >> rail.Label(
            'No') >> search_project_manager
        search_project_manager >> is_projectmanager_present
        is_projectmanager_present >> rail.Label(
            'Yes') >> is_projectmanager_enabled
        is_projectmanager_enabled >> rail.Label(
            'Yes') >> get_assigned_projectmanager_permission >> is_update_projectleader
        is_update_projectleader >> rail.Label(
            'Yes') >> project_leader_to_apply >> get_custom_fields_to_assign
        is_update_projectleader >> rail.Label(
            'No') >> get_custom_fields_to_assign
        is_projectmanager_enabled >> rail.Label(
            'No') >> get_custom_fields_to_assign
        is_projectmanager_present >> rail.Label(
            'No') >> get_custom_fields_to_assign
        get_custom_fields_to_assign >> create_project_or_apply_modifications2 >> put_task >> is_location_present
        is_location_present >> rail.Label(
            'Yes') >> is_locationuri_present
        is_locationuri_present >> rail.Label(
            'Yes') >> update_project_location >> get_departmentgroup_uri
        is_locationuri_present >> rail.Label(
            'No') >> get_departmentgroup_uri
        is_location_present >> rail.Label(
            'No') >> get_departmentgroup_uri
        get_departmentgroup_uri >> get_user_uri >> get_required_resources >> is_resources_to_assign
        is_resources_to_assign >> rail.Label(
            'Yes') >> bulk_update_project_team_members >> get_enabled_company_billingrates
        is_resources_to_assign >> rail.Label(
            'No') >> get_enabled_company_billingrates
        get_enabled_company_billingrates >> get_required_billing_rates_user >> is_required_billing_rates_user
        is_required_billing_rates_user >> rail.Label(
            'Yes') >> update_billingrate_assignment_team_members_user >> is_useruri_present
        is_useruri_present >> rail.Label(
            'Yes') >> put_project_teammember_billing_rates_user >> get_required_billing_rates_department
        is_useruri_present >> rail.Label(
            'No') >> get_required_billing_rates_department
        is_required_billing_rates_user >> rail.Label(
            'No') >> get_required_billing_rates_department
        get_required_billing_rates_department >> is_required_billing_rates_department
        is_required_billing_rates_department >> rail.Label(
            'Yes') >> update_billingrate_assignment_team_members_department >> is_departmenturi_present
        is_departmenturi_present >> rail.Label(
            'Yes') >> put_project_teammember_billing_rates_department >> is_projectcreateddate_uri_present
        is_departmenturi_present >> rail.Label(
            'No') >> is_projectcreateddate_uri_present
        is_required_billing_rates_department >> rail.Label(
            'No') >> is_projectcreateddate_uri_present
        is_projectcreateddate_uri_present >> rail.Label(
            'Yes') >> update_project_created_date_oef >> is_clientcontacturi_present
        is_projectcreateddate_uri_present >> rail.Label(
            'No') >> is_clientcontacturi_present
        is_clientcontacturi_present >> rail.Label(
            'Yes') >> update_project_contact_oef >> get_exception_message
        is_clientcontacturi_present >> rail.Label(
            'No') >> get_exception_message
        is_project_created >> rail.Label(
            'On Error') >> catch_and_log_errors
        get_exception_message >> write_project_imported_log >> rail.Label(
            'On Error') >> catch_and_log_errors
        catch_and_log_errors >> rail.Label(
            'Always') >> dag_run_log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
