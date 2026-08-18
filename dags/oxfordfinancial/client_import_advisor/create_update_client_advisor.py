from datetime import timedelta
import itertools
from airflow.models import Variable
import rail

null = None


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/oxfordfinancial/client_import_advisor/config.py


# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'oxfordfinancial_client_import_advisor_child_create_update_client_{config.instance}',
        description=f'Create/Update Advisor Client in Replicon {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config'
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        def page_handler(request, result_resp):
            if len(result_resp['rows']) > 0:
                request['page'] += 1
                return request
            return null

        def get_clientdetails(response, dag_run):
            client_name = dag_run.conf['Advisor_Full_Name']
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            filtered_client = [
                x for x in flatten_rows if x['cells'][1]['textValue'] == client_name]
            return {
                'uri': rail.smartjoin_by_delim([x['cells'][0]['uri'] for x in filtered_client]),
                'text_value': rail.smartjoin_by_delim([x['cells'][1]['textValue'] for x in filtered_client])
            }
        search_clients = rail.RepliconServicePageOperator(
            task_id='search_clients',
            endpoint='/services/ClientListService1.svc/GetData',
            data=lambda dag_run: {
                'page': 1,
                'pagesize': 10000,
                'columnUris': [
                    'urn:replicon:client-list-column:client',
                    'urn:replicon:client-list-column:name'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:client-list-filter:name'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['Advisor_Full_Name'],
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=get_clientdetails
        )

        if_client_text_value_present = rail.IfOperator(
            task_id='if_client_text_value_present',
            test="{{ result('search_clients').text_value | is_truthy }}",
            yes_task='get_client_details',
            no_task='create_client'
        )

        get_client_details = rail.RepliconServiceOperator(
            task_id='get_client_details',
            endpoint='/services/ClientService1.svc/GetClientDetails',
            data={
                'clientUri': "{{ result('search_clients').uri }}"
            }
        )

        def get_custom_field_values(dag_run):
            custom_field_values = [
                {
                    "customField": {
                        "uri": dag_run.conf['Household_Name_Custom_Field']
                    },
                    "text": dag_run.conf['Household_Firm_Name']
                },
                {
                    "customField": {
                        "uri": dag_run.conf['Household_Firm_Id_Custom_Field']
                    },
                    "text": dag_run.conf['Household_Firm_18_Digit_ID']
                },
                {
                    "customField": {
                        "uri": dag_run.conf['Salesforce_Id_Custom_Field']
                    },
                    "text": dag_run.conf['SF_18_Digit_ID']
                },
                {
                    "customField": {
                        "uri": dag_run.conf['Contact_Status_Custom_Field']
                    },
                    "text": dag_run.conf['Contact_Status']
                }
            ]
            return custom_field_values

        update_client = rail.RepliconServiceOperator(
            task_id='update_client',
            endpoint='/services/ClientService1.svc/PutClient',
            data=lambda dag_run: {
                "client": {
                    "target": {
                        "uri": rail.result('get_client_details')['uri']
                    },
                    "name": rail.result('get_client_details')['name'],
                    "billingContact": dag_run.conf['Email'],
                    "isActive": True,
                    "customFieldValues": get_custom_field_values(dag_run)
                }
            }
        )

        update_household_firm_id = rail.RepliconServiceOperator(
            task_id='update_household_firm_id',
            endpoint='/services/CustomFieldService1.svc/UpdateTextValue',
            data={
                'objectUri': "{{ result('get_client_details').uri }}",
                'customFieldUri': '{{ dag_run.conf.Household_Firm_Id_Custom_Field }}',
                'value': '{{ dag_run.conf.Household_Firm_18_Digit_ID }}'
            }
        )

        update_salesforce_id = rail.RepliconServiceOperator(
            task_id='update_salesforce_id',
            endpoint='/services/CustomFieldService1.svc/UpdateTextValue',
            data={
                'objectUri': "{{ result('get_client_details').uri }}",
                'customFieldUri': '{{ dag_run.conf.Salesforce_Id_Custom_Field }}',
                'value': '{{ dag_run.conf.SF_18_Digit_ID }}'
            }
        )

        def get_projectdetails(response, dag_run):
            project_name = f"Business Development ({dag_run.conf['Advisor_Full_Name']})"
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            filtered_project = [
                x for x in flatten_rows if x['cells'][1]['textValue'] == project_name]
            return {
                'uri': rail.smartjoin_by_delim([x['cells'][0]['uri'] for x in filtered_project]),
                'slug': rail.smartjoin_by_delim([x['cells'][0]['slug'] for x in filtered_project])
            }
        search_project = rail.RepliconServicePageOperator(
            task_id='search_project',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=lambda dag_run: {
                'page': 1,
                'pagesize': 10000,
                'columnUris': [
                    'urn:replicon:project-list-column:project',
                    'urn:replicon:project-list-column:name'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:project-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': f"Business Development ({dag_run.conf['Advisor_Full_Name']})",
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=get_projectdetails
        )

        is_project_slug_blank = rail.IfOperator(
            task_id='is_project_slug_blank',
            test="{{ result('search_project').slug | is_falsy }}",
            yes_task='create_project',
            no_task='update_project_client'
        )

        create_project = rail.RepliconServiceOperator(
            task_id='create_project',
            endpoint='/services/ProjectService1.svc/PutProjectInfo2',
            data=lambda dag_run: {
                "target": {
                    "name": f"Business Development ({dag_run.conf['Advisor_Full_Name']})"
                },
                "projectInfo": {
                    "name": f"Business Development ({dag_run.conf['Advisor_Full_Name']})",
                    "client": {
                        "uri": rail.result('get_client_details')['uri']
                    },
                    "customFieldValues": [{
                        "customField": {
                            "uri": dag_run.conf['Service_Name_Custom_Field']
                        },
                        "dropDownOption": {
                            "name": "Business Development"
                        }
                    }]
                }
            }
        )

        get_company_department = rail.RepliconServiceOperator(
            task_id='get_company_department',
            endpoint="/services/DepartmentService1.svc/GetCompanyDepartment"
        )

        update_project_team_members = rail.RepliconServiceOperator(
            task_id='update_project_team_members',
            endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
            data={
                "projectUri": "{{ result('create_project').uri }}",
                "resourceUri": "{{ result('get_company_department').uri }}",
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            }
        )

        update_project_client = rail.RepliconServiceOperator(
            task_id='update_project_client',
            endpoint="/services/ProjectService1.svc/ApplyNewClient2",
            data={
                "projectUri": "{{ result('search_project').uri }}",
                "clientUri": "{{ result('get_client_details').uri }}",
                "optionUri": "urn:replicon:project-apply-new-client-option:keep-existing-billing-rates-and-expense-codes"
            }
        )

        write_update_client_success = rail.WriteLogOperator(
            task_id='write_update_client_success',
            log="{{ result('create_log') }}",
            message='Updated',
            severity='Updated',
            properties={
                'sf18digitid': '{{ dag_run.conf.SF_18_Digit_ID }}',
                'status': 'Updated',
                'reason': 'Success'
            }
        )

        create_client = rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint='/services/ClientService1.svc/PutClient',
            data=lambda dag_run: {
                "client": {
                    "target": {
                        "name": dag_run.conf['Advisor_Full_Name']
                    },
                    "name": dag_run.conf['Advisor_Full_Name'],
                    "billingContact": dag_run.conf['Email'],
                    "isActive": True,
                    "customFieldValues": get_custom_field_values(dag_run)
                }
            }
        )

        search_project_2 = rail.RepliconServicePageOperator(
            task_id='search_project_2',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=lambda dag_run: {
                'page': 1,
                'pagesize': 10000,
                'columnUris': [
                    'urn:replicon:project-list-column:project',
                    'urn:replicon:project-list-column:name'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:project-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': f"Business Development ({dag_run.conf['Advisor_Full_Name']})",
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=get_projectdetails
        )

        is_project_slug_blank_2 = rail.IfOperator(
            task_id='is_project_slug_blank_2',
            test="{{ result('search_project_2').slug | is_falsy }}",
            yes_task='create_project_2',
            no_task='update_project_client_2'
        )

        create_project_2 = rail.RepliconServiceOperator(
            task_id='create_project_2',
            endpoint='/services/ProjectService1.svc/PutProjectInfo2',
            data=lambda dag_run: {
                "target": {
                    "name": f"Business Development ({dag_run.conf['Advisor_Full_Name']})"
                },
                "projectInfo": {
                    "name": f"Business Development ({dag_run.conf['Advisor_Full_Name']})",
                    "client": {
                        "uri": rail.result('create_client')['uri']
                    },
                    "customFieldValues": [{
                        "customField": {
                            "uri": dag_run.conf['Service_Name_Custom_Field']
                        },
                        "dropDownOption": {
                            "name": "Business Development"
                        }
                    }]
                }
            }
        )

        get_company_department_2 = rail.RepliconServiceOperator(
            task_id='get_company_department_2',
            endpoint="/services/DepartmentService1.svc/GetCompanyDepartment"
        )

        update_project_team_members_2 = rail.RepliconServiceOperator(
            task_id='update_project_team_members_2',
            endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
            data={
                "projectUri": "{{ result('create_project_2').uri }}",
                "resourceUri": "{{ result('get_company_department_2').uri }}",
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            }
        )

        update_project_client_2 = rail.RepliconServiceOperator(
            task_id='update_project_client_2',
            endpoint="/services/ProjectService1.svc/ApplyNewClient2",
            data={
                "projectUri": "{{ result('search_project_2').uri }}",
                "clientUri": "{{ result('create_client').uri }}",
                "optionUri": "urn:replicon:project-apply-new-client-option:keep-existing-billing-rates-and-expense-codes"
            }
        )

        write_update_client_success_2 = rail.WriteLogOperator(
            task_id='write_update_client_success_2',
            log="{{ result('create_log') }}",
            message='Success',
            severity='Created',
            properties={
                'sf18digitid': '{{ dag_run.conf.SF_18_Digit_ID }}',
                'status': 'Created',
                'reason': 'Success'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_log') }}",
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties={
                'sf18digitid': '{{ dag_run.conf.SF_18_Digit_ID }}',
                'status': 'Error',
                'reason': '{{ get_error_message() }}'
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> create_log >> search_clients
        search_clients >> if_client_text_value_present
        if_client_text_value_present >> rail.Label(
            'Yes') >> get_client_details >> update_client >> update_household_firm_id >> update_salesforce_id >> search_project >> is_project_slug_blank
        is_project_slug_blank >> rail.Label(
            'Yes') >> create_project >> get_company_department >> update_project_team_members >> write_update_client_success
        is_project_slug_blank >> rail.Label(
            'No') >> update_project_client >> write_update_client_success
        if_client_text_value_present >> rail.Label(
            'No') >> create_client >> search_project_2 >> is_project_slug_blank_2
        is_project_slug_blank_2 >> rail.Label(
            'Yes') >> create_project_2 >> get_company_department_2 >> update_project_team_members_2 >> write_update_client_success_2
        is_project_slug_blank_2 >> rail.Label(
            'No') >> update_project_client_2 >> write_update_client_success_2

        write_update_client_success >> catch_and_log_errors
        write_update_client_success_2 >> catch_and_log_errors

        catch_and_log_errors >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_dag)
