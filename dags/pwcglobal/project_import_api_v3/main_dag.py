import datetime
from airflow.models import Variable
import rail
from rail.lib.log import get_master_log_artifact_name
from pwcglobal.project_import_api_v3 import python_callable_method
from pwcglobal.project_import_api_v3 import request_payload
from pwcglobal.project_import_api_v3 import response_filter

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/project_import_api_v3/config.py


# pylint:disable = too-many-statements
def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.project_import_api_process_payload_child_dag_id,
        description=f'Project Client data sync_Master V8 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.webhook_secret),
        start_date=datetime.datetime(2022, 1, 1),
        max_active_runs=config.master_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='was_triggered_by_pwc'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='was_triggered_by_pwc',
            execution_timeout=datetime.timedelta(
                days=config.execution_timeout_days),
            end_task='import_end',
        )

        was_triggered_by_pwc = rail.EmptyOperator(
            task_id='was_triggered_by_pwc')

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets'
        )

        get_all_legal_entities = rail.RepliconServiceOperator(
            task_id='get_all_legal_entities',
            endpoint='/services/DivisionListService1.svc/GetData',
            data=request_payload.get_enabled_division_list,
            response_filter=response_filter.map_legal_entities_list
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint='/services/LocationService1.svc/GetAllLocations'
        )

        get_all_client_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_client_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={"objectUri": "urn:replicon:object-type:client"}
        )

        get_all_project_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_project_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={"objectUri": "urn:replicon:object-type:project"}
        )

        get_all_project_object_extension_field_details = rail.RepliconServiceOperator(
            task_id='get_all_project_object_extension_field_details',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={"bindingContextUri": "urn:replicon:object-type:project"}
        )

        get_object_extension_tag_definition_details = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details',
            endpoint='/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails',
            data={"objectExtensionTagDefinitionUri": "{{ result('get_all_project_object_extension_field_details') | \
                find_first_by_attr_and_get_attr('name', 'Type', 'uri') }}"}
        )

        get_confidential_project_custom_field_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_confidential_project_custom_field_dropdown_options',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={"customFieldUri": "{{ result('get_all_project_custom_fields') | \
                find_first_by_attr_and_get_attr('displayText', 'Confidential Project', 'uri') }}"}
        )

        get_mandatory_text_custom_field_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_mandatory_text_custom_field_dropdown_options',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={"customFieldUri": "{{ result('get_all_project_custom_fields') | \
                find_first_by_attr_and_get_attr('displayText', 'Mandatory Text', 'uri') }}"}
        )

        get_client_data = rail.PythonOperator(
            task_id='get_client_data',
            python_callable=python_callable_method.get_client_name_code_partyid
        )

        should_process_client = rail.IfOperator(
            task_id="should_process_client",
            test=lambda: bool(rail.result('get_client_data')['client_name']) and
            bool(rail.result('get_client_data')['client_code']),
            yes_task='get_client_list_from_party_alternate_identifier',
            no_task='should_process_projects'
        )

        get_client_list_from_party_alternate_identifier = rail.RepliconServiceOperator(
            task_id='get_client_list_from_party_alternate_identifier',
            endpoint='/services/ClientListService1.svc/GetData',
            data=request_payload.get_client_list_payload,
            response_filter=response_filter.map_client_list
        )

        is_client_present_in_replicon = rail.IfOperator(
            task_id="is_client_present_in_replicon",
            # pylint:disable = line-too-long
            test="{{ result('get_client_list_from_party_alternate_identifier') | find_first_by_attr_and_get_attr('code', result('get_client_data').client_code, 'uri') | sn | is_truthy }}",
            yes_task='is_client_name_equals_primary_party_name',
            no_task='create_client_in_replicon'
        )

        is_client_name_equals_primary_party_name = rail.IfOperator(
            task_id="is_client_name_equals_primary_party_name",
            test=lambda: rail.result('get_client_data')['client_name'] == rail.find_first_by_attr_and_get_attr(rail.result(
                'get_client_list_from_party_alternate_identifier'), 'code', rail.result('get_client_data')['client_code'], 'name'),
            yes_task='get_client_details',
            no_task='update_client_name_in_replicon'
        )

        create_client_in_replicon = rail.RepliconServiceOperator(
            task_id='create_client_in_replicon',
            endpoint='/services/ClientService1.svc/CreateClientOrApplyModifications',
            data=request_payload.get_create_client_payload
        )

        is_create_client_success = rail.IfOperator(
            task_id="is_create_client_success",
            trigger_rule="all_done",
            test=lambda: bool(rail.result('get_client_data')['client_name']) and bool(rail.result('get_client_data')['client_code']) and
            python_callable_method.get_task_state("create_client_in_replicon") == "success",
            yes_task="create_client_success",
            no_task="if_create_client_skipped"
        )

        create_client_success = rail.EmptyOperator(task_id="create_client_success")

        if_create_client_skipped = rail.IfOperator(
            task_id="if_create_client_skipped",
            test=lambda:bool(python_callable_method.get_task_state("create_client_in_replicon") == "skipped"),
            yes_task="should_process_projects",
            no_task="check_create_client_error_msg"
        )

        check_create_client_error_msg = rail.IfOperator(
            task_id="check_create_client_error_msg",
            test=lambda: bool((rail.result('create_client_in_replicon',
                'error')['response']['json']['error']['details']['notifications'][0]['displayText'])
                == 'Client code should be unique. A client already exists with the specified code.'),
            yes_task="get_client_list_from_party_alternate_identifier_2",
            no_task="client_error"
        )

        client_error = rail.WriteLogOperator(
            task_id="client_error",
            message='{{get_error_message}}',
            properties=lambda dag_run: {
                'SenderID': f"{dag_run.conf['webhook']['data']['Sender']} | Client",
                'Project Name|Project Code': 'nil',
                'Client Name|Client Code': rail.render_template("{{ result('get_client_data').client_name }} | {{ result('get_client_data').client_code }}"),
                'Task Name|Task Code': 'nil',
                'status': 'Error',
                'details':rail.result('create_client_in_replicon',"error"),
                'UnitLoggedDateTime': rail.render_template("{{ current_time() }}"),
                'Action': get_details_action("action")
            }
        )

        get_client_list_from_party_alternate_identifier_2 = rail.RepliconServiceOperator(
            task_id='get_client_list_from_party_alternate_identifier_2',
            endpoint='/services/ClientListService1.svc/GetData',
            data=request_payload.get_client_list_payload,
            response_filter=response_filter.map_client_list
        )

        is_client_name_equals_primary_party_name_2 = rail.IfOperator(
            task_id="is_client_name_equals_primary_party_name_2",
            test=lambda: rail.result('get_client_data')['client_name'] == rail.find_first_by_attr_and_get_attr(rail.result(
                'get_client_list_from_party_alternate_identifier_2'), 'code', rail.result('get_client_data')['client_code'], 'name'),
            yes_task='get_client_details2',
            no_task='update_client_name_in_replicon_2'
        )

        update_client_name_in_replicon_2 = rail.RepliconServiceOperator(
            task_id="update_client_name_in_replicon_2",
            endpoint='/services/ClientService1.svc/UpdateName',
            data=lambda: {
                'clientUri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_client_list_from_party_alternate_identifier_2'), 'code', rail.result('get_client_data')['client_code'], 'uri'),
                'name': rail.result('get_client_data')['client_name']
            }
        )

        end_update_client_name_2 = rail.EmptyOperator(task_id="end_update_client_name_2")

        get_client_details2 = rail.RepliconServiceOperator(
            task_id = "get_client_details2",
            endpoint="/services/ClientService1.svc/BulkGetClientDetails",
            data=lambda : {
                "clientUris" : [
                    rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_client_list_from_party_alternate_identifier_2'), 'code', rail.result('get_client_data')['client_code'], 'uri')
                ]
            }
        )

        def test_can_update_the_client_party_id2():
            if not rail.result("get_client_details2"):
                return False
            current_party_id = rail.find_first_by_attr_and_get_attr(rail.result("get_client_details2")[0]['customFields'], 'customField.displayText', 'Client Party Id', 'text')
            if not current_party_id:
                if rail.result('get_client_data')['client_party_id']:
                    return True
            return False

        can_update_the_client_party_id2 = rail.IfOperator(
            task_id = "can_update_the_client_party_id2",
            test=test_can_update_the_client_party_id2,
            yes_task="update_client_party_id2",
            no_task="can_log_update2"
        )

        update_client_party_id2 = rail.RepliconServiceOperator(
            task_id = "update_client_party_id2",
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda: {
                "objectUri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_client_list_from_party_alternate_identifier_2'), 'code', rail.result('get_client_data')['client_code'], 'uri'),
                "customFieldUri": rail.find_first_by_attr_and_get_attr(
                        rail.result('get_all_client_custom_fields'), 'displayText', 'Client Party Id', 'uri'),
                "value": rail.result('get_client_data')['client_party_id']
            }
        )

        def can_log_update_test2():
            return (not check_can_update_name2()) or test_can_update_the_client_party_id2()


        can_log_update2 = rail.IfOperator(
            task_id = "can_log_update2",
            test=can_log_update_test2,
            yes_task="dummy_can_log_update2_yes_task",
            no_task="dummy_can_log_update_no2_task"
        )

        dummy_can_log_update2_yes_task = rail.EmptyOperator(
            task_id = "dummy_can_log_update2_yes_task"
        )

        dummy_can_log_update_no2_task = rail.EmptyOperator(
            task_id = "dummy_can_log_update_no2_task"
        )

        update_client_name_in_replicon = rail.RepliconServiceOperator(
            task_id="update_client_name_in_replicon",
            endpoint='/services/ClientService1.svc/UpdateName',
            data=lambda: {
                'clientUri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_client_list_from_party_alternate_identifier'), 'code', rail.result('get_client_data')['client_code'], 'uri'),
                'name': rail.result('get_client_data')['client_name']
            }
        )

        end_update_client_name = rail.EmptyOperator(task_id="end_update_client_name")

        get_client_details = rail.RepliconServiceOperator(
            task_id = "get_client_details",
            endpoint="/services/ClientService1.svc/BulkGetClientDetails",
            data=lambda : {
                "clientUris" : [
                    rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_client_list_from_party_alternate_identifier'), 'code', rail.result('get_client_data')['client_code'], 'uri')
                ]
            }
        )

        def test_can_update_the_client_party_id():
            if not rail.result("get_client_details"):
                return False
            current_party_id = rail.find_first_by_attr_and_get_attr(rail.result("get_client_details")[0]['customFields'], 'customField.displayText', 'Client Party Id', 'text')
            if not current_party_id:
                if rail.result('get_client_data')['client_party_id']:
                    return True
            return False

        can_update_the_client_party_id = rail.IfOperator(
            task_id = "can_update_the_client_party_id",
            test=test_can_update_the_client_party_id,
            yes_task="update_client_party_id",
            no_task="can_log_update"
        )

        update_client_party_id = rail.RepliconServiceOperator(
            task_id = "update_client_party_id",
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda: {
                "objectUri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_client_list_from_party_alternate_identifier'), 'code', rail.result('get_client_data')['client_code'], 'uri'),
                "customFieldUri": rail.find_first_by_attr_and_get_attr(
                        rail.result('get_all_client_custom_fields'), 'displayText', 'Client Party Id', 'uri'),
                "value": rail.result('get_client_data')['client_party_id']
            }
        )

        def can_log_update_test():
            return (not check_can_update_name1()) or test_can_update_the_client_party_id()


        can_log_update = rail.IfOperator(
            task_id = "can_log_update",
            test=can_log_update_test,
            yes_task="dummy_can_log_update_yes_task",
            no_task="dummy_can_log_update_no_task"
        )

        dummy_can_log_update_yes_task = rail.EmptyOperator(
            task_id = "dummy_can_log_update_yes_task"
        )

        dummy_can_log_update_no_task = rail.EmptyOperator(
            task_id = "dummy_can_log_update_no_task"
        )


        def check_can_update_name1():
            return (rail.result('get_client_data')['client_name'] == rail.find_first_by_attr_and_get_attr(rail.result(
                'get_client_list_from_party_alternate_identifier'), 'code', rail.result('get_client_data')['client_code'], 'name', ''))

        def check_can_update_name2():
            return (rail.result('get_client_data')['client_name'] == rail.find_first_by_attr_and_get_attr(rail.result(
                'get_client_list_from_party_alternate_identifier_2'), 'code', rail.result('get_client_data')['client_code'], 'name', ''))

        def get_details_action(caller):
            if caller == "action":
                return "Add" if bool(rail.result("create_client_in_replicon")) else "Update"
            if bool(rail.result("create_client_in_replicon")):
                return "Client created"
            if not (check_can_update_name1() or check_can_update_name2()):
                if test_can_update_the_client_party_id() or test_can_update_the_client_party_id2():
                    return "Client name update. Client Party Id Added"
                return "Client name updated"
            if test_can_update_the_client_party_id() or test_can_update_the_client_party_id2():
                return "Client Party Id Added"


        log_create_update_client = rail.WriteLogOperator(
            task_id="log_create_update_client",
            message="{{ result('get_client_data').client_name }} is processed",
            severity='Success',
            properties=lambda dag_run: {
                'SenderID': f"{dag_run.conf['webhook']['data']['Sender']} | Client",
                'Project Name|Project Code': 'nil',
                'Client Name|Client Code': rail.render_template("{{ result('get_client_data').client_name }} | {{ result('get_client_data').client_code }}"),
                'Task Name|Task Code': 'nil',
                'status': 'Success',
                'details': get_details_action("details"),
                'UnitLoggedDateTime': "{{ current_time() }}",
                'Action': get_details_action("action")
            }
        )

        should_process_projects = rail.IfOperator(
            task_id='should_process_projects',
            test="{{ dag_run.conf.webhook.data | attr_or_default('WorkManagement') | first_or_default | \
                attr_or_default('ChargeCode') | length > 0 }}",
            yes_task='process_projects',
            no_task='process_log_pregeneration'
        )

        process_projects = rail.TriggerDagRunForEachItemOperator(
            task_id='process_projects',
            items=lambda dag_run: dag_run.conf['webhook']['data']['WorkManagement'][0]['ChargeCode'],
            execution_timeout=datetime.timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'pwc_project_import_child_process_project_b1_{config.instance}_v3',
            conf=request_payload.get_process_project_conf
        )

        wait_for_process_projects = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_projects',
            dag_runs='{{ result("process_projects") }}',
            execution_timeout=datetime.timedelta(
                days=config.execution_timeout_days)
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_logs',
            dag_runs="{{ result('process_projects') }}",
            dagrun_task_id='create_log',
            flatten=True,
        )

        process_log_pregeneration = rail.TriggerDagRunForEachItemOperator(
            task_id='process_log_pregeneration',
            items=lambda dag_run: [dag_run.conf['webhook']['data']],
            execution_timeout=datetime.timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'pwc_project_import_child_log_pregeneration_{config.instance}_v3',
            conf=lambda item: {
                'master_log': get_master_log_artifact_name(rail.get_current_context()),
                'child_log': rail.result('gather_logs'),
                'sender': item['Sender'],
                'identifier': item['Identifier']
            }
        )

        import_end = rail.EmptyOperator(task_id="import_end")

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.dagrun_log_conn_id,
            trigger_rule='all_done',
            extra_info={
                'PayloadIdentifier': '{{ dag_run.conf.webhook.data.Identifier }}',
                # pylint:disable = line-too-long
                'Client': "{{ result('get_client_data').client_name if result('get_client_data') }} | {{ result('get_client_data').client_code if result('get_client_data')  }}",
                'No_of_Projects': "{{ dag_run.conf.webhook.data | attr_or_default('WorkManagement') | first_or_default | \
                    attr_or_default('ChargeCode') | length }}",
                'Sender': '{{ dag_run.conf.webhook.data.Sender }}'
            }
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> import_end
        can_run_batch_task >> rail.Label('No') >> was_triggered_by_pwc

        was_triggered_by_pwc >> get_all_permission_sets >> get_all_legal_entities >> get_all_locations >> \
            get_all_client_custom_fields >> get_all_project_custom_fields >> \
            get_all_project_object_extension_field_details >> get_object_extension_tag_definition_details >> \
            get_confidential_project_custom_field_dropdown_options >> \
            get_mandatory_text_custom_field_dropdown_options >> get_client_data >> \
            should_process_client

        should_process_client >> rail.Label(
            "Yes") >> get_client_list_from_party_alternate_identifier >> is_client_present_in_replicon
        should_process_client >> rail.Label(
            "No") >> should_process_projects

        is_client_present_in_replicon >> rail.Label(
            "Yes") >> is_client_name_equals_primary_party_name

        is_client_name_equals_primary_party_name >> rail.Label(
            "Yes") >> get_client_details >> can_update_the_client_party_id >> rail.Label("No") >> can_log_update >> rail.Label("No") >> dummy_can_log_update_no_task >> should_process_projects
        can_update_the_client_party_id >> rail.Label("Yes") >> update_client_party_id >> can_log_update >> rail.Label("Yes") >> dummy_can_log_update_yes_task >> log_create_update_client

        is_client_name_equals_primary_party_name >> rail.Label(
            "No") >> update_client_name_in_replicon >> end_update_client_name >> get_client_details

        is_client_present_in_replicon >> rail.Label(
            "No") >> create_client_in_replicon >> is_create_client_success

        is_create_client_success >> rail.Label(
            "Yes") >> create_client_success >> log_create_update_client
        is_create_client_success >> rail.Label(
            "No") >> if_create_client_skipped >> rail.Label("Yes") >> should_process_projects
        if_create_client_skipped >> rail.Label("No") >>\
        check_create_client_error_msg >> rail.Label(
            "No") >> client_error >> process_log_pregeneration
        check_create_client_error_msg >> rail.Label(
            "Yes") >> get_client_list_from_party_alternate_identifier_2 >> is_client_name_equals_primary_party_name_2

        is_client_name_equals_primary_party_name_2 >> rail.Label(
            "Yes") >> get_client_details2
        is_client_name_equals_primary_party_name_2 >> rail.Label(
            "No") >> update_client_name_in_replicon_2 >> end_update_client_name_2 >> get_client_details2

        get_client_details2 >> can_update_the_client_party_id2 >> rail.Label("No") >> can_log_update2 >> rail.Label("No") >> dummy_can_log_update_no2_task >> should_process_projects
        can_update_the_client_party_id2 >> rail.Label("Yes") >> update_client_party_id2 >> can_log_update2 >> rail.Label("Yes") >> dummy_can_log_update2_yes_task >> log_create_update_client

        log_create_update_client >> should_process_projects

        should_process_projects >> rail.Label(
            "Yes") >> process_projects >> wait_for_process_projects >> gather_logs >> process_log_pregeneration

        should_process_projects >> rail.Label(
            "No") >> process_log_pregeneration

        process_log_pregeneration >> import_end >> log_dagrun_to_sumo >> can_fail_dag >> fail_dagrun

    return dag


rail.for_each_instance(create_main_airflow_dag)
