from datetime import timedelta
import rail
from dxctechnology.compass_attributes_1_and_2.utils import request_payload
from dxctechnology.compass_attributes_1_and_2.utils import python_callable_method
from dxctechnology.compass_attributes_1_and_2.utils import custom_methods

null = None

# pylint: disable=too-many-statements


def create_attribute_1_process_wbs_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_attribute_1_process_wbs_child_{config.dag_id_postfix}',
        description=f'DXC_Compass_Attribute 1 Child - Process each WBS V1.0 {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_project_details_based_on_wbs = rail.RepliconServiceOperator(
            task_id='get_project_details_based_on_wbs',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": '{{ dag_run.conf.wbs }}',
                        "code": null,
                        "parameterCorrelationId": null
                    }
                ]
            },
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                                          {"projectDetails": null}])[0]['projectDetails']
        )

        check_wbs_exists = rail.IfOperator(
            task_id='check_wbs_exists',
            test=lambda: bool(rail.result('get_project_details_based_on_wbs') and
                              rail.result(
                'get_project_details_based_on_wbs')['uri']),
            yes_task='check_wbs_is_archived',
            no_task='log_wbs_not_available'
        )

        log_wbs_not_available = rail.WriteLogOperator(
            task_id='log_wbs_not_available',
            message='All attributes failed to sync, since WBS not available in Replicon',
            severity='Error',
            items='{{ dag_run.conf.attributes | to_json }}',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '',
                'attributenumber': '',
                'action': 'skipped',
                'status': 'Error',
                'details': 'All attributes failed to sync, since WBS not available in Replicon',
                'recordcount': '{{ dag_run.conf.attributes | length }}'
            }
        )

        check_wbs_is_archived = rail.IfOperator(
            task_id='check_wbs_is_archived',
            test=lambda: rail.result('get_project_details_based_on_wbs')[
                'status']['name'] == 'Archived',
            yes_task='log_wbs_is_archived',
            no_task='get_project_date_range',
        )

        log_wbs_is_archived = rail.WriteLogOperator(
            task_id='log_wbs_is_archived',
            # pylint: disable=line-too-long
            message='All attributes were skipped, since this WBS is in Archive status. Attribute Count - {{ dag_run.conf.attributes | length }}',
            severity='Exception',
            items='{{ dag_run.conf.attributes | to_json }}',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '',
                'attributenumber': '',
                'action': 'pre-check',
                'status': 'skipped',
                'details': 'All attributes were skipped, since this WBS is in Archive status. Attribute Count - {{ dag_run.conf.attributes | length }}',
                'recordcount': '{{ dag_run.conf.attributes | length }}'
            }
        )

        get_project_date_range = rail.PythonOperator(
            task_id='get_project_date_range',
            python_callable=python_callable_method.project_date_range
        )

        assign_status_to_attributes = rail.PythonOperator(
            task_id='assign_status_to_attributes',
            python_callable=python_callable_method.get_attributes_status_list
        )

        create_attribute_1_collection = rail.CreateCollectionOperator(
            task_id='create_attribute_1_collection',
            source='{{ result("assign_status_to_attributes") | to_json }}',
            name='attributes'
        )

        query_blank_attribute = rail.QueryCollectionOperator(
            task_id='query_blank_attribute',
            query="""SELECT * FROM attributes WHERE (Attribute = '' OR AttributeNumber = '' OR EndDate = '' OR AttributeNumber != '1' OR enddatestatus = '')"""
        )

        query_attribute_1_result = rail.QueryCollectionOperator(
            task_id='query_attribute_1_result',
            query="""SELECT * FROM attributes WHERE
            (AttributeNumber= '1' AND Attribute != '' AND AttributeNumber != '' AND EndDate != '' AND enddatestatus='valid' AND descriptionstatus='valid')"""
        )

        attribute_1_present = rail.IfOperator(
            task_id='attribute_1_present',
            test=lambda: len(custom_methods.get_data_from_document(
                rail.result("query_attribute_1_result"))) > 0,
            yes_task='get_all_project_team_member_details',
            no_task='finish'
        )

        get_all_project_team_member_details = rail.RepliconServiceOperator(
            task_id='get_all_project_team_member_details',
            endpoint='/services/ProjectService1.svc/GetAllProjectTeamMemberDetails',
            data={
                'projectUri': '{{ result("get_project_details_based_on_wbs").uri }}',
                'asOfDate': null},
            data_handler=lambda data: list(
                map(lambda assignment: assignment['resource']['uri'], data))
        )

        get_children_task_details = rail.RepliconServiceOperator(
            task_id='get_children_task_details',
            endpoint='/services/TaskService1.svc/GetChildrenTaskDetails',
            data={
                'parentUri': '{{ result("get_project_details_based_on_wbs").uri }}'
            }
        )

        get_tasks_from_project = rail.PythonOperator(
            task_id='get_tasks_from_project',
            python_callable=python_callable_method.retrieve_task_list,
            op_args=['get_children_task_details']
        )

        tasks_from_project_collection = rail.CreateCollectionOperator(
            task_id='tasks_from_project_collection',
            source='{{ result("get_tasks_from_project") | to_json }}',
            columns=[
                'name',
                'code',
                'enddate',
                'oef',
                'uri',
                'md5'],
            name='tasks_from_project'
        )

        query_task_list = rail.QueryCollectionOperator(
            task_id='query_task_list',
            query="""SELECT * FROM tasks_from_project WHERE oef = 'Attribute 1'"""
        )

        get_attribute_1_toprocess_frominput = rail.PythonOperator(
            task_id='get_attribute_1_toprocess_frominput',
            python_callable=python_callable_method.retrive_attributes_from_input,
            op_args=['query_attribute_1_result', 'query_task_list']
        )

        attribute_1_to_process_collection = rail.CreateCollectionOperator(
            task_id='attribute_1_to_process_collection',
            source='{{ result("get_attribute_1_toprocess_frominput") | to_json }}',
            name='attribute1toprocess_frominput'
        )

        query_attribute_1_to_create = rail.QueryCollectionOperator(
            task_id='query_attribute_1_to_create',
            query="""SELECT * FROM attribute1toprocess_frominput WHERE uri IS NULL AND enddatestatus = 'valid' AND descriptionstatus = 'valid'"""
        )

        attribute_1_to_create_collection = rail.CreateCollectionOperator(
            task_id='attribute_1_to_create_collection',
            source='{{ result("query_attribute_1_to_create") }}',
            name='tasktocreate'
        )

        query_attribute_1_to_update = rail.QueryCollectionOperator(
            task_id='query_attribute_1_to_update',
            # pylint: disable=line-too-long
            query="""SELECT * FROM attribute1toprocess_frominput WHERE uri IS NOT NULL AND enddatestatus = 'valid' AND descriptionstatus = 'valid' AND md5 NOT IN
                    (SELECT DISTINCT md5 FROM tasks_from_project)"""
        )

        query_attribute_1_to_skip = rail.QueryCollectionOperator(
            task_id='query_attribute_1_to_skip',
            # pylint: disable=line-too-long
            query="""SELECT * FROM attribute1toprocess_frominput WHERE uri IS NOT NULL AND enddatestatus = 'valid' AND descriptionstatus = 'valid' AND md5 IN
                    (SELECT DISTINCT md5 FROM tasks_from_project)"""
        )

        query_invalid_enddate = rail.QueryCollectionOperator(
            task_id='query_invalid_enddate',
            query="""SELECT * FROM attributes WHERE enddatestatus = 'invalid'"""
        )

        query_invalid_description = rail.QueryCollectionOperator(
            task_id='query_invalid_description',
            query="""SELECT * FROM attributes WHERE descriptionstatus = 'invalid'"""
        )

        is_name_present_in_create_list = rail.IfOperator(
            task_id='is_name_present_in_create_list',
            test=lambda: python_callable_method.check_key_values_present_in_list(
                'attribute_1_to_create_collection', 'name'),
            yes_task='is_uri_not_present_in_task_list',
            no_task='collect_logs'
        )

        is_uri_not_present_in_task_list = rail.IfOperator(
            task_id='is_uri_not_present_in_task_list',
            test=lambda: python_callable_method.check_key_values_present_in_list(
                'query_task_list', 'uri'),
            yes_task='get_task_query_name',
            no_task='get_first_task',
        )

        get_first_task = rail.PythonOperator(
            task_id='get_first_task',
            python_callable=lambda: python_callable_method.retrieve_first_task(
                'query_attribute_1_to_create')
        )

        create_attribute_1 = rail.TriggerDagRunForEachItemOperator(
            task_id='create_attribute_1',
            retries=0,
            items=lambda: [custom_methods.get_conf()],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_compass_attribute_1_create_task_child_{config.dag_id_postfix}',
            conf=lambda dag_run: request_payload.get_create_task_conf(
                dag_run, rail.result('get_first_task'), 'get_project_details_based_on_wbs', 'get_all_project_team_member_details'),
        )

        wait_for_create_attribute_1 = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_attribute_1',
            dag_runs='{{ result("create_attribute_1") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_task_uri_to_process_copy = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_task_uri_to_process_copy',
            dag_runs="{{ result('create_attribute_1') }}",
            dagrun_task_id='put_task_from_wbs',
            flatten=True
        )

        query_attribute_1_to_create_excluding_first_task = rail.QueryCollectionOperator(
            task_id='query_attribute_1_to_create_excluding_first_task',
            query="""SELECT * FROM tasktocreate WHERE name != :name""",
            query_params={
                "name": '{{ result("get_first_task")["name"] }}'
            }
        )

        is_name_present_in_excluded_first_task = rail.IfOperator(
            task_id='is_name_present_in_excluded_first_task',
            test=lambda: python_callable_method.check_key_values_present_in_list(
                'query_attribute_1_to_create_excluding_first_task', 'name'),
            yes_task='get_task_query_name',
            no_task='collect_logs'
        )

        get_task_query_name = rail.PythonOperator(
            task_id='get_task_query_name',
            python_callable=python_callable_method.query_result_for_copy_batch,
            op_args=['query_task_list', 'query_attribute_1_to_create_excluding_first_task',
                     'gather_task_uri_to_process_copy',
                     'attribute_1_to_create_collection']

        )

        process_copy_batch_data = rail.TriggerDagRunForEachItemOperator(
            task_id='process_copy_batch_data',
            retries=0,
            items=lambda: [custom_methods.get_conf()],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            batch_size=config.post_batch_size,
            trigger_dag_id=f'dxctechnology_compass_attribute_1_process_copy_data_child_{config.dag_id_postfix}',
            conf=lambda dag_run: request_payload.get_copy_data_conf(dag_run, 'get_project_details_based_on_wbs', rail.result(
                "get_task_query_name"))
        )

        wait_for_process_copy_batch_data = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_copy_batch_data',
            dag_runs='{{ result("process_copy_batch_data") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        is_uri_present_in_update_list = rail.IfOperator(
            task_id='is_uri_present_in_update_list',
            test=lambda: python_callable_method.check_key_values_present_in_list(
                'query_attribute_1_to_update', 'uri'),
            yes_task='attribute_1_to_update_collection',
            no_task='collect_logs'
        )

        attribute_1_to_update_collection = rail.CreateCollectionOperator(
            task_id='attribute_1_to_update_collection',
            source='{{ result("query_attribute_1_to_update") }}',
            name='taskstoupdate'
        )

        process_update_task_hierarchy = rail.TriggerDagRunForEachItemOperator(
            task_id='process_update_task_hierarchy',
            retries=0,
            items=lambda: custom_methods.get_data_from_document(
                rail.result("attribute_1_to_update_collection")),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_compass_attribute_1_process_update_task_hierarchy_child_{config.dag_id_postfix}',
            conf=lambda dag_run, item: request_payload.get_update_task_conf(
                dag_run, item, 'get_project_details_based_on_wbs')
        )

        wait_for_process_update_task_hierarchy = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_task_hierarchy',
            dag_runs='{{ result("process_update_task_hierarchy") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        collect_logs = rail.EmptyOperator(
            task_id='collect_logs'
        )

        log_no_change_received = rail.WriteLogOperator(
            task_id='log_no_change_received',
            message='No change received for attribute 1',
            items=lambda: custom_methods.get_data_from_document(
                rail.result("query_attribute_1_to_skip")),
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '{{ item.name }}',
                'attributenumber': '{{ item.number }}',
                'action': 'pre-check',
                'status': 'Skipped',
                'details': 'No change received for attribute 1',
                'recordcount': ''
            }
        )

        log_mandatory_fields_not_present = rail.WriteLogOperator(
            task_id='log_mandatory_fields_not_present',
            message='Attribute not added/updated as one or more mandatory fields are not present.',
            items=lambda: custom_methods.get_data_from_document(
                rail.result('query_blank_attribute')),
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '{{ item.Attribute }}',
                'attributenumber': '{{ item.AttributeNumber }}',
                'action': 'pre-check',
                'status': 'Exception',
                'details': 'Attribute not added/updated as one or more mandatory fields are not present.',
                'recordcount': ''
            }
        )

        log_invalid_enddate = rail.WriteLogOperator(
            task_id='log_invalid_enddate',
            message='Attribute not added/updated as enddate is invalid.',
            items=lambda: custom_methods.get_data_from_document(
                rail.result('query_invalid_enddate')),
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '{{ item.Attribute }}',
                'attributenumber': '{{ item.AttributeNumber }}',
                'action': 'pre-check',
                'status': 'Exception',
                'details': 'Attribute not added/updated as enddate is invalid/before Project start date.',
                'recordcount': ''
            }
        )

        log_invalid_description = rail.WriteLogOperator(
            task_id='log_invalid_description',
            message='Attribute not added/updated as description exceeded maximum length of 50 characters.',
            items=lambda: custom_methods.get_data_from_document(
                rail.result('query_invalid_description')),
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '{{ item.Attribute }}',
                'attributenumber': '{{ item.AttributeNumber }}',
                'action': 'pre-check',
                'status': 'Exception',
                'details': 'Attribute not added/updated as description exceeded maximum length of 50 characters.',
                'recordcount': ''
            }
        )

        def get_iwo_wbs_element():
            return rail.find_first_by_attr_and_get_attr(
                rail.result('get_project_details_based_on_wbs')[
                    'extensionFieldValues'], 'definition.displayText',
                'IWO WBS Element', 'textValue') if rail.result('get_project_details_based_on_wbs') else null

        get_iwo_wbs_project = rail.PythonOperator(
            task_id='get_iwo_wbs_project',
            python_callable=get_iwo_wbs_element
        )

        is_iwo_wbs_present_in_project = rail.IfOperator(
            task_id='is_iwo_wbs_present_in_project',
            test=lambda: bool(rail.result('get_iwo_wbs_project')),
            yes_task='get_project_details_based_on_iwo_wbs',
            no_task='finish'
        )

        get_project_details_based_on_iwo_wbs = rail.RepliconServiceOperator(
            task_id='get_project_details_based_on_iwo_wbs',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={"projects": [
                {"name": '{{ result("get_iwo_wbs_project") }}'}]},
            response_filter=lambda resp: resp.json(
            )['d'][0].get('projectDetails', null),
        )

        is_uri_present_in_iwo_wbs_project = rail.IfOperator(
            task_id='is_uri_present_in_iwo_wbs_project',
            test=lambda: bool(rail.result('get_project_details_based_on_iwo_wbs') and
                              rail.result('get_project_details_based_on_iwo_wbs')['uri']),
            yes_task='get_children_task_details_of_iwo_wbs',
            no_task='log_no_change_received_iwo',
        )

        get_children_task_details_of_iwo_wbs = rail.RepliconServiceOperator(
            task_id='get_children_task_details_of_iwo_wbs',
            endpoint='/services/TaskService1.svc/GetChildrenTaskDetails',
            data={
                'parentUri': '{{ result("get_project_details_based_on_iwo_wbs").uri }}'
            }
        )

        get_all_project_team_member_details_of_iwo_wbs = rail.RepliconServiceOperator(
            task_id='get_all_project_team_member_details_of_iwo_wbs',
            endpoint='/services/ProjectService1.svc/GetAllProjectTeamMemberDetails',
            data={
                'projectUri': '{{ result("get_project_details_based_on_iwo_wbs").uri }}',
                'asOfDate': null},
            data_handler=lambda data: list(
                map(lambda assignment: assignment['resource']['uri'], data))
        )

        get_tasks_from_iwo_project = rail.PythonOperator(
            task_id='get_tasks_from_iwo_project',
            python_callable=python_callable_method.retrieve_task_list,
            op_args=['get_children_task_details_of_iwo_wbs']
        )

        tasks_from_iwo_project_collection = rail.CreateCollectionOperator(
            task_id='tasks_from_iwo_project_collection',
            source='{{ result("get_tasks_from_iwo_project") | to_json }}',
            columns=[
                'name',
                'code',
                'enddate',
                'oef',
                'uri',
                'md5'],
            name='tasks_from_iwo_project'
        )

        query_iwo_task_list = rail.QueryCollectionOperator(
            task_id='query_iwo_task_list',
            query="""SELECT * FROM tasks_from_iwo_project WHERE oef = 'Attribute 1'"""
        )

        get_iwo_attribute_1_toprocess_frominput = rail.PythonOperator(
            task_id='get_iwo_attribute_1_toprocess_frominput',
            python_callable=python_callable_method.retrive_attributes_from_input,
            op_args=['query_attribute_1_result', 'query_iwo_task_list']
        )

        attribute_1_to_iwo_process_collection = rail.CreateCollectionOperator(
            task_id='attribute_1_to_iwo_process_collection',
            source='{{ result("get_iwo_attribute_1_toprocess_frominput") | to_json }}',
            name='attribute1to_iwo_process_frominput'
        )

        query_attribute_1_iwo_to_create = rail.QueryCollectionOperator(
            task_id='query_attribute_1_iwo_to_create',
            query="""SELECT * FROM attribute1to_iwo_process_frominput WHERE uri IS NULL AND enddatestatus = 'valid' AND descriptionstatus = 'valid'"""
        )

        attribute_1_iwo_to_create_collection = rail.CreateCollectionOperator(
            task_id='attribute_1_iwo_to_create_collection',
            source='{{ result("query_attribute_1_iwo_to_create") }}',
            name='tasktocreate_iwo'
        )

        query_attribute_1_iwo_to_update = rail.QueryCollectionOperator(
            task_id='query_attribute_1_iwo_to_update',
            # pylint: disable=line-too-long
            query="""SELECT * FROM attribute1to_iwo_process_frominput WHERE uri IS NOT NULL AND enddatestatus= 'valid' AND descriptionstatus= 'valid' AND md5 NOT IN
                    (SELECT DISTINCT md5 FROM tasks_from_iwo_project)"""
        )

        query_attribute_1_iwo_to_skip = rail.QueryCollectionOperator(
            task_id='query_attribute_1_iwo_to_skip',
            # pylint: disable=line-too-long
            query="""SELECT * FROM attribute1to_iwo_process_frominput WHERE uri IS NOT NULL AND enddatestatus = 'valid' AND descriptionstatus = 'valid' AND md5 IN
                    (SELECT DISTINCT md5 FROM tasks_from_iwo_project)"""
        )

        is_name_present_in_iwo_create_list = rail.IfOperator(
            task_id='is_name_present_in_iwo_create_list',
            test=lambda: python_callable_method.check_key_values_present_in_list(
                'attribute_1_iwo_to_create_collection', 'name'),
            yes_task='is_uri_not_present_in_iwo_task_list',
            no_task='log_no_change_received_iwo'
        )

        is_uri_not_present_in_iwo_task_list = rail.IfOperator(
            task_id='is_uri_not_present_in_iwo_task_list',
            test=lambda: python_callable_method.check_key_values_present_in_list(
                'query_iwo_task_list', 'uri'),
            yes_task='get_task_query_name_iwo',
            no_task='get_first_iwo_task',
        )

        get_first_iwo_task = rail.PythonOperator(
            task_id='get_first_iwo_task',
            python_callable=lambda: python_callable_method.retrieve_first_task(
                'query_attribute_1_iwo_to_create')
        )

        create_attribute_1_iwo = rail.TriggerDagRunForEachItemOperator(
            task_id='create_attribute_1_iwo',
            retries=0,
            items=lambda: [custom_methods.get_conf()],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_compass_attribute_1_create_task_child_{config.dag_id_postfix}',
            conf=lambda dag_run: request_payload.get_create_task_conf(
                dag_run, rail.result(
                    'get_first_iwo_task'), 'get_project_details_based_on_iwo_wbs',
                'get_all_project_team_member_details_of_iwo_wbs'),
        )

        wait_for_create_attribute_1_iwo = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_attribute_1_iwo',
            dag_runs='{{ result("create_attribute_1_iwo") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_task_uri_iwo_to_process_copy = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_task_uri_iwo_to_process_copy',
            dag_runs="{{ result('create_attribute_1_iwo') }}",
            dagrun_task_id='put_task_from_wbs',
            flatten=True
        )

        query_attribute_1_to_create_excluding_first_task_iwo = rail.QueryCollectionOperator(
            task_id='query_attribute_1_to_create_excluding_first_task_iwo',
            query="""SELECT * FROM tasktocreate_iwo WHERE name != :name""",
            query_params={
                "name": '{{ result("get_first_iwo_task")["name"] }}'
            }
        )

        is_name_present_in_excluded_first_task_iwo = rail.IfOperator(
            task_id='is_name_present_in_excluded_first_task_iwo',
            test=lambda: python_callable_method.check_key_values_present_in_list(
                'query_attribute_1_to_create_excluding_first_task_iwo', 'name'),
            yes_task='get_task_query_name_iwo',
            no_task='log_no_change_received_iwo'
        )

        get_task_query_name_iwo = rail.PythonOperator(
            task_id='get_task_query_name_iwo',
            python_callable=python_callable_method.query_result_for_copy_batch,
            op_args=['query_iwo_task_list', 'query_attribute_1_to_create_excluding_first_task_iwo',
                     'gather_task_uri_iwo_to_process_copy',
                     'attribute_1_iwo_to_create_collection']
        )

        process_copy_batch_data_iwo = rail.TriggerDagRunForEachItemOperator(
            task_id='process_copy_batch_data_iwo',
            retries=0,
            items=lambda: [custom_methods.get_conf()],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_compass_attribute_1_process_copy_data_child_{config.dag_id_postfix}',
            conf=lambda dag_run: request_payload.get_copy_data_conf(dag_run, 'get_project_details_based_on_iwo_wbs',
                                                                    rail.result('get_task_query_name_iwo'))
        )

        wait_for_process_copy_batch_data_iwo = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_copy_batch_data_iwo',
            dag_runs='{{ result("process_copy_batch_data_iwo") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        is_uri_present_in_iwo_update_list = rail.IfOperator(
            task_id='is_uri_present_in_iwo_update_list',
            test=lambda: python_callable_method.check_key_values_present_in_list(
                'query_attribute_1_iwo_to_update', 'uri'),
            yes_task='attribute_1_to_update_iwo_collection',
            no_task='log_no_change_received_iwo'
        )

        attribute_1_to_update_iwo_collection = rail.CreateCollectionOperator(
            task_id='attribute_1_to_update_iwo_collection',
            source='{{ result("query_attribute_1_iwo_to_update") }}',
            name='taskstoupdate_iwo'
        )

        process_update_task_hierarchy_iwo = rail.TriggerDagRunForEachItemOperator(
            task_id='process_update_task_hierarchy_iwo',
            retries=0,
            items=lambda: custom_methods.get_data_from_document(
                rail.result("attribute_1_to_update_iwo_collection")),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_compass_attribute_1_process_update_task_hierarchy_child_{config.dag_id_postfix}',
            conf=lambda dag_run, item: request_payload.get_update_task_conf(
                dag_run, item, 'get_project_details_based_on_iwo_wbs')
        )

        wait_for_process_update_task_hierarchy_iwo = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_task_hierarchy_iwo',
            dag_runs='{{ result("process_update_task_hierarchy_iwo") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        log_no_change_received_iwo = rail.WriteLogOperator(
            task_id='log_no_change_received_iwo',
            message='No change received for attribute 1 in IWO project',
            items=lambda: custom_methods.get_data_from_document(
                rail.result("query_attribute_1_iwo_to_skip")),
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '',
                'attributenumber': '',
                'action': 'pre-check',
                'status': 'Skipped',
                'details': 'No change received for attribute 1 in IWO project',
                'recordcount': ''
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'status': 'Error',
                # pylint: disable=line-too-long
                'details': '{{ get_error_message() }}',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'wbs ': '{{ dag_run.conf.wbs }}',
                'attributecount': '{{ dag_run.conf.attributes | length }}',
                'filename': '{{ dag_run.conf.filename }}'
            }
        )

        get_project_details_based_on_wbs >> check_wbs_exists

        check_wbs_exists >> rail.Label('Yes') >> log_wbs_not_available
        check_wbs_exists >> rail.Label('No') >> check_wbs_is_archived

        check_wbs_is_archived >> rail.Label('Yes') >> log_wbs_is_archived
        check_wbs_is_archived >> rail.Label('No') >> get_project_date_range >> assign_status_to_attributes >> \
            create_attribute_1_collection >> query_blank_attribute >> query_attribute_1_result >> attribute_1_present

        attribute_1_present >> rail.Label('Yes') >> get_all_project_team_member_details \
            >> get_children_task_details >> get_tasks_from_project >> tasks_from_project_collection >> query_task_list >> get_attribute_1_toprocess_frominput \
            >> attribute_1_to_process_collection >> query_attribute_1_to_create >> attribute_1_to_create_collection \
            >> query_attribute_1_to_update >> query_attribute_1_to_skip >> query_invalid_enddate >> query_invalid_description >> \
            [is_name_present_in_create_list, is_uri_present_in_update_list]
        attribute_1_present >> rail.Label('No') >> finish

        is_name_present_in_create_list >> rail.Label(
            'Yes') >> is_uri_not_present_in_task_list
        is_name_present_in_create_list >> rail.Label(
            'No') >> collect_logs

        is_uri_not_present_in_task_list >> rail.Label(
            'Yes') >> get_task_query_name >> process_copy_batch_data >> wait_for_process_copy_batch_data >> \
            collect_logs
        is_uri_not_present_in_task_list >> rail.Label(
            'No') >> get_first_task >> create_attribute_1 >> wait_for_create_attribute_1 >> \
            gather_task_uri_to_process_copy >> query_attribute_1_to_create_excluding_first_task \
            >> is_name_present_in_excluded_first_task

        is_name_present_in_excluded_first_task >> rail.Label(
            'Yes') >> get_task_query_name >> process_copy_batch_data
        is_name_present_in_excluded_first_task >> rail.Label(
            'No') >> collect_logs

        is_uri_present_in_update_list >> rail.Label(
            'Yes') >> attribute_1_to_update_collection >> process_update_task_hierarchy >> wait_for_process_update_task_hierarchy \
            >> collect_logs
        is_uri_present_in_update_list >> rail.Label(
            'No') >> collect_logs

        collect_logs >> [log_no_change_received, log_mandatory_fields_not_present, log_invalid_enddate, log_invalid_description] \
            >> get_iwo_wbs_project >> is_iwo_wbs_present_in_project

        is_iwo_wbs_present_in_project >> rail.Label(
            'Yes') >> get_project_details_based_on_iwo_wbs >> is_uri_present_in_iwo_wbs_project
        is_iwo_wbs_present_in_project >> rail.Label(
            'No') >> finish

        is_uri_present_in_iwo_wbs_project >> rail.Label(
            'Yes') >> get_children_task_details_of_iwo_wbs >> get_all_project_team_member_details_of_iwo_wbs \
            >> get_tasks_from_iwo_project >> tasks_from_iwo_project_collection >> query_iwo_task_list >> \
            get_iwo_attribute_1_toprocess_frominput >> attribute_1_to_iwo_process_collection >> \
            query_attribute_1_iwo_to_create >> attribute_1_iwo_to_create_collection \
            >> query_attribute_1_iwo_to_update >> query_attribute_1_iwo_to_skip >> [is_name_present_in_iwo_create_list, is_uri_present_in_iwo_update_list]
        is_uri_present_in_iwo_wbs_project >> rail.Label(
            'No') >> log_no_change_received_iwo >> finish

        is_name_present_in_iwo_create_list >> rail.Label(
            'Yes') >> is_uri_not_present_in_iwo_task_list
        is_name_present_in_iwo_create_list >> rail.Label(
            'No') >> log_no_change_received_iwo >> finish

        is_uri_not_present_in_iwo_task_list >> rail.Label(
            'Yes') >> get_task_query_name_iwo >> process_copy_batch_data_iwo >> wait_for_process_copy_batch_data_iwo >> \
            log_no_change_received_iwo >> finish
        is_uri_not_present_in_iwo_task_list >> rail.Label(
            'No') >> get_first_iwo_task >> create_attribute_1_iwo >> wait_for_create_attribute_1_iwo >> \
            gather_task_uri_iwo_to_process_copy >> query_attribute_1_to_create_excluding_first_task_iwo \
            >> is_name_present_in_excluded_first_task_iwo

        is_name_present_in_excluded_first_task_iwo >> rail.Label(
            'Yes') >> get_task_query_name_iwo >> process_copy_batch_data_iwo >> wait_for_process_copy_batch_data_iwo \
            >> log_no_change_received_iwo >> finish
        is_name_present_in_excluded_first_task_iwo >> rail.Label(
            'No') >> log_no_change_received_iwo >> finish

        is_uri_present_in_iwo_update_list >> rail.Label(
            'Yes') >> attribute_1_to_update_iwo_collection >> process_update_task_hierarchy_iwo >> wait_for_process_update_task_hierarchy_iwo >> \
            log_no_change_received_iwo >> finish
        is_uri_present_in_iwo_update_list >> rail.Label(
            'No') >> log_no_change_received_iwo >> finish

        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_attribute_1_process_wbs_child_dag)
