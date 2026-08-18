from datetime import timedelta
import rail
from dxctechnology.compass_attributes_1_and_2.utils import request_payload
from dxctechnology.compass_attributes_1_and_2.utils import python_callable_method
from dxctechnology.compass_attributes_1_and_2.utils import custom_methods

null = None

# pylint: disable=too-many-statements


def create_attribute_2_process_wbs_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_attribute_2_process_wbs_child_{config.dag_id_postfix}',
        description=f'DXC_Compass_Attribute 2 Child - Process each WBS V1.0 {config.dag_id_postfix}',
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

        check_uri_in_project_details = rail.IfOperator(
            task_id='check_uri_in_project_details',
            test=lambda: bool(rail.result('get_project_details_based_on_wbs') and
                              rail.result('get_project_details_based_on_wbs')['uri']),
            yes_task='get_project_date_range',
            no_task='log_attribute_not_added'
        )

        get_project_date_range = rail.PythonOperator(
            task_id='get_project_date_range',
            python_callable=python_callable_method.project_date_range
        )

        assign_status_to_attributes = rail.PythonOperator(
            task_id='assign_status_to_attributes',
            python_callable=python_callable_method.get_attributes_status_list
        )

        create_attribute_2_collection = rail.CreateCollectionOperator(
            task_id='create_attribute_2_collection',
            source='{{ result("assign_status_to_attributes") | to_json }}',
            name='attributes'
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
            },
            data_handler=lambda data: list(
                map(lambda assignment: assignment['name'], data))
        )

        is_name_not_present_in_task_details = rail.IfOperator(
            task_id='is_name_not_present_in_task_details',
            test='{{ result("get_children_task_details")| length < 0 }}',
            yes_task='log_no_attribute_1_in_wbs',
            no_task='query_blank_attribute'
        )

        log_no_attribute_1_in_wbs = rail.WriteLogOperator(
            task_id='log_no_attribute_1_in_wbs',
            message='No Attribute 1 is available in WBS',
            items='{{ result("create_attribute_2_collection") | to_json }}',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '{{ item.Attribute }}',
                'attributenumber': '{{ item.AttributeNumber }}',
                'action': 'process wbs',
                'status': 'Exception',
                'details': 'No Attribute 1 is available in WBS',
                'recordcount': ''
            }
        )

        query_blank_attribute = rail.QueryCollectionOperator(
            task_id='query_blank_attribute',
            query="""SELECT * FROM attributes WHERE (Attribute = '' OR AttributeNumber = '' OR EndDate = '' OR AttributeNumber != '2' OR enddatestatus = '')"""
        )

        query_invalid_attribute = rail.QueryCollectionOperator(
            task_id='query_invalid_attribute',
            query="""SELECT * FROM attributes WHERE (AttributeNumber != '1' AND AttributeNumber != '2')"""
        )

        query_attribute_2_task = rail.QueryCollectionOperator(
            task_id='query_attribute_2_task',
            # pylint: disable=line-too-long
            query="""SELECT * FROM attributes WHERE (AttributeNumber = '2' AND Attribute != '' AND EndDate != '' AND enddatestatus='valid' AND descriptionstatus='valid')"""
        )

        query_invalid_enddate = rail.QueryCollectionOperator(
            task_id='query_invalid_enddate',
            query="""SELECT * FROM attributes WHERE enddatestatus = 'invalid'"""
        )

        query_invalid_description = rail.QueryCollectionOperator(
            task_id='query_invalid_description',
            query="""SELECT * FROM attributes WHERE descriptionstatus = 'invalid'"""
        )

        log_mandatory_fields_not_present = rail.WriteLogOperator(
            task_id='log_mandatory_fields_not_present',
            message='Attribute not added/updated as one or more mandatory fields are not present.',
            items=lambda: custom_methods.get_data_from_document(
                rail.result("query_blank_attribute")),
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

        log_invalid_attribute = rail.WriteLogOperator(
            task_id='log_invalid_attribute',
            message='The received Attribute Number is invalid',
            items=lambda: custom_methods.get_data_from_document(
                rail.result("query_invalid_attribute")),
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '{{ item.Attribute }}',
                'attributenumber': '{{ item.AttributeNumber }}',
                'action': 'pre-check',
                'status': 'Exception',
                'details': 'The received Attribute Number is invalid',
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
                'details': 'Attribute not added/updated as enddate is invalid.',
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

        check_valid_attribute_2_present = rail.IfOperator(
            task_id='check_valid_attribute_2_present',
            test='{{ result("query_attribute_2_task", "length") > 0 }}',
            yes_task='get_children_task_details_wbs',
            no_task='finish'
        )

        get_children_task_details_wbs = rail.RepliconServiceOperator(
            task_id='get_children_task_details_wbs',
            endpoint='/services/TaskService1.svc/GetChildrenTaskDetails',
            data={
                'parentUri': '{{ result("get_project_details_based_on_wbs").uri }}'
            },
        )

        is_name_is_present_in_task_details_wbs = rail.IfOperator(
            task_id='is_name_is_present_in_task_details_wbs',
            test=lambda: python_callable_method.check_key_values_present_in_list(
                'get_children_task_details_wbs', 'name'),
            yes_task='get_tasks_details',
            no_task='finish'
        )

        def retrieve_task_list(task_details):
            return list(
                map(lambda x: {'name': x['name'],
                               'code': x['code'],
                               'startdate': (str(x['timeEntryDateRange']['startDate']['day']) + '/' + str(x['timeEntryDateRange']['startDate']['month'])
                                             + '/' + str(x['timeEntryDateRange']['startDate']['year'])) if bool(x['timeEntryDateRange']['startDate']) else null,
                               'enddate': (str(x['timeEntryDateRange']['endDate']['day']) + '/' + str(x['timeEntryDateRange']['endDate']['month'])
                                           + '/' + str(x['timeEntryDateRange']['endDate']['year'])) if bool(x['timeEntryDateRange']['endDate']) else null,
                               'tasktype': rail.find_first_by_attr_and_get_attr(x['customFields'], "customField.displayText", "Task Type", "text"),
                               'uri': x['uri']
                               }, rail.result(task_details)
                    )) if rail.result(task_details) else []

        get_tasks_details = rail.PythonOperator(
            task_id='get_tasks_details',
            python_callable=retrieve_task_list,
            op_args=['get_children_task_details_wbs']
        )

        create_level1_task_list = rail.CreateCollectionOperator(
            task_id='create_level1_task_list',
            source='{{ result("get_tasks_details") | to_json }}',
            columns=[
                'name',
                'code',
                'startdate',
                'enddate',
                'tasktype',
                'uri'],
            name='lvltasklist'
        )

        query_level1_task_list = rail.QueryCollectionOperator(
            task_id='query_level1_task_list',
            query="""SELECT * FROM lvltasklist WHERE tasktype = 'Attribute 1'"""
        )

        check_if_level1_task_list_present = rail.IfOperator(
            task_id='check_if_level1_task_list_present',
            test=lambda: len(custom_methods.get_data_from_document(
                rail.result("query_level1_task_list"))) > 0,
            yes_task='process_each_wbs_attribute_1',
            no_task='log_attribute_2_not_synced'
        )

        process_each_wbs_attribute_1 = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_wbs_attribute_1',
            retries=0,
            items=lambda: custom_methods.get_data_from_document(
                rail.result("query_level1_task_list")),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_compass_attribute_2_process_attribute_1_child_{config.dag_id_postfix}',
            conf=lambda dag_run, item: request_payload.get_process_each_wbs_attribute_1(
                dag_run, item, 'get_project_details_based_on_wbs')
        )

        wait_for_process_each_wbs_attribute_1 = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_wbs_attribute_1',
            dag_runs='{{ result("process_each_wbs_attribute_1") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_oef_iwo_wbs = rail.RepliconServiceOperator(
            task_id='get_oef_iwo_wbs',
            endpoint='/services/ObjectExtensionService1.svc/GetObjectExtensionFieldValues',
            data={
                'objectUri': '{{ result("get_project_details_based_on_wbs").uri }}',
                'bindingContextUri': null},
        )

        def get_iwo_wbs_value():
            return rail.find_first_by_attr_and_get_attr(
                rail.result('get_oef_iwo_wbs'), 'definition.displayText',
                'IWO WBS Element', 'textValue') if rail.result('get_oef_iwo_wbs') else null

        get_iwo_wbs_project = rail.PythonOperator(
            task_id='get_iwo_wbs_project',
            python_callable=get_iwo_wbs_value
        )

        is_iwo_wbs_value_present = rail.IfOperator(
            task_id='is_iwo_wbs_value_present',
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

        is_uri_present_in_iwo_wbs = rail.IfOperator(
            task_id='is_uri_present_in_iwo_wbs',
            test=lambda: bool(rail.result('get_project_details_based_on_iwo_wbs')
                              and rail.result('get_project_details_based_on_iwo_wbs')['uri']),
            yes_task='get_all_iwo_project_team_member_details',
            no_task='log_attribute_not_added_iwo'
        )

        get_all_iwo_project_team_member_details = rail.RepliconServiceOperator(
            task_id='get_all_iwo_project_team_member_details',
            endpoint='/services/ProjectService1.svc/GetAllProjectTeamMemberDetails',
            data={
                'projectUri': '{{ result("get_project_details_based_on_iwo_wbs").uri }}',
                'asOfDate': null},
            data_handler=lambda data: list(
                map(lambda assignment: assignment['resource']['uri'], data))
        )

        get_children_task_details_iwo_wbs = rail.RepliconServiceOperator(
            task_id='get_children_task_details_iwo_wbs',
            endpoint='/services/TaskService1.svc/GetChildrenTaskDetails',
            data={
                'parentUri': '{{ result("get_project_details_based_on_iwo_wbs").uri }}'
            },
        )

        is_name_is_present_in_task_details_iwo_wbs = rail.IfOperator(
            task_id='is_name_is_present_in_task_details_iwo_wbs',
            test=lambda: python_callable_method.check_key_values_present_in_list(
                'get_children_task_details_iwo_wbs', 'name'),
            yes_task='get_tasks_details_iwo',
        )

        get_tasks_details_iwo = rail.PythonOperator(
            task_id='get_tasks_details_iwo',
            python_callable=retrieve_task_list,
            op_args=['get_children_task_details_iwo_wbs']
        )

        create_iwo_level1_task_list = rail.CreateCollectionOperator(
            task_id='create_iwo_level1_task_list',
            source='{{ result("get_tasks_details_iwo") | to_json }}',
            columns=[
                'name',
                'code',
                'startdate',
                'enddate',
                'tasktype',
                'uri'],
            name='IWOlvltasklist'
        )

        query_iwo_level1_task_list = rail.QueryCollectionOperator(
            task_id='query_iwo_level1_task_list',
            query="""SELECT * FROM IWOlvltasklist WHERE tasktype = 'Attribute 1'"""
        )

        check_if_level1_task_list_iwo_present = rail.IfOperator(
            task_id='check_if_level1_task_list_iwo_present',
            test=lambda: len(custom_methods.get_data_from_document(
                rail.result("query_iwo_level1_task_list"))) > 0,
            yes_task='process_each_iwo_wbs_attribute_1',
            no_task='log_attribute_2_not_synced_iwo'
        )

        process_each_iwo_wbs_attribute_1 = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_iwo_wbs_attribute_1',
            retries=0,
            items=lambda: custom_methods.get_data_from_document(
                rail.result("query_iwo_level1_task_list")),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_compass_attribute_2_process_attribute_1_child_{config.dag_id_postfix}',
            conf=lambda dag_run, item: request_payload.get_process_each_wbs_attribute_1(
                dag_run, item, 'get_project_details_based_on_iwo_wbs')
        )

        wait_for_process_each_iwo_wbs_attribute_1 = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_iwo_wbs_attribute_1',
            dag_runs='{{ result("process_each_iwo_wbs_attribute_1") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        log_attribute_2_not_synced = rail.WriteLogOperator(
            task_id='log_attribute_2_not_synced',
            message='Attribute 2 not synced as there are no Attribute 1\'s in the WBS',
            items='{{ dag_run.conf.attributes | to_json }}',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '{{ item.Attribute }}',
                'attributenumber': '{{ item.AttributeNumber }}',
                'action': 'pre-check',
                'status': 'Skipped',
                'details': 'Attribute 2 not synced as there are no Attribute 1\'s in the WBS',
                'recordcount': ''
            }
        )

        log_attribute_not_added = rail.WriteLogOperator(
            task_id='log_attribute_not_added',
            message='Attribute not add/updated as the required WBS is not present in Replicon',
            items='{{ dag_run.conf.attributes | to_json }}',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '{{ item.Attribute }}',
                'attributenumber': '{{ item.AttributeNumber }}',
                'action': 'pre-check',
                'status': 'Skipped',
                'details': 'Attribute not add/updated as the required WBS is not present in Replicon',
                'recordcount': ''
            }
        )

        log_attribute_not_added_iwo = rail.WriteLogOperator(
            task_id='log_attribute_not_added_iwo',
            message='Attribute not add/updated as the required WBS is not present in Replicon',
            items='{{ dag_run.conf.attributes | to_json }}',
            severity='Exception',
            properties={
                'wbs': '{{ result("get_iwo_wbs_project") }}',
                'attributename': '{{ item.Attribute }}',
                'attributenumber': '{{ item.AttributeNumber }}',
                'action': 'pre-check',
                'status': 'Skipped',
                'details': 'Attribute not add/updated as the required WBS is not present in Replicon',
                'recordcount': ''
            }
        )

        log_attribute_2_not_synced_iwo = rail.WriteLogOperator(
            task_id='log_attribute_2_not_synced_iwo',
            message='Attribute 2 not synced as there are no Attribute 1\'s in the WBS',
            items='{{ dag_run.conf.attributes | to_json }}',
            severity='Exception',
            properties={
                'wbs': '{{ result("get_iwo_wbs_project") }}',
                'attributename': '{{ item.Attribute }}',
                'attributenumber': '{{ item.AttributeNumber }}',
                'action': 'pre-check',
                'status': 'Skipped',
                'details': 'Attribute 2 not synced as there are no Attribute 1\'s in the WBS',
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
            items='{{ dag_run.conf.attributes | to_json }}',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'attributename': '{{ item.Attribute }}',
                'attributenumber': '{{ item.AttributeNumber }}',
                'action': 'pre-check',
                'status': 'Error',
                # pylint: disable=line-too-long
                'details': '{{ get_error_message() }}',
                'recordcount': ''
            }
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

        get_project_details_based_on_wbs >> check_uri_in_project_details

        check_uri_in_project_details >> rail.Label(
            'Yes') >> get_project_date_range >> assign_status_to_attributes >> create_attribute_2_collection >> \
            get_all_project_team_member_details >> get_children_task_details >> is_name_not_present_in_task_details
        check_uri_in_project_details >> rail.Label(
            'No') >> log_attribute_not_added >> finish

        is_name_not_present_in_task_details >> rail.Label(
            'Yes') >> log_no_attribute_1_in_wbs >> finish
        is_name_not_present_in_task_details >> rail.Label(
            'No') >> query_blank_attribute >> query_invalid_attribute >> query_attribute_2_task >> \
            query_invalid_enddate >> query_invalid_description >> \
            [log_mandatory_fields_not_present, log_invalid_attribute, log_invalid_enddate, log_invalid_description] >> \
            check_valid_attribute_2_present

        check_valid_attribute_2_present >> rail.Label(
            'Yes') >> get_children_task_details_wbs >> is_name_is_present_in_task_details_wbs
        check_valid_attribute_2_present >> rail.Label(
            'No') >> finish

        is_name_is_present_in_task_details_wbs >> rail.Label(
            'Yes') >> get_tasks_details >> create_level1_task_list >> query_level1_task_list >> check_if_level1_task_list_present
        is_name_is_present_in_task_details_wbs >> rail.Label(
            'No') >> finish

        check_if_level1_task_list_present >> rail.Label(
            'Yes') >> process_each_wbs_attribute_1 >> wait_for_process_each_wbs_attribute_1 >> \
            get_oef_iwo_wbs >> get_iwo_wbs_project >> is_iwo_wbs_value_present
        check_if_level1_task_list_present >> rail.Label(
            'No') >> log_attribute_2_not_synced >> finish

        is_iwo_wbs_value_present >> rail.Label(
            'Yes') >> get_project_details_based_on_iwo_wbs >> is_uri_present_in_iwo_wbs
        is_iwo_wbs_value_present >> rail.Label(
            'No') >> finish

        is_uri_present_in_iwo_wbs >> rail.Label(
            'Yes') >> get_all_iwo_project_team_member_details >> get_children_task_details_iwo_wbs >> is_name_is_present_in_task_details_iwo_wbs
        is_uri_present_in_iwo_wbs >> rail.Label(
            'No') >> log_attribute_not_added_iwo >> finish

        is_name_is_present_in_task_details_iwo_wbs >> rail.Label(
            'Yes') >> get_tasks_details_iwo >> create_iwo_level1_task_list >> query_iwo_level1_task_list >> check_if_level1_task_list_iwo_present

        check_if_level1_task_list_iwo_present >> rail.Label(
            'Yes') >> process_each_iwo_wbs_attribute_1 >> wait_for_process_each_iwo_wbs_attribute_1 >> finish
        check_if_level1_task_list_iwo_present >> rail.Label(
            'No') >> log_attribute_2_not_synced_iwo >> finish

        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_attribute_2_process_wbs_child_dag)
