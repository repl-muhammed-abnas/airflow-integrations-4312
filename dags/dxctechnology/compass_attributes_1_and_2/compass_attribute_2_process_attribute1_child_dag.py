from datetime import timedelta
import rail

from dxctechnology.compass_attributes_1_and_2.utils import python_callable_method
from dxctechnology.compass_attributes_1_and_2.utils import custom_methods

null = None

# pylint: disable=too-many-statements


def create_attribute_2_process_attribute1_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_attribute_2_process_attribute_1_child_{config.dag_id_postfix}',
        description=f'DXC_Compass_Attribute 2 Child - Process each Attribute 1 V1.0 {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        query_attributes_collection = rail.QueryCollectionOperator(
            task_id='query_attributes_collection',
            query="""SELECT * FROM attributes"""
        )

        check_wbs_uri_present = rail.IfOperator(
            task_id='check_wbs_uri_present',
            test='{{ dag_run.conf.wbsuri | is_truthy }}',
            yes_task='get_all_project_team_member_details',
            no_task='log_attribute_not_added'
        )

        get_all_project_team_member_details = rail.RepliconServiceOperator(
            task_id='get_all_project_team_member_details',
            endpoint='/services/ProjectService1.svc/GetAllProjectTeamMemberDetails',
            data={
                'projectUri': '{{ dag_run.conf.wbsuri }}',
                'asOfDate': None},
            data_handler=lambda data: list(
                map(lambda assignment: assignment['resource']['uri'], data))
        )

        get_children_task_details = rail.RepliconServiceOperator(
            task_id='get_children_task_details',
            endpoint='/services/TaskService1.svc/GetChildrenTaskDetails',
            data={
                'parentUri': '{{ dag_run.conf.attribute1uri }}'
            },
        )

        get_tasks_from_project = rail.PythonOperator(
            task_id='get_tasks_from_project',
            python_callable=python_callable_method.retrieve_task_list,
            op_args=['get_children_task_details']
        )

        task_list_collection = rail.CreateCollectionOperator(
            task_id='task_list_collection',
            source='{{ result("get_tasks_from_project") | to_json }}',
            columns=[
                'name',
                'code',
                'enddate',
                'oef',
                'uri',
                'md5'],
            name='attribute2inreplicon'
        )

        query_attribute_2_data = rail.QueryCollectionOperator(
            task_id='query_attribute_2_data',
            query="""SELECT * FROM attribute2inreplicon WHERE oef = 'Attribute 2'"""
        )

        query_attribute_2_tasks = rail.QueryCollectionOperator(
            task_id='query_attribute_2_tasks',
            query="""SELECT * FROM attributes WHERE
            (AttributeNumber = '2' AND Attribute != '' AND EndDate != '' AND enddatestatus='valid' AND descriptionstatus='valid')"""
        )

        get_attribute_2_toprocess = rail.PythonOperator(
            task_id='get_attribute_2_toprocess',
            python_callable=python_callable_method.retrive_attributes_from_input,
            op_args=['query_attribute_2_tasks', 'query_attribute_2_data']
        )

        attribute_2_to_process_collection = rail.CreateCollectionOperator(
            task_id='attribute_2_to_process_collection',
            source='{{ result("get_attribute_2_toprocess") | to_json }}',
            name='attribute2toprocess'
        )

        query_attribute_2_to_create = rail.QueryCollectionOperator(
            task_id='query_attribute_2_to_create',
            query="""SELECT * FROM attribute2toprocess WHERE uri IS NULL AND enddatestatus = 'valid' AND descriptionstatus = 'valid'"""
        )

        query_attribute_2_to_update = rail.QueryCollectionOperator(
            task_id='query_attribute_2_to_update',
            query="""SELECT * FROM attribute2toprocess WHERE uri IS NOT NULL AND enddatestatus = 'valid' AND descriptionstatus = 'valid'
            AND md5 NOT IN (SELECT DISTINCT md5 FROM attribute2inreplicon)"""
        )

        query_attribute_2_to_skip = rail.QueryCollectionOperator(
            task_id='query_attribute_2_to_skip',
            query="""SELECT * FROM attribute2toprocess WHERE uri IS NOT NULL AND enddatestatus = 'valid' AND descriptionstatus = 'valid'
            AND md5 IN (SELECT DISTINCT md5 FROM attribute2inreplicon)"""
        )

        log_no_attribute2_change = rail.WriteLogOperator(
            task_id='log_no_attribute2_change',
            message='No change received for attribute 2',
            items=lambda: custom_methods.get_data_from_document(
                rail.result("query_attribute_2_to_skip")),
            properties={
                'wbs': '{{ dag_run.conf.records.wbs }}',
                'attributename': '{{ item.name }}',
                'attributenumber': '{{ item.number }}',
                'action': 'pre-check',
                'status': 'Skipped',
                'details': 'No change received for attribute 2'
            }
        )

        log_attribute_not_added = rail.WriteLogOperator(
            task_id='log_attribute_not_added',
            message='Attribute not add/updated as the required WBS is not present in Replicon',
            items=lambda: custom_methods.get_data_from_document(
                rail.result("query_attributes_record_collection")),
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.records.wbs }}',
                'attributename': '{{ dag_run.conf.records.attribute.Attribute }}',
                'attributenumber': '{{ dag_run.conf.records.attribute.AttributeNumber }}',
                'action': 'pre-check',
                'status': 'Skipped',
                'details': 'Attribute not add/updated as the required WBS is not present in Replicon',
            }
        )

        #pylint: disable=too-many-arguments
        def get_task_2_conf(dag_run, item, taskuri=False, parenttaskstartdate=False,
                            parenttaskendate=False, level0taskuri=False):
            return {
                'level': item['number'],
                'name': item['name'],
                'description': item['code'] if item['code'] else null,
                'enddate': custom_methods.get_end_date(dag_run.conf['wbsenddate'], item['enddate'], '%d/%m/%Y'),
                'startdate': dag_run.conf['wbsstartdate'],
                'projecturi': dag_run.conf['wbsuri'],
                'taskuri': null if not taskuri else item['uri'],
                'parenttaskuri': dag_run.conf['attribute1uri'],
                'tasktypeuri': dag_run.conf['tasktypeuri'],
                'tasktypeoptionuri': item['tasktype'],
                'iwowbsprojecturi': null,
                'iwostartdate': null,
                'iwoenddate': null,
                'iwoparenttaskuri': null,
                'userlist': custom_methods.get_userlist('get_all_project_team_member_details'),
                'iwouserlist': custom_methods.get_userlist('get_all_project_team_member_details'),
                'parenttaskstartdate': null if not parenttaskstartdate else dag_run.conf['attribute1startdate'],
                'parenttaskendate': null if not parenttaskendate else dag_run.conf['attribute1enddate'],
                'isiwoproject': null,
                'projectname': dag_run.conf['records']['wbs'],
                'level0taskuri': null if not level0taskuri else dag_run.conf['attribute1uri'],
                'filename': dag_run.conf['filename']
            }

        create_attribute_2 = rail.TriggerDagRunForEachItemOperator(
            task_id='create_attribute_2',
            retries=0,
            items=lambda: custom_methods.get_data_from_document(
                rail.result('query_attribute_2_to_create')),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_compass_attribute_2_create_task_child_{config.dag_id_postfix}',
            conf=lambda dag_run, item: get_task_2_conf(
                dag_run, item, True, True, True, True)
        )

        wait_for_create_attribute_2 = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_attribute_2',
            dag_runs='{{ result("create_attribute_2") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        update_attribute_2 = rail.TriggerDagRunForEachItemOperator(
            task_id='update_attribute_2',
            retries=0,
            items=lambda: custom_methods.get_data_from_document(
                rail.result('query_attribute_2_to_update')),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_compass_attribute_2_update_task_child_{config.dag_id_postfix}',
            conf=lambda dag_run, item: get_task_2_conf(
                dag_run, item, True, True, True, True)
        )

        wait_for_update_attribute_2 = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_attribute_2',
            dag_runs='{{ result("update_attribute_2") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
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
                'wbs ': '{{ dag_run.conf.records.wbs }}',
                'attributecount': '{{ dag_run.conf.records.attributes | length }}',
                'filename': '{{ dag_run.conf.filename }}'
            }
        )

        query_attributes_collection >> check_wbs_uri_present

        check_wbs_uri_present >> rail.Label('Yes') >> get_all_project_team_member_details \
            >> get_children_task_details >> get_tasks_from_project >> task_list_collection \
            >> query_attribute_2_data >> query_attribute_2_tasks >> get_attribute_2_toprocess \
            >> attribute_2_to_process_collection >> query_attribute_2_to_create >> query_attribute_2_to_update \
            >> query_attribute_2_to_skip >> log_no_attribute2_change >> create_attribute_2 >> wait_for_create_attribute_2 \
            >> update_attribute_2 >> wait_for_update_attribute_2 >> finish

        check_wbs_uri_present >> rail.Label(
            'No') >> log_attribute_not_added >> finish

        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_attribute_2_process_attribute1_child_dag)
