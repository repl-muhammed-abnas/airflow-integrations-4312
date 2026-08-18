from datetime import timedelta
import rail
from dxctechnology.compass_attribute_1_2_new_v1 import request_payload
from dxctechnology.compass_attribute_1_2_new_v1 import response_filter


def create_child_sync_attribute_1_2_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_child_sync_attributes_1_2_file_v1{dag_id_postfix}_{config.sub_erp}_{config.attribute}',
        description=f'Sync Attributes 1 or 2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_sync_attribute_1_2_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        query_project_attribute_entires = rail.QueryCollectionOperator(
            task_id="query_project_attribute_entires",
            name="wbsattributeentries",
            query="SELECT * FROM eligibleattributewbsrecords WHERE WBS = :WBS",
            query_params={
                "WBS": "{{dag_run.conf.wbs}}"
            }
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=request_payload.get_project_details,
            response_filter=response_filter.map_get_project_details
        )

        is_wbs_present = rail.IfOperator(
            task_id="is_wbs_present",
            test="{{ result('get_project_details') | length > 0}}",
            yes_task="get_all_columns",
            no_task="log_wbs_not_present",
        )

        get_all_columns = rail.RepliconServiceOperator(
            task_id="get_all_columns",
            endpoint="services/ProjectListService1.svc/GetAllColumns",
            data={},
            response_filter=response_filter.map_parent_column_uri
        )

        get_all_filter_defination = rail.RepliconServiceOperator(
            task_id="get_all_filter_defination",
            endpoint="services/ProjectListService1.svc/GetAllFilterDefinitions",
            data={},
            response_filter=response_filter.map_parent_wbs_oef_uri
        )

        get_all_child_wbs_details = rail.RepliconServiceOperator(
            task_id="get_all_child_wbs_details",
            endpoint="services/ProjectListService1.svc/GetData",
            data=request_payload.get_child_wbs_payload,
            response_filter=response_filter.map_child_wbs
        )

        is_child_wbs_present = rail.IfOperator(
            task_id="is_child_wbs_present",
            test='{{ result("get_all_child_wbs_details") | length > 0 }}',
            yes_task='sync_child_wbs_attribute_1_2',
            no_task='is_wbs_start_date_empty',
        )

        sync_child_wbs_attribute_1_2 = rail.TriggerDagRunForEachItemOperator(
            task_id='sync_child_wbs_attribute_1_2',
            retries=0,
            items=lambda: rail.result('get_all_child_wbs_details'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_child_wbs_sync_attributes_1_2_file_v1_{config.instance}_{config.sub_erp}_{config.attribute}',
            conf=lambda item: {
                'wbs': request_payload.get_conf()['wbs'],
                'attribute_1_2_uri': request_payload.get_conf()['attribute_1_2_uri'],
                'attribute_number': request_payload.get_conf()['attribute_number'],
                'childWbs': item.split(" - ")[0].strip()
            }
        )

        wait_for_sync_child_wbs_attribute_1_2 = rail.WaitForDagRunsSensor(
            task_id='wait_for_sync_child_wbs_attribute_1_2',
            dag_runs='{{ result("sync_child_wbs_attribute_1_2") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        log_wbs_not_present = rail.WriteLogOperator(
            task_id='log_wbs_not_present',
            message="All attributes failed to sync, since WBS not available in Replicon",
            properties={
                'Level': "Project",
                'wbs': '{{dag_run.conf.wbs}}',
                'attributename': "",
                'attributenumber': "",
                'action': 'Skipped',
                'status': "Exception",
                'recordcount': "{{result('query_project_attribute_entires','length')}}",
            }
        )

        is_wbs_start_date_empty = rail.IfOperator(
            task_id="is_wbs_start_date_empty",
            test=lambda: bool(rail.result('get_project_details')[
                              0]['start_date_year']),
            yes_task="is_wbs_in_progress",
            no_task="log_wbs_start_date_empty",
        )

        log_wbs_start_date_empty = rail.WriteLogOperator(
            task_id='log_wbs_start_date_empty',
            message="All attributes failed to sync, since WBS Start Date is Empty",
            properties={
                'Level': "Project",
                'wbs': '{{dag_run.conf.wbs}}',
                'attributename': "",
                'attributenumber': "",
                'action': 'Skipped',
                'status': "Exception",
                'recordcount': "{{result('query_project_attribute_entires','length')}}",
            }
        )

        is_wbs_in_progress = rail.IfOperator(
            task_id="is_wbs_in_progress",
            test=lambda: rail.result('get_project_details')[
                0]['status'] == "In Progress",
            yes_task="sync_each_atrribute_project_level",
            no_task="log_wbs_not_in_progress",
        )

        log_wbs_not_in_progress = rail.WriteLogOperator(
            task_id='log_wbs_not_in_progress',
            message="All attributes were skipped, since this WBS is not in In Progress status.",
            properties={
                'Level': "Project",
                'wbs': '{{dag_run.conf.wbs}}',
                'attributename': "",
                'attributenumber': "",
                'action': 'pre-check',
                'status': "Exception",
                'recordcount': "{{result('query_project_attribute_entires','length')}}",
            }
        )

        sync_each_atrribute_project_level = rail.TriggerDagRunForEachItemOperator(
            task_id='sync_each_atrribute_project_level',
            retries=0,
            items="{{ result('query_project_attribute_entires') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_child_sync_each_attribute_project_level_v1_{config.instance}_{config.sub_erp}_{config.attribute}',
            conf=lambda item: {
                'WBS': item['WBS'],
                'AttributeNumber': item['AttributeNumber'],
                'attribute_value': item['Attribute'],
                'Description': item['Description'] if item['Description'] else "",
                'EndDate': item['EndDate'],
                'attribute_1_2_uri': request_payload.get_conf()['attribute_1_2_uri'],
                'start_date_year': rail.result('get_project_details')[0]['start_date_year'],
                'start_date_month': rail.result('get_project_details')[0]['start_date_month'],
                'start_date_day': rail.result('get_project_details')[0]['start_date_day'],
                'end_date_year': rail.result('get_project_details')[0]['end_date_year'],
                'end_date_month': rail.result('get_project_details')[0]['end_date_month'],
                'end_date_day': rail.result('get_project_details')[0]['end_date_day'],
            }
        )

        wait_for_sync_each_atrribute_project_level = rail.WaitForDagRunsSensor(
            task_id='wait_for_sync_each_atrribute_project_level',
            dag_runs='{{ result("sync_each_atrribute_project_level") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                'Level': "Project",
                'wbs': "{{dag_run.conf.wbs}}",
                'attributename': "na",
                'attributenumber': "{{dag_run.conf.attribute_number}}",
                'action': 'na',
                'status': "Error",
                'recordcount': '1',
            })

        query_project_attribute_entires >> get_project_details
        get_project_details >> is_wbs_present
        is_wbs_present >> rail.Label(
            "NO") >> log_wbs_not_present >> catch_and_log_errors
        is_wbs_present >> rail.Label(
            "YES") >> get_all_columns >> get_all_filter_defination >> get_all_child_wbs_details >> is_child_wbs_present
        is_child_wbs_present >> rail.Label(
            "YES") >> sync_child_wbs_attribute_1_2 >> wait_for_sync_child_wbs_attribute_1_2 >> is_wbs_start_date_empty
        is_child_wbs_present >> rail.Label("NO") >> is_wbs_start_date_empty
        is_wbs_start_date_empty >> rail.Label("YES") >> is_wbs_in_progress
        is_wbs_start_date_empty >> rail.Label(
            "NO") >> log_wbs_start_date_empty >> catch_and_log_errors
        is_wbs_in_progress >> rail.Label(
            "NO") >> log_wbs_not_in_progress >> catch_and_log_errors
        is_wbs_in_progress >> rail.Label(
            "YES") >> sync_each_atrribute_project_level
        sync_each_atrribute_project_level >> wait_for_sync_each_atrribute_project_level >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_sync_attribute_1_2_dag)
