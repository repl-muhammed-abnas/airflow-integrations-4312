from datetime import timedelta
import json
import rail
from airflow.models import Variable
from dxctechnology.c1_labour_types_import_v1 import request_payload
from dxctechnology.c1_labour_types_import_v1 import response_filter
from dxctechnology.c1_labour_types_import_v1 import python_callable_method

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_labour_types_import_v1/config.py


# pylint: disable=too-many-statements
def create_child_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_c1_labour_types_process_distinct_wbs_item{dag_id_postfix}_v1',
        description=f'DXC_C1_Labour Types Automation Child V2.0 - B1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_wbs_max_active_runs,
    ) as dag:

        key_name_space = "DXC_WBSLabourTypeDetails"
        project_name = "{{ dag_run.conf.wbs }}"

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='view_dagrun_config'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='view_dagrun_config',
            end_task='catch_and_log_errors',
        )

        view_dagrun_config = rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        query_billing_rates_for_wbs = rail.QueryCollectionOperator(
            task_id="query_billing_rates_for_wbs",
            name="query_billing_rates_for_wbs",
            query="""SELECT DISTINCT wbs, labourtypes, description FROM inputcombineddata WHERE wbs=:wbs""",
            query_params={
                "wbs": project_name
            }
        )

        get_assignable_billing_rates = rail.PythonOperator(
            task_id="get_assignable_billing_rates",
            python_callable=python_callable_method.get_assignable_billing_rates
        )

        assignable_billing_rates_for_wbs = rail.CreateCollectionOperator(
            task_id="assignable_billing_rates_for_wbs",
            source=lambda: rail.result('get_assignable_billing_rates'),
            name="assignablebillingrates"
        )

        get_project_info_from_project_service = rail.RepliconServiceOperator(
            task_id='get_project_info_from_project_service',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=request_payload.get_project_payload,
            response_filter=response_filter.map_project_response
        )

        is_project_not_exists = rail.IfOperator(
            task_id="is_project_not_exists",
            test=lambda: not bool(rail.result(
                'get_project_info_from_project_service')),
            yes_task="log_project_doesnt_exist",
            no_task="get_data_for_compass_child",
        )

        log_project_doesnt_exist = rail.WriteLogOperator(
            task_id="log_project_doesnt_exist",
            message='WBS Element is not present in Replicon',
            items='{{ result("assignable_billing_rates_for_wbs") }}',
            severity='Exception',
            properties={
                'wbs': project_name,
                'billingrate': '{{ item.name }}',
                'status': 'Exception'
            }
        )

        get_data_for_compass_child = rail.RepliconServiceOperator(
            task_id='get_data_for_compass_child',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=request_payload.get_project_list_payload,
            response_filter=response_filter.get_filtered_data
        )

        has_compass_child = rail.IfOperator(
            task_id="has_compass_child",
            test=lambda: bool(rail.result('get_data_for_compass_child')),
            yes_task="process_compass_child",
            no_task="get_project_info_from_import_service",
        )

        process_compass_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_compass_child',
            retries=0,
            items="{{ result('get_data_for_compass_child') | to_json }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_c1_labour_types_compass_child_process_wbs{dag_id_postfix}_v1',
            conf=request_payload.get_process_compass_child_conf
        )

        get_project_info_from_import_service = rail.RepliconServiceOperator(
            task_id='get_project_info_from_import_service',
            endpoint='/services/ImportService1.svc/BulkGetProjects2',
            data=request_payload.get_project_payload
        )

        get_key_value_from_generic_key_store = rail.RepliconServiceOperator(
            task_id='get_key_value_from_generic_key_store',
            endpoint='/services/GenericKeyValueStoreService1.svc/GetKeyValue',
            data=request_payload.get_key_value_from_wbs(
                key_name_space, project_name)
        )

        billing_rates_to_assign_in_blob = rail.PythonOperator(
            task_id="billing_rates_to_assign_in_blob",
            python_callable=python_callable_method.get_billing_rates_to_assign_in_blob,
            op_args=[
                'assignable_billing_rates_for_wbs',
                'get_project_info_from_import_service']
        )

        get_project_billing_rates_to_assign = rail.PythonOperator(
            task_id="get_project_billing_rates_to_assign",
            python_callable=python_callable_method.get_project_billing_rates_to_assign,
            op_args=[
                project_name,
                'assignable_billing_rates_for_wbs',
                '{{ result("get_project_info_from_import_service").results[0].project.uri }}',
                'billing_rates_to_assign_in_blob']
        )

        has_no_key_in_generic_store = rail.IfOperator(
            task_id='has_no_key_in_generic_store',
            test=lambda: not bool(rail.result(
                'get_key_value_from_generic_key_store')),
            yes_task="add_projectbillingratesassign_to_labour_types_blob",
            no_task="has_json_value_for_wbs"
        )

        add_projectbillingratesassign_to_labour_types_blob = rail.CreateCollectionOperator(
            task_id="add_projectbillingratesassign_to_labour_types_blob",
            name="add_projectbillingratesassign_to_labour_types_blob",
            source=lambda: rail.result('get_project_billing_rates_to_assign')
        )

        put_key_values_to_wbs = rail.RepliconServiceOperator(
            task_id='put_key_values_to_wbs',
            endpoint='/services/GenericKeyValueStoreService1.svc/PutKeyValue',
            data=request_payload.put_key_value(
                key_name_space,
                project_name,
                "{{ result('get_project_billing_rates_to_assign') | to_json }}")
        )

        has_json_value_for_wbs = rail.IfOperator(
            task_id='has_json_value_for_wbs',
            test="{{ result('get_key_value_from_generic_key_store') | is_truthy and \
                result('get_key_value_from_generic_key_store').jsonValue | from_json | length > 0 and \
                    result('get_key_value_from_generic_key_store').jsonValue | from_json | first_or_default | \
                        attr_or_default('wbsUri') | is_truthy }}",
            yes_task="write_existing_blob_records",
            no_task="add_projectbillingratesassign_to_labour_types_blob2"
        )

        write_existing_blob_records = rail.WriteCSVFileOperator(
            task_id="write_existing_blob_records",
            source=lambda: json.loads(rail.result('get_key_value_from_generic_key_store')[
                'jsonValue']),
            header=[
                'wbs',
                'labourtype',
                'labourtypeuri',
                'startdate',
                'enddate',
                'id'],
            row=request_payload.get_blob_rows
        )

        existing_blob_records = rail.CreateCollectionOperator(
            task_id='existing_blob_records',
            source="{{ result('write_existing_blob_records') }}",
            name="existingblobrecords"
        )

        write_new_blob_records = rail.WriteCSVFileOperator(
            task_id="write_new_blob_records",
            source=lambda: rail.result('get_project_billing_rates_to_assign'),
            header=[
                'wbs',
                'labourtype',
                'labourtypeuri',
                'startdate',
                'enddate',
                'id'],
            row=request_payload.get_blob_rows
        )

        new_blob_records = rail.CreateCollectionOperator(
            task_id='new_blob_records',
            source="{{ result('write_new_blob_records') }}",
            name="newblobrecords"
        )

        check_new_blob_records = rail.QueryCollectionOperator(
            task_id='check_new_blob_records',
            query='SELECT * FROM newblobrecords WHERE id NOT IN (SELECT DISTINCT id FROM existingblobrecords)',
            name="checknewblobrecords"
        )

        md5_check = rail.IfOperator(
            task_id="md5_check",
            test="{{ result('check_new_blob_records', 'length') > 0 }}",
            yes_task='existing_blobs_not_in_new_blob',
            no_task='get_labour_type_blobs'
        )

        add_projectbillingratesassign_to_labour_types_blob2 = rail.CreateCollectionOperator(
            task_id="add_projectbillingratesassign_to_labour_types_blob2",
            name="add_projectbillingratesassign_to_labour_types_blob2",
            source=lambda: rail.result('get_project_billing_rates_to_assign')
        )

        existing_blobs_not_in_new_blob = rail.QueryCollectionOperator(
            task_id='existing_blobs_not_in_new_blob',
            query='SELECT * FROM existingblobrecords WHERE labourtype NOT IN (SELECT DISTINCT labourtype FROM newblobrecords)',
            name="existingblobsnotinnewblob"
        )

        new_blob_records_same_as_existing_blobs = rail.QueryCollectionOperator(
            task_id='new_blob_records_same_as_existing_blobs',
            query='SELECT * FROM newblobrecords WHERE id IN (SELECT DISTINCT id FROM existingblobrecords)',
            name="existingblobssameasnewblob"
        )

        join_new_existing_records_to_labour_type_blob = rail.QueryCollectionOperator(
            task_id='join_new_existing_records_to_labour_type_blob',
            name="join_new_existing_records_to_labour_type_blob",
            query='SELECT * FROM existingblobsnotinnewblob UNION SELECT * FROM existingblobssameasnewblob UNION SELECT * FROM checknewblobrecords'
        )

        get_labour_type_blobs = rail.PythonOperator(
            task_id="get_labour_type_blobs",
            python_callable=python_callable_method.get_labour_type_blobs,
            op_args=['{{ result("get_project_info_from_import_service").results[0].project.uri }}',
                     'join_new_existing_records_to_labour_type_blob', 'add_projectbillingratesassign_to_labour_types_blob2']
        )

        has_labour_type_blobs = rail.IfOperator(
            task_id="has_labour_type_blobs",
            test="{{ result('get_labour_type_blobs') | length > 0 }}",
            yes_task='put_key_value_to_generic_store_from_doc',
            no_task='has_billing_rates_for_project'
        )

        put_key_value_to_generic_store_from_doc = rail.RepliconServiceOperator(
            task_id='put_key_value_to_generic_store_from_doc',
            endpoint='/services/GenericKeyValueStoreService1.svc/PutKeyValue',
            data=request_payload.put_key_value(
                key_name_space, project_name, "{{ result('get_labour_type_blobs') | to_json }}")
        )

        has_billing_rates_for_project = rail.IfOperator(
            task_id="has_billing_rates_for_project",
            test="{{ result('assignable_billing_rates_for_wbs', 'length') > 0 }}",
            yes_task='has_billing_rates_not_assigned_to_project',
            no_task='catch_and_log_errors'
        )

        has_billing_rates_not_assigned_to_project = rail.IfOperator(
            task_id="has_billing_rates_not_assigned_to_project",
            test='{{ result("billing_rates_to_assign_in_blob") | filter_by_attr("availableinproject", "equals", "No") | length > 0 }}',
            yes_task='add_billing_rates',
            no_task='has_billing_rates_already_assigned_to_project'
        )

        has_billing_rates_already_assigned_to_project = rail.IfOperator(
            task_id="has_billing_rates_already_assigned_to_project",
            test='{{ result("billing_rates_to_assign_in_blob") | filter_by_attr("availableinproject", "equals", "Yes") | length > 0 }}',
            yes_task='log_billing_rates_already_in_wbs',
            no_task='catch_and_log_errors'
        )

        add_billing_rates = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_billing_rates',
            endpoint='/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers',
            items='{{ result("billing_rates_to_assign_in_blob") | filter_by_attr("availableinproject", "equals", "No") | to_json }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=request_payload.update_billing_rates_for_team_members(
                '{{ result("get_project_info_from_import_service").results[0].project.uri }}', '{{ item.uri }}')
        )

        successfully_updated_billing_rates = rail.WriteLogOperator(
            task_id="successfully_updated_billing_rates",
            message="Added Successfully",
            items=lambda: python_callable_method.get_unique_billing_rate_names_by_attr(
                'availableinproject', 'No'),
            properties={
                'wbs': project_name,
                'billingrate': "{{ item }}",
                'status': 'Success'
            }
        )

        log_billing_rates_already_in_wbs = rail.WriteLogOperator(
            task_id="log_billing_rates_already_in_wbs",
            message='Already available in WBS',
            items=lambda: python_callable_method.get_unique_billing_rate_names_by_attr(
                'availableinproject', 'Yes'),
            properties={
                'wbs': project_name,
                'billingrate': '{{ item }}',
                'status': 'Skipped'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            items='{{ result("assignable_billing_rates_for_wbs") }}',
            message='{{ get_error_message() }}',
            properties={
                'wbs': project_name,
                'billingrate': '{{ item.name }}',
                'status': 'Error'
            })

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            'No') >> view_dagrun_config

        view_dagrun_config >> query_billing_rates_for_wbs >> get_assignable_billing_rates >> \
            assignable_billing_rates_for_wbs >> get_project_info_from_project_service >> \
            is_project_not_exists

        is_project_not_exists >> rail.Label(
            "Yes") >> log_project_doesnt_exist >> catch_and_log_errors
        is_project_not_exists >> rail.Label(
            "No") >> get_data_for_compass_child >> has_compass_child >> rail.Label("Yes") >> process_compass_child >> get_project_info_from_import_service

        has_compass_child >> rail.Label(
            "No") >> get_project_info_from_import_service

        get_project_info_from_import_service >> get_key_value_from_generic_key_store >> billing_rates_to_assign_in_blob >> \
            get_project_billing_rates_to_assign >> has_no_key_in_generic_store
        has_no_key_in_generic_store >> rail.Label(
            "Yes") >> add_projectbillingratesassign_to_labour_types_blob >> \
            put_key_values_to_wbs >> has_billing_rates_for_project
        has_no_key_in_generic_store >> rail.Label(
            "No") >> has_json_value_for_wbs

        has_json_value_for_wbs >> rail.Label(
            "Yes") >> write_existing_blob_records >> existing_blob_records >> write_new_blob_records >> new_blob_records >> check_new_blob_records >> md5_check
        md5_check >> rail.Label(
            "Yes") >> existing_blobs_not_in_new_blob >> new_blob_records_same_as_existing_blobs >> \
            join_new_existing_records_to_labour_type_blob >> get_labour_type_blobs >> has_labour_type_blobs
        md5_check >> rail.Label(
            "No") >> get_labour_type_blobs

        has_json_value_for_wbs >> rail.Label(
            "No") >> add_projectbillingratesassign_to_labour_types_blob2

        add_projectbillingratesassign_to_labour_types_blob2 >> get_labour_type_blobs >> has_labour_type_blobs

        has_labour_type_blobs >> rail.Label(
            "Yes") >> put_key_value_to_generic_store_from_doc >> has_billing_rates_for_project
        has_labour_type_blobs >> rail.Label(
            "No") >> has_billing_rates_for_project

        has_billing_rates_for_project >> rail.Label(
            "Yes") >> has_billing_rates_not_assigned_to_project
        has_billing_rates_for_project >> rail.Label(
            "No") >> catch_and_log_errors

        has_billing_rates_not_assigned_to_project >> rail.Label(
            "Yes") >> add_billing_rates >> successfully_updated_billing_rates >> catch_and_log_errors
        has_billing_rates_not_assigned_to_project >> rail.Label(
            "No") >> has_billing_rates_already_assigned_to_project >> catch_and_log_errors

        has_billing_rates_already_assigned_to_project >> rail.Label(
            "Yes") >> log_billing_rates_already_in_wbs >> catch_and_log_errors
        has_billing_rates_already_assigned_to_project >> rail.Label(
            "No") >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
