from datetime import timedelta
import json
import ast
import rail
from airflow.models import Variable


from dxctechnology.c1_labour_types_import import request_payload
from dxctechnology.c1_labour_types_import import response_filter
from dxctechnology.c1_labour_types_import import python_callable_method

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_labour_types_import/config.py


# pylint: disable=too-many-statements
def create_child_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_c1_labour_types_compass_child_process_wbs{dag_id_postfix}',
        description=f'DXC_C1 Child COMPASS_Labour_Types Automation Child V3.0 - B1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.compass_child_dag_process_wbs_max_active_runs,
    ) as dag:

        key_name_space = "DXC_CompassWBSLabourTypeDetails"
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        project_name = "{{ dag_run.conf.wbs }}"

        get_billing_rates_to_asssign_compass = rail.PythonOperator(
            task_id="get_billing_rates_to_asssign_compass",
            python_callable=python_callable_method.get_billing_rate_to_assign_compass,
            op_args=['{{ dag_run.conf.billingrates }}']
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
            yes_task="finish",
            no_task="has_division",
        )

        has_division = rail.IfOperator(
            task_id="has_division",
            test=lambda: bool(rail.result(
                'get_project_info_from_project_service')['division']),
            yes_task="get_division_details",
            no_task="finish"
        )

        get_division_details = rail.RepliconServiceOperator(
            task_id='get_division_details',
            endpoint='/services/DivisionService1.svc/GetDivisionDetails',
            data=request_payload.get_division_payload,
            response_filter=response_filter.map_division_name_or_code
        )

        def get_division_names():
            value = Variable.get(config.division_variable)
            value = ast.literal_eval(value)
            if not isinstance(value, list):
                # pylint: disable=line-too-long
                raise Exception(f"The variable `{config.division_variable}` is not in correct format. Excepted `list` got {type(value)}. Found Variable Value: `{value}`")
            if len(value) <= 0:
                raise Exception(f"Variable {config.division_variable} does not have any values present")
            return value

        division_names = rail.PythonOperator(
          task_id = "division_names",
          python_callable = get_division_names
        )

        is_project_valid = rail.IfOperator(
            task_id="is_project_valid",
            test=lambda dag_run: bool(rail.find_first_by_attr_and_get_attr(rail.result('get_project_info_from_project_service')[
                                      'extensionFieldValues'], 'textValue', dag_run.conf[
                'c1parentname'], 'textValue') and (rail.result('get_division_details') in rail.result('division_names')))
            if rail.result('get_project_info_from_project_service')['extensionFieldValues'] else False,
            yes_task="date_validation_update",
            no_task="finish"
        )

        date_validation_update = rail.WriteCSVFileOperator(
            task_id="date_validation_update",
            source=lambda: rail.result('get_billing_rates_to_asssign_compass'),
            header=[
                'name',
                'taskassignmentstartdate',
                'taskassignmentenddate',
                'blanklabortype'],
            row=request_payload.get_data_validation_rows
        )

        billing_rates_to_assign_list = rail.CreateCollectionOperator(
            task_id="billing_rates_to_assign_list",
            source=lambda: rail.result('date_validation_update'),
        )

        query_billing_rates_to_assign_validated = rail.QueryCollectionOperator(
            task_id="query_billing_rates_to_assign_validated",
            query="""SELECT * FROM billing_rates_to_assign_list""",
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

        billing_rates_to_assign_in_compass_blob = rail.PythonOperator(
            task_id="billing_rates_to_assign_in_compass_blob",
            python_callable=python_callable_method.get_billing_rates_to_assign_in_compass_blob,
            op_args=['get_project_info_from_import_service'],
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        has_key_in_generic_store = rail.IfOperator(
            task_id='has_key_in_generic_store',
            test=lambda: bool(rail.result(
                'get_key_value_from_generic_key_store')),
            yes_task="has_json_value_for_wbs",
            no_task="get_project_billing_rates_to_assign_compass"
        )

        get_project_billing_rates_to_assign_compass = rail.PythonOperator(
            task_id="get_project_billing_rates_to_assign_compass",
            python_callable=python_callable_method.get_project_billing_rates_to_assign_compass,
            op_args=['billing_rates_to_assign_in_compass_blob',
                     '{{ result("get_project_info_from_import_service").results[0].project.uri}}', project_name]
        )

        add_projectbillingratesassign_to_labour_types_blob_compass = rail.CreateCollectionOperator(
            task_id="add_projectbillingratesassign_to_labour_types_blob_compass",
            source=lambda: rail.result(
                'get_project_billing_rates_to_assign_compass')
        )

        put_key_values_to_wbs_compass = rail.RepliconServiceOperator(
            task_id='put_key_values_to_wbs_compass',
            endpoint='/services/GenericKeyValueStoreService1.svc/PutKeyValue',
            data=request_payload.put_key_value(key_name_space, project_name,
                                               "{{ result('get_project_billing_rates_to_assign_compass') | to_json }}")
        )

        has_json_value_for_wbs = rail.IfOperator(
            task_id='has_json_value_for_wbs',
            test="{{ result('get_key_value_from_generic_key_store') | is_truthy and \
                result('get_key_value_from_generic_key_store').jsonValue | from_json | length > 0 and \
                    result('get_key_value_from_generic_key_store').jsonValue | from_json | first_or_default | \
                        attr_or_default('wbsUri') | is_truthy }}",
            yes_task="write_existing_blob_records",
            no_task="get_project_billing_rates_to_assign_compass_2"
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

        existing_blob_records_compass = rail.CreateCollectionOperator(
            task_id='existing_blob_records_compass',
            source="{{ result('write_existing_blob_records') }}",
            name="existingblobrecordscompass"
        )

        write_new_blob_records = rail.WriteCSVFileOperator(
            task_id="write_new_blob_records",
            source=lambda: rail.result(
                'billing_rates_to_assign_in_compass_blob'),
            header=[
                'wbs',
                'labourtype',
                'labourtypeuri',
                'startdate',
                'enddate',
                'id'],
            row=request_payload.get_new_blob_rows
        )

        new_blob_records_compass = rail.CreateCollectionOperator(
            task_id='new_blob_records_compass',
            source="{{ result('write_new_blob_records') }}",
            name="newblobrecordscompass"
        )

        new_records_not_in_blob = rail.QueryCollectionOperator(
            task_id='new_records_not_in_blob',
            query='SELECT * FROM newblobrecordscompass WHERE id NOT IN (SELECT DISTINCT id FROM existingblobrecordscompass)',
            name='newrecordsnotinblobcompass'
        )

        new_records = rail.CreateCollectionOperator(
            task_id='new_records',
            source="{{ result('new_records_not_in_blob') }}"
        )

        has_new_blob_records = rail.IfOperator(
            task_id="has_new_blob_records",
            test="{{ result('new_records', 'length') > 0 }}",
            yes_task='existing_blobs_not_in_new_blob',
            no_task='get_labour_type_blobs_compass'
        )

        get_project_billing_rates_to_assign_compass_2 = rail.PythonOperator(
            task_id="get_project_billing_rates_to_assign_compass_2",
            python_callable=python_callable_method.get_project_billing_rates_to_assign_compass_2,
            op_args=['billing_rates_to_assign_in_compass_blob',
                     '{{ result("get_project_info_from_import_service").results[0].project.uri}}', project_name]
        )

        add_projectbillingratesassign_to_labour_types_blob_compass_2 = rail.CreateCollectionOperator(
            task_id="add_projectbillingratesassign_to_labour_types_blob_compass_2",
            source=lambda: rail.result(
                'get_project_billing_rates_to_assign_compass_2')
        )

        existing_blobs_not_in_new_blob = rail.QueryCollectionOperator(
            task_id='existing_blobs_not_in_new_blob',
            query='SELECT * FROM existingblobrecordscompass WHERE labourtype NOT IN (SELECT DISTINCT labourtype FROM newblobrecordscompass)',
            name="existingblobsnotinnewblobcompass"
        )

        new_blob_records_same_as_existing_blobs = rail.QueryCollectionOperator(
            task_id='new_blob_records_same_as_existing_blobs',
            query='SELECT * FROM newblobrecordscompass WHERE id IN (SELECT DISTINCT id FROM existingblobrecordscompass)',
            name="existingblobssameasnewblobcompass"
        )

        join_new_existing_records_to_labour_type_blob_compass = rail.QueryCollectionOperator(
            task_id='join_new_existing_records_to_labour_type_blob_compass',
            query='''SELECT * FROM existingblobsnotinnewblobcompass UNION
                    SELECT * FROM newrecordsnotinblobcompass UNION SELECT * FROM existingblobssameasnewblobcompass'''
        )

        get_labour_type_blobs_compass = rail.PythonOperator(
            task_id="get_labour_type_blobs_compass",
            python_callable=python_callable_method.get_labour_type_blobs_compass,
            op_args=['{{ result("get_project_info_from_import_service").results[0].project.uri }}',
                     'join_new_existing_records_to_labour_type_blob_compass', 'add_projectbillingratesassign_to_labour_types_blob_compass_2']
        )

        has_labour_type_blobs = rail.IfOperator(
            task_id="has_labour_type_blobs",
            test="{{ result('get_labour_type_blobs_compass') | length > 0 }}",
            yes_task='put_key_value_to_generic_store_from_doc_compass',
            no_task='has_billing_rates_for_project_compass'
        )

        put_key_value_to_generic_store_from_doc_compass = rail.RepliconServiceOperator(
            task_id='put_key_value_to_generic_store_from_doc_compass',
            endpoint='/services/GenericKeyValueStoreService1.svc/PutKeyValue',
            data=request_payload.put_key_value(
                key_name_space, project_name, "{{ result('get_labour_type_blobs_compass') | to_json }}")
        )

        has_billing_rates_for_project_compass = rail.IfOperator(
            task_id="has_billing_rates_for_project_compass",
            test="{{ result('get_billing_rates_to_asssign_compass') | is_truthy }}",
            yes_task=['has_billing_rates_not_assigned_to_project_compass',
                      'has_billing_rates_already_assigned_to_project_compass'],
            no_task="finish"
        )

        has_billing_rates_not_assigned_to_project_compass = rail.IfOperator(
            task_id="has_billing_rates_not_assigned_to_project_compass",
            test='{{ result("billing_rates_to_assign_in_compass_blob") | filter_by_attr("availableinproject", "equals", "No") | length > 0 }}',
            yes_task='add_billing_rates_compass',
            no_task='finish'
        )

        has_billing_rates_already_assigned_to_project_compass = rail.IfOperator(
            task_id="has_billing_rates_already_assigned_to_project_compass",
            test='{{ result("billing_rates_to_assign_in_compass_blob") | filter_by_attr("availableinproject", "equals", "Yes") | length > 0 }}',
            yes_task='billing_rates_already_in_wbs',
            no_task='finish'
        )

        add_billing_rates_compass = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_billing_rates_compass',
            endpoint='/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers',
            items='{{ result("billing_rates_to_assign_in_compass_blob") | filter_by_attr("availableinproject", "equals", "No") | to_json }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=request_payload.update_billing_rates_for_team_members(
                '{{ result("get_project_info_from_import_service").results[0].project.uri }}', '{{ item.uri }}')
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        billing_rates_already_in_wbs = rail.EmptyOperator(
            task_id='billing_rates_already_in_wbs'
        )

        get_billing_rates_to_asssign_compass >> get_project_info_from_project_service >> is_project_not_exists

        is_project_not_exists >> rail.Label(
            'No') >> has_division >> rail.Label('Yes') >> get_division_details >> division_names >> is_project_valid

        is_project_not_exists >> rail.Label(
            'Yes') >> finish

        has_division >> rail.Label(
            'No') >> finish

        is_project_valid >> rail.Label(
            'Yes') >> date_validation_update >> billing_rates_to_assign_list

        is_project_valid >> rail.Label(
            'No') >> finish

        billing_rates_to_assign_list >> query_billing_rates_to_assign_validated >> get_project_info_from_import_service >> get_key_value_from_generic_key_store
        get_key_value_from_generic_key_store >> billing_rates_to_assign_in_compass_blob >> has_key_in_generic_store
        has_key_in_generic_store >> rail.Label(
            'Yes') >> has_json_value_for_wbs >> rail.Label(
                'Yes') >> write_existing_blob_records
        write_existing_blob_records >> existing_blob_records_compass
        has_json_value_for_wbs >> rail.Label(
            'No') >> get_project_billing_rates_to_assign_compass_2

        get_project_billing_rates_to_assign_compass_2 >> add_projectbillingratesassign_to_labour_types_blob_compass_2
        add_projectbillingratesassign_to_labour_types_blob_compass_2 >> get_labour_type_blobs_compass
        existing_blob_records_compass >> write_new_blob_records >> new_blob_records_compass >> new_records_not_in_blob
        new_records_not_in_blob >> new_records >> has_new_blob_records

        has_new_blob_records >> rail.Label(
            'Yes') >> existing_blobs_not_in_new_blob >> new_blob_records_same_as_existing_blobs >> join_new_existing_records_to_labour_type_blob_compass >> \
            get_labour_type_blobs_compass
        has_new_blob_records >> rail.Label(
            'No') >> get_labour_type_blobs_compass >> has_labour_type_blobs

        has_labour_type_blobs >> rail.Label(
            'Yes') >> put_key_value_to_generic_store_from_doc_compass >> \
            has_billing_rates_for_project_compass
        has_labour_type_blobs >> rail.Label(
            'No') >> has_billing_rates_for_project_compass

        has_key_in_generic_store >> rail.Label(
            'No') >> get_project_billing_rates_to_assign_compass

        get_project_billing_rates_to_assign_compass >> add_projectbillingratesassign_to_labour_types_blob_compass
        add_projectbillingratesassign_to_labour_types_blob_compass >> put_key_values_to_wbs_compass >> has_billing_rates_for_project_compass

        has_billing_rates_for_project_compass >> rail.Label(
            'Yes') >> [has_billing_rates_not_assigned_to_project_compass, has_billing_rates_already_assigned_to_project_compass]

        has_billing_rates_for_project_compass >> rail.Label(
            'No') >> finish

        has_billing_rates_not_assigned_to_project_compass >> rail.Label(
            'Yes') >> add_billing_rates_compass

        add_billing_rates_compass >> finish

        has_billing_rates_already_assigned_to_project_compass >> rail.Label(
            'Yes') >> billing_rates_already_in_wbs >> finish

        has_billing_rates_already_assigned_to_project_compass >> rail.Label(
            'No') >> finish

        has_billing_rates_not_assigned_to_project_compass >> rail.Label(
            'No') >> finish

    return dag


rail.for_each_instance(create_child_dag)
