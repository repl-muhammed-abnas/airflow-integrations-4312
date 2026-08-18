
from datetime import timedelta
import uuid
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'intercontinentalexchange_process_groups_child_v2_{config.instance}',
        description=f'IntercontinentalExchange - Process groups - Child V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
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
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_list_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_list_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_list_3 = rail.CreateCollectionOperator(
            task_id='create_list_3',
            source="{{dag_run.conf.group | to_json}}",
            name="groupvaluesfromfeedfile",
        )

        if_request_grouptype_equals_to_department_5 = rail.IfOperator(
            task_id='if_request_grouptype_equals_to_department_5',
            test='''{{ dag_run.conf.grouptype == 'Department' }}''',
            yes_task="get_data_department_group_list_service1_6",
            no_task="if_request_grouptype_equals_to_costcenter_14",
        )

        def get_filtered_groups_data(response):
            data = response.json()['d']['rows']
            groups_info = list(map(lambda item: {
                "code": item['cells'][0].get('textValue'),
                "textvalue": item['cells'][1]['textValue'],
                "uri": item['cells'][1].get('uri'),
            }, data))
            return groups_info if groups_info else []

        get_data_department_group_list_service1_6 = rail.RepliconServiceOperator(
            task_id='get_data_department_group_list_service1_6',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:code",
                    "urn:replicon:department-group-list-column:department-group"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=get_filtered_groups_data
        )

        create_list_8 = rail.CreateCollectionOperator(
            task_id='create_list_8',
            source="{{ result('get_data_department_group_list_service1_6') | to_json }}",
            name="groupvaluesinreplicon",
        )

        query_list_checkfornewdropdownvalues_9 = rail.QueryCollectionOperator(
            task_id='query_list_checkfornewdropdownvalues_9',
            query="""SELECT * FROM  groupvaluesfromfeedfile WHERE  groupvaluesfromfeedfile.code NOT IN (SELECT  groupvaluesinreplicon.code FROM  groupvaluesinreplicon WHERE ( groupvaluesinreplicon.code!="" AND  groupvaluesinreplicon.code IS NOT NULL))""",
        )

        if_query_list_checkfornewdropdownvalues_9_rows_greater_than_0_11 = rail.IfOperator(
            task_id='if_query_list_checkfornewdropdownvalues_9_rows_greater_than_0_11',
            test='{{ result("query_list_checkfornewdropdownvalues_9", "length") > 0 }}',
            yes_task="foreach_query_list_checkfornewdropdownvalues_9_12",
            no_task="if_request_grouptype_equals_to_costcenter_14",
        )

        foreach_query_list_checkfornewdropdownvalues_9_12 = rail.ForEachOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_9_12',
            items="{{ result('query_list_checkfornewdropdownvalues_9') }}",
            start_task='create_department_group_or_apply_modification_13',
            end_task='foreach_query_list_checkfornewdropdownvalues_9_12_end'
        )

        create_department_group_or_apply_modification_13 = rail.RepliconServiceOperator(
            task_id='create_department_group_or_apply_modification_13',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data={
                "departmentGroup": {
                    "uri": null,
                    "parent": {
                        "uri": null,
                        "parent": null,
                        "name": "Intercontinental Exchange Holdings, Inc.",
                        "parameterCorrelationId": null
                    },
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ result('foreach_query_list_checkfornewdropdownvalues_9_12').display_text }}",
                    "codeToApply": {
                        "value": "{{ result('foreach_query_list_checkfornewdropdownvalues_9_12').code }}"
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ result('foreach_query_list_checkfornewdropdownvalues_9_12').display_text }}" + str(uuid.uuid4())
            }
        )

        foreach_query_list_checkfornewdropdownvalues_9_12_end = rail.EmptyOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_9_12_end',
        )

        if_request_grouptype_equals_to_costcenter_14 = rail.IfOperator(
            task_id='if_request_grouptype_equals_to_costcenter_14',
            test='''{{ dag_run.conf.grouptype == 'CostCenter' }}''',
            yes_task="get_data_cost_center_list_service1_15",
            no_task="if_request_grouptype_equals_to_division_22",
        )

        get_data_cost_center_list_service1_15 = rail.RepliconServiceOperator(
            task_id='get_data_cost_center_list_service1_15',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:code",
                    "urn:replicon:cost-center-list-column:cost-center"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=get_filtered_groups_data
        )

        create_list_17 = rail.CreateCollectionOperator(
            task_id='create_list_17',
            source="{{ result('get_data_cost_center_list_service1_15') | to_json }}",
            name="groupvaluesinreplicon",
        )

        query_list_checkfornewdropdownvalues_18 = rail.QueryCollectionOperator(
            task_id='query_list_checkfornewdropdownvalues_18',
            query="""SELECT * FROM  groupvaluesfromfeedfile WHERE  groupvaluesfromfeedfile.code NOT IN ( SELECT DISTINCT  groupvaluesinreplicon.Code FROM  groupvaluesinreplicon  WHERE  groupvaluesinreplicon.Code!="" AND  groupvaluesinreplicon.Code IS NOT NULL)""",
        )

        if_query_list_checkfornewdropdownvalues_18_rows_greater_than_0_19 = rail.IfOperator(
            task_id='if_query_list_checkfornewdropdownvalues_18_rows_greater_than_0_19',
            test='{{ result("query_list_checkfornewdropdownvalues_18", "length") > 0 }}',
            yes_task="foreach_query_list_checkfornewdropdownvalues_18_20",
            no_task="if_request_grouptype_equals_to_division_22",
        )

        foreach_query_list_checkfornewdropdownvalues_18_20 = rail.ForEachOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_18_20',
            items="{{ result('query_list_checkfornewdropdownvalues_18') }}",
            start_task='create_cost_center_or_apply_modification_21',
            end_task='foreach_query_list_checkfornewdropdownvalues_18_20_end'
        )

        create_cost_center_or_apply_modification_21 = rail.RepliconServiceOperator(
            task_id='create_cost_center_or_apply_modification_21',
            endpoint="/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            data={
                "costCenter": null,
                "modifications": {
                    "name": "{{ result('foreach_query_list_checkfornewdropdownvalues_18_20').display_text }}",
                    "codeToApply": {
                        "value": "{{ result('foreach_query_list_checkfornewdropdownvalues_18_20').code }}"
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ result('foreach_query_list_checkfornewdropdownvalues_18_20').display_text }}" + str(uuid.uuid4())
            }
        )

        foreach_query_list_checkfornewdropdownvalues_18_20_end = rail.EmptyOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_18_20_end',
        )

        if_request_grouptype_equals_to_division_22 = rail.IfOperator(
            task_id='if_request_grouptype_equals_to_division_22',
            test='''{{ dag_run.conf.grouptype == 'Division' }}''',
            yes_task="get_data_division_list_service1_23",
            no_task="if_request_grouptype_equals_to_employeetype_30",
        )

        get_data_division_list_service1_23 = rail.RepliconServiceOperator(
            task_id='get_data_division_list_service1_23',
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:division-list-column:code",
                    "urn:replicon:division-list-column:division"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=get_filtered_groups_data
        )

        create_list_25 = rail.CreateCollectionOperator(
            task_id='create_list_25',
            source="{{ result('get_data_division_list_service1_23') | to_json }}",
            name="groupvaluesinreplicon",
        )

        query_list_checkfornewdropdownvalues_26 = rail.QueryCollectionOperator(
            task_id='query_list_checkfornewdropdownvalues_26',
            query="""SELECT * FROM  groupvaluesfromfeedfile WHERE  groupvaluesfromfeedfile.code NOT IN ( SELECT DISTINCT  groupvaluesinreplicon.Code FROM  groupvaluesinreplicon WHERE  groupvaluesinreplicon.Code!="" AND  groupvaluesinreplicon.Code IS NOT NULL)""",
        )

        if_query_list_checkfornewdropdownvalues_26_rows_greater_than_0_27 = rail.IfOperator(
            task_id='if_query_list_checkfornewdropdownvalues_26_rows_greater_than_0_27',
            test='{{ result("query_list_checkfornewdropdownvalues_26", "length") > 0 }}',
            yes_task="foreach_query_list_checkfornewdropdownvalues_26_28",
            no_task="if_request_grouptype_equals_to_employeetype_30",
        )

        foreach_query_list_checkfornewdropdownvalues_26_28 = rail.ForEachOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_26_28',
            items="{{ result('query_list_checkfornewdropdownvalues_26') }}",
            start_task='create_division_or_apply_modification_29',
            end_task='foreach_query_list_checkfornewdropdownvalues_26_28_end'
        )

        create_division_or_apply_modification_29 = rail.RepliconServiceOperator(
            task_id='create_division_or_apply_modification_29',
            endpoint="/services/DivisionService1.svc/CreateDivisionOrApplyModification",
            data={
                "division": null,
                "modifications": {
                    "name": "{{ result('foreach_query_list_checkfornewdropdownvalues_26_28').display_text }}",
                    "codeToApply": {
                        "value": "{{ result('foreach_query_list_checkfornewdropdownvalues_26_28').code }}"
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ result('foreach_query_list_checkfornewdropdownvalues_26_28').display_text }}" +str(uuid.uuid4())
            }
        )

        foreach_query_list_checkfornewdropdownvalues_26_28_end = rail.EmptyOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_26_28_end',
        )

        if_request_grouptype_equals_to_employeetype_30 = rail.IfOperator(
            task_id='if_request_grouptype_equals_to_employeetype_30',
            test='''{{ dag_run.conf.grouptype == 'Employee Type' }}''',
            yes_task="get_enabled_employee_type_groups_31",
            no_task="finish",
        )

        get_enabled_employee_type_groups_31 = rail.RepliconServiceOperator(
            task_id='get_enabled_employee_type_groups_31',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups",
            data=None
        )

        create_list_32 = rail.CreateCollectionOperator(
            task_id='create_list_32',
            source="{{ result('get_enabled_employee_type_groups_31') | to_json }}",
            name="groupvaluesinreplicon",
        )

        query_list_checkfornewdropdownvalues_33 = rail.QueryCollectionOperator(
            task_id='query_list_checkfornewdropdownvalues_33',
            query="""SELECT * FROM  groupvaluesfromfeedfile WHERE  groupvaluesfromfeedfile.display_text NOT IN ( SELECT DISTINCT  groupvaluesinreplicon.displayText FROM  groupvaluesinreplicon)""",
        )

        if_query_list_checkfornewdropdownvalues_33_rows_greater_than_0_34 = rail.IfOperator(
            task_id='if_query_list_checkfornewdropdownvalues_33_rows_greater_than_0_34',
            test='{{ result("query_list_checkfornewdropdownvalues_33", "length") > 0 }}',
            yes_task="foreach_query_list_checkfornewdropdownvalues_33_35",
            no_task="finish",
        )

        foreach_query_list_checkfornewdropdownvalues_33_35 = rail.ForEachOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_33_35',
            items="{{ result('query_list_checkfornewdropdownvalues_33') }}",
            start_task='create_employee_type_group_or_apply_modification_36',
            end_task='foreach_query_list_checkfornewdropdownvalues_33_35_end'
        )

        create_employee_type_group_or_apply_modification_36 = rail.RepliconServiceOperator(
            task_id='create_employee_type_group_or_apply_modification_36',
            endpoint="/services/EmployeeTypeGroupService1.svc/CreateEmployeeTypeGroupOrApplyModification",
            data={
                "employeeTypeGroup": null,
                "modifications": {
                    "name": "{{ result('foreach_query_list_checkfornewdropdownvalues_33_35').display_text }}",
                    "codeToApply": {
                        "value": null
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ result('foreach_query_list_checkfornewdropdownvalues_33_35').display_text }}" + str(uuid.uuid4())
            }
        )

        foreach_query_list_checkfornewdropdownvalues_33_35_end = rail.EmptyOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_33_35_end',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> create_list_3
        create_list_3 >> if_request_grouptype_equals_to_department_5
        if_request_grouptype_equals_to_department_5 >> rail.Label(
            'Yes') >> get_data_department_group_list_service1_6 >> create_list_8 >> query_list_checkfornewdropdownvalues_9 >> if_query_list_checkfornewdropdownvalues_9_rows_greater_than_0_11
        if_query_list_checkfornewdropdownvalues_9_rows_greater_than_0_11 >> rail.Label(
            'Yes') >> foreach_query_list_checkfornewdropdownvalues_9_12 >> create_department_group_or_apply_modification_13 >> foreach_query_list_checkfornewdropdownvalues_9_12_end
        foreach_query_list_checkfornewdropdownvalues_9_12 >> foreach_query_list_checkfornewdropdownvalues_9_12_end >> if_request_grouptype_equals_to_costcenter_14
        if_query_list_checkfornewdropdownvalues_9_rows_greater_than_0_11 >> rail.Label(
            'No') >> if_request_grouptype_equals_to_costcenter_14
        if_request_grouptype_equals_to_department_5 >> rail.Label(
            'No') >> if_request_grouptype_equals_to_costcenter_14
        if_request_grouptype_equals_to_costcenter_14 >> rail.Label(
            'Yes') >> get_data_cost_center_list_service1_15 >> create_list_17 >> query_list_checkfornewdropdownvalues_18 >> if_query_list_checkfornewdropdownvalues_18_rows_greater_than_0_19
        if_query_list_checkfornewdropdownvalues_18_rows_greater_than_0_19 >> rail.Label(
            'Yes') >> foreach_query_list_checkfornewdropdownvalues_18_20 >> create_cost_center_or_apply_modification_21 >> foreach_query_list_checkfornewdropdownvalues_18_20_end
        foreach_query_list_checkfornewdropdownvalues_18_20 >> foreach_query_list_checkfornewdropdownvalues_18_20_end >> if_request_grouptype_equals_to_division_22
        if_query_list_checkfornewdropdownvalues_18_rows_greater_than_0_19 >> rail.Label(
            'No') >> if_request_grouptype_equals_to_division_22
        if_request_grouptype_equals_to_costcenter_14 >> rail.Label(
            'No') >> if_request_grouptype_equals_to_division_22
        if_request_grouptype_equals_to_division_22 >> rail.Label(
            'Yes') >> get_data_division_list_service1_23 >> create_list_25 >> query_list_checkfornewdropdownvalues_26 >> \
            if_query_list_checkfornewdropdownvalues_26_rows_greater_than_0_27
        if_query_list_checkfornewdropdownvalues_26_rows_greater_than_0_27 >> rail.Label(
            'Yes') >> foreach_query_list_checkfornewdropdownvalues_26_28 >> create_division_or_apply_modification_29 >> foreach_query_list_checkfornewdropdownvalues_26_28_end
        foreach_query_list_checkfornewdropdownvalues_26_28 >> foreach_query_list_checkfornewdropdownvalues_26_28_end >> if_request_grouptype_equals_to_employeetype_30
        if_query_list_checkfornewdropdownvalues_26_rows_greater_than_0_27 >> rail.Label(
            'No') >> if_request_grouptype_equals_to_employeetype_30
        if_request_grouptype_equals_to_division_22 >> rail.Label(
            'No') >> if_request_grouptype_equals_to_employeetype_30
        if_request_grouptype_equals_to_employeetype_30 >> rail.Label(
            'Yes') >> get_enabled_employee_type_groups_31 >> create_list_32 >> query_list_checkfornewdropdownvalues_33 >> if_query_list_checkfornewdropdownvalues_33_rows_greater_than_0_34
        if_query_list_checkfornewdropdownvalues_33_rows_greater_than_0_34 >> rail.Label(
            'Yes') >> foreach_query_list_checkfornewdropdownvalues_33_35 >> create_employee_type_group_or_apply_modification_36 >> foreach_query_list_checkfornewdropdownvalues_33_35_end
        foreach_query_list_checkfornewdropdownvalues_33_35 >> foreach_query_list_checkfornewdropdownvalues_33_35_end >> finish
        if_query_list_checkfornewdropdownvalues_33_rows_greater_than_0_34 >> rail.Label(
            'No') >> finish
        if_request_grouptype_equals_to_employeetype_30 >> rail.Label(
            'No') >> finish

    return dag


rail.for_each_instance(create_dag)
