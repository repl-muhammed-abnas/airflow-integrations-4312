
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.horizonmedia_user_import_groups_check_child,
        description=f'Horizonmedia_Groups_Check- Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='has_group_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='has_group_data',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        has_group_data = rail.IfOperator(
            task_id='has_group_data',
            test="{{ dag_run.conf.group | is_truthy }}",
            yes_task='create_list_3',
            no_task='log_to_sumo'
        )

        create_list_3 = rail.CreateCollectionOperator(
            task_id='create_list_3',
            source=lambda: rail.get_dag_run_conf()['group'],
            name='groupvaluesfromfeedfile',
        )

        if_request_grouptype_equals_to_location_4 = rail.IfOperator(
            task_id='if_request_grouptype_equals_to_location_4',
            test='''{{ dag_run.conf.grouptype == 'Location' }}''',
            yes_task="get_enabled_locations_5",
            no_task="if_request_grouptype_equals_to_division_11",
        )

        get_enabled_locations_5 = rail.RepliconServiceOperator(
            task_id='get_enabled_locations_5',
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
        )

        create_list_6 = rail.CreateCollectionOperator(
            task_id='create_list_6',
            source=lambda: rail.result('get_enabled_locations_5'),
            name='groupvaluesinreplicon'
        )

        query_list_checkfornewdropdownvalues_7 = rail.QueryCollectionOperator(
            task_id='query_list_checkfornewdropdownvalues_7',
            query="""SELECT * FROM  groupvaluesfromfeedfile WHERE  groupvaluesfromfeedfile.displayText NOT IN ( SELECT DISTINCT  groupvaluesinreplicon.displayText FROM  groupvaluesinreplicon)  GROUP BY groupvaluesfromfeedfile.displayText""",
        )

        if_query_list_checkfornewdropdownvalues_7_rows_greater_than_0_8 = rail.IfOperator(
            task_id='if_query_list_checkfornewdropdownvalues_7_rows_greater_than_0_8',
            test='''{{ result('query_list_checkfornewdropdownvalues_7','length') > 0 }}''',
            yes_task="foreach_query_list_checkfornewdropdownvalues_7_9",
            no_task="if_request_grouptype_equals_to_division_11",
        )

        foreach_query_list_checkfornewdropdownvalues_7_9 = rail.ForEachOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_7_9',
            items="{{ result('query_list_checkfornewdropdownvalues_7') }}",
            start_task='create_location_or_apply_modification_10',
            end_task='foreach_query_list_checkfornewdropdownvalues_7_9_end'
        )

        create_location_or_apply_modification_10 = rail.RepliconServiceOperator(
            task_id='create_location_or_apply_modification_10',
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data={
                "location": null,
                "modifications": {
                    "name": "{{ result('foreach_query_list_checkfornewdropdownvalues_7_9').displayText }}",
                    "codeToApply": {
                        "value": "{{ result('foreach_query_list_checkfornewdropdownvalues_7_9').code }}"
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "unitOfWorkId_{{ result('foreach_query_list_checkfornewdropdownvalues_7_9').code }}_{{ dag_run_ecid() }}"
            }
        )

        foreach_query_list_checkfornewdropdownvalues_7_9_end = rail.EmptyOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_7_9_end',
        )

        if_request_grouptype_equals_to_division_11 = rail.IfOperator(
            task_id='if_request_grouptype_equals_to_division_11',
            test='''{{ dag_run.conf.grouptype == 'Division' }}''',
            yes_task="get_enabled_divisions_12",
            no_task="if_request_grouptype_equals_to_department_18",
        )

        get_enabled_divisions_12 = rail.RepliconServiceOperator(
            task_id='get_enabled_divisions_12',
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",

        )

        create_list_13 = rail.CreateCollectionOperator(
            task_id='create_list_13',
            source=lambda: rail.result('get_enabled_divisions_12'),
            name='groupvaluesinreplicon'
        )

        query_list_checkfornewdropdownvalues_14 = rail.QueryCollectionOperator(
            task_id='query_list_checkfornewdropdownvalues_14',
            query="""SELECT * FROM  groupvaluesfromfeedfile WHERE  groupvaluesfromfeedfile.displayText NOT IN ( SELECT DISTINCT  groupvaluesinreplicon.displayText FROM  groupvaluesinreplicon)  GROUP BY groupvaluesfromfeedfile.displayText""",
        )

        if_query_list_checkfornewdropdownvalues_14_rows_greater_than_0_15 = rail.IfOperator(
            task_id='if_query_list_checkfornewdropdownvalues_14_rows_greater_than_0_15',
            test='''{{ result('query_list_checkfornewdropdownvalues_14','length') > 0 }}''',
            yes_task="foreach_query_list_checkfornewdropdownvalues_14_16",
            no_task="if_request_grouptype_equals_to_department_18",
        )

        foreach_query_list_checkfornewdropdownvalues_14_16 = rail.ForEachOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_14_16',
            items="{{ result('query_list_checkfornewdropdownvalues_14') }}",
            start_task='create_division_or_apply_modification_17',
            end_task='foreach_query_list_checkfornewdropdownvalues_14_16_end'
        )

        create_division_or_apply_modification_17 = rail.RepliconServiceOperator(
            task_id='create_division_or_apply_modification_17',
            endpoint="/services/DivisionService1.svc/CreateDivisionOrApplyModification",
            data={
                "division": null,
                "modifications": {
                    "name": "{{ result('foreach_query_list_checkfornewdropdownvalues_14_16').displayText }}",
                    "codeToApply": {
                        "value": "{{ result('foreach_query_list_checkfornewdropdownvalues_14_16').code }}"
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "unitOfWorkId_{{ result('foreach_query_list_checkfornewdropdownvalues_14_16').code }}_{{ dag_run_ecid() }}"
            }
        )

        foreach_query_list_checkfornewdropdownvalues_14_16_end = rail.EmptyOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_14_16_end',
        )

        if_request_grouptype_equals_to_department_18 = rail.IfOperator(
            task_id='if_request_grouptype_equals_to_department_18',
            test='''{{ dag_run.conf.grouptype == 'Department' }}''',
            yes_task="get_enabled_department_groups_19",
            no_task="if_request_grouptype_equals_to_servicecenter_25",
        )

        get_enabled_department_groups_19 = rail.RepliconServiceOperator(
            task_id='get_enabled_department_groups_19',
            endpoint="/services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups",

        )

        create_list_20 = rail.CreateCollectionOperator(
            task_id='create_list_20',
            source=lambda: rail.result('get_enabled_department_groups_19'),
            name='groupvaluesinreplicon',
        )

        query_list_checkfornewdropdownvalues_21 = rail.QueryCollectionOperator(
            task_id='query_list_checkfornewdropdownvalues_21',
            query="""SELECT * FROM  groupvaluesfromfeedfile WHERE  groupvaluesfromfeedfile.displayText NOT IN ( SELECT DISTINCT  groupvaluesinreplicon.displayText FROM  groupvaluesinreplicon)  GROUP BY groupvaluesfromfeedfile.displayText""",
        )

        if_query_list_checkfornewdropdownvalues_21_rows_greater_than_0_22 = rail.IfOperator(
            task_id='if_query_list_checkfornewdropdownvalues_21_rows_greater_than_0_22',
            test='''{{ result('query_list_checkfornewdropdownvalues_21','length') > 0 }}''',
            yes_task="foreach_query_list_checkfornewdropdownvalues_21_23",
            no_task="if_request_grouptype_equals_to_servicecenter_25",
        )

        foreach_query_list_checkfornewdropdownvalues_21_23 = rail.ForEachOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_21_23',
            items="{{ result('query_list_checkfornewdropdownvalues_21') }}",
            start_task='create_department_group_or_apply_modification_24',
            end_task='foreach_query_list_checkfornewdropdownvalues_21_23_end'
        )

        create_department_group_or_apply_modification_24 = rail.RepliconServiceOperator(
            task_id='create_department_group_or_apply_modification_24',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data={
                "departmentGroup": {
                    "uri": null,
                    "parent": {
                        "uri": null,
                        "parent": null,
                        "name": "Horizon Media",
                        "parameterCorrelationId": null
                    },
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ result('foreach_query_list_checkfornewdropdownvalues_21_23').displayText }}",
                    "codeToApply": {
                        "value": "{{ result('foreach_query_list_checkfornewdropdownvalues_21_23').code }}"
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "unitOfWorkId_{{ result('foreach_query_list_checkfornewdropdownvalues_21_23').code }}_{{ dag_run_ecid() }}"
            }
        )

        foreach_query_list_checkfornewdropdownvalues_21_23_end = rail.EmptyOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_21_23_end',
        )

        if_request_grouptype_equals_to_servicecenter_25 = rail.IfOperator(
            task_id='if_request_grouptype_equals_to_servicecenter_25',
            test='''{{ dag_run.conf.grouptype == 'Service Center' }}''',
            yes_task="get_enabled_service_centers_26",
            no_task="if_request_grouptype_equals_to_employeetype_32",
        )

        get_enabled_service_centers_26 = rail.RepliconServiceOperator(
            task_id='get_enabled_service_centers_26',
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",

        )

        create_list_27 = rail.CreateCollectionOperator(
            task_id='create_list_27',
            source=lambda: rail.result('get_enabled_service_centers_26'),
            name='groupvaluesinreplicon',
        )

        query_list_checkfornewdropdownvalues_28 = rail.QueryCollectionOperator(
            task_id='query_list_checkfornewdropdownvalues_28',
            query="""SELECT * FROM  groupvaluesfromfeedfile WHERE  groupvaluesfromfeedfile.displayText NOT IN ( SELECT DISTINCT  groupvaluesinreplicon.displayText FROM  groupvaluesinreplicon )  GROUP BY groupvaluesfromfeedfile.displayText""",
        )

        if_query_list_checkfornewdropdownvalues_28_rows_greater_than_0_29 = rail.IfOperator(
            task_id='if_query_list_checkfornewdropdownvalues_28_rows_greater_than_0_29',
            test='''{{ result('query_list_checkfornewdropdownvalues_28','length') > 0 }}''',
            yes_task="foreach_query_list_checkfornewdropdownvalues_28_30",
            no_task="if_request_grouptype_equals_to_employeetype_32",
        )

        foreach_query_list_checkfornewdropdownvalues_28_30 = rail.ForEachOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_28_30',
            items="{{ result('query_list_checkfornewdropdownvalues_28') }}",
            start_task='create_service_center_or_apply_modification_31',
            end_task='foreach_query_list_checkfornewdropdownvalues_28_30_end'
        )

        create_service_center_or_apply_modification_31 = rail.RepliconServiceOperator(
            task_id='create_service_center_or_apply_modification_31',
            endpoint="/services/ServiceCenterService1.svc/CreateServiceCenterOrApplyModification",
            data={
                "serviceCenter": null,
                "modifications": {
                    "name": "{{ result('foreach_query_list_checkfornewdropdownvalues_28_30').displayText }}",
                    "codeToApply": {
                        "value": "{{ result('foreach_query_list_checkfornewdropdownvalues_28_30').code }}"
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "unitOfWorkId_{{ result('foreach_query_list_checkfornewdropdownvalues_28_30').code }}_{{ dag_run_ecid() }}"
            }
        )

        foreach_query_list_checkfornewdropdownvalues_28_30_end = rail.EmptyOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_28_30_end',
        )

        if_request_grouptype_equals_to_employeetype_32 = rail.IfOperator(
            task_id='if_request_grouptype_equals_to_employeetype_32',
            test='''{{ dag_run.conf.grouptype == 'Employee Type' }}''',
            yes_task="get_enabled_employee_type_groups_33",
            no_task="if_request_grouptype_equals_to_costcenter_39",
        )

        get_enabled_employee_type_groups_33 = rail.RepliconServiceOperator(
            task_id='get_enabled_employee_type_groups_33',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups",

        )

        create_list_34 = rail.CreateCollectionOperator(
            task_id='create_list_34',
            source=lambda: rail.result('get_enabled_employee_type_groups_33'),
            name='groupvaluesinreplicon'
        )

        query_list_checkfornewdropdownvalues_35 = rail.QueryCollectionOperator(
            task_id='query_list_checkfornewdropdownvalues_35',
            query="""SELECT * FROM  groupvaluesfromfeedfile WHERE  groupvaluesfromfeedfile.displayText NOT IN ( SELECT DISTINCT  groupvaluesinreplicon.displayText FROM  groupvaluesinreplicon)  GROUP BY groupvaluesfromfeedfile.displayText""",
        )

        if_query_list_checkfornewdropdownvalues_35_rows_greater_than_0_36 = rail.IfOperator(
            task_id='if_query_list_checkfornewdropdownvalues_35_rows_greater_than_0_36',
            test='''{{ result('query_list_checkfornewdropdownvalues_35','length') > 0 }}''',
            yes_task="foreach_query_list_checkfornewdropdownvalues_35_37",
            no_task="if_request_grouptype_equals_to_costcenter_39",
        )

        foreach_query_list_checkfornewdropdownvalues_35_37 = rail.ForEachOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_35_37',
            items="{{ result('query_list_checkfornewdropdownvalues_35') }}",
            start_task='create_employee_type_group_or_apply_modification_38',
            end_task='foreach_query_list_checkfornewdropdownvalues_35_37_end'
        )

        create_employee_type_group_or_apply_modification_38 = rail.RepliconServiceOperator(
            task_id='create_employee_type_group_or_apply_modification_38',
            endpoint="/services/EmployeeTypeGroupService1.svc/CreateEmployeeTypeGroupOrApplyModification",
            data={
                "employeeTypeGroup": null,
                "modifications": {
                    "name": "{{ result('foreach_query_list_checkfornewdropdownvalues_35_37').displayText }}",
                    "codeToApply": {
                        "value": null
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "unitOfWorkId_{{ result('foreach_query_list_checkfornewdropdownvalues_35_37').code }}_{{ dag_run_ecid() }}"
            }
        )

        foreach_query_list_checkfornewdropdownvalues_35_37_end = rail.EmptyOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_35_37_end',
        )

        if_request_grouptype_equals_to_costcenter_39 = rail.IfOperator(
            task_id='if_request_grouptype_equals_to_costcenter_39',
            test='''{{ dag_run.conf.grouptype == 'Cost Center' }}''',
            yes_task="get_enabled_cost_centers_40",
            no_task="log_to_sumo",
        )

        get_enabled_cost_centers_40 = rail.RepliconServiceOperator(
            task_id='get_enabled_cost_centers_40',
            endpoint="/services/CostCenterService1.svc/GetEnabledCostCenters",

        )

        create_list_41 = rail.CreateCollectionOperator(
            task_id='create_list_41',
            source=lambda: rail.result('get_enabled_cost_centers_40'),
            name='groupvaluesinreplicon'
        )

        query_list_checkfornewdropdownvalues_42 = rail.QueryCollectionOperator(
            task_id='query_list_checkfornewdropdownvalues_42',
            query="""SELECT * FROM  groupvaluesfromfeedfile WHERE  groupvaluesfromfeedfile.displayText NOT IN ( SELECT DISTINCT  groupvaluesinreplicon.displayText FROM  groupvaluesinreplicon)  GROUP BY groupvaluesfromfeedfile.displayText""",
        )

        if_query_list_checkfornewdropdownvalues_42_rows_greater_than_0_43 = rail.IfOperator(
            task_id='if_query_list_checkfornewdropdownvalues_42_rows_greater_than_0_43',
            test='''{{ result('query_list_checkfornewdropdownvalues_42','length') > 0 }}''',
            yes_task="foreach_query_list_checkfornewdropdownvalues_42_44",
            no_task="log_to_sumo",
        )

        foreach_query_list_checkfornewdropdownvalues_42_44 = rail.ForEachOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_42_44',
            items="{{ result('query_list_checkfornewdropdownvalues_42') }}",
            start_task='create_employee_type_group_or_apply_modification_45',
            end_task='foreach_query_list_checkfornewdropdownvalues_42_44_end'
        )

        create_employee_type_group_or_apply_modification_45 = rail.RepliconServiceOperator(
            task_id='create_employee_type_group_or_apply_modification_45',
            endpoint="/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            data={
                "costCenter": null,
                "modifications": {
                    "name": "{{ result('foreach_query_list_checkfornewdropdownvalues_42_44').displayText }}",
                    "codeToApply": {
                        "value": null
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "unitOfWorkId_{{ result('foreach_query_list_checkfornewdropdownvalues_42_44').displayText }}_{{ dag_run_ecid() }}"
            }
        )

        foreach_query_list_checkfornewdropdownvalues_42_44_end = rail.EmptyOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_42_44_end',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> has_group_data
        has_group_data >> rail.Label('yes') >> create_list_3
        has_group_data >> rail.Label('no') >> log_to_sumo
        create_list_3 >> if_request_grouptype_equals_to_location_4
        if_request_grouptype_equals_to_location_4 >> rail.Label(
            'Yes') >> get_enabled_locations_5 >> create_list_6 >> query_list_checkfornewdropdownvalues_7 >> if_query_list_checkfornewdropdownvalues_7_rows_greater_than_0_8
        if_query_list_checkfornewdropdownvalues_7_rows_greater_than_0_8 >> rail.Label(
            'Yes') >> foreach_query_list_checkfornewdropdownvalues_7_9 >> create_location_or_apply_modification_10 >> foreach_query_list_checkfornewdropdownvalues_7_9_end
        foreach_query_list_checkfornewdropdownvalues_7_9 >> foreach_query_list_checkfornewdropdownvalues_7_9_end >> if_request_grouptype_equals_to_division_11
        if_query_list_checkfornewdropdownvalues_7_rows_greater_than_0_8 >> rail.Label(
            'No') >> if_request_grouptype_equals_to_division_11
        if_request_grouptype_equals_to_location_4 >> rail.Label(
            'No') >> if_request_grouptype_equals_to_division_11
        if_request_grouptype_equals_to_division_11 >> rail.Label(
            'Yes') >> get_enabled_divisions_12 >> create_list_13 >> query_list_checkfornewdropdownvalues_14 >> if_query_list_checkfornewdropdownvalues_14_rows_greater_than_0_15
        if_query_list_checkfornewdropdownvalues_14_rows_greater_than_0_15 >> rail.Label(
            'Yes') >> foreach_query_list_checkfornewdropdownvalues_14_16 >> create_division_or_apply_modification_17 >> foreach_query_list_checkfornewdropdownvalues_14_16_end
        foreach_query_list_checkfornewdropdownvalues_14_16 >> foreach_query_list_checkfornewdropdownvalues_14_16_end >> if_request_grouptype_equals_to_department_18
        if_query_list_checkfornewdropdownvalues_14_rows_greater_than_0_15 >> rail.Label(
            'No') >> if_request_grouptype_equals_to_department_18
        if_request_grouptype_equals_to_division_11 >> rail.Label(
            'No') >> if_request_grouptype_equals_to_department_18
        if_request_grouptype_equals_to_department_18 >> rail.Label(
            'Yes') >> get_enabled_department_groups_19 >> create_list_20 >> query_list_checkfornewdropdownvalues_21 >> if_query_list_checkfornewdropdownvalues_21_rows_greater_than_0_22
        if_query_list_checkfornewdropdownvalues_21_rows_greater_than_0_22 >> rail.Label(
            'Yes') >> foreach_query_list_checkfornewdropdownvalues_21_23 >> create_department_group_or_apply_modification_24 >> foreach_query_list_checkfornewdropdownvalues_21_23_end
        foreach_query_list_checkfornewdropdownvalues_21_23 >> foreach_query_list_checkfornewdropdownvalues_21_23_end >> if_request_grouptype_equals_to_servicecenter_25
        if_query_list_checkfornewdropdownvalues_21_rows_greater_than_0_22 >> rail.Label(
            'No') >> if_request_grouptype_equals_to_servicecenter_25
        if_request_grouptype_equals_to_department_18 >> rail.Label(
            'No') >> if_request_grouptype_equals_to_servicecenter_25
        if_request_grouptype_equals_to_servicecenter_25 >> rail.Label(
            'Yes') >> get_enabled_service_centers_26 >> create_list_27 >> query_list_checkfornewdropdownvalues_28 >> if_query_list_checkfornewdropdownvalues_28_rows_greater_than_0_29
        if_query_list_checkfornewdropdownvalues_28_rows_greater_than_0_29 >> rail.Label(
            'Yes') >> foreach_query_list_checkfornewdropdownvalues_28_30 >> create_service_center_or_apply_modification_31 >> foreach_query_list_checkfornewdropdownvalues_28_30_end
        foreach_query_list_checkfornewdropdownvalues_28_30 >> foreach_query_list_checkfornewdropdownvalues_28_30_end >> if_request_grouptype_equals_to_employeetype_32
        if_query_list_checkfornewdropdownvalues_28_rows_greater_than_0_29 >> rail.Label(
            'No') >> if_request_grouptype_equals_to_employeetype_32
        if_request_grouptype_equals_to_servicecenter_25 >> rail.Label(
            'No') >> if_request_grouptype_equals_to_employeetype_32
        if_request_grouptype_equals_to_employeetype_32 >> rail.Label(
            'Yes') >> get_enabled_employee_type_groups_33 >> create_list_34 >> query_list_checkfornewdropdownvalues_35 >> if_query_list_checkfornewdropdownvalues_35_rows_greater_than_0_36
        if_query_list_checkfornewdropdownvalues_35_rows_greater_than_0_36 >> rail.Label(
            'Yes') >> foreach_query_list_checkfornewdropdownvalues_35_37 >> create_employee_type_group_or_apply_modification_38 >> foreach_query_list_checkfornewdropdownvalues_35_37_end
        foreach_query_list_checkfornewdropdownvalues_35_37 >> foreach_query_list_checkfornewdropdownvalues_35_37_end >> if_request_grouptype_equals_to_costcenter_39
        if_query_list_checkfornewdropdownvalues_35_rows_greater_than_0_36 >> rail.Label(
            'No') >> if_request_grouptype_equals_to_costcenter_39
        if_request_grouptype_equals_to_employeetype_32 >> rail.Label(
            'No') >> if_request_grouptype_equals_to_costcenter_39
        if_request_grouptype_equals_to_costcenter_39 >> rail.Label(
            'Yes') >> get_enabled_cost_centers_40 >> create_list_41 >> query_list_checkfornewdropdownvalues_42 >> if_query_list_checkfornewdropdownvalues_42_rows_greater_than_0_43
        if_query_list_checkfornewdropdownvalues_42_rows_greater_than_0_43 >> rail.Label(
            'Yes') >> foreach_query_list_checkfornewdropdownvalues_42_44 >> create_employee_type_group_or_apply_modification_45 >> foreach_query_list_checkfornewdropdownvalues_42_44_end
        foreach_query_list_checkfornewdropdownvalues_42_44 >> foreach_query_list_checkfornewdropdownvalues_42_44_end >> log_to_sumo
        if_query_list_checkfornewdropdownvalues_42_rows_greater_than_0_43 >> rail.Label(
            'No') >> log_to_sumo
        if_request_grouptype_equals_to_costcenter_39 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
