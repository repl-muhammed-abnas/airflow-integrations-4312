
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'kla_user_import_usa_cost_center_department_check_{config.instance}',
        description=f'kla_user_import_usa_cost_center_department_check {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        get_costcenters = rail.RepliconServiceOperator(
            task_id='get_costcenters',
            endpoint='/services/CostCenterListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:cost-center",
                    "urn:replicon:cost-center-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: list(map(lambda row: {
                "name": row['cells'][0]['textValue'],
                "uri": row['cells'][0]['uri'],
                "status": row['cells'][1]['textValue']
            }, data['rows']))
        )

        get_departments = rail.RepliconServiceOperator(
            task_id='get_departments',
            endpoint='/services/DepartmentListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:department-list-column:name",
                    "urn:replicon:department-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: list(map(lambda row: {
                "name": row['cells'][0]['textValue'],
                "uri": row['cells'][0]['uri'],
                "status": row['cells'][1]['textValue']
            }, data['rows']))
        )

        get_locations = rail.RepliconServiceOperator(
            task_id='get_locations',
            endpoint='/services/LocationListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris":  [
                    "urn:replicon:location-list-column:code",
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: list(map(lambda row: {
                "name": row['cells'][1].get('textValue'),
                "uri": row['cells'][1].get('uri'),
                "status": row['cells'][2].get('textValue'),
                "code": row['cells'][0].get('textValue'),
            }, data['rows']))
        )

        def do_get_emp_view():
            row = rail.get_current_context()['dag_run'].conf['emp_view']
            if row and isinstance(row, list):
                return row
            return [row]

        get_empview = rail.PythonOperator(
            task_id='get_empview',
            python_callable=do_get_emp_view
        )

        create_user_collection = rail.CreateCollectionOperator(
            task_id='create_user_collection',
            source="{{ result('get_empview') | to_json }}",
            name="user"
        )

        query_list_uniquelistofdepartments = rail.QueryCollectionOperator(
            task_id='query_list_uniquelistofdepartments',
            query='''SELECT DISTINCT deptname,deptid FROM user
                        WHERE (EMPLOYEEID IS NOT NULL AND EMPLOYEEID != '') AND
                        (user.CAMPUSCOUNTRY= "USA") OR
                        (user.HASUSADIRECTREPORTS= "Y" AND NOT user.CAMPUSCOUNTRY= "USA" AND NOT user.CAMPUSCOUNTRY= "JPN")
             ''',
        )

        query_list_uniquelistofcostcenters = rail.QueryCollectionOperator(
            task_id='query_list_uniquelistofcostcenters',
            query='''SELECT DISTINCT costcenter FROM user WHERE  costcenter != "" AND costcenter IS NOT NULL AND (EMPLOYEEID IS NOT NULL AND EMPLOYEEID != '') ''',
        )

        get_items_enable_department = rail.PythonOperator(
            task_id='get_items_enable_department',
            python_callable=lambda: list(map(lambda x: {
                "departmentUri": x['uri']
            }, filter(lambda x: 'False' == x['status'],
                      list(map(
                          lambda x: {
                              "name": f"{x['DEPTNAME']}-{x['DEPTID']}",
                              "uri": rail.find_first_by_attr_and_get_attr(rail.result(get_departments.task_id), 'name', f"{x['DEPTNAME']}-{x['DEPTID']}", 'uri'),
                              "status": rail.find_first_by_attr_and_get_attr(rail.result(get_departments.task_id), 'name', f"{x['DEPTNAME']}-{x['DEPTID']}", 'status'),
                          },
                          rail.load_all_records(rail.result(
                              'query_list_uniquelistofdepartments'))
                      ))
                      )))
        )

        enable_department = rail.RepliconServiceCallForEachItemOperator(
            task_id='enable_department',
            endpoint='/services/DepartmentService1.svc/Enable',
            items="{{ result('get_items_enable_department') | to_json }}",
            data={
                "departmentUri": "{{ item.departmentUri }}"
            }
        )

        get_items_put_department = rail.PythonOperator(
            task_id='get_items_put_department',
            python_callable=lambda: list(map(lambda x: {
                "name": x['name']
            }, filter(lambda x: not x['uri'],
                      list(map(
                          lambda x: {
                              "name": f"{x['DEPTNAME']}-{x['DEPTID']}",
                              "uri": rail.find_first_by_attr_and_get_attr(rail.result(get_departments.task_id), 'name', f"{x['DEPTNAME']}-{x['DEPTID']}", 'uri'),
                              "status": rail.find_first_by_attr_and_get_attr(rail.result(get_departments.task_id), 'name', f"{x['DEPTNAME']}-{x['DEPTID']}", 'status'),
                          },
                          rail.load_all_records(rail.result(
                              'query_list_uniquelistofdepartments'))
                      ))
                      )))
        )

        put_department = rail.RepliconServiceCallForEachItemOperator(
            task_id='put_department',
            endpoint='/services/DepartmentService1.svc/PutDepartment',
            items=lambda: rail.result('get_items_put_department'),
            data={
                "department": {
                    "target": {
                        "uri": null,
                        "name": "{{ item.name }}",
                        "parent": {
                            "uri": null,
                            "name": "KLA Corporation",
                            "parent": null,
                            "parameterCorrelationId": null
                        },
                        "parameterCorrelationId": null
                    },
                    "name": "{{ item.name }}",
                    "code": null,
                    "comments": null,
                    "isEnabled": "true",
                    "customFieldValues": []
                }
            },
        )

        get_items_enable_costcenter = rail.PythonOperator(
            task_id='get_items_enable_costcenter',
            python_callable=lambda: list(map(lambda x: {
                "costCenterUri": x['uri']
            }, filter(lambda x: 'False' == x['status'],
                      list(map(
                          lambda x: {
                              "name": x['COSTCENTER'],
                              "uri": rail.find_first_by_attr_and_get_attr(rail.result(get_costcenters.task_id), 'name', x['COSTCENTER'], 'uri'),
                              "status": rail.find_first_by_attr_and_get_attr(rail.result(get_costcenters.task_id), 'name', x['COSTCENTER'], 'status'),
                          },
                          rail.load_all_records(rail.result(
                              'query_list_uniquelistofcostcenters'))
                      ))
                      )))
        )

        enable_costcenter = rail.RepliconServiceCallForEachItemOperator(
            task_id='enable_costcenter',
            endpoint='/services/CostCenterService1.svc/Enable',
            items="{{ result('get_items_enable_costcenter') | to_json }}",
            data={
                "costCenterUri": "{{ item.costCenterUri }}"
            }
        )

        get_items_create_cost_center = rail.PythonOperator(
            task_id='get_items_create_cost_center',
            python_callable=lambda: list(map(lambda x: {
                "name": x['name']
            }, filter(lambda x: not x['uri'],
                      list(map(
                          lambda x: {
                              "name": x['COSTCENTER'],
                              "uri": rail.find_first_by_attr_and_get_attr(rail.result(get_costcenters.task_id), 'name', x['COSTCENTER'], 'uri'),
                              "status": rail.find_first_by_attr_and_get_attr(rail.result(get_costcenters.task_id), 'name', x['COSTCENTER'], 'status'),
                          },
                          rail.load_all_records(rail.result(
                              'query_list_uniquelistofcostcenters'))
                      ))
                      )))
        )

        create_cost_center_or_apply_modification = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_cost_center_or_apply_modification',
            endpoint='/services/CostCenterService1.svc/CreateCostCenterOrApplyModification',
            items=lambda: rail.result('get_items_create_cost_center'),
            data={
                "costCenter": {
                    "name": null,
                    "uri": null,
                    "parent": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ item.name}}",
                    "codeToApply": {
                        "value":  "{{ item.name}}",
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() + item.name }}"
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        get_costcenters >> get_departments >> get_locations >> get_empview >> \
            create_user_collection >> query_list_uniquelistofdepartments >> query_list_uniquelistofcostcenters >> \
            get_items_enable_department >> enable_department >> get_items_put_department >> put_department >>\
            get_items_enable_costcenter >> enable_costcenter >> get_items_create_cost_center >> create_cost_center_or_apply_modification >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
