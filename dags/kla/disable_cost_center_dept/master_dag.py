
from datetime import timedelta
import json
import rail
from kla.disable_cost_center_dept.util.data_formatting import get_final_dept_list, get_disabled_cost_center_list, get_final_costcenter_disable_list

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'kla_disable_costcentre_and_department_master_{config.instance}',
        description=f'KLA Disable Cost Centre and Department Master_V2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs
    ) as dag:

        start=rail.EmptyOperator(
            task_id='start',
        )

        cost_center_view_data = rail.SimpleHttpOperator(
            task_id='cost_center_view_data',
            method='GET',
            http_conn_id=config.http_conn_id,
            endpoint='/RESTAdapter/pdr/query/CostCenterView',
            data={
                "REQUESTOR": "REPLICON",
                "STATUS": "DISABLED"
            },
            headers={
                "Content-Type": 'application/json; charset=utf-8'
            },
            extra_options={
                'verify': False
            }
        )

        department_view_data = rail.SimpleHttpOperator(
            task_id='department_view_data',
            method='GET',
            http_conn_id=config.http_conn_id,
            endpoint='/RESTAdapter/pdr/query/DeptView',
            data={
                "REQUESTOR": "REPLICON",
                "STATUS": "DISABLED"
            },
            headers={
                "Content-Type": 'application/json; charset=utf-8'
            },
            extra_options={
                'verify': False
            }
        )

        # Dept. logic begins here
        get_deptviews = rail.PythonOperator(
            task_id='get_deptviews',
            python_callable=lambda:rail.result('department_view_data')
        )

        log_message_deptviews = rail.PythonOperator(
            task_id='log_message_deptviews',
            python_callable=lambda: json.loads(rail.result(
                'get_deptviews'))['Department_ViewXSD_response']['Statement1_response']['row']
        )

        create_all_dept_collection = rail.CreateCollectionOperator(
            task_id='create_all_dept_collection',
            source = '{{ result("log_message_deptviews") | tojson }}',
            name = "All_Department",
        )

        query_list_get_inactivedepartments_6=rail.QueryCollectionOperator(
            task_id='query_list_get_inactivedepartments_6',
            query="""SELECT * FROM  All_Department WHERE  All_Department.EFF_STATUS='I'""",
        )

        records_inactive_dept = rail.PythonOperator(
            task_id= 'records_inactive_dept',
            python_callable= lambda: rail.load_all_records(rail.result("query_list_get_inactivedepartments_6"))
        )

        get_enabled_departmentsfrom_replicon=rail.RepliconServiceOperator(
            task_id='get_enabled_departmentsfrom_replicon',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments"
        )

        create_enable_replicon_dept = rail.CreateCollectionOperator(
            task_id='create_enable_replicon_dept',
            source = '{{ result("get_enabled_departmentsfrom_replicon") | tojson }}',
            name = "Enable_Replicon_Dept",
        )

        query_enable_replicon_dept=rail.QueryCollectionOperator(
            task_id='query_enable_replicon_dept',
            query="""SELECT * FROM  Enable_Replicon_Dept WHERE URI IS NOT NULL""",
        )

        records_enable_replicon_dept = rail.PythonOperator(
            task_id= 'records_enable_replicon_dept',
            python_callable= lambda: rail.load_all_records(rail.result("query_enable_replicon_dept"))
        )

        final_dept_list =  rail.PythonOperator(
            task_id= 'final_dept_list',
            python_callable= get_final_dept_list,
            op_args=['{{ result("records_enable_replicon_dept") | tojson }}',
                     '{{ result("records_inactive_dept") | tojson }}']
        )

        disable_cost_centre_and_department_child=rail.TriggerDagRunForEachItemOperator(
            task_id='disable_cost_centre_and_department_child',
            retries=0,
            items="{{ result('final_dept_list')  | tojson }}",
            trigger_dag_id=f'kla_disable_costcentre_and_department_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item : {
                "type": "department",
                "uri": item["uri"],
                "name": item["name"]
            }
        )

        wait_for_disable_cost_centre_and_department_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_disable_cost_centre_and_department_child',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("disable_cost_centre_and_department_child") }}'
        )

        # cost center logic begins here
        get_costcenterviews = rail.PythonOperator(
            task_id='get_costcenterviews',
            python_callable=lambda:rail.result('cost_center_view_data')
        )

        log_message_costcenterviews = rail.PythonOperator(
            task_id='log_message_costcenterviews',
            python_callable=lambda: json.loads(rail.result(
                'get_costcenterviews'))['CostCenter_ViewXSD_response']['Statement1_response']['row']
        )

        create_all_disable_cost_center_collection = rail.CreateCollectionOperator(
            task_id='create_all_disable_cost_center_collection',
            source = '{{ result("log_message_costcenterviews") | tojson }}',
            name = "Disabled_Cost_Center",
        )

        query_list_get_disabled_cost_center=rail.QueryCollectionOperator(
            task_id='query_list_get_disabled_cost_center',
            query="""SELECT REPLACE(LTRIM(REPLACE( Disabled_Cost_Center.COST_CENTER,'0',' ')),' ','0') FROM  Disabled_Cost_Center""",
        )

        records_disabled_cost_center = rail.PythonOperator(
            task_id= 'records_disabled_cost_center',
            python_callable= lambda: rail.load_all_records(rail.result("query_list_get_disabled_cost_center"))
        )

        fetch_disabled_costcenter_name = rail.PythonOperator(
            task_id= 'fetch_disabled_costcenter_name',
            python_callable= get_disabled_cost_center_list,
            op_args= [('{{ result("records_disabled_cost_center") | tojson }}')]
        )

        get_enabled_cost_center_from_replicon=rail.RepliconServiceOperator(
            task_id='get_enabled_cost_center_from_replicon',
            endpoint="/services/CostCenterService1.svc/GetEnabledCostCenters"
        )

        create_enabled_replicon_cost_center = rail.CreateCollectionOperator(
            task_id='create_enabled_replicon_cost_center',
            source = '{{ result("get_enabled_cost_center_from_replicon") | tojson }}',
            name = "Enable_Replicon_Cost_Center",
        )

        query_enable_replicon_cost_center=rail.QueryCollectionOperator(
            task_id='query_enable_replicon_cost_center',
            query="""SELECT * FROM  Enable_Replicon_Cost_Center""",
        )

        records_enable_replicon_cost_center = rail.PythonOperator(
            task_id= 'records_enable_replicon_cost_center',
            python_callable= lambda: rail.load_all_records(rail.result("query_enable_replicon_cost_center"))
        )

        final_disabled_costcenter_list = rail.PythonOperator(
            task_id= 'final_disabled_costcenter_list',
            python_callable= get_final_costcenter_disable_list,
            op_args=['{{ result("records_enable_replicon_cost_center") | tojson }}',
                     '{{ result("fetch_disabled_costcenter_name") | tojson }}']
        )

        disable_cost_centre_and_department_child_for_cost_center=rail.TriggerDagRunForEachItemOperator(
            task_id='disable_cost_centre_and_department_child_for_cost_center',
            retries=0,
            items="{{ result('final_disabled_costcenter_list') | tojson }}",
            trigger_dag_id=f'kla_disable_costcentre_and_department_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item : {
                "type": "cost centre",
                "uri": item["uri"],
                "name": item["displayText"]
            }
        )

        wait_for_disable_cost_centre_and_department_child_for_cost_center = rail.WaitForDagRunsSensor(
            task_id='wait_for_disable_cost_centre_and_department_child_for_cost_center',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("disable_cost_centre_and_department_child_for_cost_center") }}'
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        start >> cost_center_view_data >> department_view_data >> get_deptviews >> log_message_deptviews >> create_all_dept_collection\
        >> query_list_get_inactivedepartments_6\
        >> records_inactive_dept >> get_enabled_departmentsfrom_replicon >> create_enable_replicon_dept >> query_enable_replicon_dept\
        >> records_enable_replicon_dept >> final_dept_list >> disable_cost_centre_and_department_child >> wait_for_disable_cost_centre_and_department_child
        # Cost center logic tasks
        wait_for_disable_cost_centre_and_department_child >> get_costcenterviews >> log_message_costcenterviews\
        >> create_all_disable_cost_center_collection >> query_list_get_disabled_cost_center\
        >> records_disabled_cost_center >> fetch_disabled_costcenter_name >> get_enabled_cost_center_from_replicon >> create_enabled_replicon_cost_center\
        >> query_enable_replicon_cost_center >> records_enable_replicon_cost_center >>  final_disabled_costcenter_list\
        >> disable_cost_centre_and_department_child_for_cost_center >> wait_for_disable_cost_centre_and_department_child_for_cost_center >> finish
    return dag


rail.for_each_instance(create_dag)
