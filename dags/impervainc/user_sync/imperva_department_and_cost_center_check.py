import json
from airflow.models import Variable
import rail
from impervainc.user_sync.utils import python_callable

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.imperva_department_and_cost_center_check_child,
        description=f'impervainc department and cost center child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_use_conf_payload = rail.IfOperator(
            task_id='can_use_conf_payload',
            test=lambda: Variable.get(
                config.can_use_conf_payload_var_name, default_var='false').lower() == 'true',
            yes_task='get_conf_payload',
            no_task='get_workdayreport_http_payload'
        )

        get_conf_payload = rail.PythonOperator(
            task_id='get_conf_payload',
            python_callable=lambda: json.dumps(rail.get_dag_run_conf()['conf'])
        )

        get_workdayreport_http_payload = rail.SimpleHttpOperator(
            task_id='get_workdayreport_http_payload',
            method='GET',
            endpoint=config.workday_report_endpoint,
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json; charset=utf-8'
            },
            extra_options={
                'verify': False
            }
        )

        workdays_report_data = rail.PythonOperator(
            task_id='workdays_report_data',
            python_callable=lambda: json.loads(rail.result(
                'get_conf_payload') or rail.result('get_workdayreport_http_payload'))
        )

        create_csv_lines_from_report_data=rail.WriteCSVFileOperator(
            task_id='create_csv_lines_from_report_data',
            source="{{result('workdays_report_data')['report'] | to_json }}",
            header=['Satus',
                'Employee ID',
                'Legal first name',
                'Legal last name',
                'Primary work email',
                'Username',
                'Authentication ID',
                'Hire date',
                'Original hire date',
                'Termination date',
                'Manager',
                'Imperva worker type',
                'Imperva employee type',
                'Time type',
                'Pay rate type',
                'Hourly pay',
                'Currency',
                'Job code',
                'Cost center ID',
                'Cost center name',
                'Imperva organization',
                'Time zone of location',
                'Work address country',
                'Country ISO code',
                'Work address state province',
                'State ISO code',
                'Exempt status',
                'Is manager',
                'md5'],
            row= lambda item: [
                item['Status'],
                item['Employee_ID'],
                item['Legal_First_Name'],
                item['Legal_Last_Name'],
                item['primaryWorkEmail'],
                item['Username'],
                item['Authentication_ID'],
                item['Hire_Date'],
                item['Original_Hire_Date'],
                item['termination_date'],
                item['Manager'],
                item['Imperva_Worker_Type'],
                item['Imperva_Employee_Type'],
                item['Time_Type'],
                item['Pay_Rate_Type'],
                item['Hourly_Pay'],
                item['Currency'],
                item['Job_Code'],
                item['Cost_Center_ID'],
                item['Cost_Center_Name'],
                item['Imperva_Organization'],
                item['Time_Zone_of_Location_of_Worker_s_Primary_Position'],
                item['Work_Address_Country'],
                item['Country_ISO_Code'],
                item['Work_Address_State_Province'],
                item['State_ISO_Code'],
                item['Exempt_Status'],
                item['isManager'],
                python_callable.get_md5(item)
            ],
        )

        create_collection_workdaydata = rail.CreateCollectionOperator(
            task_id='create_collection_workdaydata',
            source = "{{ result('create_csv_lines_from_report_data') }}",
            name = "workdaydata",
            columns = {
                'Status':'status', 
                'Employee ID':'Employee_ID', 
                'Legal First Name':'Legal_First_Name', 
                'Legal Last Name':'Legal_Last_Name', 
                'primaryWorkEmail':'primaryWorkEmail', 
                'Username':'Username', 
                'Authentication ID':'Authentication_ID', 
                'Hire Date':'Hire_Date', 
                'Original Hire Date':'Original_Hire_Date', 
                'termination date':'termination_date', 
                'Manager':'Manager', 
                'Imperva Worker Type':'Imperva_Worker_Type', 
                'Imperva Employee Type':'Imperva_Employee_Type', 
                'Time Type':'Time_Type', 
                'Pay Rate Type':'Pay_Rate_Type', 
                'Hourly Pay':'Hourly_Pay', 
                'Currency':'Currency', 
                'Job Code':'Job_Code', 
                'Cost Center ID':'Cost_Center_ID', 
                'Cost Center Name':'Cost_Center_Name', 
                'Imperva Organization':'Imperva_Organization', 
                'timezone':'timezone', 
                'Work Address Country':'Work_Address_Country', 
                'Country ISO Code':'Country_ISO_Code', 
                'Work Address State Province':'Work_Address_State_Province', 
                'State ISO Code':'State_ISO_Code', 
                'Exempt Status':'Exempt_Status', 
                'isManager':'isManager', 
                'md5':'md_5'
            }
        )

        get_all_cost_center = rail.RepliconServiceOperator(
            task_id="get_all_cost_center",
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
        )

        parent_cost_center_uri = rail.PythonOperator(
            task_id='parent_cost_center_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_cost_center'),
                "displayText",
                "Imperva Inc",
                "uri"
                )
        )

        get_all_department_hierarchy = rail.RepliconServiceOperator(
            task_id="get_all_department_hierarchy",
            endpoint="/services/DepartmentService1.svc/GetAllDepartmentHierarchyDetails",
        )

        create_department_details_list = rail.PythonOperator(
            task_id='create_department_details_list',
            python_callable=python_callable.create_department_details_list
        )

        get_company_department = rail.RepliconServiceOperator(
            task_id='get_company_department',
            endpoint="/services/DepartmentService1.svc/GetCompanyDepartment"
        )

        create_departmentlist_collection = rail.CreateCollectionOperator(
            task_id='create_departmentlist_collection',
            source="{{ result('create_department_details_list') | to_json }}",
            name='departmentlist'
        )

        create_costcenterlist_collection = rail.CreateCollectionOperator(
            task_id='create_costcenterlist_collection',
            source="{{ result('get_all_cost_center') | to_json }}",
            name='costcenterlist'
        )

        get_new_value_add_to_department = rail.QueryCollectionOperator(
            task_id='get_new_value_add_to_department',
            query="SELECT DISTINCT Cost_Center_Name as department FROM workdaydata WHERE Cost_Center_Name NOT IN \
                (SELECT DISTINCT name FROM departmentlist)"
        )

        get_new_value_add_to_costcenter = rail.QueryCollectionOperator(
            task_id='get_new_value_add_to_costcenter',
            query="SELECT DISTINCT Cost_Center_Name as costcenter FROM workdaydata WHERE Cost_Center_Name NOT IN \
                (SELECT DISTINCT displayText FROM costcenterlist)"
        )

        put_department = rail.RepliconServiceCallForEachItemOperator(
            task_id='put_department',
            endpoint='/services/DepartmentService1.svc/PutDepartment',
            items=lambda: rail.result('get_new_value_add_to_department'),
            data={
                "department": {
                    "target": {
                        "uri": null,
                        "name": "{{ item.department }}",
                        "parent": {
                            "uri": "{{result('get_company_department').uri}}",
                            "name": null,
                            "parent": null,
                            "parameterCorrelationId": null
                        },
                        "parameterCorrelationId": null
                    },
                    "name": "{{ item.department }}",
                    "code": null,
                    "comments": null,
                    "isEnabled": "true",
                    "customFieldValues": []
                }
            },
        )

        foreach_costcenter = rail.ForEachOperator(
            task_id='foreach_costcenter',
            items="{{result('get_new_value_add_to_costcenter') | load_all_records() | to_json}}",
            start_task='create_costcenter_draft',
            end_task='foreach_costcenter_end'
        )

        create_costcenter_draft= rail.RepliconServiceOperator(
            task_id='create_costcenter_draft',
            endpoint="/services/CostCenterService1.svc/CreateNewDraft",
            data= {
                "parentCostCenterUri": "{{result('parent_cost_center_uri')}}"
            }
        )

        update_costcenter_name= rail.RepliconServiceOperator(
            task_id='update_costcenter_name',
            endpoint="/services/CostCenterService1.svc/UpdateName",
            data= {
                    "costCenterUri": "{{result('create_costcenter_draft')}}",
                    "name": "{{result('foreach_costcenter').costcenter}}"
                }
        )

        publish_costcenter_draft= rail.RepliconServiceOperator(
            task_id='publish_costcenter_draft',
            endpoint="/services/CostCenterService1.svc/PublishDraft",
            data= {
                "draftUri": "{{result('create_costcenter_draft')}}"
            }
        )

        foreach_costcenter_end = rail.EmptyOperator(
            task_id='foreach_costcenter_end'
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_use_conf_payload >> rail.Label("Yes") >> get_conf_payload >> workdays_report_data
        can_use_conf_payload >> rail.Label("No") >> get_workdayreport_http_payload >> workdays_report_data
        workdays_report_data >> create_csv_lines_from_report_data >> create_collection_workdaydata >> \
        get_all_cost_center >> parent_cost_center_uri >> get_all_department_hierarchy >> create_department_details_list >> \
        get_company_department >> create_departmentlist_collection >> create_costcenterlist_collection >> get_new_value_add_to_department >> \
        get_new_value_add_to_costcenter >> put_department >> foreach_costcenter >> create_costcenter_draft >> update_costcenter_name >> \
        publish_costcenter_draft >> foreach_costcenter_end
        foreach_costcenter >> foreach_costcenter_end >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
