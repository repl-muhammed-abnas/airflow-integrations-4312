import rail
from unisys.workday_user_import_v1.utils import custom_method

null = None

def create_child_dag(config):
    
    with rail.create_airflow_dag(
        dag_id=config.process_projects,
        description='Unisys Workday User Import - Process Projects',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_divisions,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        query_resource_data = rail.QueryCollectionOperator(
            task_id="query_resource_data",
            query="""SELECT * FROM project_resources WHERE projectcode=:prjt_cd""",
            query_params={
                'prjt_cd': '{{ dag_run.conf.projectcode }}'
            }
        )

        get_resources_to_assign = rail.PythonOperator(
            task_id=f"get_resources_to_assign",
            python_callable=custom_method.get_resources_to_add
        )

        add_task_resource = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_task_resource',
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            items= '{{ result("get_resources_to_assign") | to_json }}',
            data=lambda item: {
                "taskUri": item['taskUri'],
                "resourceUris": item['uris'],
                "isAssigned": "true"
            }
        )

        catch_and_log = rail.WriteLogOperator(
            task_id='catch_and_log',
            trigger_rule='one_failed',
            message='Error While assigning resource',
            log='{{ dag_run.conf.user_log }}',
            severity='Error',
            properties={
                'lastname': '',
                'firstname': '',
                'loginname': '',
                'employeeid': '',
                'manager': '',
                "userstatus": '',
                "co_costcenter": '',
                "location": '',
                'action': '',
                'status': 'Error',
                'details': "Error While assigning resource {{ get_error_message() }}"
            }
        )

        query_resource_data >> get_resources_to_assign >> add_task_resource >> catch_and_log

    return dag

rail.for_each_instance(create_child_dag)
