from pendulum import datetime
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id = config.project_dates_update_child,
        description=f"wipro process project start and enddates Child {config.instance}",
        replicon_conn_id=config.replicon_conn_id,
        start_date= datetime(2023,9,1),
        company_key=config.company_key,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_location_config")

        query_project_details = rail.QueryCollectionOperator(
            task_id = "query_project_details",
            query="""SELECT MIN(Assignment_Start_Date), MAX(Assignment_End_Date) FROM task_assignment_report_collection \
                    WHERE Project_Code == :project_code AND \
                    NULLIF(Assignment_Start_Date, '') IS NOT NULL AND \
                    NULLIF(Assignment_End_Date, '') IS NOT NULL""",
            name="project_data",
            query_params= {
                'project_code': '{{ dag_run.conf.project_code }}'
            }
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run:{
                "projects": [
                    {
                        "code": dag_run.conf['project_code']
                    }
                ]
            },
            data_handler=lambda response: response[0].get('projectDetails')
        )

        def get_project_dates_payload():
            data = rail.load_all_records(rail.result("query_project_details"))[0]
            return {
                "projectUri": rail.result("get_project_details")['uri'],
                "dateRange": {
                    "startDate": rail.parse_date(data['MIN_Assignment_Start_Date_'],'%Y/%m/%d'),
                    "endDate": rail.parse_date(data['MAX_Assignment_End_Date_'],'%Y/%m/%d')
                }
            }

        update_project_daterange = rail.RepliconServiceOperator(
            task_id= 'update_project_daterange',
            endpoint= '/services/ProjectService1.svc/UpdateTimeEntryDateRange',
            data= get_project_dates_payload
        )

        query_project_details >> get_project_details >> update_project_daterange

    return dag

rail.for_each_instance(create_main_dag)
