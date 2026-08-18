
from datetime import timedelta
import rail


null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.process_new_projects_dag,
        description=f'Mccarthy - Trigger Dags To Process New projects in Replicon Child {config.instance} V1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        query_all_tasks_for_the_project=rail.QueryCollectionOperator(
            task_id='query_all_tasks_for_the_project',
            query="""SELECT  inputfile.TaskName, inputfile.TaskCode, inputfile.TaskStartDate,
                    inputfile.TaskEndDate FROM  inputfile WHERE  inputfile.ProjectName = '{{ dag_run.conf.projectname }}'""",
        )

        def get_tasklist():
            tasklist = [ {
                'Taskname': row['TaskName'],
                'Taskcode': row['TaskCode'],
                'Taskstartdate': row['TaskStartDate'],
                'Taskenddate': row['TaskEndDate']
            }
            for row in rail.load_all_records(rail.result('query_all_tasks_for_the_project'))]
            return tasklist

        def get_payload_create_child(dag_run):
            return {
                "Regionname": dag_run.conf['regionname'],
                "Projectname": dag_run.conf['projectname'],
                "Projectcode": dag_run.conf['projectcode'],
                "Projectdescription": dag_run.conf['projectdescription'],
                "Projectstartdate": dag_run.conf['projectstartdate'],
                "Projectenddate": dag_run.conf['projectenddate'],
                "Tasklist": get_tasklist() if dag_run.conf['projectname'] else [],
                "JobID": dag_run.conf['jobid'],
                'lookuptable': dag_run.conf['lookuptable'],
                'inputfilename': dag_run.conf['inputfilename']
            }

        trigger_child_to_create_project=rail.TriggerDagRunOperator(
            task_id='trigger_child_to_create_project',
            trigger_dag_id=config.create_projects_dag,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=get_payload_create_child
        )

        wait_for_trigger_child_to_create_project = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_trigger_child_to_create_project',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('trigger_child_to_create_project')}}"
        )


        query_all_tasks_for_the_project >> trigger_child_to_create_project >> wait_for_trigger_child_to_create_project

    return dag


rail.for_each_instance(create_dag)
