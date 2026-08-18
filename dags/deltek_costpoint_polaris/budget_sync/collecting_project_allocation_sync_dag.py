from datetime import datetime
import rail


null = None

# pylint: disable=too-many-statements


def create_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_collecting_project_allocation_{config.instance}',
        description=f'deltek_costpoint_collecting_project_allocation_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=[
            rail.WebhookConf(
                hmac_secret_var=config.cp_polaris_webhook_secret_project),
            rail.WebhookConf(
                hmac_secret_var=config.cp_project_polaris_teamMember_allocation),
            rail.WebhookConf(
                hmac_secret_var=config.cp_polaris_webhook_secret_task)
        ],
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id
        },
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        def is_project_present(dag_run):
            if dag_run.conf['webhook']['data'].get('project'):
                return True
            return False

        if_project_present = rail.IfOperator(
            task_id='if_project_present',
            test=lambda dag_run: is_project_present(dag_run),
            yes_task="get_project_details",
            no_task="get_task_details",
        )

        get_task_details = rail.RepliconServiceOperator(
            task_id='get_task_details',
            endpoint="/services/TaskService1.svc/GetTaskDetails",
            data={
                "taskUri": '''{{ dag_run.conf.webhook.data.task.uri }}'''
            }
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint="/services/ImportService1.svc/BulkGetProjects2",
            data={
                "projects": [
                    {
                        "uri": '''{{ dag_run.conf.webhook.data.project.uri }}''',
                        "name": null,
                        "code": null,
                        "parameterCorrelationId": null
                    }
                ]
            },
            data_handler=lambda data: null if data['errors'] else data['results'][0]
        )

        def get_project_informations():
            project_info = rail.result('get_project_details')
            task_info = rail.result('get_task_details')
            return {
                "task": {
                    "uri": task_info['uri'],
                    "task_start_date": datetime.strftime(datetime(task_info['timeEntryDateRange']['startDate']['year'],
                                                                  task_info['timeEntryDateRange']['startDate']['month'],
                                                                  task_info['timeEntryDateRange']['startDate']['day']), config.polaris_date_format),
                    "task_end_date": datetime.strftime(datetime(task_info['timeEntryDateRange']['endDate']['year'],
                                                                task_info['timeEntryDateRange']['endDate']['month'],
                                                                task_info['timeEntryDateRange']['endDate']['day']), config.polaris_date_format)
                } if task_info else None,
                "project": {
                    "uri": project_info['project']['uri'],
                    "project_start_date": datetime.strftime(datetime(project_info['project']['timeEntryDateRange']['startDate']['year'],
                                                                     project_info['project']['timeEntryDateRange']['startDate']['month'],
                                                                     project_info['project']['timeEntryDateRange']['startDate']['day']), config.polaris_date_format),
                    "project_end_date": datetime.strftime(datetime(project_info['project']['timeEntryDateRange']['endDate']['year'],
                                                                   project_info['project']['timeEntryDateRange']['endDate']['month'],
                                                                   project_info['project']['timeEntryDateRange']['endDate']['day']), config.polaris_date_format)
                } if project_info else None
            }

        def is_budgeted_task_project():
            project_info = rail.result('get_project_details')
            project_budget_type = rail.find_first_by_attr_and_get_attr(
                project_info['project']['customFields'], "customField.name", config.project_task_budget_type_oef, "text") \
                if project_info else None
            if project_budget_type and project_budget_type.lower() == "bud":
                return True
            task_info = rail.result('get_task_details')
            task_budget_type = rail.find_first_by_attr_and_get_attr(
                task_info['customFields'], "customField.name", config.project_task_budget_type_oef, "text") if task_info else None
            if task_budget_type and task_budget_type.lower() == "bud":
                return True
            return False

        if_project_task_budgeted = rail.IfOperator(
            task_id='if_project_task_budgeted',
            test=is_budgeted_task_project,
            yes_task="collecting_project_data",
            no_task="finish",
        )

        collecting_project_data = rail.PythonOperator(
            task_id='collecting_project_data',
            python_callable=get_project_informations
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        if_project_present
        if_project_present >> rail.Label(
            'No') >> get_task_details >> if_project_task_budgeted
        if_project_task_budgeted >> rail.Label(
            'Yes') >> collecting_project_data >> finish
        if_project_task_budgeted >> rail.Label('No') >> finish
        if_project_present >> rail.Label(
            'Yes') >> get_project_details >> if_project_task_budgeted

    return dag


rail.for_each_instance(create_dag)
