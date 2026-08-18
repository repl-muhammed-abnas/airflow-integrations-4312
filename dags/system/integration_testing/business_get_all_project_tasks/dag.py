"""
### System Integration Testing Business Get All Project Tasks Operators

#### Purpose:
- This DAG tests all the operators under the <u>[rail/operators](https://github.com/replicon/replicon-airflow-library/tree/main/rail/rail/operators)</u> folder
- PWD state link: <u>https://pwd.rplcn.co/plid=2812</u>

#### Test Cases:
- Added tests for retrieving all tasks of project
"""

from datetime import datetime, timedelta
import rail
from system.integration_testing import config
from system.integration_testing.business_get_all_project_tasks import python_callable_method
null = None

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/system/integration_testing/config.py


with rail.create_airflow_dag(
    dag_id="system_integration_testing_business_get_all_project_tasks_operators",
    description="System Integration Testing Business Get All Project Tasks Operators",
    company_key=config.company_key,
    replicon_conn_id=config.replicon_conn_id,
    start_date=datetime(2022, 1, 1),
    group='system',
    max_active_runs=10,
    is_paused_upon_creation=True,
    default_args={
        'owner': 'system',
        'replicon_conn_id': config.replicon_conn_id,
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'doc': __doc__
    }
) as dag:

    rail.ViewDagRunConfOperator(
        task_id='view_dagrun_config')

    batch_task_operator = rail.BatchTaskRunOperator(
        task_id="batch_task_operator",
        start_task="create_project",
        end_task="delete_this_dagrun",
        execution_timeout=timedelta(hours=config.execution_timeout_hours),
    )

    create_project = rail.RepliconServiceOperator(
        task_id='create_project',
        endpoint="/services/ProjectService1.svc/PutProjectInfo2",
        data={
                "target": {
                    "uri": null,
                    "name": "{{ dag_run.conf.project_name }}",
                    "code": null,
                    "parameterCorrelationId": null
                },
            "projectInfo": {
                    "name": "{{ dag_run.conf.project_name }}",
                    "code": "{{ dag_run.conf.project_id }}",
                    "description": null,
                    "timeEntryDateRange": null,
                    "projectStatusLabel": {
                        "uri": null,
                        "name": "In Progress"
                    },
                    "percentCompleted": "0",
                    "client": null,
                    "clientRepresentative": null,
                    "program": null,
                    "projectLeader": null,
                    "customFieldValues": [],
                    "isTimeEntryAllowed": "1",
                    "costTypeUri": null,
                    "estimatedHours": null,
                    "estimatedCost": null,
                    "estimatedExpenses": null,
                    "budget": null,
                    "isProjectLeaderApprovalRequired": "1",
                    "estimationModeUri": null,
                    "billingTypeUri": "urn:replicon:billing-type:time-and-material",
                    "timeAndMaterials": {
                        "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                        "billingRateFrequency": null,
                        "billingRateFrequencyDuration": null,
                        "billingRates": []
                    },
                    "defaultBillingCurrency": null
                }
        }
    )

    create_tasks = rail.RepliconServiceCallForEachItemOperator(
        task_id="create_tasks",
        endpoint="/services/ProjectService1.svc/PutTask",
        items='{{ dag_run.conf.tasks | to_json }}',
        execution_timeout=timedelta(days=14),
        flatten=True,
        data=lambda item: {
                "project": {
                    "uri": rail.result(
                        'create_project')['uri'],
                    "name": null,
                    "parameterCorrelationId": null
                },
            "task": {
                    "target": {
                        "uri": null,
                        "name": item['key'],
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "name": item["key"],
                    "code": item["issue_id"],
                    "description": null,
                    "timeEntryDateRange": {
                        "startDate": {
                            "year": item['created'].split('-')[0],
                            "month": item['created'].split('-')[1],
                            "day": item['created'].split('-')[2]
                        },
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "1",
                    "estimatedHours": null,
                    "isClosed": "0",
                    "customFieldValues": [],
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "timeAndExpenseEntryTypeUri": null,
                    "assignedResources": [
                        {
                            "uri": null,
                            "resourcePlaceholderParameterCorrelationId": null,
                            "user": null,
                            "department": {
                                "uri": null,
                                "name": "Company",
                                "parent": null,
                                "parameterCorrelationId": null
                            },
                            "placeholder": null,
                            "location": null,
                            "division": null,
                            "costCenter": null,
                            "serviceCenter": null,
                            "departmentGroup": null,
                            "employeeTypeGroup": null
                        }
                    ]
            }
        }
    )

    load_project = rail.RepliconServiceOperator(
        task_id='load_project',
        endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
        data={"projects": [{"name": "{{ dag_run.conf.project_name }}"}]},
        response_filter=lambda resp: (resp.json()['d'][0:1] or [
            {"projectDetails": None}])[0]['projectDetails'],
    )

    load_team_members = rail.RepliconServiceOperator(
        task_id='load_team_members',
        endpoint='/services/ProjectService1.svc/GetAllProjectTeamMemberDetails',
        data={
                "projectUri": "{{ result('load_project').uri }}",
                "asOfDate": None},
        data_handler=lambda data: list(
            map(lambda assignment: assignment['resource']['uri'], data)),
    )

    load_all_tasks_of_project = rail.GetAllProjectTasksOperator(
        task_id='load_all_tasks_of_project',
        project_uri="{{ result('load_project').uri }}",
    )

    error_message = "Response data mismatch for run id:{{ dag_run_ecid() }} "

    assert_report_response = rail.PythonOperator(
        task_id="assert_report_response",
        python_callable=python_callable_method.assert_response,
        op_args=[error_message],
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id="delete_this_dagrun", trigger_rule="none_failed"
    )

    (
        batch_task_operator
        >> rail.Label("Test user details reports")
        >> create_project
        >> create_tasks
        >> load_project
        >> load_team_members
        >> load_all_tasks_of_project
        >> assert_report_response
        >> delete_this_dagrun
    )

    batch_task_operator >> rail.Label(
        "DAGrun for deletion") >> delete_this_dagrun
