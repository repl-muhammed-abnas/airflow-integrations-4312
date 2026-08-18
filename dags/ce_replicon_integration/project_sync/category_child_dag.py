from datetime import timedelta
import rail
from rail.lib.identity import get_identity_for_conn_id
from uuid import uuid4
null = None


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.category_child_dag_id,
        company_key=config.company_key,
        description='Computerease To Replicon Category Sync Child DAG',
        max_active_runs=config.child_dag_max_active_runs,
        multi_tenant=True
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_if_valid_category',
            end_task='catch_category_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        check_if_valid_category = rail.IfOperator(
            task_id='check_if_valid_category',
            test=lambda dag_run:  dag_run.conf['category_data']['status'] == 'open' or (dag_run.conf['category_data']['status'] == 'closed' and
            dag_run.conf['category_data']['subTaskUri']),
            yes_task='sync_category_to_replicon',
            no_task='catch_category_error'
        )

        def build_subtask_payload(dag_run):
            category_data = dag_run.conf['category_data']
            taskUri = category_data.get('taskUri', '')
            subTaskUri = category_data.get('subTaskUri', null)
            if subTaskUri:
                target = {
                    'uri':subTaskUri
                }
            else:
                target = {
                    'parent': {
                        'name': category_data.get('phase_description', ''),
                        'project': {
                            'code': category_data.get('job_code', '')
                        }
                    }
                }
            
            return {
                'project': {
                    'code': category_data.get('job_code', '')
                },
                'taskHierarchy': [
                    {
                    'target': target,
                    'taskModificationToApply': {
                        'name': category_data.get('description', ''),
                        'codeToApply': {
                        'value': category_data.get('code', '')
                        },
                        "isClosed": True if category_data.get('status', '') != 'open' else False,
                        'timeAndExpenseEntryTypeToApply': {
                        'value': 'urn:replicon:time-and-expense-entry-type:billable-and-non-billable'
                        },
                        'isTimeEntryAllowed': True,
                    'resourceTaskAssignmentModifications': {
                        'resourceAllocationsToAdd': [
                            {
                            'resource': {
                                'department': {
                                'uri': 'urn:replicon-tenant:' + rail.get_tenant_slug() + ':department:1'
                                }
                            }
                            }
                        ]
                        }
                    }
                    }
                ],
                'taskModificationOptionUri': 'urn:replicon:task-modification-option:save',
                'unitOfWorkId': str(uuid4())
            }
                
        sync_category_to_replicon = rail.RepliconServiceOperator(
            task_id='sync_category_to_replicon',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=build_subtask_payload
        )

        is_category_sync_successful = rail.IfOperator(
            task_id='is_category_sync_successful',
            test=lambda: rail.result("sync_category_to_replicon") and rail.result("sync_category_to_replicon")[0] and rail.result("sync_category_to_replicon")[0]["task"],
            yes_task='catch_category_error',
            no_task='is_sync_failed_for_duplicate_task_name'
        )

        def is_duplicate_category(dag_run):
            category_sync_result = rail.result("sync_category_to_replicon")
            if  category_sync_result[0] and category_sync_result[0]['error'] and category_sync_result[0]['error']['notifications'] and category_sync_result[0]['error']['notifications'][0]['displayText'] == "The specified Task already exists.":
                dag_run.conf['category_data']['description'] = dag_run.conf['category_data']['description'] + ' - ' + dag_run.conf['category_data']['code']
                return True
            return False

        is_sync_failed_for_duplicate_task_name = rail.IfOperator(
            task_id='is_sync_failed_for_duplicate_task_name',
            test= lambda dag_run: is_duplicate_category(dag_run) ,
            yes_task='sync_duplicate_category_to_replicon',
            no_task='catch_category_error'
        )

        sync_duplicate_category_to_replicon = rail.RepliconServiceOperator(
            task_id='sync_duplicate_category_to_replicon',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=build_subtask_payload
        )

        def get_downstreamtasks_error(category_code, error_message):
            return {
                'error': f'Error with category {category_code} - {error_message}'
            }

        catch_category_error = rail.PythonOperator(
            task_id='catch_category_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.category_data.code }}',
                     '{{ get_error_message() }}']
        )

        batch_task >> rail.Label('On Error') >> catch_category_error
        batch_task >> check_if_valid_category

        check_if_valid_category >> rail.Label(
            'Yes') >> sync_category_to_replicon >> is_category_sync_successful
        check_if_valid_category >> rail.Label('No') >> catch_category_error

        is_category_sync_successful >> rail.Label(
            'Yes') >> catch_category_error
        is_category_sync_successful >> rail.Label('No') >> is_sync_failed_for_duplicate_task_name

        is_sync_failed_for_duplicate_task_name >> rail.Label(
            'Yes') >> sync_duplicate_category_to_replicon >> catch_category_error
        is_sync_failed_for_duplicate_task_name >> rail.Label('No') >> catch_category_error

        return dag


rail.for_each_instance(create_dag_instance)
