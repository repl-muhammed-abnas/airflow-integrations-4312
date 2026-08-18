from datetime import timedelta
import rail
from rail.lib.identity import get_identity_for_conn_id
from uuid import uuid4
null = None

def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.phases_child_dag_id,
        company_key=config.company_key,
        description='Computerease To Replicon Phases Sync Child DAG',
        max_active_runs=config.child_dag_max_active_runs,
        multi_tenant=True
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_if_valid_phase',
            end_task='catch_phase_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        check_if_valid_phase = rail.IfOperator(
            task_id='check_if_valid_phase',
            test=lambda dag_run:  dag_run.conf['phase_data']['status'] == 'open' or (dag_run.conf['phase_data']['status'] == 'closed' and
            dag_run.conf['phase_data']['task_details'] and dag_run.conf['phase_data']['task_details']['task'] and dag_run.conf['phase_data']['task_details']['task']['uri']),
            yes_task='sync_phase_to_replicon',
            no_task='catch_phase_error'
        )

        def build_task_payload(dag_run):
            phase_data = dag_run.conf['phase_data']
            task_details = phase_data.get('task_details', '')
            target = null
            if task_details:
                target = {
                    'uri': task_details['task']['uri']
                }
                        
            return {
                'project': {
                    'code': phase_data.get('job_code', '')
                },
                'taskHierarchy': [
                    {
                    'target': target,
                    'taskModificationToApply': {
                        'name': phase_data.get('description', ''),
                        'codeToApply': {
                        'value': phase_data.get('code', '')
                        },
                        "isClosed": True if phase_data.get('status', '') != 'open' else False,
                        'timeAndExpenseEntryTypeToApply': {
                        'value': 'urn:replicon:time-and-expense-entry-type:billable-and-non-billable'
                        },
                        'isTimeEntryAllowed': False if 'CAT' in phase_data.get('wbs_type','').upper() else True,
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
                
        sync_phase_to_replicon = rail.RepliconServiceOperator(
            task_id='sync_phase_to_replicon',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=build_task_payload
        )        

        is_phase_sync_successful = rail.IfOperator(
            task_id='is_phase_sync_successful',
            test=lambda: rail.result("sync_phase_to_replicon") and rail.result("sync_phase_to_replicon")[0] and rail.result("sync_phase_to_replicon")[0]["task"],
            yes_task='check_if_has_categories',
            no_task='is_sync_failed_for_duplicate_task_name'
        )

        def is_duplicate_phase(dag_run):
            phase_sync_result = rail.result("sync_phase_to_replicon")
            if  phase_sync_result[0] and phase_sync_result[0]['error'] and phase_sync_result[0]['error']['notifications'] and phase_sync_result[0]['error']['notifications'][0]['displayText'] == "A task with this name already exists.":
                dag_run.conf['phase_data']['description'] = dag_run.conf['phase_data']['description'] + ' - ' + dag_run.conf['phase_data']['code']
                return True
            return False

        is_sync_failed_for_duplicate_task_name = rail.IfOperator(
            task_id='is_sync_failed_for_duplicate_task_name',
            test= lambda dag_run: is_duplicate_phase(dag_run) ,
            yes_task='sync_duplicate_phase_to_replicon',
            no_task='catch_phase_error'
        )

        sync_duplicate_phase_to_replicon = rail.RepliconServiceOperator(
            task_id='sync_duplicate_phase_to_replicon',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=build_task_payload
        )

        check_if_has_categories = rail.IfOperator(
            task_id='check_if_has_categories',
            test='{{ "CAT" in dag_run.conf.phase_data.wbs_type.upper() }}',
            yes_task='fetch_categories_for_phase',
            no_task='catch_phase_error'
        )

        fetch_categories_for_phase = rail.ComputereaseAPIOperator(
            task_id='fetch_categories_for_phase',
            endpoint='/catalog/category',
            request_method='GET',
            query_params={
                'job_code': '{{ dag_run.conf.phase_data.job_code }}',
                'phase_code': '{{ dag_run.conf.phase_data.code }}'
            },
            computerease_conn_id='{{ dag_run.conf.computerease_conn_id }}'
        )

        def getSubTaskUri(task_details, subTaskCode):
            if task_details:
                childTasks = task_details['childTasks']
                for childTask in childTasks:
                    if childTask['task'] and childTask['task']['code'] == subTaskCode:
                        return childTask['task']['uri']
        
        trigger_categories_sync_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_categories_sync_child_dag',
            items=lambda: [cat for cat in rail.result("fetch_categories_for_phase").get(
                'data', []) if cat.get('code', '').strip()],
            trigger_dag_id=config.category_child_dag_id,
            conf=lambda dag_run, item: {
                'category_data': {
                    'code': item.get('code', ''),
                    'description': item.get('description', '') if item.get('description', '') else item.get('code', ''),
                    'status': item.get('status', ''),
                    'job_code': dag_run.conf['phase_data']['job_code'],
                    'phase_description': dag_run.conf['phase_data']['description'],
                    'subTaskUri': getSubTaskUri(dag_run.conf['phase_data']['task_details'], item.get('code', '')),
                    'taskUri': rail.result("sync_phase_to_replicon")[0]['task']["uri"] if rail.result("sync_phase_to_replicon")[0]['task'] and rail.result("sync_phase_to_replicon")[0]['task']["uri"] else rail.result("sync_duplicate_phase_to_replicon")[0]['task']["uri"]
                },
                'computerease_conn_id': dag_run.conf['computerease_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id']
            }
        )

        wait_for_categories_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_categories_completion',
            dag_runs='{{ result("trigger_categories_sync_child_dag") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_category_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_category_errors',
            dag_runs="{{ result('trigger_categories_sync_child_dag') }}",
            dagrun_task_id='catch_category_error',
            flatten=True
        )

        is_category_error = rail.IfOperator(
            task_id='is_category_error',
            test="{{ (get_task_state('gather_category_errors') == 'success' and result('gather_category_errors') | length > 0) }}",
            yes_task='catch_phase_error',
            no_task='catch_phase_error'
        )

        def get_downstreamtasks_error(phase_code, error_message):
            return {
                'error': f'Error with {phase_code} - {error_message}'
            }

        catch_phase_error = rail.PythonOperator(
            task_id='catch_phase_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.phase_data.code }}',
                     '{{ get_error_message() }}']
        )

        batch_task >> rail.Label('On Error') >> catch_phase_error
        batch_task >> check_if_valid_phase

        check_if_valid_phase >> rail.Label(
            'Yes') >> sync_phase_to_replicon >> is_phase_sync_successful
        check_if_valid_phase >> rail.Label('No') >> catch_phase_error

        is_phase_sync_successful >> rail.Label(
            'Yes') >> check_if_has_categories
        is_phase_sync_successful >> rail.Label('No') >> is_sync_failed_for_duplicate_task_name

        is_sync_failed_for_duplicate_task_name >> rail.Label(
            'Yes') >> sync_duplicate_phase_to_replicon >> check_if_has_categories
        is_sync_failed_for_duplicate_task_name >> rail.Label('No') >> catch_phase_error

        check_if_has_categories >> rail.Label(
            'Yes') >> fetch_categories_for_phase >> trigger_categories_sync_child_dag >> wait_for_categories_completion >> gather_category_errors >> is_category_error
        check_if_has_categories >> rail.Label('No') >> catch_phase_error

        is_category_error >> catch_phase_error

        return dag


rail.for_each_instance(create_dag_instance)
