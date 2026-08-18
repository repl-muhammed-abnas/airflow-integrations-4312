from datetime import timedelta
import rail
from rail.lib.identity import get_identity_for_conn_id
from uuid import uuid4
null=None

def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.job_child_dag_id,
        company_key=config.company_key,
        description=f'{config.company_key} Computerease To Replicon Job Sync Child DAG',
        max_active_runs=config.child_dag_max_active_runs,
        multi_tenant=True
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_project_details',
            end_task='catch_job_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def get_project_details_body(dag_run):
            job_data=dag_run.conf['job_data']
            if not job_data.get('code'):
                raise ValueError("Job Code is required for sync")
            body={
                'projects': [
                    {
                        'uri': null,
                        'name': null,
                        'code': job_data.get('code'),
                        'parameterCorrelationId': null
                    },
                    {
                        'uri': null,
                        'name': job_data.get('description'),
                        'code': null,
                        'parameterCorrelationId': null
                    }
                ]
            }
            return body
        
        def append_project_code(resp, dag_run):
            if (resp and resp[0]['projectDetails'] and resp[1]['projectDetails'] and resp[0]['projectDetails']['uri'] != resp[1]['projectDetails']['uri']) or (resp and not resp[0]['projectDetails'] and resp[1]['projectDetails'] and resp[1]['projectDetails']['uri']):
                dag_run.conf['job_data']['description'] = dag_run.conf['job_data']['description'] + ' - ' + dag_run.conf['job_data']['code']

            return resp[0]['projectDetails'] if resp and resp[0] and resp[0]['projectDetails'] else null                


        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=get_project_details_body,
            data_handler=lambda resp, dag_run: append_project_code(resp, dag_run) 
        )

        check_if_job_sync_requiredb = rail.IfOperator(
            task_id='check_if_job_sync_requiredb',
            test=lambda dag_run:  (dag_run.conf['job_data']['status'] == 'In Progress' or (dag_run.conf['job_data']['status'] != 'In Progress' and rail.result("get_project_details") and rail.result("get_project_details")["uri"])),
            yes_task='sync_project_to_replicon',
            no_task='catch_job_error'
        )

        def build_sync_project_payload(dag_run):
            job_data = dag_run.conf['job_data']
            target = null                                                                                                                                                                                                                                                                                                                                                                                           
            if rail.result('get_project_details'):
                target={
                    'uri': null,
                    'name': null,
                    'code': job_data.get('code',''),
                    'parameterCorrelationId': null
                }
            
            return {
                'target': target,
                'modifications': {
                    'nameToApply': {
                        'value': job_data.get('description', '')
                    },
                    'codeToApply': {
                        'value': job_data.get('code', '')
                    },
                    'startDateToApply': {
                        'date': job_data.get('jobdate_open', '')
                    },
                    'endDateToApply': {
                        'date': job_data.get('jobdate_due', '')
                    },
                    'statusToApply': {
                        'name': job_data.get('status','')
                    },
                    'timeAndMaterials': {
                        'timeAndExpenseEntryTypeUri': 'urn:replicon:time-and-expense-entry-type:billable-and-non-billable'
                    },
                    'resourceProjectAssignmentModifications': {
                        'resourcesToAdd': [
                            {
                                'resource': {
                                    'department': {
                                        'uri': 'urn:replicon-tenant:' + rail.get_tenant_slug() + ':department:1'
                                    }
                                }

                            }
                        ]
                    },
                    'objectExtensionFieldsToApply': [
                        {
                            'definition': {
                                'name': 'Company_uuid'
                            },
                            'textValue': job_data.get('company_uuid', '')
                        },
                        {
                            'definition': {
                                'name': 'Payroll_Date'
                            },
                            'textValue': job_data.get('first_payroll_date', '')
                        },
                        {
                            'definition': {
                                'name': 'wbs_type'
                            },
                            'textValue': job_data.get('wbs_type', '')
                        }
                    ],
                    'isTimeEntryAllowed': True if job_data.get('wbs_type','') == 'T&M' else False,
                },
                'projectModificationOptionUri': 'urn:replicon:project-modification-option:save',
                'unitOfWorkId': str(uuid4())
            }
                
        sync_project_to_replicon = rail.RepliconServiceOperator(
            task_id='sync_project_to_replicon',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=build_sync_project_payload
        )

        check_if_successful_sync_for_job = rail.IfOperator(
            task_id='check_if_successful_sync_for_job',
            test=lambda: rail.result("sync_project_to_replicon") and rail.result("sync_project_to_replicon")["uri"],
            yes_task='check_existing_tasks',
            no_task='catch_job_error'
        )

        check_existing_tasks = rail.IfOperator(
            task_id='check_existing_tasks',
            test=lambda: rail.result("get_project_details") and rail.result("get_project_details")["uri"],
            yes_task='fetch_existing_task_details',
            no_task='check_if_has_phases'
        )

        fetch_existing_task_details = rail.RepliconServiceOperator(
            task_id='fetch_existing_task_details',
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                'parentUri': '{{result("get_project_details").uri}}'
            }
        )

        check_if_has_phases = rail.IfOperator(
            task_id='check_if_has_phases',
            test='{{ "PHASE" in dag_run.conf.job_data.wbs_type.upper() }}',
            yes_task='fetch_phases_for_job',
            no_task='check_if_has_categories_only'
        )

        fetch_phases_for_job = rail.ComputereaseAPIOperator(
            task_id='fetch_phases_for_job',
            endpoint='/catalog/phase',
            request_method='GET',
            query_params={
                'job_code': '{{ dag_run.conf.job_data.code }}'
            },
            computerease_conn_id='{{ dag_run.conf.computerease_conn_id }}'
        )

        def get_task_details(phase_code):
            if rail.result('get_project_details') and rail.result('get_project_details')['uri']:
                for task in rail.result('fetch_existing_task_details'):
                    if task['task'] and task['task']['code'] == phase_code:
                        return task

        trigger_phases_sync_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_phases_sync_child_dag',
            items=lambda: [phase for phase in rail.result('fetch_phases_for_job').get(
                'data', []) if phase.get('code', '').strip()],
            trigger_dag_id=config.phases_child_dag_id,
            conf=lambda dag_run, item: {
                'phase_data': {
                    'code': item.get('code', ''),
                    'description': item.get('description', '') if item.get('description', '') else item.get('code', ''),
                    'status': item.get('status', ''),
                    'job_code': dag_run.conf['job_data']['code'],
                    'wbs_type': dag_run.conf['job_data']['wbs_type'],
                    'task_details': get_task_details(item.get('code', ''))
                },
                'computerease_conn_id': dag_run.conf['computerease_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id']
            }
        )

        wait_for_phases_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_phases_completion',
            dag_runs='{{ result("trigger_phases_sync_child_dag") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        check_if_has_categories_only = rail.IfOperator(
            task_id='check_if_has_categories_only',
            test=lambda dag_run:dag_run.conf['job_data']['wbs_type'].upper() == config.wbs_type_job_and_categories,
            yes_task='fetch_categories',
            no_task='catch_job_error'
        )

        fetch_categories = rail.ComputereaseAPIOperator(
            task_id='fetch_categories',
            endpoint='/catalog/category',
            request_method='GET',
            query_params={
                'job_code': '{{ dag_run.conf.job_data.code }}'
            },
            computerease_conn_id='{{ dag_run.conf.computerease_conn_id }}'
        )

        trigger_categories_sync_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_categories_sync_child_dag',
            items=lambda: [cat for cat in rail.result("fetch_categories").get(
                'data', []) if cat.get('code', '').strip()],
            trigger_dag_id=config.phases_child_dag_id,
            conf=lambda dag_run, item: {
                'phase_data': {
                    'code': item.get('code', ''),
                    'description': item.get('description', '') if item.get('description', '') else item.get('code', ''),
                    'status': item.get('status', ''),
                    'job_code': dag_run.conf['job_data']['code'],
                    'wbs_type': '',
                    'task_details': get_task_details(item.get('code', ''))
                }
            }
        )

        wait_for_categories_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_categories_completion',
            dag_runs='{{ result("trigger_categories_sync_child_dag") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_phase_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_phase_errors',
            dag_runs="{{ result('trigger_phases_sync_child_dag') }}",
            dagrun_task_id='catch_phase_error',
            flatten=True
        )

        is_phase_error = rail.IfOperator(
            task_id='is_phase_error',
            test="{{ (get_task_state('gather_phase_errors') == 'success' and result('gather_phase_errors') | length > 0) }}",
            yes_task='catch_job_error',
            no_task='check_if_has_categories_only'
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
            yes_task='catch_job_error',
            no_task='catch_job_error'
        )

        def get_downstreamtasks_error(job_code, error_message):
            return {
                'error': f'Error with {job_code} - {error_message}'
            }

        catch_job_error = rail.PythonOperator(
            task_id='catch_job_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.job_data.code }}',
                     '{{ get_error_message() }}']
        )

        batch_task >> rail.Label('On Error') >> catch_job_error
        batch_task >> get_project_details >> check_if_job_sync_requiredb

        check_if_job_sync_requiredb >> rail.Label(
            'Yes') >> sync_project_to_replicon >> check_if_successful_sync_for_job
        check_if_job_sync_requiredb >> rail.Label('No') >> catch_job_error

        check_if_successful_sync_for_job >> rail.Label(
            'Yes') >> check_existing_tasks
        check_if_successful_sync_for_job >> rail.Label('No') >> catch_job_error

        check_existing_tasks >> rail.Label(
            'Yes') >> fetch_existing_task_details >> check_if_has_phases
        check_existing_tasks >> rail.Label(
            'No') >> check_if_has_phases

        check_if_has_phases >> rail.Label(
            'Yes') >> fetch_phases_for_job >> trigger_phases_sync_child_dag >> wait_for_phases_completion >> gather_phase_errors >> is_phase_error
        check_if_has_phases >> rail.Label('No') >> check_if_has_categories_only

        is_phase_error >> rail.Label('Yes') >> catch_job_error
        is_phase_error >> rail.Label('No') >> check_if_has_categories_only

        check_if_has_categories_only >> rail.Label(
            'Yes') >> fetch_categories >> trigger_categories_sync_child_dag >> wait_for_categories_completion >> gather_category_errors >> is_category_error
        check_if_has_categories_only >> rail.Label('No') >> catch_job_error

        is_category_error >> catch_job_error

        return dag


rail.for_each_instance(create_dag_instance)
