from datetime import timedelta
import rail
import pycountry
from ce_procore_integration.job_structure_sync.utils.job_parser import parse_job_data


def create_dag_instance(config):  # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.job_child_dag_id,
        description='Computerease to Procore job sync Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.job_child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_if_need_job_fetch',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        check_if_need_job_fetch = rail.IfOperator(
            task_id='check_if_need_job_fetch',
            test=lambda dag_run: dag_run.conf.get('job_data') is None,
            yes_task='fetch_job_data',
            no_task='prepare_job_data'
        )

        fetch_job_data = rail.ComputereaseAPIOperator(
            task_id='fetch_job_data',
            endpoint='/catalog/job',
            request_method='GET',
            query_params={
                'code': '{{ dag_run.conf.job_code }}'
            }
        )

        def get_job_data_for_sync(dag_run):
            # Use provided job_data or parse fetched data.
            if dag_run.conf.get('job_data'):
                return dag_run.conf['job_data']
            job_raw_data = rail.result('fetch_job_data').get('data', [])
            if job_raw_data:
                return parse_job_data(
                    job_raw_data[0],
                    dag_run.conf.get('department_lookup', {}),
                    dag_run.conf.get('project_template_udf_id')
                )
            raise ValueError(
                f"Job not found in CE: {dag_run.conf['job_code']}.")

        prepare_job_data = rail.PythonOperator(
            task_id='prepare_job_data',
            python_callable=get_job_data_for_sync
        )


        def get_procore_projects(response, dag_run):
            if not response:
                return None
            if len(response) == 1:
                return response[0]['id']

            project_id = None
            for project in response:
                if project['origin_id'] == f"CE_{dag_run.conf['job_code']}":
                    project_id = project['id']

            if project_id is None:
                raise ValueError(
                    f"Multiple Procore projects found with project number: {dag_run.conf['job_code']}."
                )
            return project_id

        fetch_procore_project = rail.ProcoreApiOperator(
            task_id='fetch_procore_project',
            endpoint='/projects',
            version='1.1',
            method='GET',
            query_params=lambda dag_run: {
                'view': 'normal',
                'company_id': dag_run.conf['procore_company_id'],
                'filters[project_number]': dag_run.conf['job_code']
            },
            data_handler=lambda response, dag_run: get_procore_projects(response, dag_run)
        )

        def get_project_template_id(project_templates_lookup, project_template_udf_value):
            project_template_id = ""
            if project_template_udf_value and project_templates_lookup.get(project_template_udf_value):
                project_template_id = project_templates_lookup[project_template_udf_value]
            elif config.default_project_template_name in project_templates_lookup:
                project_template_id = project_templates_lookup[config.default_project_template_name]
            return project_template_id

        def validate_state_code(state_code, country_code='US'):
            if not state_code:
                return ""
            try:
                state_code = state_code.strip().upper()

                subdivisions = pycountry.subdivisions.get(
                    country_code=country_code)
                for subdivision in subdivisions:
                    if subdivision.code.split('-')[-1].upper() == state_code:
                        return state_code

                return ""
            except:  # pylint: disable=bare-except
                return ""

        def build_sync_project_payload(dag_run):
            job_data = rail.result('prepare_job_data')
            procore_project_id = rail.result('fetch_procore_project')

            if not job_data.get('description'):
                raise ValueError("Job Name is required for sync.")

            # Generate origin_id using CE job ID with prefix
            origin_id = f"CE_{job_data.get('code', '')}" if job_data.get(
                'code') else None

            # Determine project template ID based on UDF value
            project_templates_lookup = dag_run.conf.get(
                'project_templates_lookup', {})
            project_template_udf_value = job_data.get(
                'project_template_udf_value', '')
            project_template_id = get_project_template_id(
                project_templates_lookup, project_template_udf_value)

            payload = {
                "company_id": dag_run.conf['procore_company_id'],
                "updates": [
                    {
                        "origin_id": origin_id,  # to uniquely identify projects in Procore
                        "project_template_id": project_template_id,
                        "name": job_data.get('description', ''),
                        "project_number": job_data.get('code', ''),
                        "active": job_data.get('status', True),
                        "address": job_data.get('address', ''),
                        "city": job_data.get('city', ''),
                        "state_code": validate_state_code(job_data.get('state', ''), config.country_code),
                        "zip": job_data.get('zipcode', ''),
                        "start_date": job_data.get('jobdate_open', ''),
                        "completion_date": job_data.get('jobdate_due', ''),
                        "department_ids": job_data.get('department_ids', []),
                        "country_code": config.country_code,
                        "enable_copy_of_standard_cost_codes": config.enable_copy_of_standard_cost_codes,
                    }
                ]
            }
            if procore_project_id:
                payload['updates'][0]['id'] = procore_project_id

            return payload

        sync_project_to_procore = rail.ProcoreApiOperator(
            task_id='sync_project_to_procore',
            endpoint='/projects/sync',
            method='PATCH',
            data=build_sync_project_payload
        )

        check_if_successful_sync_for_job = rail.IfOperator(
            task_id='check_if_successful_sync_for_job',
            test='{{ result("sync_project_to_procore").entities | length > 0 }}',
            yes_task='fetch_costcode_segment_id',
            no_task='log_sync_success_with_exceptions'
        )

        fetch_costcode_segment_id = rail.ProcoreApiOperator(
            task_id='fetch_costcode_segment_id',
            endpoint='/projects/{{ result("sync_project_to_procore").entities[0].id }}/work_breakdown_structure/segments',
            method='GET',
            data_handler=lambda response: next((seg['id'] for seg in response if seg.get('type') == config.cost_code_segment_type and seg.get(
                'name') == config.cost_code_segment_name and seg.get('tiered') == True), None)
        )

        fetch_existing_segment_items = rail.ProcoreApiOperator(
            task_id='fetch_existing_segment_items',
            endpoint='/projects/{{ result("sync_project_to_procore").entities[0].id }}/work_breakdown_structure/segments/{{ result("fetch_costcode_segment_id") }}/segment_items',
            method='GET',
            data_handler=lambda items: {
                'parent_id_lookup': {
                    item['code']: item['id']
                    for item in items
                    if item.get('parent_id') is None and item.get('code') and item.get('id')
                },
                'child_id_lookup': {
                    f"{item['parent_id']}:{item['code']}": item['id']
                    for item in items
                    if item.get('parent_id') is not None and item.get('code') and item.get('id')
                }
            }
        )

        check_if_has_phases = rail.IfOperator(
            task_id='check_if_has_phases',
            test=lambda dag_run: len(dag_run.conf.get('phases', {})) > 0,
            yes_task='trigger_phases_sync_child_dag',
            no_task='check_if_has_direct_categories'
        )

        def prepare_phases_for_sync(dag_run):
            phases = list(dag_run.conf.get('phases', {}).values())
            phase_items = []
            parent_id_lookup = rail.result('fetch_existing_segment_items')['parent_id_lookup']
            child_id_lookup = rail.result('fetch_existing_segment_items')['child_id_lookup']
            for phase in phases:
                phase_items.append({
                    **phase,
                    "phase_data": {
                        **phase['phase_data'],
                        'id': parent_id_lookup.get(phase['phase_code'], None),
                    } if phase['phase_data'] else None,
                    "job_code": dag_run.conf['job_code'],
                    "parent_id_lookup": parent_id_lookup,
                    "child_id_lookup": child_id_lookup,
                    'wbs_type': rail.result('prepare_job_data')['wbs_type'],
                    'procore_project_id': rail.result('sync_project_to_procore')['entities'][0]['id'],
                    'procore_company_id': dag_run.conf['procore_company_id'],
                    'cost_code_segment_id': rail.result('fetch_costcode_segment_id') if rail.result('fetch_costcode_segment_id') else None
                })

            return phase_items

        trigger_phases_sync_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_phases_sync_child_dag',
            items=prepare_phases_for_sync,
            trigger_dag_id=config.phases_child_dag_id,
            conf=lambda item: item
        )

        wait_for_phases_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_phases_completion',
            dag_runs='{{ result("trigger_phases_sync_child_dag") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        check_if_has_direct_categories = rail.IfOperator(
            task_id='check_if_has_direct_categories',
            test=lambda dag_run: len(
                dag_run.conf.get('direct_categories', {})) > 0,
            yes_task='trigger_categories_sync_child_dag',
            no_task='trigger_prime_contract_sync'
        )

        def prepare_categories_for_sync(dag_run):
            categories = list(dag_run.conf.get(
                'direct_categories', {}).values())
            category_items = []
            parent_id_lookup = rail.result('fetch_existing_segment_items')['parent_id_lookup']
            for category in categories:
                category_items.append({
                    **category,
                    "category_data": {
                        **category['category_data'],
                        'id': parent_id_lookup.get(category['category_code'], None),
                    } if category['category_data'] else None,
                    "job_code": dag_run.conf['job_code'],
                    'wbs_type': rail.result('prepare_job_data')['wbs_type'],
                    'procore_project_id': rail.result('sync_project_to_procore')['entities'][0]['id'],
                    'procore_company_id': dag_run.conf['procore_company_id'],
                    'cost_code_segment_id': rail.result('fetch_costcode_segment_id') if rail.result('fetch_costcode_segment_id') else None,
                    'parent_type': 'cost_code_segment'
                })

            return category_items
        trigger_categories_sync_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_categories_sync_child_dag',
            items=prepare_categories_for_sync,
            trigger_dag_id=config.category_child_dag_id,
            conf=lambda item: item
        )

        wait_for_categories_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_categories_completion',
            dag_runs='{{ result("trigger_categories_sync_child_dag") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        trigger_prime_contract_sync = rail.TriggerDagRunOperator(
            task_id='trigger_prime_contract_sync',
            trigger_dag_id=config.prime_contract_child_dag_id,
            conf=lambda dag_run: {
                'job_code': dag_run.conf['job_code'],
                'customer_name': rail.result('prepare_job_data').get('customer_name', ''),
                'customer_code': rail.result('prepare_job_data').get('customer_code', ''),
                'job_status': rail.result('prepare_job_data').get('status', True),
                'procore_project_id': rail.result('sync_project_to_procore')['entities'][0]['id'],
                'procore_company_id': dag_run.conf['procore_company_id']
            }
        )

        wait_for_prime_contract_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_prime_contract_completion',
            dag_runs='{{ result("trigger_prime_contract_sync") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def get_error_message(errors):
            if not errors or not isinstance(errors, dict):
                return ""
            messages = []
            for key, msgs in errors.items():
                if isinstance(msgs, list):
                    msg_str = ", ".join(str(m) for m in msgs)
                else:
                    msg_str = str(msgs)
                messages.append(f"{key}: {msg_str}")
            return "Project not created/updated due to - " + "; ".join(messages)

        log_sync_success_with_exceptions = rail.WriteLogOperator(
            task_id='log_sync_success_with_exceptions',
            message='na',
            severity='Error/Exception',
            items=lambda dag_run: [
                {
                    'entity_type': 'JOB',
                    'entity_code': rail.result('prepare_job_data').get('code', ''),
                    'full_code': rail.result('prepare_job_data').get('code', ''),
                    'entity_name': rail.result('prepare_job_data').get('description', ''),
                    'error_message': get_error_message(err.get('errors', {}))
                }
                for err in rail.result('sync_project_to_procore').get('errors', [])
            ],
            properties=lambda item: item
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'JOB',
                'entity_code': rail.result('prepare_job_data').get('code', '') if rail.result('prepare_job_data') else dag_run.conf.get('job_code', ''),
                'full_code': rail.result('prepare_job_data').get('code', '') if rail.result('prepare_job_data') else dag_run.conf.get('job_code', ''),
                'entity_name': rail.result('prepare_job_data').get('description', '') if rail.result('prepare_job_data') else '',
                'error_message': "Job not synced - {{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_error
        batch_task >> check_if_need_job_fetch

        check_if_need_job_fetch >> rail.Label('Yes') >> fetch_job_data >> prepare_job_data
        check_if_need_job_fetch >> rail.Label('No') >> prepare_job_data
        prepare_job_data >> fetch_procore_project >> sync_project_to_procore >> check_if_successful_sync_for_job

        check_if_successful_sync_for_job >> rail.Label(
            'Yes') >> fetch_costcode_segment_id >> fetch_existing_segment_items >> check_if_has_phases
        check_if_successful_sync_for_job >> rail.Label(
            'No') >> log_sync_success_with_exceptions >> catch_error

        check_if_has_phases >> rail.Label(
            'Yes') >> trigger_phases_sync_child_dag >> wait_for_phases_completion >> trigger_prime_contract_sync
        check_if_has_phases >> rail.Label(
            'No') >> check_if_has_direct_categories

        check_if_has_direct_categories >> rail.Label(
            'Yes') >> trigger_categories_sync_child_dag >> wait_for_categories_completion >> trigger_prime_contract_sync
        check_if_has_direct_categories >> rail.Label(
            'No') >> trigger_prime_contract_sync

        trigger_prime_contract_sync >> wait_for_prime_contract_completion >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
