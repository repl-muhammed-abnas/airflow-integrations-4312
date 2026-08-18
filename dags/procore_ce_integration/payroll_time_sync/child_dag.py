import rail
from datetime import timedelta

def create_dag_instance(config):  # pylint: disable = too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description='Procore To Computerease Payroll Time Syn - Child Dagc',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        ce_time_entries_payload = []

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch',
            start_task='for_each_procore_time_entry',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        for_each_procore_time_entry = rail.ForEachOperator(
            task_id='for_each_procore_time_entry',
            items= lambda dag_run: dag_run.conf.get('time_data', []),
            start_task='prepare_payload',
            end_task='end_for_each_procore_time_entry_item'
        )

        end_for_each_procore_time_entry_item = rail.EmptyOperator(
            task_id='end_for_each_procore_time_entry_item'
        )

        prepare_payload = rail.PythonOperator(
            task_id='prepare_payload',
            python_callable=lambda dag_run: prepare_payload_for_each_time_entry(dag_run, rail.result("for_each_procore_time_entry"))
        )

        def prepare_payload_for_each_time_entry(dag_run, time_data):                        
            employee = None
            job = None
            phase = None
            category = None
            pay_types = dag_run.conf.get('pay_types', {})
            DEFAULT_STATUS = "approved"

            origin_id_by_user_id = dag_run.conf.get('procore_user_details', {})
            origin_id_by_project_id = dag_run.conf.get('fetched_procore_projects', {})
            
            if dag_run.conf.get('employee_based_on_origin_id', False):
                user_id = time_data.get('id')
                if user_id and user_id in origin_id_by_user_id and origin_id_by_user_id[user_id]:
                    employee = origin_id_by_user_id[user_id][3:]
            else:
                employee = time_data.get('id')

            if dag_run.conf.get('project_based_on_origin_id', False):
                project_id = time_data.get('project_id')
                if project_id and project_id in origin_id_by_project_id and origin_id_by_project_id[project_id]:
                    job = origin_id_by_project_id[project_id][3:]
            else:
                job = time_data.get('job_code')

            cost_code_full_code = time_data.get('cost_code_full_code')
            if cost_code_full_code:
                cost_code = time_data.get('cost_code')
                if cost_code and cost_code.get('parent', {}).get('id') is not None:
                    category = cost_code.get('code', '')
                    phase = cost_code_full_code.removesuffix(f'-{category}') if category else cost_code_full_code
                else:
                    category = cost_code_full_code

            time_type = time_data.get('time_type')
            pay_type = pay_types.get(time_type) if time_type else None

            date = time_data.get('date')
            description = (time_data.get('description') or '')[:config.MAX_CHAR_LENGTH]
            hours = time_data.get('hours', 0)
            
            total_time_in_sec = int(float(hours) * 3600)

            payload = {
                "employee": employee,
                "date": date,
                "job": job,
                "phase": phase,
                "category": category,
                "description": description,
                "pay_type": pay_type,
                "total_time": total_time_in_sec,
                "status": DEFAULT_STATUS
            }

            return {'payload': payload}
        
        check_if_procore_employee_data_exist = rail.IfOperator(
            task_id='check_if_procore_employee_data_exist',
            test= lambda: rail.result('prepare_payload').get('payload', {}).get('employee') is not None,
            yes_task='get_ce_employee_details',
            no_task='catch_error'
        )

        get_ce_employee_details = rail.ComputereaseAPIOperator(
            task_id='get_ce_employee_details',
            endpoint='/catalog/employee',
            request_method='GET',
            query_params=lambda: {
                'code': rail.result('prepare_payload').get('payload', {}).get('employee')
            },
            data_handler=lambda resp: resp.get('data', []) if resp and len(resp.get('data', [])) > 0 and resp['data'][0].get('active', False) else []
        )

        check_if_ce_employee_exist = rail.IfOperator(
            task_id='check_if_ce_employee_exist',
            test= lambda: len(rail.result('get_ce_employee_details')) > 0,
            yes_task='check_if_procore_job_data_exist',
            no_task='catch_error'
        )

        check_if_procore_job_data_exist = rail.IfOperator(
            task_id='check_if_procore_job_data_exist',
            test= lambda: rail.result('prepare_payload').get('payload', {}).get('job') is not None,
            yes_task='check_if_procore_category_data_exist',
            no_task='catch_error'
        )

        check_if_procore_category_data_exist = rail.IfOperator(
            task_id='check_if_procore_category_data_exist',
            test= lambda: rail.result('prepare_payload').get('payload', {}).get('category') is not None,
            yes_task='get_ce_category_details',
            no_task='get_ce_job_details'
        )        

        get_ce_job_details = rail.ComputereaseAPIOperator(
            task_id='get_ce_job_details',
            endpoint='/catalog/job',
            request_method='GET',
            query_params=lambda: {
                'code': rail.result('prepare_payload').get('payload', {}).get('job')
            },
            data_handler=lambda resp: resp.get('data', []) if resp and len(resp.get('data', [])) > 0 and resp['data'][0].get('status') == 'active' else []
        )

        get_ce_category_details = rail.ComputereaseAPIOperator(
            task_id='get_ce_category_details',
            endpoint='/catalog/category',
            request_method='GET',
            query_params=lambda: {
                'job_code': rail.result('prepare_payload').get('payload', {}).get('job'),                
                'code': rail.result('prepare_payload').get('payload', {}).get('category')
            },
            data_handler=lambda resp: resp.get('data', []) if resp and len(resp.get('data', [])) > 0 and resp['data'][0].get('status') == 'open' else []
        )

        def validate_ce_project_data():
            payload = rail.result('prepare_payload').get('payload', {})
            category = payload.get('category')
            if category:
                ce_category_details = rail.result('get_ce_category_details')
                if ce_category_details and len(ce_category_details) > 0:
                    ce_time_entries_payload.append(payload)
                    return True
            elif rail.result('get_ce_job_details') and len(rail.result('get_ce_job_details')) > 0:
                ce_time_entries_payload.append(payload)
                return True
            return False

        check_if_ce_project_details_exist = rail.IfOperator(
            task_id='check_if_ce_project_details_exist',
            test= lambda: validate_ce_project_data(),
            yes_task='end_for_each_procore_time_entry_item',
            no_task='catch_error'
        )

        def get_status_and_reason_for_failure(err):
            if isinstance(err, str):
                status = 'Error'
                reason = err
            else:
                status = err.get('response', {}).get('status_code', 'Error')
                reason = err.get('response', {}).get('json', {}).get('error', {}).get('reason', str(err))
            return reason, status

        def get_error_details():
            time_entry = rail.result("for_each_procore_time_entry") or {}
            procore_time_entry_id = time_entry.get('time_entry_id', '')
            
            payload = rail.result('prepare_payload').get('payload', {})
            employee = payload.get('employee', '')
            job = payload.get('job', '')
            phase = payload.get('phase', '')
            category = payload.get('category', '')
            date = payload.get('date', '')
            reason = ''
            status = ''
            
            try:
                ce_employee_details = rail.result('get_ce_employee_details') or []
                ce_job_details = rail.result('get_ce_job_details') or []
                ce_category_details = rail.result('get_ce_category_details') or []
                
                if not employee or len(ce_employee_details) == 0:
                    reason = 'Employee does not exist or is inactive. '
                elif not job or (not category and len(ce_job_details) == 0):
                    reason = 'Job does not exist or is inactive. '
                elif not category or len(ce_category_details) == 0:
                    reason = 'Job/Category does not exist or is inactive. '

                err = rail.render_template('{{ get_error_message() }}')
                failure_reason, status = get_status_and_reason_for_failure(err)
                reason += failure_reason
            except Exception as e:
                status = "Exception"
                reason += f"An exception occurred: {str(e)}"
            
            return {
                'procore_time_entry_id': procore_time_entry_id,
                'employee': employee,
                'job': job,
                'phase': phase,
                'category': category,
                'date': date,
                'reason': reason,
                'status': status
            }

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=get_error_details
        )

        check_if_valid_time_entries_payload = rail.IfOperator(
            task_id='check_if_valid_time_entries_payload',
            test= lambda: len(ce_time_entries_payload) > 0,
            yes_task='sync_time_entries',
            no_task='log_payload_not_found'
        )

        def get_no_payload_log_details(dag_run):
            status = ''
            time_data = dag_run.conf.get('time_data', [])
            procore_time_entry_id = time_data[0].get('time_entry_id', '') if time_data and len(time_data) > 0 else ''
            employee = time_data[0].get('id', '') if time_data and len(time_data) > 0 else ''
            date = time_data[0].get('date', '') if time_data and len(time_data) > 0 else ''
            
            err = rail.render_template('{{ get_error_message() }}')
            reason, status = get_status_and_reason_for_failure(err)
            reason = 'Payload not found. ' + reason
            
            return {
                'procore_time_entry_id': procore_time_entry_id,
                'employee': employee,
                'job': '',
                'phase': '',
                'category': '',
                'date': date,
                'reason': reason,
                'status': status
            }
        
        log_payload_not_found = rail.WriteLogOperator(
            task_id='log_payload_not_found',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: get_no_payload_log_details(dag_run)
        )

        sync_time_entries = rail.ComputereaseAPIOperator(
            task_id='sync_time_entries',
            endpoint='/timesheet/entry',
            request_method='POST',
            request_body=ce_time_entries_payload
        )

        def get_failure_details(dag_run):
            time_data = dag_run.conf.get('time_data', [])
            employee = time_data[0].get('id', '') if time_data else ''
            
            err = rail.render_template('{{ get_error_message() }}')
            reason, status = get_status_and_reason_for_failure(err)
            
            return {
                'procore_time_entry_id': '',
                'employee': employee,
                'job': '',
                'phase': '',
                'category': '',
                'date': '',
                'reason': reason,
                'status': status
            }

        catch_unhandled_error = rail.WriteLogOperator(
            task_id='catch_unhandled_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: get_failure_details(dag_run)
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Task dependencies
        batch_task >> log_to_sumo
        batch_task >> for_each_procore_time_entry >> prepare_payload >> check_if_procore_employee_data_exist

        check_if_procore_employee_data_exist >> rail.Label('Yes') >> get_ce_employee_details >> check_if_ce_employee_exist
        check_if_procore_employee_data_exist >> rail.Label('No') >> catch_error

        check_if_ce_employee_exist >> rail.Label('Yes') >> check_if_procore_job_data_exist
        check_if_ce_employee_exist >> rail.Label('No') >> catch_error

        check_if_procore_job_data_exist >> rail.Label('Yes') >> check_if_procore_category_data_exist
        check_if_procore_job_data_exist >> rail.Label('No') >> catch_error

        check_if_procore_category_data_exist >> rail.Label('Yes') >> get_ce_category_details >> check_if_ce_project_details_exist
        check_if_procore_category_data_exist >> rail.Label('No') >> get_ce_job_details >> check_if_ce_project_details_exist

        check_if_ce_project_details_exist >> rail.Label('Yes') >> end_for_each_procore_time_entry_item
        check_if_ce_project_details_exist >> rail.Label('No') >> catch_error >> end_for_each_procore_time_entry_item

        for_each_procore_time_entry >> end_for_each_procore_time_entry_item

        end_for_each_procore_time_entry_item >> check_if_valid_time_entries_payload

        check_if_valid_time_entries_payload >> rail.Label('Yes') >> sync_time_entries >> catch_unhandled_error >> log_to_sumo
        check_if_valid_time_entries_payload >> rail.Label('No') >> log_payload_not_found >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)