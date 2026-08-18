"""
Computerease-Replicon Time Sync Per Employee Child DAG

This child DAG processes time entries for a single employee:
1. Receives employee time entries from main DAG
2. Transforms entries to ComputerEase format
3. Syncs time entries to ComputerEase
4. Approves synced entries
5. Logs errors if any

Batch Scope: One employee's time entries
"""
from datetime import datetime, timedelta
import rail

def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=f'{config.child_dag_id}',
        description=f'{config.company_key} Replicon To Computerease Time Sync Per Employee Child DAG',
        company_key=config.company_key,
        max_active_runs=config.child_max_active_runs,
        multi_tenant=True
    ) as dag:
        
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='transform_entries',
            end_task='catch_time_entries_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_entry_date(date_str, date_format_uri):
            """Convert Replicon date format to YYYY-MM-DD"""
            DATE_FORMAT_MAPPING = {
                'urn:replicon:date-format:month-dd-comma-yy': '%B %d, %y',
                'urn:replicon:date-format:month-dd-comma-yyyy': '%B %d, %Y',
                'urn:replicon:date-format:dd-month-yy': '%d %B %y',
                'urn:replicon:date-format:dd-month-yyyy': '%d %B %Y',
                'urn:replicon:date-format:yy-month-dd': '%y %B %d',
                'urn:replicon:date-format:yyyy-month-dd': '%Y %B %d',
                'urn:replicon:date-format:mm-slash-dd-slash-yy': '%m/%d/%y',
                'urn:replicon:date-format:mm-slash-dd-slash-yyyy': '%m/%d/%Y',
                'urn:replicon:date-format:dd-slash-mm-slash-yy': '%d/%m/%y',
                'urn:replicon:date-format:dd-slash-mm-slash-yyyy': '%d/%m/%Y',
                'urn:replicon:date-format:yy-slash-mm-slash-dd': '%y/%m/%d',
                'urn:replicon:date-format:yyyy-slash-mm-slash-dd': '%Y/%m/%d',
                'urn:replicon:date-format:yy-slash-dd-slash-mm': '%y/%d/%m',
                'urn:replicon:date-format:yyyy-slash-dd-slash-mm': '%Y/%d/%m',
                'urn:replicon:date-format:mon-dd-comma-yy': '%b %d, %y',
                'urn:replicon:date-format:mon-dd-comma-yyyy': '%b %d, %Y',
                'urn:replicon:date-format:mm-dot-dd-dot-yy': '%m.%d.%y',
                'urn:replicon:date-format:mm-dot-dd-dot-yyyy': '%m.%d.%Y',
                'urn:replicon:date-format:dd-dot-mm-dot-yy': '%d.%m.%y',
                'urn:replicon:date-format:dd-dot-mm-dot-yyyy': '%d.%m.%Y'
            }

            date_format = DATE_FORMAT_MAPPING.get(date_format_uri, '%Y-%m-%d')
            date_obj = datetime.strptime(date_str, date_format)
            return date_obj.strftime('%Y-%m-%d')

        def to_unix_timestamp(date_str, time_str, date_format_uri):
            """Convert date + time string to Unix timestamp"""
            DATE_FORMAT_MAPPING = {
                'urn:replicon:date-format:month-dd-comma-yy': '%B %d, %y',
                'urn:replicon:date-format:month-dd-comma-yyyy': '%B %d, %Y',
                'urn:replicon:date-format:dd-month-yy': '%d %B %y',
                'urn:replicon:date-format:dd-month-yyyy': '%d %B %Y',
                'urn:replicon:date-format:yy-month-dd': '%y %B %d',
                'urn:replicon:date-format:yyyy-month-dd': '%Y %B %d',
                'urn:replicon:date-format:mm-slash-dd-slash-yy': '%m/%d/%y',
                'urn:replicon:date-format:mm-slash-dd-slash-yyyy': '%m/%d/%Y',
                'urn:replicon:date-format:dd-slash-mm-slash-yy': '%d/%m/%y',
                'urn:replicon:date-format:dd-slash-mm-slash-yyyy': '%d/%m/%Y',
                'urn:replicon:date-format:yy-slash-mm-slash-dd': '%y/%m/%d',
                'urn:replicon:date-format:yyyy-slash-mm-slash-dd': '%Y/%m/%d',
                'urn:replicon:date-format:yy-slash-dd-slash-mm': '%y/%d/%m',
                'urn:replicon:date-format:yyyy-slash-dd-slash-mm': '%Y/%d/%m',
                'urn:replicon:date-format:mon-dd-comma-yy': '%b %d, %y',
                'urn:replicon:date-format:mon-dd-comma-yyyy': '%b %d, %Y',
                'urn:replicon:date-format:mm-dot-dd-dot-yy': '%m.%d.%y',
                'urn:replicon:date-format:mm-dot-dd-dot-yyyy': '%m.%d.%Y',
                'urn:replicon:date-format:dd-dot-mm-dot-yy': '%d.%m.%y',
                'urn:replicon:date-format:dd-dot-mm-dot-yyyy': '%d.%m.%Y'
            }
            date_format = DATE_FORMAT_MAPPING.get(date_format_uri, '%Y-%m-%d')
            dt = datetime.strptime(f"{date_str} {time_str.strip()}", f"{date_format} %I:%M:%S %p")
            return int(dt.timestamp())

        def transform_time_entries(dag_run):
            entries = dag_run.conf['entries']
            date_format_uri = dag_run.conf['date_format_uri']
            time_entries = []

            for row in entries:
                distributed_time_type = row.get('Distributed Time Type Name', '').strip()

                if distributed_time_type == '':
                    continue

                employee = row.get('Login Name', '').strip()
                entry_date = row.get('Entry Date', '').strip()
                project_code = row.get('Project Code', '').strip()
                task_code = row.get('Task Code', '').strip()
                hours_str = row.get('Hours', '0').strip()
                amount_str = row.get('Amount')
                task_code_full_path = row.get('Task Code (Full Path)', '').strip()
                comments = row.get('Comments', '').strip()
                in_time = row.get('In Time', '').strip()
                out_time = row.get('Out Time', '').strip()

                phase_code = ''
                if task_code_full_path and '/' in task_code_full_path:
                    phase_code = task_code_full_path.split('/')[0].strip()

                try:
                    hours = float(hours_str) if hours_str else 0
                except ValueError:
                    hours = 0

                try:
                    amount = float(amount_str) if amount_str else None
                except ValueError:
                    amount = None

                # Convert In/Out time to Unix timestamps if available
                start_time = to_unix_timestamp(entry_date, in_time, date_format_uri) if in_time else None
                end_time = to_unix_timestamp(entry_date, out_time, date_format_uri) if out_time else None

                pay_type = config.paycodes_to_paytypes.get(distributed_time_type, 'regular')

                time_entry = {
                    "employee": employee,
                    "date": get_entry_date(entry_date, date_format_uri),
                    "job": project_code if project_code else None,
                    "phase": phase_code if phase_code else None,
                    "category": task_code if task_code else None,
                    "pay_type": pay_type,
                    "start_time": start_time,
                    "end_time": end_time,
                    "total_time": int(hours * 3600),
                    "amount": int(amount) if amount else None,
                    "status": "signed",
                    "description": comments if comments else None
                }

                time_entries.append(time_entry)

            return time_entries

        transform_entries = rail.PythonOperator(
            task_id='transform_entries',
            python_callable=transform_time_entries
        )

        sync_time_entries = rail.ComputereaseAPIOperator(
            task_id='sync_time_entries',
            endpoint='/timesheet/entry',
            request_method='POST',
            request_body=lambda: rail.result('transform_entries'),
            computerease_conn_id='{{ dag_run.conf.computerease_conn_id }}'
        )

        is_time_entries_created_successfully = rail.IfOperator(
            task_id='is_time_entries_created_successfully',
            trigger_rule="none_skipped",
            test=lambda: rail.result("sync_time_entries") and rail.result("sync_time_entries").get("message") == "Entries have been created.",
            yes_task='approve_time_entries',
            no_task='catch_time_entries_error'
        )

        def get_time_entry_uuids():
            time_entry_data = rail.result('sync_time_entries').get("data", [])
            time_entry_uuids = [entry["uuid"] for entry in time_entry_data]
            return {
                "uuid": time_entry_uuids
            }

        approve_time_entries = rail.ComputereaseAPIOperator(
            task_id='approve_time_entries',
            endpoint='/timesheet/entry/approve',
            request_method='PUT',
            request_body=get_time_entry_uuids,
            computerease_conn_id='{{ dag_run.conf.computerease_conn_id }}'
        )

        def get_downstreamtasks_error(employee, error_message):
            return {
                'error': f'Error with {employee} - {error_message}'
            }

        catch_time_entries_error = rail.PythonOperator(
            task_id='catch_time_entries_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.employee }}',
                     '{{ get_error_message() }}']
        )

        batch_task >> transform_entries >> sync_time_entries >> is_time_entries_created_successfully
        batch_task >> rail.Label('On Error') >> catch_time_entries_error

        is_time_entries_created_successfully >> rail.Label('Yes') >> approve_time_entries >> rail.Label('On Error') >> catch_time_entries_error
        is_time_entries_created_successfully >> rail.Label('No') >> catch_time_entries_error

        return dag


rail.for_each_instance(create_dag_instance)
