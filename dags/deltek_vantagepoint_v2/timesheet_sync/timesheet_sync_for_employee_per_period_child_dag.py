from datetime import datetime, timedelta
import uuid
from airflow.models import Variable
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.timesheet_for_employee_per_period_dag_id,
        description=f'{config.company_key} Syncs the time data for an Employee per period to Vantagepoint as timesheets',
        company_key=config.company_key,
        max_active_runs=config.max_active_runs_per_employee_timesheet,
        multi_tenant=True
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_timesheet_batch_name'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_timesheet_batch_name',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_timesheet_batch_name = rail.PythonOperator(
            task_id='get_timesheet_batch_name',
            python_callable=lambda: (str(uuid.uuid4())).replace('-','')
        )

        def get_dynamic_laborcodelevels(dag_run):
            return dag_run.conf.get('laborcodelevels') or config.laborcodelevels

        def is_budget_labor_code_enabled():
            raw = getattr(config, 'enable_budget_labor_codes_level', False)
            if isinstance(raw, str):
                raw = raw.strip().lower() == 'true'
            return bool(raw) and getattr(config, 'budget_labor_codes_level', '') in ('Task', 'TimesheetFields')

        def is_budget_task_labor_code_enabled():
            return is_budget_labor_code_enabled() and getattr(config, 'budget_labor_codes_level', '') == 'Task'

        def is_budget_timesheet_field_labor_code_enabled():
            return is_budget_labor_code_enabled() and getattr(config, 'budget_labor_codes_level', '') == 'TimesheetFields'

        def get_timesheet_field_lc_caption():
            oef_name = getattr(config, 'timesheet_field_oef_name_for_lc', '') or ''
            return (oef_name.replace(' ', '_') + '__Code_') if oef_name else ''

        def get_wbs2_wbs3_from_task_full_path(entry):
            task_code_full_path = entry.get('Task_Code_Full_Path') or ''
            labor_code = entry.get('Task_Code') or ''
            if task_code_full_path == labor_code:
                return " ", " "
            task_code_full_path_filtered = task_code_full_path.rsplit(
                '/', 1)[0] if '/' in task_code_full_path else ''
            if task_code_full_path_filtered.count('/') >= 2:
                task_code_full_path_filtered = task_code_full_path_filtered.split('/', 1)[1]
                wbs2, wbs3 = ((task_code_full_path_filtered).split(
                    '/', 1) if '/' in task_code_full_path_filtered else [task_code_full_path_filtered, " "]) if task_code_full_path_filtered else [" ", " "]
            else:
                wbs2, wbs3 = (task_code_full_path_filtered or " "), " "
            return wbs2, wbs3

        def get_project_laborcode_and_laborcategory_details(entry, allow_lc_update, default_labor_code, time_categories, laborcodelevels):
            if entry.get('Time_Off_Type_Name'):
                time_off_type_name = (entry['Time_Off_Type_Name'].split('-'))[0]
                time_category_details = rail.find_first_by_attr_and_get_attr(time_categories, 'Category', time_off_type_name)
                if len(time_categories) == 0 or (not time_category_details):
                    raise Exception(f"Time Category details not found for Time Off Type {entry['Time_Off_Type_Name']}")
                return {
                    "WBS1": time_category_details['WBS1'],
                    "WBS2": time_category_details['WBS2'],
                    "WBS3": time_category_details['WBS3'],
                    "LaborCode": time_category_details['LaborCode'],
                    "BillCategory": time_category_details['BillCategory']
                }
            wbs1 = entry['Project_Code']
            task_code = entry['Task_Code']
            if is_budget_task_labor_code_enabled():
                wbs2, wbs3 = get_wbs2_wbs3_from_task_full_path(entry)
                return {
                    "WBS1": wbs1,
                    "WBS2": wbs2,
                    "WBS3": wbs3,
                    "LaborCode": task_code,
                    "BillCategory": entry.get(config.laborcategorycode_caption)
                }
            if is_budget_timesheet_field_labor_code_enabled():
                wbs2, wbs3 = ((task_code).split(
                    '/') if '/' in task_code else [task_code, " "]) if task_code else [" ", " "]
                return {
                    "WBS1": wbs1,
                    "WBS2": wbs2,
                    "WBS3": wbs3,
                    "LaborCode": entry.get(get_timesheet_field_lc_caption()) or "",
                    "BillCategory": entry.get(config.laborcategorycode_caption)
                }
            wbs2, wbs3 = ((task_code).split(
                '/') if '/' in task_code else [task_code, " "]) if task_code else [" ", " "]
            return {
                "WBS1": wbs1,
                "WBS2": wbs2,
                "WBS3": wbs3,
                "LaborCode": get_labor_code_value(entry, laborcodelevels=laborcodelevels) if allow_lc_update else default_labor_code,
                "BillCategory": entry.get(config.laborcategorycode_caption)
            }

        def get_labor_code_value(entry, use_default=False, laborcodelevels=None):
            laborcode = []
            keys = entry.keys()
            if laborcodelevels is None:
                laborcodelevels = config.laborcodelevels
            if use_default:
                laborcodelevels = [
                    'Default_' + laborcodelevel for laborcodelevel in laborcodelevels]
            for laborcodelevel in laborcodelevels:
                if laborcodelevel in keys and entry[laborcodelevel]:
                    laborcode.append(entry[laborcodelevel])
            return (config.laborcode_delimiter).join(laborcode)

        def get_ts_detail(dag_run):
            sumregularhours = 0
            overtimehours = 0
            detail = []
            time_entries = dag_run.conf['entries']
            time_categories = dag_run.conf['timecategories']
            if is_budget_labor_code_enabled():
                laborcodelevels = None
                allow_lc_update = False
                default_labor_code = None
            else:
                laborcodelevels = get_dynamic_laborcodelevels(dag_run)
                allow_lc_update = dag_run.conf['allow_lc_update']
                default_labor_code = get_labor_code_value(time_entries[0], True, laborcodelevels)
            for ind, entry in enumerate(time_entries):
                distributed_time_type = entry.get('Distributed_Time_Type_Name', '').strip()
                is_overtime_entry = distributed_time_type == 'Overtime' if distributed_time_type else entry.get(config.workdistribution_caption) == 'Over Time'
                if is_overtime_entry:
                    overtimehours += float(entry['Hours'])
                else:
                    sumregularhours += float(entry['Hours'])
                detail.append({
                    "Batch": rail.result('get_timesheet_batch_name'),
                    "Employee": entry['Login_Name'],
                    "PKey": f"{entry['Login_Name']}-{entry['Entry_Date']}-{ind}",
                    "Seq": ind,
                    "TransDate": (datetime.strptime(entry['Entry_Date'], config.replicon_date_format)).isoformat(timespec='milliseconds'),
                    **get_project_laborcode_and_laborcategory_details(entry, allow_lc_update, default_labor_code, time_categories, laborcodelevels),
                    "Locale": "",
                    "RegHrs": entry['Hours'] if not is_overtime_entry else 0,
                    "OvtHrs": entry['Hours'] if is_overtime_entry else 0,
                    "SpecialOvtHrs": 0,
                    "TransComment": entry['Comments'] if config.should_post_timeentry_comments else ""
                })
            return {
                'detail': detail,
                'sumregularhours': sumregularhours,
                'overtimehours': overtimehours,
                'specialovthours': 0
            }

        def get_timesheet_creation_payload(dag_run):
            ts_detail = get_ts_detail(dag_run)
            payload = {
                "Batch": rail.result('get_timesheet_batch_name'),
                "Description": f"Timesheet - {dag_run.conf['loginname']}-{dag_run.conf['export_time']}",
                "Recurring": config.timesheet_posting_config['recurring'],
                "StartDate": dag_run.conf['start_date'],
                "EndDate": dag_run.conf['end_date'],
                "RegHrsTotal": ts_detail['sumregularhours'],
                "OvtHrsTotal": ts_detail['overtimehours'],
                "SpecialOvtHrsTotal": ts_detail['specialovthours'],
                "Selected": config.timesheet_posting_config['selected'],
                "Posted": config.timesheet_posting_config['posted'],
                "Period": dag_run.conf['active_period'],
                "Company": dag_run.conf['home_company'],
                "Diary": "",
                "tsMaster": [
                    {
                        "Batch": rail.result('get_timesheet_batch_name'),
                        "Employee": dag_run.conf['loginname'],
                        "Posted": config.timesheet_posting_config['posted'],
                        "Seq": 1,
                        "Status": config.timesheet_posting_config['tsmaster_status'],
                        "AuthorizedBy": "",
                        "RejectReason": "",
                        "ModUser": "",
                        "ModDate": ""
                    }
                ],
                "tsDetail": ts_detail['detail']
            }
            return payload

        sync_timesheet = rail.VantagepointAPIOperator(
            task_id="sync_timesheet",
            request_method='POST',
            endpoint="/DataEntry/tsControl",
            request_body=get_timesheet_creation_payload,
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}'
        )

        if_post_option_enabled = rail.IfOperator(
            task_id='if_post_option_enabled',
            test=lambda: Variable.get(
                config.post_timesheets_after_sync_var_name, default_var='false').lower() == 'true',
            yes_task='post_timeentry_vp',
            no_task='catch_error'
        )

        post_timeentry_vp = rail.VantagepointAPIOperator(
            task_id='post_timeentry_vp',
            endpoint='/DataEntry/PostTransFile',
            request_method='PUT',
            request_body=lambda dag_run: {
                "parms": {
                    "batch": rail.result('get_timesheet_batch_name'),
                    "clientcompany": dag_run.conf['home_company'],
                    "description": f"Timesheet - {dag_run.conf['loginname']}",
                    "period": dag_run.conf['active_period'],
                    "transtype": "TS"
                }
            },
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}'
        )

        def get_downstreamtasks_error(error_message):
            return {
                'error': f'Error in timesheet sync per period - {error_message}'
            }

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ get_error_message() }}']
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_error

        can_run_batch_task >> rail.Label(
            'No') >> get_timesheet_batch_name >> sync_timesheet >> if_post_option_enabled
        if_post_option_enabled >> rail.Label(
            'Yes') >> post_timeentry_vp >> catch_error
        if_post_option_enabled >> rail.Label(
            'No') >> catch_error
        return dag


rail.for_each_instance(create_dag)
