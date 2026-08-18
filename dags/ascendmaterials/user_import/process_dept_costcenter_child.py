from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.dept_costcenter_dag_id,
        description=f'Ascend Child - Department and cost center update {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_seconday_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config',
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='download'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='download',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ── Input file ───────────────────────────────────────────────────
        download = rail.SFTPDownloadFileOperator(
            task_id='download',
            remote_filepath='{{ dag_run.conf["filepath"] }}',
        )

        parse_input_csv = rail.LoadCSVFileOperator(
            task_id='parse_input_csv',
            document="{{ result('download') }}",
            delimiter=',',
            headers=['loginname', 'employeefirstname', 'employeelastname', 'employeetype', 'timetype',
                     'department', 'authenticationtype', 'enabled', 'employeeid', 'startdate', 'lastdayofwork',
                     'continuousservicedate', 'emailaddress', 'manager', 'location', 'homecountry', 'homestateprovince',
                     'homecity', 'hourlypayrollrate', 'hourlypayrollcurrency', 'costcenter', 'udf']
        )

        # Normalize input: replace | with / in costcenter paths, strip whitespace
        normalize_input_data = rail.PythonOperator(
            task_id='normalize_input_data',
            python_callable=lambda: [
                {
                    k: v for k, v in {
                        **row,
                        'department': str(row['department']).replace(' | ', '/') if row['department'] else '',
                        'costcenter': str(row['costcenter']).replace(' | ', '/') if row['costcenter'] else ''
                    }.items() if k and k.lower() != 'null'
                }
                for row in rail.load_all_records(rail.result('parse_input_csv'))
            ]
        )

        create_input_collection = rail.CreateCollectionOperator(
            task_id='create_input_collection',
            source="{{ result('normalize_input_data') | to_json }}",
            name='inputrawdata',
        )

        # Log handle passed to sub-children so they can look up Replicon URIs
        create_dept_log = rail.CreateLogOperator(
            task_id='create_dept_log'
        )

        # ── Department report ────────────────────────────────────────────
        generate_dept_report = rail.RepliconServiceOperator(
            task_id='generate_dept_report',
            endpoint="/services/reportService1.svc/GenerateReport",
            data={
                "reportUri": '{{ dag_run.conf["departmentreporturi"] }}',
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        if_dept_report_error = rail.IfOperator(
            task_id='if_dept_report_error',
            test="{{ result('generate_dept_report').error | is_truthy }}",
            yes_task='stop_dept_report_error',
            no_task='parse_dept_report',
        )

        stop_dept_report_error = rail.FailOperator(
            task_id='stop_dept_report_error',
            message="{{ result('generate_dept_report').error }}"
        )

        parse_dept_report = rail.LoadCSVFileOperator(
            task_id='parse_dept_report',
            document="{{ result('generate_dept_report').payload }}",
            delimiter=',',
            headers=['Department Name', 'Parent Department Name',
                     'Department Full Name', 'Department URI']
        )

        # Replicon report uses ' / ' (space-slash-space); feed uses '/'. Normalize to '/' so
        # SQL comparisons and find_first_by_attr_and_get_attr lookups match the input data.
        normalize_dept_report = rail.PythonOperator(
            task_id='normalize_dept_report',
            python_callable=lambda: [
                {**row, 'Department Full Name': row['Department Full Name'].replace(' / ', '/')}
                for row in rail.load_all_records(rail.result('parse_dept_report'))
            ]
        )

        create_dept_report_collection = rail.CreateCollectionOperator(
            task_id='create_dept_report_collection',
            source="{{ result('normalize_dept_report') | to_json }}",
            name='departmentreport',
        )

        # ── Find and add missing departments ─────────────────────────────
        query_missing_depts = rail.QueryCollectionOperator(
            task_id='query_missing_depts',
            query="""SELECT DISTINCT inputrawdata.department FROM inputrawdata
                     WHERE inputrawdata.department IS NOT NULL
                     AND TRIM(inputrawdata.department) != ''
                     AND LOWER(inputrawdata.department) NOT IN (
                         SELECT DISTINCT LOWER(departmentreport.Department_Full_Name) FROM departmentreport
                     )""",
        )

        if_missing_depts = rail.IfOperator(
            task_id='if_missing_depts',
            test="{{ result('query_missing_depts') | length > 0 }}",
            yes_task='trigger_dept_add',
            no_task='get_cost_center_details',
        )

        # Trigger one sub-child per missing department (parallel, accumulate results for wait)
        trigger_dept_add = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dept_add',
            retries=0,
            items="{{ result('query_missing_depts') }}",
            trigger_dag_id=config.department_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "department": item.get('department'),
                "create_dept_log": rail.result('create_dept_log'),
                "parent_dept_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result('normalize_dept_report'),
                    'Department Full Name',
                    '/'.join(item.get('department', '').split('/')[:-1]),
                    'Department URI',
                    None
                )
            }
        )

        wait_dept_add = rail.WaitForDagRunsSensor(
            task_id='wait_dept_add',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dept_add") }}'
        )

        def filter_all_costcenters_data(response):
            costcenter_info = list(map(lambda row: {
                'costcentername': row['cells'][0]['textValue'],
                'fullpath': rail.smartjoin_by_delim(
                    [cell['textValue'] for cell in row['cells'][1]['cellCollection']], '/'),
                'costcenteruri': row['cells'][0]['uri'],
            }, response['rows']))
            return costcenter_info if costcenter_info else None

        # ── Get and process cost centers ─────────────────────────────────
        get_cost_center_details = rail.RepliconServiceOperator(
            task_id='get_cost_center_details',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:cost-center",
                    "urn:replicon:cost-center-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler= filter_all_costcenters_data
        )

        create_cost_center_collection = rail.CreateCollectionOperator(
            task_id='create_cost_center_collection',
            source="{{ result('get_cost_center_details') | to_json }}",
            name='costcenterdata',
        )

        query_missing_cost_centers = rail.QueryCollectionOperator(
            task_id='query_missing_cost_centers',
            query="""SELECT DISTINCT inputrawdata.costcenter FROM inputrawdata
                     WHERE inputrawdata.costcenter IS NOT NULL
                     AND LOWER(inputrawdata.costcenter) NOT IN (
                         SELECT DISTINCT LOWER(costcenterdata.fullpath) FROM costcenterdata
                     )""",
        )

        if_missing_cost_centers = rail.IfOperator(
            task_id='if_missing_cost_centers',
            test="{{ result('query_missing_cost_centers','length') > 0 }}",
            yes_task='trigger_cost_center_add',
            no_task='dept_log_cleanup',
        )

        # Trigger one sub-child per missing cost center (parallel, accumulate for wait)
        trigger_cost_center_add = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_cost_center_add',
            retries=3,
            items="{{ result('query_missing_cost_centers') }}",
            trigger_dag_id=config.cost_center_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "costcenter": item.get('costcenter'),
                "create_dept_log": rail.result('create_dept_log'),
                "parent_costcenter_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_cost_center_details') or [],
                    'fullpath',
                    '/'.join(item.get('costcenter', '').split('/')[:-1]),
                    'costcenteruri',
                    None
                )
            }
        )

        wait_cost_center_add = rail.WaitForDagRunsSensor(
            task_id='wait_cost_center_add',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_cost_center_add") }}'
        )

        # ── Cleanup & finish ─────────────────────────────────────────────
        dept_log_cleanup = rail.FilterLogEntriesOperator(
            task_id='dept_log_cleanup',
            log="{{ result('create_dept_log') }}",
            properties={"jobid": "{{ dag_run_ecid() }}"},
            remove_filtered_entries=True
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            trigger_rule='one_failed',
            severity="Error",
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "username": "",
                "userloginname": "",
                "action": "Department/Cost Center Update",
                "status": "Error",
                "details": rail.render_template("{{ get_error_message() }}")
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        # ── Wiring ──────────────────────────────────────────────────────
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> download

        # Sequential: parse input → build collections → fetch dept report → populate log → query
        download >> parse_input_csv >> normalize_input_data >> create_input_collection >> create_dept_log
        create_dept_log >> generate_dept_report >> if_dept_report_error
        if_dept_report_error >> rail.Label('Yes') >> stop_dept_report_error
        if_dept_report_error >> rail.Label('No') >> parse_dept_report \
            >> normalize_dept_report >> create_dept_report_collection >> query_missing_depts

        query_missing_depts >> if_missing_depts
        if_missing_depts >> rail.Label('Yes') >> trigger_dept_add >> wait_dept_add \
            >> get_cost_center_details
        if_missing_depts >> rail.Label('No') >> get_cost_center_details

        # Find missing cost centers (transform API response in one step)
        get_cost_center_details >> create_cost_center_collection \
            >> query_missing_cost_centers
        query_missing_cost_centers >> if_missing_cost_centers
        if_missing_cost_centers >> rail.Label('Yes') >> trigger_cost_center_add \
            >> wait_cost_center_add >> dept_log_cleanup
        if_missing_cost_centers >> rail.Label('No') >> dept_log_cleanup

        dept_log_cleanup >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
