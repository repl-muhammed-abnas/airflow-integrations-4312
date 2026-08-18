from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'centricbrands_user_import_department_add_{config.instance}_v2',
        description=f'Centric_Brands_Department_Add {config.instance}_v2',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_csv_file_has_no_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_csv_file_has_no_data',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_csv_file_has_no_data = rail.IfOperator(
            task_id='if_csv_file_has_no_data',
            test=lambda dag_run: len(rail.load_all_records(
                dag_run.conf['downloadedfile'])) == 0,
            yes_task="catch_and_log_error",
            no_task="generate_existing_departments_report",
        )

        generate_existing_departments_report = rail.RepliconServiceOperator(
            task_id='generate_existing_departments_report',
            endpoint="/services/reportService1.svc/GenerateReport",
            data={
                "reportUri": "{{ dag_run.conf.reporturi }}",
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            },
            target='artifact'
        )

        create_collection_inputfile = rail.QueryCollectionOperator(
            task_id='create_collection_inputfile',
            name="departmentinputfile",
            query="""SELECT * FROM inputfile"""
        )

        load_existing_departments_csv = rail.LoadCSVFileOperator(
            task_id="load_existing_departments_csv",
            document="{{ (result('generate_existing_departments_report') | load_json_artifact).payload }}",
        )

        load_existing_departments = rail.PythonOperator(
            task_id='load_existing_departments',
            python_callable=lambda: rail.load_all_records(
                rail.result('load_existing_departments_csv'))
        )

        create_collection_existing_departments = rail.CreateCollectionOperator(
            task_id='create_collection_existing_departments',
            source="{{ result('load_existing_departments_csv') }}",
            name="existingdepartments",
            columns={
                'Department Name': 'department',
                'Parent Department Name': 'parentdepartment',
                'Department Full Name': 'departmentfullname',
                'department uri': 'departmenturi'
            }
        )

        query_distinct_new_departments = rail.QueryCollectionOperator(
            task_id='query_distinct_new_departments',
            query="""SELECT DISTINCT departmentinputfile.departmentfullname FROM  departmentinputfile WHERE
                departmentinputfile.departmentfullname NOT IN (SELECT  existingdepartments.departmentfullname FROM
                existingdepartments) AND  NULLIF(departmentfullname,'') IS NOT NULL""",
        )

        def get_new_departments_list():
            new_departments = rail.load_all_records(
                rail.result('query_distinct_new_departments'))
            return [
                {"level_1": dept.get('departmentfullname', '').split('/')[0],
                 "level_2": ''.join(
                    dept.get('departmentfullname', '').split('/')[1:2]) or '',
                 "level_3": ''.join(dept.get('departmentfullname', '').split('/')[2:])}
                for dept in new_departments
            ]

        create_new_departments_to_be_createdlist = rail.PythonOperator(
            task_id='create_new_departments_to_be_createdlist',
            python_callable=get_new_departments_list
        )

        create_createddepartments_list = rail.SetVariableOperator(
            task_id='create_createddepartments_list',
            append=False,
            name='createddepartmentslist',
            value=[]
        )

        foreach_new_department_to_create = rail.ForEachOperator(
            task_id='foreach_new_department_to_create',
            items=lambda: rail.result(
                'create_new_departments_to_be_createdlist'),
            start_task='create_parenturi_variable',
            end_task='foreach_new_department_to_create_end'
        )

        create_parenturi_variable = rail.SetVariableOperator(
            task_id='create_parenturi_variable',
            append=False,
            name='parenturi',
            value=None
        )

        if_level1_present = rail.IfOperator(
            task_id='if_level1_present',
            test='''{{ result('foreach_new_department_to_create').level_1 | is_truthy }}''',
            yes_task="check_for_level1_dept_uri",
            no_task="if_failure_for_particular_department",
        )

        check_for_level1_dept_uri = rail.PythonOperator(
            task_id='check_for_level1_dept_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'load_existing_departments'), 'Department Full Name', rail.result('foreach_new_department_to_create')['level_1'], 'department uri', '')
        )

        if_level1_department_present = rail.IfOperator(
            task_id='if_level1_department_present',
            test='''{{ result('check_for_level1_dept_uri') | is_truthy }}''',
            yes_task="update_parenturi_variable",
            no_task="search_in_createddepartments_list",
        )

        update_parenturi_variable = rail.SetVariableOperator(
            task_id='update_parenturi_variable',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value=lambda: rail.result('check_for_level1_dept_uri')
        )

        search_in_createddepartments_list = rail.PythonOperator(
            task_id='search_in_createddepartments_list',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var(
                'createddepartmentslist'), 'departmentname', rail.result('foreach_new_department_to_create')['level_1'], 'uri', '')
        )

        if_present_in_createddepartments_list = rail.IfOperator(
            task_id='if_present_in_createddepartments_list',
            test='''{{ result('search_in_createddepartments_list') | is_truthy }}''',
            yes_task="update_variable_parenturi",
            no_task="put_department_level1",
        )

        update_variable_parenturi = rail.SetVariableOperator(
            task_id='update_variable_parenturi',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value=lambda: rail.result('search_in_createddepartments_list')
        )

        put_department_level1 = rail.RepliconServiceOperator(
            task_id='put_department_level1',
            endpoint="/services/DepartmentService1.svc/PutDepartment",
            data={
                "department": {
                    "target": {
                        "uri": null,
                        "name": "{{ result('foreach_new_department_to_create').level_1 }}",
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "name": "{{ result('foreach_new_department_to_create').level_1 }}",
                    "code": null,
                    "comments": null,
                    "isEnabled": "true",
                    "customFieldValues": []
                }
            }
        )

        update_parenturi = rail.SetVariableOperator(
            task_id='update_parenturi',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value=lambda: rail.result('put_department_level1')['uri']
        )

        insert_to_createddepartments_list = rail.SetVariableOperator(
            task_id='insert_to_createddepartments_list',
            append=True,
            name='{{ result("create_createddepartments_list").name }}',
            value={
                "departmentname": "{{ result('foreach_new_department_to_create').level_1 }}",
                "uri": "{{ result('put_department_level1').uri }}"
            }
        )

        log_level1_department_added = rail.WriteLogOperator(
            task_id='log_level1_department_added',
            log="{{ dag_run.conf.groupslogslookuptable }}",
            message="na",
            severity="Success",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "department": """{{ result('foreach_new_department_to_create').level_1 }}
                |{{ result('foreach_new_department_to_create').level_2 }}|{{ result('foreach_new_department_to_create').level_3 }}""",
                "location": '',
                "team": '',
                "status": "Success",
                "details": "Level 1 Department Added - '{{ result('foreach_new_department_to_create').level_1 }}'",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_level2_present = rail.IfOperator(
            task_id='if_level2_present',
            test='''{{ result('foreach_new_department_to_create').level_2 | is_truthy }}''',
            yes_task="log_level2_department",
            no_task="if_failure_for_particular_department",
        )

        log_level2_department = rail.PythonOperator(
            task_id='log_level2_department',
            python_callable=lambda:  rail.result('foreach_new_department_to_create')[
                'level_1'] + " / " + rail.result('foreach_new_department_to_create')['level_2']
        )

        check_for_level2_dept_uri = rail.PythonOperator(
            task_id='check_for_level2_dept_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'load_existing_departments'), 'Department Full Name', rail.result('log_level2_department'), 'department uri', '')
        )

        if_level2_department_present = rail.IfOperator(
            task_id='if_level2_department_present',
            test='''{{ result('check_for_level2_dept_uri') | is_truthy }}''',
            yes_task="parenturi_variable_update",
            no_task="search_level2_dept_in_createddepartments_list",
        )

        parenturi_variable_update = rail.SetVariableOperator(
            task_id='parenturi_variable_update',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value="{{ result('check_for_level2_dept_uri') }}"
        )

        search_level2_dept_in_createddepartments_list = rail.PythonOperator(
            task_id='search_level2_dept_in_createddepartments_list',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var(
                'createddepartmentslist'), 'departmentname', rail.result('log_level2_department'), 'uri', '')
        )

        if_level2_dept_present_in_createddepartments_list = rail.IfOperator(
            task_id='if_level2_dept_present_in_createddepartments_list',
            test='''{{ result('search_level2_dept_in_createddepartments_list') | is_truthy }}''',
            yes_task="parent_uri_variable_update",
            no_task="put_department_level2",
        )

        parent_uri_variable_update = rail.SetVariableOperator(
            task_id='parent_uri_variable_update',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value="{{ result('search_level2_dept_in_createddepartments_list') }}"
        )

        put_department_level2 = rail.RepliconServiceOperator(
            task_id='put_department_level2',
            endpoint="/services/DepartmentService1.svc/PutDepartment",
            data=lambda: {
                "department": {
                    "target": {
                        "uri": null,
                        "name": rail.result('foreach_new_department_to_create')['level_2'],
                        "parent": {
                            "uri": rail.get_dag_run_var('parenturi'),
                            "name": null,
                            "parent": null,
                            "parameterCorrelationId": null
                        },
                        "parameterCorrelationId": null
                    },
                    "name": rail.result('foreach_new_department_to_create')['level_2'],
                    "code": null,
                    "comments": null,
                    "isEnabled": "true",
                    "customFieldValues": []
                }
            }
        )

        update_parent_uri = rail.SetVariableOperator(
            task_id='update_parent_uri',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value="{{ result('put_department_level2').uri }}"
        )

        insert_to_created_departments_list = rail.SetVariableOperator(
            task_id='insert_to_created_departments_list',
            append=True,
            name='{{ result("create_createddepartments_list").name }}',
            value={
                "departmentname": "{{ result('foreach_new_department_to_create').level_1 }} / {{ result('foreach_new_department_to_create').level_2 }}",
                "uri": "{{ result('put_department_level2').uri }}"
            }
        )

        log_level2_department_added = rail.WriteLogOperator(
            task_id='log_level2_department_added',
            log="{{ dag_run.conf.groupslogslookuptable }}",
            message="na",
            severity="Success",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "department": """{{ result('foreach_new_department_to_create').level_1 }}
                |{{ result('foreach_new_department_to_create').level_2 }}|{{ result('foreach_new_department_to_create').level_3 }}""",
                "location": '',
                "team": '',
                "status": "Success",
                "details": "Level 2 Department Added - '{{ result('foreach_new_department_to_create').level_2 }}'",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_level3_present = rail.IfOperator(
            task_id='if_level3_present',
            test='''{{ result('foreach_new_department_to_create').level_3 | is_truthy }}''',
            yes_task="log_level3_department",
            no_task="if_failure_for_particular_department",
        )

        log_level3_department = rail.PythonOperator(
            task_id='log_level3_department',
            python_callable=lambda:  rail.result('foreach_new_department_to_create')['level_1'] + " / " + rail.result(
                'foreach_new_department_to_create')['level_2'] + " / " + rail.result('foreach_new_department_to_create')['level_3']
        )

        check_for_level3_dept_uri = rail.PythonOperator(
            task_id='check_for_level3_dept_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'load_existing_departments'), 'Department Full Name', rail.result('log_level3_department'), 'department uri', '')
        )

        if_level3_department_present = rail.IfOperator(
            task_id='if_level3_department_present',
            test='''{{ result('check_for_level3_dept_uri') | is_truthy }}''',
            yes_task="update_parenturi_forscalability",
            no_task="search_level3_dept_in_createddepartments_list",
        )

        update_parenturi_forscalability = rail.SetVariableOperator(
            task_id='update_parenturi_forscalability',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value=null
        )

        search_level3_dept_in_createddepartments_list = rail.PythonOperator(
            task_id='search_level3_dept_in_createddepartments_list',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var(
                'createddepartmentslist'), 'departmentname', rail.result('log_level3_department'), 'uri', '')
        )

        if_level3_dept_present_in_createddepartments_list = rail.IfOperator(
            task_id='if_level3_dept_present_in_createddepartments_list',
            test='''{{ result('search_level3_dept_in_createddepartments_list') | is_truthy }}''',
            yes_task="update_parenturi_variable_forscalability",
            no_task="put_department_level3",
        )

        update_parenturi_variable_forscalability = rail.SetVariableOperator(
            task_id='update_parenturi_variable_forscalability',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value=null
        )

        put_department_level3 = rail.RepliconServiceOperator(
            task_id='put_department_level3',
            endpoint="/services/DepartmentService1.svc/PutDepartment",
            data=lambda: {
                "department": {
                    "target": {
                        "uri": null,
                        "name": rail.result('foreach_new_department_to_create')['level_3'],
                        "parent": {
                            "uri": rail.get_dag_run_var('parenturi'),
                            "name": null,
                            "parent": null,
                            "parameterCorrelationId": null
                        },
                        "parameterCorrelationId": null
                    },
                    "name": rail.result('foreach_new_department_to_create')['level_3'],
                    "code": null,
                    "comments": null,
                    "isEnabled": "true",
                    "customFieldValues": []
                }
            }
        )

        parenturi_variable_update_forscalability = rail.SetVariableOperator(
            task_id='parenturi_variable_update_forscalability',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value=null
        )

        insert_to_created_departmentslist = rail.SetVariableOperator(
            task_id='insert_to_created_departmentslist',
            append=True,
            name='{{ result("create_createddepartments_list").name }}',
            value={
                "departmentname": """{{ result('foreach_new_department_to_create').level_1 }} 
                / {{ result('foreach_new_department_to_create').level_2 }} / {{ result('foreach_new_department_to_create').level_3 }}""",
                "uri": "{{ result('put_department_level3').uri }}"
            }
        )

        log_level3_department_added = rail.WriteLogOperator(
            task_id='log_level3_department_added',
            log="{{ dag_run.conf.groupslogslookuptable }}",
            message="na",
            severity="Success",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "department": """{{ result('foreach_new_department_to_create').level_1 }}
                |{{ result('foreach_new_department_to_create').level_2 }}|{{ result('foreach_new_department_to_create').level_3 }}""",
                "location": '',
                "team": '',
                "status": "Success",
                "details": "Level 3 Department Added - '{{ result('foreach_new_department_to_create').level_3 }}'",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_failure_for_particular_department = rail.IfOperator(
            task_id='if_failure_for_particular_department',
            trigger_rule='all_done',
            test=lambda: bool(rail.render_template("{{get_error_message()}}") and get_task_state(
                'generate_existing_departments_report') == 'success'),
            yes_task='add_error_log_for_department',
            no_task='foreach_new_department_to_create_end'
        )

        add_error_log_for_department = rail.WriteLogOperator(
            task_id='add_error_log_for_department',
            log="{{ dag_run.conf.groupslogslookuptable }}",
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['callerjobid'],
                "department": (rail.result('foreach_new_department_to_create')['level_1'] + "|" + rail.result('foreach_new_department_to_create')['level_2'] +
                               "|" + rail.result('foreach_new_department_to_create')['level_3']),
                "location": '',
                "team": '',
                "status": "Error",
                "details": "Error adding Department - " + rail.render_template('{{get_error_message()}}'),
                "childjobid": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        foreach_new_department_to_create_end = rail.EmptyOperator(
            task_id='foreach_new_department_to_create_end',
        )

        def get_task_state(task_id):
            task_instance = rail.get_current_context(
            )['dag_run'].get_task_instance(task_id)
            return task_instance.current_state() if task_instance else null

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.groupslogslookuptable }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "department": '',
                "location": '',
                "team": '',
                "status": "Error",
                "details": 'Error processing New Departments - {{get_error_message()}}',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> if_csv_file_has_no_data
        if_csv_file_has_no_data >> rail.Label('Yes') >> catch_and_log_error
        if_csv_file_has_no_data >> rail.Label(
            'No') >> generate_existing_departments_report
        generate_existing_departments_report >> create_collection_inputfile >> load_existing_departments_csv >> load_existing_departments
        load_existing_departments >> create_collection_existing_departments >> query_distinct_new_departments >> create_new_departments_to_be_createdlist
        create_new_departments_to_be_createdlist >> create_createddepartments_list >> foreach_new_department_to_create >> create_parenturi_variable
        create_parenturi_variable >> if_level1_present
        if_level1_present >> rail.Label(
            'Yes') >> check_for_level1_dept_uri >> if_level1_department_present
        if_level1_department_present >> rail.Label(
            'Yes') >> update_parenturi_variable >> if_level2_present
        if_level1_department_present >> rail.Label(
            'No') >> search_in_createddepartments_list >> if_present_in_createddepartments_list
        if_present_in_createddepartments_list >> rail.Label(
            'Yes') >> update_variable_parenturi >> if_level2_present
        if_present_in_createddepartments_list >> rail.Label(
            'No') >> put_department_level1 >> update_parenturi >> insert_to_createddepartments_list >> log_level1_department_added >> if_level2_present
        if_level2_present >> rail.Label(
            'Yes') >> log_level2_department >> check_for_level2_dept_uri >> if_level2_department_present
        if_level2_department_present >> rail.Label(
            'Yes') >> parenturi_variable_update >> if_level3_present
        if_level2_department_present >> rail.Label(
            'No') >> search_level2_dept_in_createddepartments_list >> if_level2_dept_present_in_createddepartments_list
        if_level2_dept_present_in_createddepartments_list >> rail.Label(
            'Yes') >> parent_uri_variable_update >> if_level3_present
        if_level2_dept_present_in_createddepartments_list >> rail.Label(
            'No') >> put_department_level2 >> update_parent_uri >> insert_to_created_departments_list >> log_level2_department_added >> if_level3_present
        if_level3_present >> rail.Label(
            'Yes') >> log_level3_department >> check_for_level3_dept_uri >> if_level3_department_present
        if_level3_department_present >> rail.Label(
            'Yes') >> update_parenturi_forscalability >> if_failure_for_particular_department
        if_level3_department_present >> rail.Label(
            'No') >> search_level3_dept_in_createddepartments_list >> if_level3_dept_present_in_createddepartments_list
        if_level3_dept_present_in_createddepartments_list >> rail.Label(
            'Yes') >> update_parenturi_variable_forscalability >> if_failure_for_particular_department
        if_level3_dept_present_in_createddepartments_list >> rail.Label(
            'No') >> put_department_level3 >> parenturi_variable_update_forscalability >> insert_to_created_departmentslist >> log_level3_department_added
        log_level3_department_added >> if_failure_for_particular_department
        if_level3_present >> rail.Label(
            'No') >> if_failure_for_particular_department
        if_level2_present >> rail.Label(
            'No') >> if_failure_for_particular_department
        if_level1_present >> rail.Label(
            'No') >> if_failure_for_particular_department
        if_failure_for_particular_department >> rail.Label(
            'Yes') >> add_error_log_for_department >> foreach_new_department_to_create_end
        if_failure_for_particular_department >> rail.Label(
            'No') >> foreach_new_department_to_create_end
        foreach_new_department_to_create >> foreach_new_department_to_create_end >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
