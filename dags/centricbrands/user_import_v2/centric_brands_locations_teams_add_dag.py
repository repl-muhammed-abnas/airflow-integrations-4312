
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'centricbrands_user_import_locations_teams_add_{config.instance}_v2',
        description=f'Centric_Brands_Locations(Teams)_Add {config.instance}_v2',
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
            no_task='get_all_locations'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_locations',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:location-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            }
        )

        create_collection_inputfile = rail.QueryCollectionOperator(
            task_id='create_collection_inputfile',
            name="teaminputfile",
            query="""SELECT * FROM inputfile"""
        )

        def get_existing_teams_list():
            existing_locations = rail.result('get_all_locations')['rows']
            return [{
                'location': cell['cellCollection'][-1]['textValue'] if cell['cellCollection'] and cell['cellCollection'][0]['dataType'] else null,
                'locationfullname': '|'.join([name['textValue'] for name in cell['cellCollection']]) if (
                    cell['cellCollection'] and cell['cellCollection'][0]['dataType']) else null,
                'uri': cell['cellCollection'][-1]['uri'] if cell['cellCollection'] and cell['cellCollection'][0]['dataType'] else null
            } for location in existing_locations for cell in location['cells']]

        create_existing_teams_list = rail.PythonOperator(
            task_id='create_existing_teams_list',
            python_callable=get_existing_teams_list
        )

        create_existingteams_collection = rail.CreateCollectionOperator(
            task_id='create_existingteams_collection',
            source=lambda: rail.result('create_existing_teams_list'),
            name="existingteams",
        )

        query_new_teams = rail.QueryCollectionOperator(
            task_id='query_new_teams',
            query="""SELECT DISTINCT  teaminputfile.team as newlocationfullname FROM  teaminputfile WHERE
                teaminputfile.team NOT IN (SELECT  existingteams.locationfullname FROM  existingteams) AND  NULLIF(team,'') IS NOT NULL""",
        )

        def get_new_teams_list():
            new_teams = rail.load_all_records(rail.result('query_new_teams'))
            return [{
                'level_1': ((team['newlocationfullname'].split('|'))[0] if '|' in team['newlocationfullname'] else team['newlocationfullname']).strip(),
                'level_2': ((team['newlocationfullname'].split('|'))[1] if '|' in team['newlocationfullname'] else '').strip(),
                'level_3': ((team['newlocationfullname'].split('|'))[-1] if '|' in team['newlocationfullname'] and len(
                    (team['newlocationfullname'].split('|'))) > 2 else '').strip()
            }for team in new_teams]

        create_new_teams_list = rail.PythonOperator(
            task_id='create_new_teams_list',
            python_callable=get_new_teams_list
        )

        declare_createdteamslist = rail.SetVariableOperator(
            task_id='declare_createdteamslist',
            append=False,
            name='createdteamslist',
            value=[]
        )

        foreach_new_location_to_create = rail.ForEachOperator(
            task_id='foreach_new_location_to_create',
            items=lambda: rail.result('create_new_teams_list'),
            start_task='create_parenturi_variable',
            end_task='foreach_new_location_to_create_end'
        )

        create_parenturi_variable = rail.SetVariableOperator(
            task_id='create_parenturi_variable',
            append=False,
            name='parenturi',
            value=None
        )

        if_level1_present = rail.IfOperator(
            task_id='if_level1_present',
            test='''{{ result('foreach_new_location_to_create').level_1 | is_truthy }}''',
            yes_task="check_for_level1_location_uri",
            no_task="if_failure_for_particular_location",
        )

        check_for_level1_location_uri = rail.PythonOperator(
            task_id='check_for_level1_location_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'create_existing_teams_list'), 'locationfullname', rail.result('foreach_new_location_to_create')['level_1'], 'uri', '')
        )

        if_level1_location_present = rail.IfOperator(
            task_id='if_level1_location_present',
            test='''{{ result('check_for_level1_location_uri') | is_truthy }}''',
            yes_task="update_parenturi_variable",
            no_task="search_in_createdteams_list",
        )

        update_parenturi_variable = rail.SetVariableOperator(
            task_id='update_parenturi_variable',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value=lambda: rail.result('check_for_level1_location_uri')
        )

        search_in_createdteams_list = rail.PythonOperator(
            task_id='search_in_createdteams_list',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var(
                'createdteamslist'), 'teamname', rail.result('foreach_new_location_to_create')['level_1'], 'uri', '')
        )

        if_present_in_createdteams_list = rail.IfOperator(
            task_id='if_present_in_createdteams_list',
            test='''{{ result('search_in_createdteams_list') | is_truthy }}''',
            yes_task="update_variable_parenturi",
            no_task="create_level1_location",
        )

        update_variable_parenturi = rail.SetVariableOperator(
            task_id='update_variable_parenturi',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value=lambda: rail.result('search_in_createdteams_list')
        )

        create_level1_location = rail.RepliconServiceOperator(
            task_id='create_level1_location',
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data=lambda: {
                "location": null,
                "modifications": {
                    "name": rail.result('foreach_new_location_to_create')['level_1'],
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": datetime.now().strftime('%Y%m%dT%H%M%S%L')
            }
        )

        update_parenturi = rail.SetVariableOperator(
            task_id='update_parenturi',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value=lambda: rail.result('create_level1_location')['uri']
        )

        insert_to_createdteams_list = rail.SetVariableOperator(
            task_id='insert_to_createdteams_list',
            append=True,
            name='{{ result("declare_createdteamslist").name }}',
            value={
                "locationname": "{{ result('foreach_new_location_to_create').level_1 }}",
                "uri": "{{ result('create_level1_location').uri }}"
            }
        )

        log_level1_team_added = rail.WriteLogOperator(
            task_id='log_level1_team_added',
            log="{{ dag_run.conf.groupslogslookuptable }}",
            message="na",
            severity="Success",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "department": "",
                "location": "",
                "team": "{{ result('foreach_new_location_to_create').level_1 }}",
                "status": "Success",
                "details": "Level 1 Team Added - '{{ result('foreach_new_location_to_create').level_1 }}'",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_level2_present = rail.IfOperator(
            task_id='if_level2_present',
            test='''{{ result('foreach_new_location_to_create').level_2 | is_truthy }}''',
            yes_task="log_level2_location",
            no_task="if_failure_for_particular_location",
        )

        log_level2_location = rail.PythonOperator(
            task_id='log_level2_location',
            python_callable=lambda:  rail.result('foreach_new_location_to_create')[
                'level_1'] + "|" + rail.result('foreach_new_location_to_create')['level_2']
        )

        check_for_level2_location_uri = rail.PythonOperator(
            task_id='check_for_level2_location_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'create_existing_teams_list'), 'locationfullname', rail.result('log_level2_location'), 'uri', '')
        )

        if_level2_location_present = rail.IfOperator(
            task_id='if_level2_location_present',
            test='''{{ result('check_for_level2_location_uri') | is_truthy }}''',
            yes_task="parenturi_variable_update",
            no_task="search_level2_location_in_createdteams_list",
        )

        parenturi_variable_update = rail.SetVariableOperator(
            task_id='parenturi_variable_update',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value=lambda: rail.result('check_for_level2_location_uri')
        )

        search_level2_location_in_createdteams_list = rail.PythonOperator(
            task_id='search_level2_location_in_createdteams_list',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var(
                'createdteamslist'), 'locationname', rail.result('log_level2_location'), 'uri', '')
        )

        if_level2_location_present_in_createdteams_list = rail.IfOperator(
            task_id='if_level2_location_present_in_createdteams_list',
            test='''{{ result('search_level2_location_in_createdteams_list') | is_truthy }}''',
            yes_task="parent_uri_variable_update",
            no_task="create_level2_location",
        )

        parent_uri_variable_update = rail.SetVariableOperator(
            task_id='parent_uri_variable_update',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value=lambda: rail.result(
                'search_level2_location_in_createdteams_list')
        )

        create_level2_location = rail.RepliconServiceOperator(
            task_id='create_level2_location',
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data=lambda: {
                "location": {
                    "name": null,
                    "uri": null,
                    "parent": {
                        "name": null,
                        "uri": rail.get_dag_run_var('parenturi'),
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": rail.result('foreach_new_location_to_create')['level_2'],
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": datetime.now().strftime('%Y%m%dT%H%M%S%L')
            }
        )

        update_parent_uri = rail.SetVariableOperator(
            task_id='update_parent_uri',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value=lambda: rail.result('create_level2_location')['uri']
        )

        insert_to_created_teams_list = rail.SetVariableOperator(
            task_id='insert_to_created_teams_list',
            append=True,
            name='{{ result("declare_createdteamslist").name }}',
            value={
                "locationname": "{{ result('foreach_new_location_to_create').level_1 }}|{{ result('foreach_new_location_to_create').level_2 }}",
                "uri": "{{ result('create_level2_location').uri }}"
            }
        )

        log_level2_team_added = rail.WriteLogOperator(
            task_id='log_level2_team_added',
            log="{{ dag_run.conf.groupslogslookuptable }}",
            message="na",
            severity="Success",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "department": "",
                "location": "{{ result('foreach_new_location_to_create').level_1 }}|{{ result('foreach_new_location_to_create').level_2 }}",
                "team": '',
                "status": "Success",
                "details": "Level 2 Team Added - '{{ result('foreach_new_location_to_create').level_2 }}'",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_level3_present = rail.IfOperator(
            task_id='if_level3_present',
            test='''{{ result('foreach_new_location_to_create').level_3 | is_truthy }}''',
            yes_task="log_level3_location",
            no_task="if_failure_for_particular_location",
        )

        log_level3_location = rail.PythonOperator(
            task_id='log_level3_location',
            python_callable=lambda:  rail.result('foreach_new_location_to_create')['level_1'] + "|" + rail.result(
                'foreach_new_location_to_create')['level_2'] + "|" + rail.result('foreach_new_location_to_create')['level_3']
        )

        check_for_level3_location_uri = rail.PythonOperator(
            task_id='check_for_level3_location_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'create_existing_teams_list'), 'locationfullname', rail.result('log_level3_location'), 'uri', '')
        )

        if_level3_location_present = rail.IfOperator(
            task_id='if_level3_location_present',
            test='''{{ result('check_for_level3_location_uri') | is_truthy }}''',
            yes_task="update_parenturi_forscalability",
            no_task="search_level3_location_in_createdteams_list",
        )

        update_parenturi_forscalability = rail.SetVariableOperator(
            task_id='update_parenturi_forscalability',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value=null
        )

        search_level3_location_in_createdteams_list = rail.PythonOperator(
            task_id='search_level3_location_in_createdteams_list',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var(
                'createdteamslist'), 'locationname', rail.result('log_level3_location'), 'uri', '')
        )

        if_level3_location_present_in_createdteams_list = rail.IfOperator(
            task_id='if_level3_location_present_in_createdteams_list',
            test='''{{ result('search_level3_location_in_createdteams_list') | is_truthy }}''',
            yes_task="update_parenturi_variable_forscalability",
            no_task="create_level3_location",
        )

        update_parenturi_variable_forscalability = rail.SetVariableOperator(
            task_id='update_parenturi_variable_forscalability',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value=null
        )

        create_level3_location = rail.RepliconServiceOperator(
            task_id='create_level3_location',
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data=lambda: {
                "location": {
                    "name": null,
                    "uri": null,
                    "parent": {
                        "name": null,
                        "uri": rail.get_dag_run_var('parenturi'),
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": rail.result('foreach_new_location_to_create')['level_3'],
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": datetime.now().strftime('%Y%m%dT%H%M%S%L')
            }
        )

        parenturi_variable_update_forscalability = rail.SetVariableOperator(
            task_id='parenturi_variable_update_forscalability',
            append=False,
            name='{{ result("create_parenturi_variable").name }}',
            value=null
        )

        insert_to_created_teamslist = rail.SetVariableOperator(
            task_id='insert_to_created_teamslist',
            append=True,
            name='{{ result("declare_createdteamslist").name }}',
            value={
                "locationname": """{{ result('foreach_new_location_to_create').level_1 }}
                |{{ result('foreach_new_location_to_create').level_2 }}
                |{{ result('foreach_new_location_to_create').level_3 }}""",
                "uri": "{{ result('create_level3_location').uri }}"
            }
        )

        log_level3_team_added = rail.WriteLogOperator(
            task_id='log_level3_team_added',
            log="{{ dag_run.conf.groupslogslookuptable }}",
            message="na",
            severity="Success",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "department": "",
                "location": """{{ result('foreach_new_location_to_create').level_1 }}
                |{{ result('foreach_new_location_to_create').level_2 }}
                |{{ result('foreach_new_location_to_create').level_3 }}""",
                "team": '',
                "status": "Success",
                "details": "Level 3 Team Added - '{{ result('foreach_new_location_to_create').level_3 }}'",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_failure_for_particular_location = rail.IfOperator(
            task_id='if_failure_for_particular_location',
            trigger_rule='all_done',
            test=lambda: bool(rail.render_template(
                "{{get_error_message()}}") and get_task_state('get_all_locations') == 'success'),
            yes_task='add_error_log_for_location',
            no_task='foreach_new_location_to_create_end'
        )

        add_error_log_for_location = rail.WriteLogOperator(
            task_id='add_error_log_for_location',
            log="{{ dag_run.conf.groupslogslookuptable }}",
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['callerjobid'],
                "department": "",
                "location": "",
                "team": (rail.result('foreach_new_location_to_create')['level_1'] + "|" + rail.result('foreach_new_location_to_create')['level_2'] + "|" +
                         rail.result('foreach_new_location_to_create')['level_3']),
                "status": "Error",
                "details": "Error adding Team - " + (rail.result('foreach_new_location_to_create')['level_1'] + "|" + rail.result(
                    'foreach_new_location_to_create')['level_2'] + "|" + rail.result('foreach_new_location_to_create')['level_3']),
                "childjobid": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        foreach_new_location_to_create_end = rail.EmptyOperator(
            task_id='foreach_new_location_to_create_end',
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
                "department": "",
                "location": '',
                "team": '',
                "status": "Error",
                "details": 'Error processing New Teams - {{get_error_message()}}',
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
        can_run_batch_task >> rail.Label(
            'No') >> get_all_locations >> create_collection_inputfile >> create_existing_teams_list
        create_existing_teams_list >> create_existingteams_collection >> query_new_teams >> create_new_teams_list
        create_new_teams_list >> declare_createdteamslist >> foreach_new_location_to_create >> create_parenturi_variable >> if_level1_present
        if_level1_present >> rail.Label(
            'Yes') >> check_for_level1_location_uri >> if_level1_location_present
        if_level1_location_present >> rail.Label(
            'Yes') >> update_parenturi_variable >> if_level2_present
        if_level1_location_present >> rail.Label(
            'No') >> search_in_createdteams_list >> if_present_in_createdteams_list
        if_present_in_createdteams_list >> rail.Label(
            'Yes') >> update_variable_parenturi >> if_level2_present
        if_present_in_createdteams_list >> rail.Label(
            'No') >> create_level1_location >> update_parenturi >> insert_to_createdteams_list >> log_level1_team_added >> if_level2_present
        if_level2_present >> rail.Label(
            'Yes') >> log_level2_location >> check_for_level2_location_uri >> if_level2_location_present
        if_level2_location_present >> rail.Label(
            'Yes') >> parenturi_variable_update >> if_level3_present
        if_level2_location_present >> rail.Label(
            'No') >> search_level2_location_in_createdteams_list >> if_level2_location_present_in_createdteams_list
        if_level2_location_present_in_createdteams_list >> rail.Label(
            'Yes') >> parent_uri_variable_update >> if_level3_present
        if_level2_location_present_in_createdteams_list >> rail.Label(
            'No') >> create_level2_location >> update_parent_uri >> insert_to_created_teams_list >> log_level2_team_added >> if_level3_present
        if_level3_present >> rail.Label(
            'Yes') >> log_level3_location >> check_for_level3_location_uri >> if_level3_location_present
        if_level3_location_present >> rail.Label(
            'Yes') >> update_parenturi_forscalability >> if_failure_for_particular_location
        if_level3_location_present >> rail.Label(
            'No') >> search_level3_location_in_createdteams_list >> if_level3_location_present_in_createdteams_list
        if_level3_location_present_in_createdteams_list >> rail.Label(
            'Yes') >> update_parenturi_variable_forscalability >> if_failure_for_particular_location
        if_level3_location_present_in_createdteams_list >> rail.Label(
            'No') >> create_level3_location >> parenturi_variable_update_forscalability >> insert_to_created_teamslist
        insert_to_created_teamslist >> log_level3_team_added >> if_failure_for_particular_location
        if_level3_present >> rail.Label(
            'No') >> if_failure_for_particular_location
        if_level2_present >> rail.Label(
            'No') >> if_failure_for_particular_location
        if_level1_present >> rail.Label(
            'No') >> if_failure_for_particular_location
        if_failure_for_particular_location >> rail.Label(
            'Yes') >> add_error_log_for_location >> foreach_new_location_to_create_end
        if_failure_for_particular_location >> rail.Label(
            'No') >> foreach_new_location_to_create_end
        foreach_new_location_to_create >> foreach_new_location_to_create_end >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
