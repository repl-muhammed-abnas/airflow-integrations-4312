from datetime import timedelta
import itertools
import uuid
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/mccarthy/user_import/config.py


# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'mccarthy_user_import_child_department_add_{config.instance}',
        description=f'LIVE | Mccarthy Child_department add V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_department_length_greater_7'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_department_length_greater_7',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        is_department_length_greater_7 = rail.IfOperator(
            task_id='is_department_length_greater_7',
            test="{{ dag_run.conf.department | split('|') | length > 7 }}",
            yes_task="dagrun_log_to_sumo",
            no_task="is_department_length_2"
        )

        is_department_length_2 = rail.IfOperator(
            task_id='is_department_length_2',
            test="{{ dag_run.conf.department | split('|') | length == 2 }}",
            yes_task="create_departmentgroup_level2",
            no_task="get_department_group_details"
        )

        create_departmentgroup_level2 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level2',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": dag_run.conf['parentdepartmenturi']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-1],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def get_department_groups(response):
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            return list(map(lambda item: {
                'departmentname': item['cells'][0]['textValue'],
                'departmenturi': item['cells'][0]['uri'],
                'fullpath': rail.smartjoin_by_delim(
                    [x['textValue'] for x in item['cells'][1]['cellCollection']], '|') if [
                        x['textValue'] for x in item['cells'][1]['cellCollection']] else ''
            }, flatten_rows)) if flatten_rows else []
        get_department_group_details = rail.RepliconServicePageOperator(
            task_id='get_department_group_details',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 1000000,
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path"
                ]
            },
            page_handler=page_handler,
            all_result_data_handler=get_department_groups
        )

        def get_department_levels_uri():
            department = rail.get_current_context(
            )['dag_run'].conf['department']
            department_list_output = rail.result(
                'get_department_group_details')
            department_uris = [item['departmenturi']
                               for item in department_list_output if item['departmenturi']]
            department_length = len(department.split('|'))
            level2_department_uri = ''
            level3_department_uri = ''
            level4_department_uri = ''
            level5_department_uri = ''
            level6_department_uri = ''
            if department_uris:
                if department_length >= 3:
                    level2_department = rail.smartjoin_by_delim(
                        department.split('|')[0:-1], '|')
                    level2_department_uri = rail.find_first_by_attr_and_get_attr(
                        department_list_output, 'fullpath', level2_department, 'departmenturi', '')
                if department_length >= 4:
                    level3_department = rail.smartjoin_by_delim(
                        department.split('|')[0:-2], '|')
                    level3_department_uri = rail.find_first_by_attr_and_get_attr(
                        department_list_output, 'fullpath', level3_department, 'departmenturi', '')
                if department_length >= 5:
                    level4_department = rail.smartjoin_by_delim(
                        department.split('|')[0:-3], '|')
                    level4_department_uri = rail.find_first_by_attr_and_get_attr(
                        department_list_output, 'fullpath', level4_department, 'departmenturi', '')
                if department_length >= 6:
                    level5_department = rail.smartjoin_by_delim(
                        department.split('|')[0:-4], '|')
                    level5_department_uri = rail.find_first_by_attr_and_get_attr(
                        department_list_output, 'fullpath', level5_department, 'departmenturi', '')
                if department_length == 7:
                    level6_department = rail.smartjoin_by_delim(
                        department.split('|')[0:-5], '|')
                    level6_department_uri = rail.find_first_by_attr_and_get_attr(
                        department_list_output, 'fullpath', level6_department, 'departmenturi', '')
            return {
                'level2_department_uri': level2_department_uri,
                'level3_department_uri': level3_department_uri,
                'level4_department_uri': level4_department_uri,
                'level5_department_uri': level5_department_uri,
                'level6_department_uri': level6_department_uri
            }
        get_levels_department_uri = rail.PythonOperator(
            task_id='get_levels_department_uri',
            python_callable=get_department_levels_uri
        )

        is_department_length_3 = rail.IfOperator(
            task_id='is_department_length_3',
            test="{{ dag_run.conf.department | split('|') | length == 3 }}",
            yes_task="is_level2_not_present_for_level3",
            no_task="is_department_length_4"
        )

        is_level2_not_present_for_level3 = rail.IfOperator(
            task_id='is_level2_not_present_for_level3',
            test="{{ result('get_levels_department_uri').level2_department_uri | is_falsy }}",
            yes_task="create_departmentgroup_level2_2",
            no_task="is_level2_present_for_level3"
        )

        create_departmentgroup_level2_2 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level2_2',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": dag_run.conf['parentdepartmenturi']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[1],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level3 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level3',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level2_2')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_level2_present_for_level3 = rail.IfOperator(
            task_id='is_level2_present_for_level3',
            test="{{ result('get_levels_department_uri').level2_department_uri | is_truthy }}",
            yes_task="create_departmentgroup_level3_2",
            no_task="is_department_length_4"
        )

        create_departmentgroup_level3_2 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level3_2',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('get_levels_department_uri')['level2_department_uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_department_length_4 = rail.IfOperator(
            task_id='is_department_length_4',
            test="{{ dag_run.conf.department | split('|') | length == 4 }}",
            yes_task="is_level2_present_for_level4",
            no_task="is_department_length_5"
        )

        is_level2_present_for_level4 = rail.IfOperator(
            task_id='is_level2_present_for_level4',
            test="{{ result('get_levels_department_uri').level2_department_uri | is_truthy }}",
            yes_task="create_departmentgroup_level4",
            no_task="is_level3_not_present_for_level4"
        )

        create_departmentgroup_level4 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level4',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('get_levels_department_uri')['level2_department_uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[3],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_level3_not_present_for_level4 = rail.IfOperator(
            task_id='is_level3_not_present_for_level4',
            test="{{ result('get_levels_department_uri').level3_department_uri | is_falsy }}",
            yes_task="create_departmentgroup_level2_3",
            no_task="is_level3_present_for_level4"
        )

        create_departmentgroup_level2_3 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level2_3',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": dag_run.conf['parentdepartmenturi']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[1],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level3_3 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level3_3',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level2_3')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level4_2 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level4_2',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level3_3')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_level3_present_for_level4 = rail.IfOperator(
            task_id='is_level3_present_for_level4',
            test="{{ result('get_levels_department_uri').level3_department_uri | is_truthy }}",
            yes_task="create_departmentgroup_level3_4",
            no_task="is_department_length_5"
        )

        create_departmentgroup_level3_4 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level3_4',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('get_levels_department_uri')['level3_department_uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level4_3 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level4_3',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level3_4')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_department_length_5 = rail.IfOperator(
            task_id='is_department_length_5',
            test="{{ dag_run.conf.department | split('|') | length == 5 }}",
            yes_task="is_level2_present_for_level5",
            no_task="is_department_length_6"
        )

        is_level2_present_for_level5 = rail.IfOperator(
            task_id='is_level2_present_for_level5',
            test="{{ result('get_levels_department_uri').level2_department_uri | is_truthy }}",
            yes_task="create_departmentgroup_level5",
            no_task="is_level3_present_for_level5"
        )

        create_departmentgroup_level5 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level5',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('get_levels_department_uri')['level2_department_uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[4],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_level3_present_for_level5 = rail.IfOperator(
            task_id='is_level3_present_for_level5',
            test="{{ result('get_levels_department_uri').level3_department_uri | is_truthy }}",
            yes_task="create_departmentgroup_level4_4",
            no_task="is_level4_not_present_for_level5"
        )

        create_departmentgroup_level4_4 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level4_4',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('get_levels_department_uri')['level3_department_uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[3],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level5_1 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level5_1',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level4_4')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[4],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_level4_not_present_for_level5 = rail.IfOperator(
            task_id='is_level4_not_present_for_level5',
            test="{{ result('get_levels_department_uri').level4_department_uri | is_falsy }}",
            yes_task="create_departmentgroup_level2_4",
            no_task="is_level4_present_for_level5"
        )

        create_departmentgroup_level2_4 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level2_4',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": dag_run.conf['parentdepartmenturi']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[1],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level3_5 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level3_5',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level2_4')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level4_5 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level4_5',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level3_5')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level5_2 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level5_2',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level4_5')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[4],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_level4_present_for_level5 = rail.IfOperator(
            task_id='is_level4_present_for_level5',
            test="{{ result('get_levels_department_uri').level4_department_uri | is_truthy }}",
            yes_task="create_departmentgroup_level3_6",
            no_task="is_department_length_6"
        )

        create_departmentgroup_level3_6 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level3_6',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('get_levels_department_uri')['level4_department_uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level4_6 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level4_6',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level3_6')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[3],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level5_3 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level5_3',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level4_6')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[4],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_department_length_6 = rail.IfOperator(
            task_id='is_department_length_6',
            test="{{ dag_run.conf.department | split('|') | length == 6 }}",
            yes_task="is_level2_present_for_level6",
            no_task="is_department_length_7"
        )

        is_level2_present_for_level6 = rail.IfOperator(
            task_id='is_level2_present_for_level6',
            test="{{ result('get_levels_department_uri').level2_department_uri | is_truthy }}",
            yes_task="create_departmentgroup_level6",
            no_task="is_level3_present_for_level6"
        )

        create_departmentgroup_level6 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level6',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('get_levels_department_uri')['level2_department_uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[5],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_level3_present_for_level6 = rail.IfOperator(
            task_id='is_level3_present_for_level6',
            test="{{ result('get_levels_department_uri').level3_department_uri | is_truthy }}",
            yes_task="create_departmentgroup_level5_4",
            no_task="is_level4_present_for_level6"
        )

        create_departmentgroup_level5_4 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level5_4',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('get_levels_department_uri')['level3_department_uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level6_2 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level6_2',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level5_4')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-1],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_level4_present_for_level6 = rail.IfOperator(
            task_id='is_level4_present_for_level6',
            test="{{ result('get_levels_department_uri').level4_department_uri | is_truthy }}",
            yes_task="create_departmentgroup_level4_7",
            no_task="is_level5_present_for_level6"
        )

        create_departmentgroup_level4_7 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level4_7',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('get_levels_department_uri')['level4_department_uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-3],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level5_5 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level5_5',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level4_7')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level6_3 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level6_3',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level5_5')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-1],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_level5_present_for_level6 = rail.IfOperator(
            task_id='is_level5_present_for_level6',
            test="{{ result('get_levels_department_uri').level5_department_uri | is_truthy }}",
            yes_task="create_departmentgroup_level3_7",
            no_task="is_level5_not_present_for_level6"
        )

        create_departmentgroup_level3_7 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level3_7',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('get_levels_department_uri')['level5_department_uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-4],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level4_8 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level4_8',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level3_7')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-3],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level5_6 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level5_6',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level4_8')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level6_4 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level6_4',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level5_6')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-1],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_level5_not_present_for_level6 = rail.IfOperator(
            task_id='is_level5_not_present_for_level6',
            test="{{ result('get_levels_department_uri').level5_department_uri | is_falsy }}",
            yes_task="create_departmentgroup_level2_5",
            no_task="is_department_length_7"
        )

        create_departmentgroup_level2_5 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level2_5',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": dag_run.conf['parentdepartmenturi']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[1],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level3_8 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level3_8',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level2_5')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-4],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level4_9 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level4_9',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level3_8')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-3],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level5_7 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level5_7',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level4_9')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level6_5 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level6_5',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level5_7')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-1],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_department_length_7 = rail.IfOperator(
            task_id='is_department_length_7',
            test="{{ dag_run.conf.department | split('|') | length == 7 }}",
            yes_task="is_level2_present_for_level7",
            no_task="dagrun_log_to_sumo"
        )

        is_level2_present_for_level7 = rail.IfOperator(
            task_id='is_level2_present_for_level7',
            test="{{ result('get_levels_department_uri').level2_department_uri | is_truthy }}",
            yes_task="create_departmentgroup_level7",
            no_task="is_level3_present_for_level7"
        )

        create_departmentgroup_level7 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level7',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('get_levels_department_uri')['level2_department_uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[6],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_level3_present_for_level7 = rail.IfOperator(
            task_id='is_level3_present_for_level7',
            test="{{ result('get_levels_department_uri').level3_department_uri | is_truthy }}",
            yes_task="create_departmentgroup_level6_6",
            no_task="is_level4_present_for_level7"
        )

        create_departmentgroup_level6_6 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level6_6',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('get_levels_department_uri')['level3_department_uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level7_2 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level7_2',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level6_6')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[6],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_level4_present_for_level7 = rail.IfOperator(
            task_id='is_level4_present_for_level7',
            test="{{ result('get_levels_department_uri').level4_department_uri | is_truthy }}",
            yes_task="create_departmentgroup_level5_8",
            no_task="is_level5_present_for_level7"
        )

        create_departmentgroup_level5_8 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level5_8',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('get_levels_department_uri')['level4_department_uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-3],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level6_7 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level6_7',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level5_8')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level7_3 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level7_3',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level6_7')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-1],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_level5_present_for_level7 = rail.IfOperator(
            task_id='is_level5_present_for_level7',
            test="{{ result('get_levels_department_uri').level5_department_uri | is_truthy }}",
            yes_task="create_departmentgroup_level4_10",
            no_task="is_level6_present_for_level7"
        )

        create_departmentgroup_level4_10 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level4_10',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('get_levels_department_uri')['level5_department_uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-4],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level5_9 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level5_9',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level4_10')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-3],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level6_8 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level6_8',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level5_9')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level7_4 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level7_4',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level6_8')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[-1],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_level6_present_for_level7 = rail.IfOperator(
            task_id='is_level6_present_for_level7',
            test="{{ result('get_levels_department_uri').level6_department_uri | is_truthy }}",
            yes_task="create_departmentgroup_level3_9",
            no_task="is_level6_not_present_for_level7"
        )

        create_departmentgroup_level3_9 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level3_9',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('get_levels_department_uri')['level6_department_uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level4_11 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level4_11',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level3_9')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[3],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level5_10 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level5_10',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level4_11')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[4],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level6_9 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level6_9',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level5_11')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[5],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level7_5 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level7_5',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level6_9')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[6],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        is_level6_not_present_for_level7 = rail.IfOperator(
            task_id='is_level6_not_present_for_level7',
            test="{{ result('get_levels_department_uri').level6_department_uri | is_falsy }}",
            yes_task="create_departmentgroup_level2_6",
            no_task="dagrun_log_to_sumo"
        )

        create_departmentgroup_level2_6 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level2_6',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": dag_run.conf['parentdepartmenturi']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[1],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level3_10 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level3_10',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level2_6')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[2],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level4_12 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level4_12',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level3_10')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[3],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level5_11 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level5_11',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level4_12')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[4],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level6_10 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level6_10',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level5_10')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[5],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_departmentgroup_level7_6 = rail.RepliconServiceOperator(
            task_id='create_departmentgroup_level7_6',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "parent": {
                        "uri": rail.result('create_departmentgroup_level6_10')['uri']
                    }
                },
                "modifications": {
                    "name": dag_run.conf['department'].split('|')[6],
                    "isEnabled": True
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> is_department_length_greater_7
        is_department_length_greater_7 >> rail.Label(
            'Yes') >> dagrun_log_to_sumo
        is_department_length_greater_7 >> rail.Label(
            'No') >> is_department_length_2
        is_department_length_2 >> rail.Label(
            'Yes') >> create_departmentgroup_level2 >> dagrun_log_to_sumo
        is_department_length_2 >> rail.Label(
            'No') >> get_department_group_details >> get_levels_department_uri >> is_department_length_3
        is_department_length_3 >> rail.Label(
            'Yes') >> is_level2_not_present_for_level3
        is_level2_not_present_for_level3 >> rail.Label(
            'Yes') >> create_departmentgroup_level2_2 >> create_departmentgroup_level3 >> dagrun_log_to_sumo
        is_level2_not_present_for_level3 >> rail.Label(
            'No') >> is_level2_present_for_level3
        is_level2_present_for_level3 >> rail.Label(
            'Yes') >> create_departmentgroup_level3_2 >> dagrun_log_to_sumo
        is_level2_present_for_level3 >> rail.Label(
            'No') >> is_department_length_4
        is_department_length_3 >> rail.Label(
            'No') >> is_department_length_4
        is_department_length_4 >> rail.Label(
            'Yes') >> is_level2_present_for_level4
        is_level2_present_for_level4 >> rail.Label(
            'Yes') >> create_departmentgroup_level4 >> dagrun_log_to_sumo
        is_level2_present_for_level4 >> rail.Label(
            'No') >> is_level3_not_present_for_level4
        is_level3_not_present_for_level4 >> rail.Label(
            'Yes') >> create_departmentgroup_level2_3 >> create_departmentgroup_level3_3 >> create_departmentgroup_level4_2 >> dagrun_log_to_sumo
        is_level3_not_present_for_level4 >> rail.Label(
            'No') >> is_level3_present_for_level4
        is_level3_present_for_level4 >> rail.Label(
            'Yes') >> create_departmentgroup_level3_4 >> create_departmentgroup_level4_3 >> dagrun_log_to_sumo
        is_level3_present_for_level4 >> rail.Label(
            'No') >> is_department_length_5
        is_department_length_4 >> rail.Label(
            'No') >> is_department_length_5
        is_department_length_5 >> rail.Label(
            'Yes') >> is_level2_present_for_level5
        is_level2_present_for_level5 >> rail.Label(
            'Yes') >> create_departmentgroup_level5 >> dagrun_log_to_sumo
        is_level2_present_for_level5 >> rail.Label(
            'No') >> is_level3_present_for_level5
        is_level3_present_for_level5 >> rail.Label(
            'Yes') >> create_departmentgroup_level4_4 >> create_departmentgroup_level5_1 >> dagrun_log_to_sumo
        is_level3_present_for_level5 >> rail.Label(
            'No') >> is_level4_not_present_for_level5
        is_level4_not_present_for_level5 >> rail.Label(
            'Yes') >> create_departmentgroup_level2_4 >> create_departmentgroup_level3_5 >> create_departmentgroup_level4_5 >> \
            create_departmentgroup_level5_2 >> dagrun_log_to_sumo
        is_level4_not_present_for_level5 >> rail.Label(
            'No') >> is_level4_present_for_level5
        is_level4_present_for_level5 >> rail.Label(
            'Yes') >> create_departmentgroup_level3_6 >> create_departmentgroup_level4_6 >> create_departmentgroup_level5_3 >> \
            dagrun_log_to_sumo
        is_level4_present_for_level5 >> rail.Label(
            'No') >> is_department_length_6
        is_department_length_5 >> rail.Label(
            'No') >> is_department_length_6
        is_department_length_6 >> rail.Label(
            'Yes') >> is_level2_present_for_level6
        is_level2_present_for_level6 >> rail.Label(
            'Yes') >> create_departmentgroup_level6 >> dagrun_log_to_sumo
        is_level2_present_for_level6 >> rail.Label(
            'No') >> is_level3_present_for_level6
        is_level3_present_for_level6 >> rail.Label(
            'Yes') >> create_departmentgroup_level5_4 >> create_departmentgroup_level6_2 >> dagrun_log_to_sumo
        is_level3_present_for_level6 >> rail.Label(
            'No') >> is_level4_present_for_level6
        is_level4_present_for_level6 >> rail.Label(
            'Yes') >> create_departmentgroup_level4_7 >> create_departmentgroup_level5_5 >> create_departmentgroup_level6_3 >> \
            dagrun_log_to_sumo
        is_level4_present_for_level6 >> rail.Label(
            'No') >> is_level5_present_for_level6
        is_level5_present_for_level6 >> rail.Label(
            'Yes') >> create_departmentgroup_level3_7 >> create_departmentgroup_level4_8 >> create_departmentgroup_level5_6 >> \
            create_departmentgroup_level6_4 >> dagrun_log_to_sumo
        is_level5_present_for_level6 >> rail.Label(
            'No') >> is_level5_not_present_for_level6
        is_level5_not_present_for_level6 >> rail.Label(
            'Yes') >> create_departmentgroup_level2_5 >> create_departmentgroup_level3_8 >> create_departmentgroup_level4_9 >> \
            create_departmentgroup_level5_7 >> create_departmentgroup_level6_5 >> dagrun_log_to_sumo
        is_level5_not_present_for_level6 >> rail.Label(
            'No') >> is_department_length_7
        is_department_length_6 >> rail.Label(
            'No') >> is_department_length_7
        is_department_length_7 >> rail.Label(
            'Yes') >> is_level2_present_for_level7
        is_level2_present_for_level7 >> rail.Label(
            'Yes') >> create_departmentgroup_level7 >> dagrun_log_to_sumo
        is_level2_present_for_level7 >> rail.Label(
            'No') >> is_level3_present_for_level7
        is_level3_present_for_level7 >> rail.Label(
            'Yes') >> create_departmentgroup_level6_6 >> create_departmentgroup_level7_2 >> dagrun_log_to_sumo
        is_level3_present_for_level7 >> rail.Label(
            'No') >> is_level4_present_for_level7
        is_level4_present_for_level7 >> rail.Label(
            'Yes') >> create_departmentgroup_level5_8 >> create_departmentgroup_level6_7 >> create_departmentgroup_level7_3 >> \
            dagrun_log_to_sumo
        is_level4_present_for_level7 >> rail.Label(
            'No') >> is_level5_present_for_level7
        is_level5_present_for_level7 >> rail.Label(
            'Yes') >> create_departmentgroup_level4_10 >> create_departmentgroup_level5_9 >> create_departmentgroup_level6_8 >> \
            create_departmentgroup_level7_4 >> dagrun_log_to_sumo
        is_level5_present_for_level7 >> rail.Label(
            'No') >> is_level6_present_for_level7
        is_level6_present_for_level7 >> rail.Label(
            'Yes') >> create_departmentgroup_level3_9 >> create_departmentgroup_level4_11 >> create_departmentgroup_level5_10 >> \
            create_departmentgroup_level6_9 >> create_departmentgroup_level7_5 >> dagrun_log_to_sumo
        is_level6_present_for_level7 >> rail.Label(
            'No') >> is_level6_not_present_for_level7
        is_level6_not_present_for_level7 >> rail.Label(
            'Yes') >> create_departmentgroup_level2_6 >> create_departmentgroup_level3_10 >> create_departmentgroup_level4_12 >> \
            create_departmentgroup_level5_11 >> create_departmentgroup_level6_10 >> create_departmentgroup_level7_6 >> dagrun_log_to_sumo
        is_level6_not_present_for_level7 >> rail.Label(
            'No') >> dagrun_log_to_sumo
        is_department_length_7 >> rail.Label(
            'No') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_dag)
