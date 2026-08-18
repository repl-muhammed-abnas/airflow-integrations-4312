import rail

from dxctechnology.compass_attributes_1_and_2.utils import custom_methods

null = None


def create_attribute_2_create_task_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_attribute_2_create_task_child_{config.dag_id_postfix}',
        description=f'DXC_Compass_Attribute 2 Child - Create Task {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        def check_is_level_2(dag_run):
            return dag_run.conf['level'] == "2"

        check_level_2 = rail.IfOperator(
            task_id='check_level_2',
            test=check_is_level_2,
            yes_task='is_iwo_wbs_projecturi_not_present',
            no_task='log_invalid_attribute_number'
        )

        is_iwo_wbs_projecturi_not_present = rail.IfOperator(
            task_id='is_iwo_wbs_projecturi_not_present',
            test='{{ dag_run.conf.iwowbsprojecturi | is_falsy }}',
            yes_task='put_task_from_wbs',
            no_task='finish'
        )

        def get_put_task_data(dag_run):
            start_date = custom_methods.get_lower_date(
                dag_run.conf['parenttaskstartdate'], dag_run.conf['startdate'], '%d/%m/%Y', '%d/%m/%Y')
            end_date = custom_methods.get_lower_date(
                dag_run.conf['parenttaskendate'], dag_run.conf['enddate'], '%d/%m/%Y', '%Y-%m-%d')
            return {
                "project": {"uri": dag_run.conf['projecturi']},
                "task": {
                    "target": {
                        "name": dag_run.conf['name'],
                        "parent": {"uri": dag_run.conf['parenttaskuri']}
                    },
                    "name": dag_run.conf['name'],
                    "code": dag_run.conf['description'],
                    "timeEntryDateRange": {
                        "startDate": {'year': start_date['year'], 'month': start_date['month'], 'day': start_date['day']} if start_date else null,
                        "endDate": {'year': end_date['year'], 'month': end_date['month'], 'day': end_date['day']}
                    },
                    "customFieldValues": [
                        {
                            "customField": {"uri": dag_run.conf['tasktypeuri']},
                            "dropDownOption": {"uri": dag_run.conf['tasktypeoptionuri']},
                        }
                    ],
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
                    "percentCompleted": 0,
                    "isTimeEntryAllowed": True,
                    "isClosed": False,
                    "assignedResources": [{'uri': user['uri']} for user in dag_run.conf['userlist']],
                }
            }

        put_task_from_wbs = rail.RepliconServiceOperator(
            task_id='put_task_from_wbs',
            endpoint='/services/ProjectService1.svc/PutTask',
            data=get_put_task_data
        )

        log_attribute_2_wbs_create = rail.WriteLogOperator(
            task_id='log_attribute_2_wbs_create',
            message='Attribute added successfully',
            properties={
                'wbs': '{{ dag_run.conf.projectname }}',
                'attributename': '{{ dag_run.conf.name }}',
                'attributenumber': '{{ dag_run.conf.level }}',
                'action': 'add',
                'status': 'Success',
                'details': 'Attribute added successfully',
                'recordcount': ''
            }
        )

        log_invalid_attribute_number = rail.WriteLogOperator(
            task_id='log_invalid_attribute_number',
            message='Invalid Attribute number',
            properties={
                'wbs': '{{ dag_run.conf.projectname }}',
                'attributename': '{{ dag_run.conf.name }}',
                'attributenumber': '{{ dag_run.conf.level }}',
                'action': 'add',
                'status': 'Exception',
                'details': 'Invalid Attribute number',
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'wbs': '{{ dag_run.conf.projectname }}',
                'attributename': '{{ dag_run.conf.name }}',
                'attributenumber': '{{ dag_run.conf.level }}',
                'action': 'add',
                'status': 'Error',
                # pylint: disable=line-too-long
                'details': '{{ get_error_message() }}',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'wbs ': '{{ dag_run.conf.projectname }}',
                'attribute': '{{ dag_run.conf.name  }}',
                'level': '{{ dag_run.conf.level  }}',
                'enddate': '{{ dag_run.conf.enddate  }}',
                'usercount': '{{ dag_run.conf.userlist | length }}',
                'iwousercount': '{{ dag_run.conf.iwouserlist | length }}',
                'details': '{{ "Attribute added successfully" if get_task_state("put_task_from_wbs") == "success" else "Attribute addition failed" }}',
                'filename': '{{ dag_run.conf.filename }}'
            }
        )

        check_level_2 >> rail.Label(
            'Yes') >> is_iwo_wbs_projecturi_not_present
        check_level_2 >> rail.Label(
            'No') >> log_invalid_attribute_number >> finish

        is_iwo_wbs_projecturi_not_present >> rail.Label(
            'Yes') >> put_task_from_wbs >> log_attribute_2_wbs_create >> finish
        is_iwo_wbs_projecturi_not_present >> rail.Label(
            'No') >> finish

        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_attribute_2_create_task_child_dag)
