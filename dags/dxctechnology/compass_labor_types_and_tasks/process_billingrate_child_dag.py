from datetime import timedelta
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_labor_types_and_tasks_process_billing_rate_child_{config.sub_erp_name}_{config.instance}',
        description=f'DXC_Compass_Labour_Type_and_Task_Automation- Process task child {config.sub_erp_name}_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_dag_run_child_process,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
        )

        has_personnelnumber = rail.IfOperator(
            task_id='has_personnelnumber',
            test="{{ dag_run.conf.personnelnumber | is_truthy }}",
            yes_task="get_user_basedon_employee_id",
        )

        get_user_basedon_employee_id = rail.RepliconServiceOperator(
            task_id='get_user_basedon_employee_id',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": "{{ dag_run.conf.personnelnumber }}",
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        map_user_info = rail.PythonOperator(
            task_id='map_user_info',
            python_callable=lambda: list(
                    map(lambda row: {
                        "name": row['cells'][0].get('textValue'),
                        "uri": row['cells'][0].get('uri'),
                        "employeeid": row['cells'][1].get('textValue'),
                        "status": row['cells'][2].get('textValue'),
                    }, rail.result(get_user_basedon_employee_id.task_id)['rows']))
        )

        log_message_user_uri = rail.PythonOperator(
            task_id='log_message_user_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result(map_user_info.task_id), 'employeeid', rail.get_current_context(
                )['dag_run'].conf['personnelnumber'], 'uri')
        )

        has_no_user_uri = rail.IfOperator(
            task_id='has_no_user_uri',
            test="{{ result('log_message_user_uri') | is_falsy }}",
            yes_task="log_invalid_user",
            no_task="has_blanklabortype",
        )

        log_invalid_user = rail.WriteLogOperator(
            task_id='log_invalid_user',
            log="{{ dag_run.conf.log }}",
            message='Required user with employee id {{dag_run.conf.personnelnumber}} not available in Replicon',
            severity='Exception',
            properties={
                'wbs': '{{dag_run.conf.wbs}}',
                'task': '',
                'billingrate': '{{dag_run.conf.name}}',
                'message': 'Required user with employee id {{dag_run.conf.personnelnumber}} not available in Replicon',
                'status': 'Exception',
            }

        )

        has_blanklabortype = rail.IfOperator(
            task_id='has_blanklabortype',
            test="{{ dag_run.conf.name | is_falsy }}",
            yes_task="blank_billing_rate_query",
            no_task="conf_billing_rate_query",
        )

        blank_billing_rate_query = rail.QueryCollectionOperator(
            task_id='blank_billing_rate_query',
            query='''SELECT displayText,uri  FROM billingratesinreplicon WHERE displayText IN ("|Billable","|Non-Billable")''',
        )

        get_blank_billingrates = rail.PythonOperator(
            task_id='get_blank_billingrates',
            python_callable=lambda: list(
                map(
                    lambda x: {
                        "name": x['displayText'],
                        "billingrateuri": x['uri']
                    },
                    rail.load_all_records(
                        rail.result('blank_billing_rate_query'))
                ))
        )

        process_blank_billingrates = rail.TriggerDagRunForEachItemOperator(
            task_id='process_blank_billingrates',
            retries=0,
            items="{{ result('get_blank_billingrates') | to_json }}",
            trigger_dag_id=f'dxctechnology_compass_labor_types_and_tasks_assign_billingrate_child_{config.sub_erp_name}_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf={
                'log': "{{dag_run.conf.log}}",
                "projectname": "{{dag_run.conf.wbs}}",
                "billingratename": '{{item.name}}',
                "projecturi": '{{ dag_run.conf.project_info.project.uri }}',
                "billingrateuri": '{{item.billingrateuri}}',
                "useruri": "{{ result('log_message_user_uri') }}",
                "day": "{{ current_time('%d') }}",
                "month": "{{ current_time('%m') }}",
                "year": "{{ current_time('%Y') }}",
                "default": "{{ dag_run.conf.billabledefault }}",
                "labortypepresent": "{{'Yes' if dag_run.conf.name | is_truthy else 'No'}}",
            }
        )

        wait_for_process_blank_billingrates = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_blank_billingrates',
            dag_runs='{{ result("process_blank_billingrates") }}',
            execution_timeout=timedelta(days=14),
        )

        conf_billing_rate_query = rail.QueryCollectionOperator(
            task_id='conf_billing_rate_query',
            # pylint: disable=line-too-long
            query='''SELECT displayText,uri  FROM billingratesinreplicon WHERE displayText IN  ("{{ dag_run.conf.name }}|Billable","{{ dag_run.conf.name }}|Non-Billable")''',
        )

        process_billing_rate = rail.TriggerDagRunForEachItemOperator(
            task_id='process_billing_rate',
            retries=0,
            items=lambda: list(
                map(
                    lambda x: {
                        "name": x['displayText'],
                        "uri": x['uri']
                    },
                    rail.load_all_records(
                        rail.result('conf_billing_rate_query'))
                )),
            trigger_dag_id=f'dxctechnology_compass_labor_types_and_tasks_assign_billingrate_child_{config.sub_erp_name}_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf={
                'log': "{{dag_run.conf.log}}",
                "projectname": "{{dag_run.conf.wbs}}",
                "billingratename": '{{item.name}}',
                "projecturi": '{{ dag_run.conf.project_info.project.uri }}',
                "billingrateuri": "{{item.uri}}",
                "useruri": "{{ result('log_message_user_uri') }}",
                "day": "{{ current_time('%d') }}",
                "month": "{{ current_time('%m') }}",
                "year": "{{ current_time('%Y') }}",
                "default": "{{ dag_run.conf.billabledefault}}",
                "labortypepresent": "{{'Yes' if dag_run.conf.name | is_truthy else 'No'}}",
            }
        )

        wait_for_process_billing_rate = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_billing_rate',
            dag_runs='{{ result("process_billing_rate") }}',
            execution_timeout=timedelta(days=14),
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        has_personnelnumber >> rail.Label('yes') >> get_user_basedon_employee_id >> map_user_info >> \
            log_message_user_uri >> has_no_user_uri
        has_no_user_uri >> rail.Label('yes') >> log_invalid_user >> log_to_sumo
        has_no_user_uri >> rail.Label('no') >> has_blanklabortype
        has_blanklabortype >> rail.Label(
            'yes') >> blank_billing_rate_query >> get_blank_billingrates >> process_blank_billingrates >> \
            wait_for_process_blank_billingrates >> log_to_sumo
        has_blanklabortype >> rail.Label(
            'no') >> conf_billing_rate_query >> process_billing_rate >> wait_for_process_billing_rate >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
