import uuid
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'mk_default_shift_assignment_per_user_v1_0_{config.instance}',
        description=f'MK_Default shift assignment_per user V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
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
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='result_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='result_data',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        result_data = rail.PythonOperator(
            task_id='result_data',
            python_callable=lambda dag_run: rail.load_all_records(
                dag_run.conf['data'])
        )

        compose_csv_with_headers = rail.WriteCSVFileOperator(
            task_id='compose_csv_with_headers',
            source="{{ result('result_data') | to_json }}",
            header=[
                'loginname',
                'entrydate',
                'starttime',
                'endtime'
            ],
            row=[
                "{{ item.loginname }}",
                "{{ item.entrydate }}",
                "{{ item.starttime }}",
                "{{ item.endtime }}"
            ]
        )

        create_collection_create_list_from_csv_3 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_3',
            source="{{ result('compose_csv_with_headers') }}",
            name="userdata",
            columns={
                'loginname': 'loginname',
                'entrydate': 'entrydate',
                'starttime': 'starttime',
                'endtime': 'endtime'
            }
        )

        query_list_getalldata_4 = rail.QueryCollectionOperator(
            task_id='query_list_getalldata_4',
            query="""SELECT * FROM userdata"""
        )

        query_list_min_date_5 = rail.QueryCollectionOperator(
            task_id='query_list_min_date_5',
            query="""SELECT MIN(entrydate) FROM userdata""",
            mode='single-row'
        )

        def get_replicon_date(date_str, date_format='%Y-%m-%d'):
            if not date_str:
                return null

            try:
                date = datetime.strptime(date_str, date_format)
                return {
                    'year': date.year,
                    'month': date.month,
                    'day': date.day
                }
            except:  # pylint: disable=bare-except
                return null

        def get_split_date(min_max_date_result, min_date=True):
            min_max_date = rail.result(min_max_date_result)
            dates = null
            if min_date:
                dates = min_max_date['MIN(entrydate)']
            else:
                dates = min_max_date['MAX(entrydate)']
            return get_replicon_date(dates, '%d/%m/%Y')

        date_split_min_6 = rail.PythonOperator(
            task_id='date_split_min_6',
            python_callable=get_split_date,
            op_args=['query_list_min_date_5']
        )

        query_list_max_date_7 = rail.QueryCollectionOperator(
            task_id='query_list_max_date_7',
            query="""SELECT MAX(entrydate) FROM userdata""",
            mode='single-row'
        )

        date_split_max_8 = rail.PythonOperator(
            task_id='date_split_max_8',
            python_callable=get_split_date,
            op_args=['query_list_max_date_7', False]
        )

        def get_shift_summary_response(response):
            data = response.json()['d']

            def get_date(date):
                return str(date['day']) + "/" + str(date['month']) + "/" + str(date['year'])

            def is_todelete(date):
                alldata = rail.load_all_records(
                    rail.result("query_list_getalldata_4"))
                entrydates = []
                for entrydate in alldata:
                    if datetime.strptime(date, "%d/%m/%Y") == datetime.strptime(entrydate['entrydate'], "%d/%m/%Y"):
                        entrydates.append(entrydate)
                return entrydates

            return list(map(lambda x: {
                "date": get_date(x['date']),
                "week": datetime.strptime(get_date(x['date']), "%d/%m/%Y").isocalendar()[1] + 1 if datetime.strptime(get_date(x['date']), "%d/%m/%Y").weekday() == 0 else datetime.strptime(get_date(x['date']), "%d/%m/%Y").isocalendar()[1],
                "shift": x['shift']['displayText'],
                "assignmenturi": x['assignmentUri'],
                "todelete": "Yes" if is_todelete(get_date(x['date'])) else "No"
            }, data))

        get_shift_schedule_summary_foruser_10 = rail.RepliconServiceOperator(
            task_id='get_shift_schedule_summary_foruser_10',
            endpoint="/services/ShiftAssignmentService1.svc/GetShiftScheduleSummary",
            data=lambda dag_run: {
                "userSearch": {
                    "includeShiftAssignmentsWithNoUser": "false",
                    "specificUserUris": [
                        dag_run.conf['useruri']
                    ]
                },
                "shiftSearch": null,
                "objectExtensionFieldSearches": [],
                "dateRange": {
                    "startDate": rail.result("date_split_min_6"),
                    "endDate": rail.result("date_split_max_8"),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
            response_filter=get_shift_summary_response
        )

        if_output_shiftlistoutput_greater_than_0_14 = rail.IfOperator(
            task_id='if_output_shiftlistoutput_greater_than_0_14',
            test='''{{ result('get_shift_schedule_summary_foruser_10') | length > 0 }}''',
            yes_task="log_shiftsto_delete_15",
            no_task="declare_list_17"
        )

        log_shiftsto_delete_15 = rail.PythonOperator(
            task_id='log_shiftsto_delete_15',
            python_callable=lambda: [shift['assignmenturi'] for shift in rail.result(
                'get_shift_schedule_summary_foruser_10') if shift['todelete'] == "Yes"]
        )

        bulk_delete_foruser_16 = rail.RepliconServiceOperator(
            task_id='bulk_delete_foruser_16',
            endpoint="/services/ShiftAssignmentService1.svc/BulkDelete",
            data=lambda: {
                "shiftAssignmentUris": rail.result('log_shiftsto_delete_15')
            }
        )

        declare_list_17 = rail.EmptyOperator(
            task_id='declare_list_17'
        )

        if_request_country_equals_to_spain_18 = rail.IfOperator(
            task_id='if_request_country_equals_to_spain_18',
            test='''{{ dag_run.conf.country == 'Spain' }}''',
            yes_task="insert_to_list_19",
            no_task="if_request_country_equals_to_unitedkingdom_20"
        )

        def get_spain_shift_list():
            data = rail.load_all_records(
                rail.result('query_list_getalldata_4'))
            user_uri = rail.get_current_context()['dag_run'].conf['useruri']

            return list(map(lambda item: {
                "date": get_replicon_date(item['entrydate'], '%d/%m/%Y'),
                "target": {
                        "uri": null
                        },
                "shift": {
                    "uri": null,
                    "name": "Shift"
                },
                "user": {
                    "uri": user_uri,
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "startTime": {
                    "hour": int(item['starttime'].split(":")[0]),
                    "minute": int(item['starttime'].rsplit(':', 1)[-1]),
                    "dayOffset": 0
                },
                "endTime": {
                    "hour": int(item['endtime'].split(":")[0]),
                    "minute": int(item['endtime'].rsplit(':', 1)[-1]),
                    "dayOffset": 0
                },
                "publishState": "urn:replicon:shift-assignment-publish-state:published",
                "note": "Published by shift automation"

            }, data)) if data else []

        insert_to_list_19 = rail.PythonOperator(
            task_id='insert_to_list_19',
            python_callable=get_spain_shift_list
        )

        if_request_country_equals_to_unitedkingdom_20 = rail.IfOperator(
            task_id='if_request_country_equals_to_unitedkingdom_20',
            test='''{{ dag_run.conf.country == 'United Kingdom' }}''',
            yes_task="insert_to_list_21",
            no_task="if_request_country_equals_to_austria_22"
        )

        def get_uk_shift_list():
            data = rail.load_all_records(
                rail.result('query_list_getalldata_4'))
            user_uri = rail.get_current_context()['dag_run'].conf['useruri']

            return list(map(lambda item: {
                "date": get_replicon_date(item['entrydate'], '%d/%m/%Y'),
                "target": {
                        "uri": null
                        },
                "shift": {
                    "uri": null,
                    "name": "Shift w/1 hr break" if (int(item['endtime'].split(":")[0]) + int(item['endtime'].rsplit(':', 1)[-1]) / 60) - (int(item['starttime'].split(":")[0]) + int(item['starttime'].rsplit(':', 1)[-1]) / 60) > 5.99 else "Shift"
                },
                "user": {
                    "uri": user_uri,
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "startTime": {
                    "hour": int(item['starttime'].split(":")[0]),
                    "minute": int(item['starttime'].rsplit(':', 1)[-1]),
                    "dayOffset": 0
                },
                "endTime": {
                    "hour": int(item['endtime'].split(":")[0]),
                    "minute": int(item['endtime'].rsplit(':', 1)[-1]),
                    "dayOffset": 0
                },
                "publishState": "urn:replicon:shift-assignment-publish-state:published",
                "note": "Published by shift automation"
            }, data)) if data else []

        insert_to_list_21 = rail.PythonOperator(
            task_id='insert_to_list_21',
            python_callable=get_uk_shift_list
        )

        if_request_country_equals_to_austria_22 = rail.IfOperator(
            task_id='if_request_country_equals_to_austria_22',
            test='''{{ dag_run.conf.country == 'Austria' }}''',
            yes_task="insert_to_list_23",
            no_task="invoke_custom_ruby_code_24"
        )

        def get_austria_shift_list():
            data = rail.load_all_records(
                rail.result('query_list_getalldata_4'))
            user_uri = rail.get_current_context()['dag_run'].conf['useruri']

            return list(map(lambda item: {
                "date": get_replicon_date(item['entrydate'], '%d/%m/%Y'),
                "target": {
                        "uri": null
                        },
                "shift": {
                    "uri": null,
                    "name": "Shift w/1 hr break" if (int(item['endtime'].split(":")[0]) + int(item['endtime'].rsplit(':', 1)[-1]) / 60) - (int(item['starttime'].split(":")[0]) + int(item['starttime'].rsplit(':', 1)[-1]) / 60) > 7.99 else "Shift" if (int(item['endtime'].split(":")[0]) + int(item['endtime'].rsplit(':', 1)[-1]) / 60) - (int(item['starttime'].split(":")[0]) + int(item['starttime'].rsplit(':', 1)[-1]) / 60) < 4 else "Shift w/30 mins break"
                },
                "user": {
                    "uri": user_uri,
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "startTime": {
                    "hour": int(item['starttime'].split(":")[0]),
                    "minute": int(item['starttime'].rsplit(':', 1)[-1]),
                    "dayOffset": 0
                },
                "endTime": {
                    "hour": int(item['endtime'].split(":")[0]),
                    "minute": int(item['endtime'].rsplit(':', 1)[-1]),
                    "dayOffset": 0
                },
                "publishState": "urn:replicon:shift-assignment-publish-state:published",
                "note": "Published by shift automation"
            }, data)) if data else []

        insert_to_list_23 = rail.PythonOperator(
            task_id='insert_to_list_23',
            python_callable=get_austria_shift_list
        )

        def get_shift_final_list():
            shift_final_list = []
            if rail.result('insert_to_list_19'):
                shift_final_list.extend(rail.result('insert_to_list_19'))
            if rail.result('insert_to_list_21'):
                shift_final_list.extend(rail.result('insert_to_list_21'))
            if rail.result('insert_to_list_23'):
                shift_final_list.extend(rail.result('insert_to_list_23'))
            return shift_final_list

        invoke_custom_ruby_code_24 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_24',
            python_callable=get_shift_final_list
        )

        if_date_year_blank_25 = rail.IfOperator(
            task_id='if_date_year_blank_25',
            test='''{{ result('invoke_custom_ruby_code_24')[0].date.year | is_falsy }}''',
            yes_task="catch_and_log_errors",
            no_task="bulk_put_shift_assignments_foruser_29"
        )

        bulk_put_shift_assignments_foruser_29 = rail.RepliconServiceOperator(
            task_id='bulk_put_shift_assignments_foruser_29',
            endpoint="/services/ShiftAssignmentService1.svc/BulkPutShiftAssignments",
            data=lambda: {
                "assignments": rail.result('invoke_custom_ruby_code_24'),
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.logger}}',
            trigger_rule='one_failed',
            message="na",
            severity="User {{ dag_run.conf.loginname }} unable to process ",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Unable to process ",
                "reason": "{{ get_error_message() }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> result_data >> compose_csv_with_headers
        compose_csv_with_headers >> create_collection_create_list_from_csv_3 >> query_list_getalldata_4 \
            >> query_list_min_date_5 >> date_split_min_6 >> query_list_max_date_7 >> date_split_max_8 >> get_shift_schedule_summary_foruser_10\
            >> if_output_shiftlistoutput_greater_than_0_14
        if_output_shiftlistoutput_greater_than_0_14 >> rail.Label(
            'Yes') >> log_shiftsto_delete_15 >> bulk_delete_foruser_16 >> declare_list_17
        if_output_shiftlistoutput_greater_than_0_14 >> rail.Label(
            'No') >> declare_list_17 >> if_request_country_equals_to_spain_18
        if_request_country_equals_to_spain_18 >> rail.Label(
            'Yes') >> insert_to_list_19 >> if_request_country_equals_to_unitedkingdom_20
        if_request_country_equals_to_spain_18 >> rail.Label(
            'No') >> if_request_country_equals_to_unitedkingdom_20
        if_request_country_equals_to_unitedkingdom_20 >> rail.Label(
            'Yes') >> insert_to_list_21 >> if_request_country_equals_to_austria_22
        if_request_country_equals_to_unitedkingdom_20 >> rail.Label(
            'No') >> if_request_country_equals_to_austria_22
        if_request_country_equals_to_austria_22 >> rail.Label(
            'Yes') >> insert_to_list_23 >> invoke_custom_ruby_code_24
        if_request_country_equals_to_austria_22 >> rail.Label(
            'No') >> invoke_custom_ruby_code_24 >> if_date_year_blank_25
        if_date_year_blank_25 >> rail.Label(
            'Yes') >> catch_and_log_errors
        if_date_year_blank_25 >> rail.Label(
            'No') >> bulk_put_shift_assignments_foruser_29 >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
