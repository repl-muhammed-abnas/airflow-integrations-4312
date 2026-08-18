import rail
from pwcglobal.user_import_v4.utils import request_payload


def get_search_user_task():
    with rail.TaskGroup(group_id='search_user_task', prefix_group_id=False) as search_user_task:

        has_loaded_report_users = rail.IfOperator(
            task_id='has_loaded_report_users',
            test='{{ True if dag_run.conf.has_loaded_report_users else False}}',
            yes_task='is_user_exist',
            no_task='search_user_uri',
        )

        def map_list_data(resp):
            data = resp.json()['d']['rows']
            return next(map(lambda item: item['cells'][1]['uri'],
                        filter(lambda item:
                        item['cells'][0]['textValue'] ==
                        rail.get_current_context(
                        )['dag_run'].conf['loginname'],
                        data)), None)

        search_user_uri = rail.RepliconServiceOperator(
            task_id='search_user_uri',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_search_user_param(),
            response_filter=map_list_data
        )

        is_user_exist = rail.IfOperator(
            task_id='is_user_exist',
            test='{{ True if dag_run.conf.useruri or result("search_user_uri") else False   }}',
            yes_task='get_user_uri',
            no_task='search_user_by_empid_status_country',
        )

        def map_empid_status_list_data(resp):
            data = resp.json()['d']['rows']
            return list(filter(lambda item:
                               item['status'] and item['employeeid'] ==
                               request_payload.get_conf()['employeeid'],
                               map(lambda item: {
                                   'uri': item['cells'][0]['uri'],
                                   'firstname': item['cells'][0]['textValue'].split(" ")[0],
                                   'displaytext': item['cells'][0]['textValue'],
                                   'status': item['cells'][1]['boolValue'],
                                   'employeeid': item['cells'][2].get('textValue'),
                                   'location': item['cells'][3].get('textValue', None),
                               }, data)))

        search_user_by_empid_status_country = rail.RepliconServiceOperator(
            task_id='search_user_by_empid_status_country',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_search_user_by_empid_status_country_param,
            response_filter=map_empid_status_list_data
        )

        is_add_user_exist_by_country = rail.IfOperator(
            task_id='is_add_user_exist_by_country',
            test=lambda: len(list(filter(lambda item:
                                         item['location'] ==
                                         request_payload.get_conf()[
                                             'country'],
                                         rail.result('search_user_by_empid_status_country')))) == 1,
            yes_task='get_user_first_name',
            no_task='is_add_user_exist_by_firstname',
        )

        get_user_first_name = rail.RepliconServiceOperator(
            task_id='get_user_first_name',
            endpoint='/services/UserService1.svc/GetUserDetails',
            data=lambda: {
                "userUri": list(filter(lambda item:
                                       item['location'] ==
                                       request_payload.get_conf()[
                                           'country'],
                                       rail.result('search_user_by_empid_status_country')))[0]['uri']

            }
        )

        has_user_first_name_match = rail.IfOperator(
            task_id='has_user_first_name_match',
            test=lambda: request_payload.get_conf()['firstname'] == rail.result(
                'get_user_first_name')['firstName'],
            yes_task='process_update_user_by_country',
            no_task='get_user_uri',
        )

        is_add_user_exist_by_firstname = rail.IfOperator(
            task_id='is_add_user_exist_by_firstname',
            test=lambda: len(list(filter(lambda item:
                                         item['firstname'] ==
                                         request_payload.get_conf()['firstname'] and
                                         item['location'] ==
                                         request_payload.get_conf()['country'],
                                         rail.result('search_user_by_empid_status_country')))) == 1,
            yes_task='process_update_user_by_firstname',
            no_task='is_user_exist_by_multiple_records',
        )

        def do_is_user_exist_by_multiple_records():
            return len(list(filter(lambda item:
                                   item['firstname'] ==
                                   request_payload.get_conf()['firstname'] and
                                   item['location'] ==
                                   request_payload.get_conf()['country'],
                                   rail.result('search_user_by_empid_status_country') or []))) > 1

        is_user_exist_by_multiple_records = rail.IfOperator(
            task_id='is_user_exist_by_multiple_records',
            test=do_is_user_exist_by_multiple_records,
            yes_task='log_multiple_records_error',
            no_task='get_user_uri',
        )

        log_multiple_records_error = rail.WriteLogOperator(
            task_id='log_multiple_records_error',
            log="{{ result('create_log') }}",
            message='User not processed as multiple active user records are available based on employee id and first name in the given country',
            severity='Exception',
            properties={
                'userpartyid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}',
                'legalentityid': '{{dag_run.conf.legalentity}}',
                'status': 'Exception',
                'message': 'User not processed as multiple active user records are available based on employee id and first name in the given country',
                'action': 'Add',
            }
        )

        process_update_user_by_firstname = rail.PythonOperator(
            task_id='process_update_user_by_firstname',
            python_callable=lambda: list(filter(lambda item:
                                                item['firstname'] ==
                                                request_payload.get_conf()['firstname'] and
                                                item['location'] ==
                                                request_payload.get_conf()[
                                                    'country'],
                                                rail.result('search_user_by_empid_status_country')))[0]['uri']
        )

        process_update_user_by_country = rail.PythonOperator(
            task_id='process_update_user_by_country',
            python_callable=lambda: list(filter(lambda item:
                                                item['location'] ==
                                                request_payload.get_conf()[
                                                    'country'],
                                                rail.result('search_user_by_empid_status_country')))[0]['uri']
        )

        def do_get_user_uri():
            if rail.result('process_update_user_by_firstname') or rail.result('process_update_user_by_country'):
                rail.set_result(True, 'loginnameupdated')
            return request_payload.get_conf()['useruri'] or rail.result('search_user_uri') or \
                rail.result('process_update_user_by_firstname') or \
                rail.result('process_update_user_by_country')

        get_user_uri = rail.PythonOperator(
            task_id='get_user_uri',
            python_callable=do_get_user_uri
        )

        has_exception_error = rail.IfOperator(
            task_id='has_exception_error',
            test=do_is_user_exist_by_multiple_records,
            no_task='dummy_finish_search_user',
        )

        dummy_finish_search_user = rail.EmptyOperator(
            task_id='dummy_finish_search_user'
        )

        has_loaded_report_users >> rail.Label('yes') >> is_user_exist
        has_loaded_report_users >> rail.Label('no') >> search_user_uri
        search_user_uri >> is_user_exist

        is_user_exist >> rail.Label(
            'yes') >> get_user_uri
        is_user_exist >> rail.Label(
            'no') >> search_user_by_empid_status_country

        search_user_by_empid_status_country >> is_add_user_exist_by_country

        is_add_user_exist_by_country >> rail.Label(
            'Yes') >> get_user_first_name >> has_user_first_name_match
        is_add_user_exist_by_country >> rail.Label(
            'No') >> is_add_user_exist_by_firstname

        is_add_user_exist_by_firstname >> rail.Label(
            'Yes') >> process_update_user_by_firstname >> get_user_uri
        is_add_user_exist_by_firstname >> rail.Label(
            'No') >> is_user_exist_by_multiple_records

        has_user_first_name_match >> rail.Label(
            'Yes') >> process_update_user_by_country >> get_user_uri
        has_user_first_name_match >> rail.Label(
            'No') >> get_user_uri

        is_user_exist_by_multiple_records >> rail.Label(
            'Yes') >> log_multiple_records_error >> get_user_uri
        is_user_exist_by_multiple_records >> rail.Label(
            'No') >> get_user_uri

        get_user_uri >> has_exception_error >> rail.Label(
            'No') >> dummy_finish_search_user

    return search_user_task
