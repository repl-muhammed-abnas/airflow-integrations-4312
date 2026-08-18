import rail
from galaxyusopcoinc.workday_user_sync.user_import_v3.utils import request_payload


def get_search_user_task():
    with rail.TaskGroup(group_id='search_user_task', prefix_group_id=False):

        def map_list_data(resp):
            data = resp.json()['d']['rows']
            employee_data = list(filter(lambda item:
                                        item['cells'][2]['textValue'] ==
                                        rail.get_current_context(
                                        )['dag_run'].conf['employeeid'],
                                        data))
            rail.set_result(key="Employee_data", val=employee_data)
            return next(map(lambda item: item['cells'][0]['uri'],
                        employee_data), None)

        def map_empid_status_list_data(resp):
            data = resp.json()['d']['rows']
            return list(filter(lambda item:
                               item['status'] and item['loginname'] ==
                               request_payload.get_conf()['workemail'],
                               map(lambda item: {
                                   'uri': item['cells'][1]['uri'],
                                   'displaytext': item['cells'][1]['textValue'],
                                   'employeeid': item['cells'][0].get('textValue'),
                                   'loginname': item['cells'][2].get('textValue'),
                                   "status": item['cells'][3].get('boolValue')
                               }, data)))

        search_user_uri = rail.RepliconServiceOperator(
            task_id='search_user_uri',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_search_user_param(),
            response_filter=map_list_data
        )

        is_user_exist = rail.IfOperator(
            task_id='is_user_exist',
            test='{{ True if result("search_user_uri") else False }}',
            yes_task='get_user_uri',
            no_task='search_user_by_loginname_status',
        )

        search_user_by_loginname_status = rail.RepliconServiceOperator(
            task_id='search_user_by_loginname_status',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_search_user_by_loginname_status_param,
            response_filter=map_empid_status_list_data
        )

        def do_is_user_exist_by_multiple_records():
            return len(list(filter(lambda item:
                                   item['loginname'] ==
                                   request_payload.get_conf()['workemail'] or
                                   item['employeeid'] ==
                                   request_payload.get_conf()['employeeid'],
                                   rail.result('search_user_by_loginname_status') or []))) > 1

        is_user_exist_by_multiple_records = rail.IfOperator(
            task_id='is_user_exist_by_multiple_records',
            test=do_is_user_exist_by_multiple_records,
            yes_task='log_multiple_records_error',
            no_task='get_user_uri',
        )

        log_multiple_records_error = rail.WriteLogOperator(
            task_id='log_multiple_records_error',
            message='User not processed as multiple active user records are available based on employee id/loginname',
            severity='Exception',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}',
                'loginname': '{{dag_run.conf.workemail}}',
                'status': 'Exception',
                'action': 'Pre-Check',
                'message': 'User not processed as multiple active user records are available based on employee id/loginname',
                "allowed_for_supervisor_dag": "False",
                "user_uri": "",
                "managerid": "{{dag_run.conf.managerid}}",
                "is_add_and_errored": "False"
            }
        )

        def do_get_user_uri():
            if rail.result('search_user_uri'):
                return rail.result('search_user_uri')
            if rail.result('search_user_by_loginname_status'):
                return rail.result('search_user_by_loginname_status')[0]['uri']
            return rail.result('search_user_uri')

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

        search_user_uri >> is_user_exist
        #has_loaded_report_users >> rail.Label('no') >> search_user_uri

        is_user_exist >> rail.Label(
            'yes') >> get_user_uri
        is_user_exist >> rail.Label(
            'no') >> search_user_by_loginname_status

        search_user_by_loginname_status >> is_user_exist_by_multiple_records

        is_user_exist_by_multiple_records >> rail.Label(
            'Yes') >> log_multiple_records_error >> get_user_uri
        is_user_exist_by_multiple_records >> rail.Label(
            'No') >> get_user_uri

        get_user_uri >> has_exception_error >> rail.Label(
            'No') >> dummy_finish_search_user

    return search_user_uri, dummy_finish_search_user
