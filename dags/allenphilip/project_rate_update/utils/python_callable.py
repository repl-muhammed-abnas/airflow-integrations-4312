from datetime import datetime
import rail
null = None


def get_current_date():
    return datetime.now().strftime("%Y-%m-%eT%H:%M%S.%f")


def get_date():
    time_now = datetime.now()
    return {'day': time_now.strftime("%e"),
            'month': time_now.strftime("%m"),
            'year': time_now.strftime("%Y")
            }


def get_useruri():
    results = rail.load_all_records(rail.result('get_success_logs'))
    user_uri = []
    user_uri = [data['properties']['useruri']
                for data in results if data['properties']['useruri'] not in user_uri]
    return user_uri


def get_loginname():
    results = rail.load_all_records(rail.result('get_success_logs'))
    login_name = []
    login_name = [value['properties']['loginname']
                  for value in results if value['properties']['loginname'] not in login_name]
    return rail.smartjoin_by_delim(login_name, '|')


def get_task_uri(payload):

    return bool(payload and payload[0] and payload[0]['task'] and payload[0]['task']['uri'])
