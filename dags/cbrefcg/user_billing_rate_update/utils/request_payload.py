from datetime import datetime
import rail

def get_child_conf():
    user_data = rail.result("for_each_user_data")
    project_data = rail.load_all_records(rail.result("query_final_project_data"))

    final_project_data= list(map(lambda item:{
        'projectname': item['projectname'],
        'userloginname': user_data['LoginName'],
        'projecturi': item['projecturi'],
        'useruri': item['useruri'],
        'currentbillingrate': user_data['UserDefaultBillingRate'].split(user_data['currency'])[-1],
        'oldbillingrate': item['billingrateamount'].split(user_data['currency'])[-1],
        'currencyuri': item['currencyuri']
    },project_data))

    return {
        'projectdata': final_project_data
    }

def get_users_billing_rate_payload(dag_run):
    return {
            "projectUri": dag_run.conf['projecturi'],
            "userUri": dag_run.conf['useruri'],
            "effectiveDate": {
                "year": datetime.now().strftime('%Y'),
                "month": datetime.now().strftime('%m'),
                "day": datetime.now().strftime('%d')
            },
            "rate": {
                "amount": dag_run.conf['currentbillingrate'].replace(",",""),
                "currencyUri": dag_run.conf['currencyuri']
        }
    }
