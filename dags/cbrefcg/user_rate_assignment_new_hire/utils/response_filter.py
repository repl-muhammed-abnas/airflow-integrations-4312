import rail

def add_user_group_details():
    data = rail.result('get_effective_user_group_membership')
    user_data = rail.result('for_each_entries')
    return {
            "userloginname": user_data['loginname'],
            "useruri": user_data['useruri'],
            "locationname": data['locations'][0]['location']['location']['displayText'] if data['locations'] else None,
            "locationuri": data['locations'][0]['location']['location']['uri'] if data['locations'] else None,
            "costcentrename": data['costCenters'][0]['costCenter']['costCenter']['displayText'] if data['costCenters'] else None,
            "costcentereuri": data['costCenters'][0]['costCenter']['costCenter']['uri'] if data['costCenters'] else None,
            "departmentgroupname": data['departments'][0]['department']['department']['displayText'] if data['departments'] else None,
            "departmentgroupuri": data['departments'][0]['department']['department']['uri'] if data['departments'] else None,
            "employeetypegroupname": data['employeeTypes'][0]['employeeType']['employeeType']['displayText'] if data['employeeTypes'] else None,
            "employeetypegroupuri": data['employeeTypes'][0]['employeeType']['employeeType']['uri'] if data['employeeTypes'] else None,
            "servicecentername": data['serviceCenters'][0]['serviceCenter']['serviceCenter']['displayText'] if data['serviceCenters'] else None,
            "servicecenteruri": data['serviceCenters'][0]['serviceCenter']['serviceCenter']['uri'] if data['serviceCenters'] else None
        }

def child_dag_conf():
    user_data = rail.result('foreach_user_group_details')
    return {
            "username": user_data['userloginname'],
            "useruri": user_data['useruri'],
            "locationname": user_data['locationname'],
            "locationuri": user_data['locationuri'],
            "departmentgroupname": user_data['departmentgroupname'],
            "costcentrename": user_data['costcentrename'],
            "servicecentername": user_data['servicecentername'],
            "employeetypegroupname": user_data['employeetypegroupname'],
            "projects": rail.load_all_records(rail.result("query_projects_by_users_location"))
        }
