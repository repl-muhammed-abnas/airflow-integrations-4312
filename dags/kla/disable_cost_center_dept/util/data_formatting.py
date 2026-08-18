import json
def get_final_dept_list(replicon_enabled_dept_raw_data, kla_inactive_dept_raw_data):
    replicon_enabled_dept_data = json.loads(replicon_enabled_dept_raw_data)
    kla_inactive_dept_data = json.loads(kla_inactive_dept_raw_data)
    final_dept_list = []
    for enabled_replicon_dept in replicon_enabled_dept_data:
        for inactive_dept in kla_inactive_dept_data:
            if f'{inactive_dept["NAME"]}-{inactive_dept["DEPTID"]}' == enabled_replicon_dept["displayText"]:
                final_dept_list.append(enabled_replicon_dept)
                break
    return final_dept_list

def get_disabled_cost_center_list(disabled_raw_costcenter_data):
    data = json.loads(disabled_raw_costcenter_data)
    disabled_costcenter_names = [i["REPLACE_LTRIM_REPLACE__Disabled_Cost_Center_COST_CENTER__0_____________0__"]\
        for i in data if "REPLACE_LTRIM_REPLACE__Disabled_Cost_Center_COST_CENTER__0_____________0__" in i]
    return disabled_costcenter_names

def get_final_costcenter_disable_list(replicon_enabled_costcenter_raw_data, kla_disable_costcenter_raw_data):
    replicon_enabled_costcenter_data = json.loads(replicon_enabled_costcenter_raw_data)
    kla_disabled_costcenter_data = json.loads(kla_disable_costcenter_raw_data)
    final_costcenter_list = []
    for enabled_replicon_dept in replicon_enabled_costcenter_data:
        if enabled_replicon_dept["displayText"] in kla_disabled_costcenter_data:
            final_costcenter_list.append(enabled_replicon_dept)
    return final_costcenter_list
