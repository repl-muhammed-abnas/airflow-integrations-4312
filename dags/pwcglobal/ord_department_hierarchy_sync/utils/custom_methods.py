import rail

null = None


def get_parenturi(departmentlist, parentname, parentfullpath):
    uri_list = list(filter(
        lambda x: x["name"] == parentname and x["fullpath"] == parentfullpath, departmentlist))
    return uri_list[0]['uri'] if uri_list else null


def get_status(departmentlist, parentname, parentfullpath):
    uri_list = list(filter(
        lambda x: x["name"] == parentname and x["fullpath"] == parentfullpath, departmentlist))
    return uri_list[0]['status'] if uri_list else null


def get_code(departmentlist, parentname, parentfullpath):
    code_list = list(filter(
        lambda x: x["name"] == parentname and x["fullpath"] == parentfullpath, departmentlist))
    return code_list[0]['code'] if code_list else null


def get_csv_rows_20(item):
    row_data = [
        item['NaturalKeyName'].replace("~", "/"),
        item['ShareNode1Name'],
        item['ShareNode2Name'],
        item['ShareNode3Name'],
        item['ShareNode4Name'],
        item['ShareNode5Name'],
        item['ShareNode6Name'],
        item['ShareNode6Code'],
        len(item['NaturalKeyName'].split("~")),
        item['ShareNode6Name']+item['ShareNode6Code'],
    ]
    return row_data


def get_csv_rows_22(item):
    fullpath = "PwC/"+item["ShareNode1Name"]+"/"+item["ShareNode2Name"]
    row_data = [
        item['NaturalKeyName'].replace("~", "/"),
        item['ShareNode1Name'],
        item['ShareNode2Name'],
        "PwC/"+item['ShareNode1Name']+"/"+item['ShareNode2Name'],
        get_parenturi(rail.result(
            'get_child_hierarchy_databasedon_level1name_16'), item['ShareNode2Name'], fullpath),
        get_status(rail.result(
            'get_child_hierarchy_databasedon_level1name_16'), item['ShareNode2Name'], fullpath)
    ]
    return row_data


def get_csv_rows_36(item):
    fullpath = "PwC/"+item["ShareNode1Name"]+"/" + \
        item["ShareNode2Name"]+"/"+item["ShareNode3Name"]
    row_data = [
        item['NaturalKeyName'].replace("~", "/"),
        item['ShareNode2Name'],
        "PwC/"+item['ShareNode1Name']+"/"+item['ShareNode2Name'],
        item['ShareNode3Name'],
        "PwC/"+item['ShareNode1Name']+"/" +
        item['ShareNode2Name']+"/"+item['ShareNode3Name'],
        get_parenturi(rail.result(
            'get_child_hierarchy_databasedon_level1uri_32'), item['ShareNode3Name'], fullpath),
        get_status(rail.result(
            'get_child_hierarchy_databasedon_level1uri_32'), item['ShareNode3Name'], fullpath)

    ]
    return row_data


def get_csv_rows_50(item):
    fullpath = "PwC/"+item["ShareNode1Name"]+"/"+item["ShareNode2Name"] + \
        "/"+item["ShareNode3Name"]+"/"+item["ShareNode4Name"]
    row_data = [
        item['NaturalKeyName'].replace("~", "/"),
        item['ShareNode3Name'],
        "PwC/"+item['ShareNode1Name']+"/" +
        item['ShareNode2Name']+"/"+item['ShareNode3Name'],
        item['ShareNode4Name'],
        "PwC/"+item['ShareNode1Name']+"/" +
        item['ShareNode2Name']+"/"+item['ShareNode3Name'] +
        "/"+item['ShareNode4Name'],
        get_parenturi(rail.result(
            'get_child_hierarchy_databasedon_level1uri_46'), item['ShareNode4Name'], fullpath),
        get_status(rail.result(
            'get_child_hierarchy_databasedon_level1uri_46'), item['ShareNode4Name'], fullpath)
    ]
    return row_data


def get_csv_rows_64(item):
    fullpath = "PwC/"+item["ShareNode1Name"]+"/"+item["ShareNode2Name"]+"/" + \
        item["ShareNode3Name"]+"/"+item["ShareNode4Name"] + \
        "/"+item["ShareNode5Name"]
    row_data = [
        item['NaturalKeyName'].replace("~", "/"),
        item['ShareNode4Name'],
        "PwC/"+item['ShareNode1Name']+"/"+item['ShareNode2Name'] +
        "/"+item['ShareNode3Name']+"/"+item['ShareNode4Name'],
        item['ShareNode5Name'],
        "PwC/"+item['ShareNode1Name']+"/" +
        item['ShareNode2Name']+"/"+item['ShareNode3Name']+"/" +
        item['ShareNode4Name']+"/"+item['ShareNode5Name'],
        get_parenturi(rail.result(
            'get_child_hierarchy_databasedon_level1uri_60'), item['ShareNode5Name'], fullpath),
        get_status(rail.result(
            'get_child_hierarchy_databasedon_level1uri_60'), item['ShareNode5Name'], fullpath)
    ]
    return row_data


def get_csv_rows_78(item):
    fullpath = "PwC/"+item["ShareNode1Name"]+"/"+item["ShareNode2Name"]+"/" + \
        item["ShareNode3Name"]+"/"+item["ShareNode4Name"] + \
        "/"+item["ShareNode5Name"]+"/" + \
        item['ShareNode6Name'] + " " + item['ShareNode6Code']
    row_data = [
        item['NaturalKeyName'].replace("~", "/"),
        item['ShareNode5Name'],
        "PwC/"+item['ShareNode1Name']+"/"+item['ShareNode2Name'] +
        "/"+item['ShareNode3Name']+"/" +
        item['ShareNode4Name']+"/"+item['ShareNode5Name'],
        item['ShareNode6Name']+" "+item['ShareNode6Code'],
        "PwC/"+item['ShareNode1Name']+"/" +
        item['ShareNode2Name']+"/"+item['ShareNode3Name']+"/" +
        item['ShareNode4Name']+"/"+item['ShareNode5Name'] +
        "/"+item['ShareNode6Name'] + " " + item['ShareNode6Code'],
        get_parenturi(rail.result(
            'get_child_hierarchy_databasedon_level1uri_74'), item['ShareNode6Name'] + " " + item['ShareNode6Code'], fullpath),
        item['ShareNode6Code'],
        get_code(rail.result(
            'get_child_hierarchy_databasedon_level1uri_74'), item['ShareNode6Name'] + " " + item['ShareNode6Code'], fullpath),
        get_status(rail.result(
            'get_child_hierarchy_databasedon_level1uri_74'), item['ShareNode6Name'] + " " + item['ShareNode6Code'], fullpath)
    ]
    return row_data
