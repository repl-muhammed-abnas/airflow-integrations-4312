import rail

null = None


def get_unique_parents():
    pass


def check_for_required_permissions(dag_run):
    permissionsets = []
    policydataaccessscopes = []
    project_management_policy_uri = 'urn:replicon:policy:project-management'
    user_policy_uri = 'urn:replicon:policy:user'
    current_user_permissions = rail.result(
        'get_permission_sets_for_project_manager')
    manager_connect_permission_set_uri = dag_run.conf['managerconnecturi']
    permission_set_project_manager = rail.find_first_by_attr_and_get_attr(
        current_user_permissions,"policyUri", project_management_policy_uri, 'permissionSet.uri')

    if not permission_set_project_manager:
        permissionsets.append(dag_run.conf['limitedwbsmanageruri'])

    permission_set_user = rail.find_first_by_attr_and_get_attr(current_user_permissions, "policyUri", user_policy_uri, 'permissionSet.uri')

    if permission_set_user not in [dag_run.conf['manageruri'], manager_connect_permission_set_uri]:
        permissionsets.append(dag_run.conf['manageruri'])

    if permission_set_user  != manager_connect_permission_set_uri:
        policy_data_access_scope = {
            "policyUri": "urn:replicon:policy:user",
            "employeeTypeGroups": list(
                map(
                    lambda x: {
                        'employeeTypeGroup': {'uri': x['uri']}
                    },
                    dag_run.conf['employeetyperestrictiongroup']))
        }
        policydataaccessscopes.append(policy_data_access_scope)


    return {'policydataaccessscopes': policydataaccessscopes,
            'permissionSets': permissionsets}


def get_all_oef_payload(dag_run):
    # pylint: disable=too-many-statements
    oefs = []
    current_parent_oef_values = rail.result('get_parent_project_details')[
        'extensionFieldValues']
    current_child_oef_values =rail.result('get_child_project_details')['extensionFieldValues']
    diwo_flag = False

    def add_text_oef(textvalue, uri):
        oefs.append(
            {
                "definition": {
                    "uri": uri,
                    "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue": textvalue,
                "fileValue": null,
                "jsonValue": null
            }
        )

    def add_dropdown_oef(definitionuri, taguri):
        oefs.append(
            {
                "definition": {
                    "uri": definitionuri,
                    "name": null
                },
                "tag": {
                    "uri": taguri,
                    "slug": null,
                    "tagName": null
                }if taguri else null,
                "numericValue": null,
                "textValue": null,
                "fileValue": null,
                "jsonValue": null
            }
        )

    if current_parent_oef_values and current_child_oef_values:
        child_project_type = rail.find_first_by_attr_and_get_attr(current_child_oef_values, 'definition.displayText', 'GSAP Project Type', 'tag.displayText')
        child_sold_to_party = rail.find_first_by_attr_and_get_attr(current_child_oef_values, 'definition.displayText', 'Sold to Party', 'textValue')
        child_controllingarea_value = rail.find_first_by_attr_and_get_attr(current_child_oef_values, 'definition.displayText', 'Controlling Area', 'textValue')
        parent_controllingarea_value = rail.find_first_by_attr_and_get_attr(
            current_parent_oef_values, 'definition.displayText', 'Controlling Area', 'textValue')
        if child_project_type and child_sold_to_party and child_controllingarea_value and parent_controllingarea_value:
            if child_project_type == '19' and child_controllingarea_value == '1004' and child_sold_to_party.startswith("I") and\
                parent_controllingarea_value == '1004':
                diwo_flag= True
                taguri = rail.find_first_by_attr_and_get_attr(dag_run.conf['wbstypetaguris'], 'name', 'DIWO', 'uri')
                add_dropdown_oef(dag_run.conf['wbstypeuri'], taguri)
            else:
                taguri = rail.find_first_by_attr_and_get_attr(dag_run.conf['wbstypetaguris'], 'name', 'GSAP', 'uri')
                add_dropdown_oef(dag_run.conf['wbstypeuri'], taguri)

    if current_parent_oef_values :
        if not diwo_flag:
            project_type_parent_tag_uri = rail.find_first_by_attr_and_get_attr(current_parent_oef_values,
             'definition.displayText', 'GSAP Project Type', 'tag.uri')
            add_dropdown_oef(dag_run.conf['gsapprojecttypeuri'], project_type_parent_tag_uri)
        psa_flag_parent_tag_uri = rail.find_first_by_attr_and_get_attr(
            current_parent_oef_values, 'definition.displayText', 'PSA Flag', 'tag.uri')
        add_dropdown_oef(dag_run.conf['psaflaguri'], psa_flag_parent_tag_uri)
        reference_mandatory_parent_tag_uri = rail.find_first_by_attr_and_get_attr(
            current_parent_oef_values, 'definition.displayText', 'Reference Mandatory', 'tag.uri')
        add_dropdown_oef(
            dag_run.conf['referencemandatoryuri'], reference_mandatory_parent_tag_uri)
        comments_mandatory_parent_tag_uri = rail.find_first_by_attr_and_get_attr(
            current_parent_oef_values, 'definition.displayText', 'Comments Mandatory', 'tag.uri')
        add_dropdown_oef(
            dag_run.conf['commentsmandatoryuri'], comments_mandatory_parent_tag_uri)
        taskindicator_tag_uri = rail.find_first_by_attr_and_get_attr(
            current_parent_oef_values, 'definition.displayText', 'GSAP Task Required', 'tag.uri')
        add_dropdown_oef(
            dag_run.conf['taskindicatoruri'], taskindicator_tag_uri)

    if current_parent_oef_values and not diwo_flag:
        profitcenter_oef_value_parent = rail.find_first_by_attr_and_get_attr(
            current_parent_oef_values, 'definition.displayText', 'Profit Center', 'textValue')
        add_text_oef(profitcenter_oef_value_parent,
                     dag_run.conf['profitcenteruri'])
        wbscurrency_oef_value_parent = rail.find_first_by_attr_and_get_attr(
            current_parent_oef_values, 'definition.displayText', 'WBS Currency', 'textValue')
        add_text_oef(wbscurrency_oef_value_parent,
                     dag_run.conf['wbscurrencyuri'])
        salesforceid_oef_value_parent = rail.find_first_by_attr_and_get_attr(
            current_parent_oef_values, 'definition.displayText', 'Salesforce Opportunity ID', 'textValue')
        add_text_oef(salesforceid_oef_value_parent,
                     dag_run.conf['salesforceoppurtunityiduri'])
        soldtoparty_oef_value_parent = rail.find_first_by_attr_and_get_attr(
            current_parent_oef_values, 'definition.displayText', 'Sold to Party', 'textValue')
        add_text_oef(soldtoparty_oef_value_parent,
                     dag_run.conf['soldtopartyuri'])
        area_oef_value_parent = rail.find_first_by_attr_and_get_attr(
            current_parent_oef_values, 'definition.displayText', 'Controlling Area', 'textValue')
        add_text_oef(area_oef_value_parent, dag_run.conf['controllingareauri'])

    if not current_parent_oef_values and not diwo_flag:
        add_text_oef(null, dag_run.conf['profitcenteruri'])
        add_text_oef(null, dag_run.conf['wbscurrencyuri'])
        add_text_oef(null, dag_run.conf['salesforceoppurtunityiduri'])
        add_text_oef(null, dag_run.conf['soldtopartyuri'])
        add_text_oef(null, dag_run.conf['controllingareauri'])

    if not current_parent_oef_values:
        if not diwo_flag:
            add_dropdown_oef(dag_run.conf['gsapprojecttypeuri'], null)
        add_dropdown_oef(dag_run.conf['psaflaguri'], null)
        add_dropdown_oef(dag_run.conf['referencemandatoryuri'], null)
        add_dropdown_oef(dag_run.conf['commentsmandatoryuri'], null)
        add_dropdown_oef(dag_run.conf['taskindicatoruri'], null)

    return oefs


def get_exception_logs(task_id):
    data = rail.load_all_records(rail.result(task_id))
    res = list(map(lambda x: x['message'], data))
    return res

def load_records(log_artifact):
    try:
        logs = rail.load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []

# pylint: disable=too-many-branches
def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    wbs_logs = dag_run.conf['wbs_logs']
    skip_logs = dag_run.conf['skip_logs']

    if wbs_logs:
        if isinstance(wbs_logs, list):
            log_artifacts.extend(wbs_logs)
        else:
            log_artifacts.append(wbs_logs)

    if skip_logs:
        if isinstance(skip_logs, list):
            log_artifacts.extend(skip_logs)
        else:
            log_artifacts.append(skip_logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid'],
            'message': log['message']
        },
            **dict(log['properties'].items()),
        }, log_records))

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: x['status'] == 'Exception', final_log_records ))))
    rail.set_result(key="skipped_record_count",val= len(list(filter(lambda x: x['status'] == 'Skipped', final_log_records ))))

    return  final_log_records
