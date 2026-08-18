from datetime import date, datetime
import rail
from dxctechnology.c1_wbs_import_v7.utils import request_payload


null = None


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_new_project_types(
        payload_project_collection_task_id,
        oef_drop_down_values_project_type_task_id,
        **_):
    unique_payload_project_types = set(
        filter(bool, map(
            lambda x: x['ProjectType'], get_data_from_document(
                rail.result(payload_project_collection_task_id)))))
    existing_project_types = set(map(lambda x: x['name'], rail.result(
        oef_drop_down_values_project_type_task_id)['tags']))
    project_types_to_add = list(
        unique_payload_project_types.difference(existing_project_types))
    return project_types_to_add


def get_unique_cost_center_to_add(
        payload_project_collection_task_id,
        cost_center_result_task_id,
        **_):
    unique_cost_center_from_payload = set(
        map(
            lambda x: x['ResponsibleCostCenter'].lower(),
            filter(
                lambda x: bool(x['ResponsibleCostCenter']),
                get_data_from_document(
                    rail.result(payload_project_collection_task_id)))))
    cost_center_in_replicon = set(
        map(lambda x: x['displayText'].lower(), rail.result(cost_center_result_task_id)))
    cost_center_to_add = list(
        unique_cost_center_from_payload.difference(cost_center_in_replicon))
    return cost_center_to_add


def get_unique_icwbsnumber_to_add(payload_project_collection_task_id):
    data = []
    unique_icwbsnumber_from_payload = set(
        map(
            lambda x: x['ICWBSNumber'],
            filter(
                lambda x: bool(x['ICWBSNumber']),
                get_data_from_document(
                    rail.result(payload_project_collection_task_id)))))
    # pylint: disable=[cell-var-from-loop]
    for i in unique_icwbsnumber_from_payload:
        child_data = list(filter(lambda x: x['parent'] == i, map(lambda item: {
            'parent': item['ICWBSNumber'],
            'child': item['ServiceOrderNumber'] if item['ServiceOrderNumber'] else item['DXCProjectID'],
        }, request_payload.get_data_from_document(rail.result(payload_project_collection_task_id)))))
        data.append(child_data)

    parent_and_child = []
    i = 0
    for j in unique_icwbsnumber_from_payload:
        parent_and_child.append({
            'parent': j,
            'child': list(map(lambda x: x['child'], data[i]))
        })
        i += 1

    return parent_and_child


def map_project_oef_field(all_project_oef_task_id, **_):
    data = rail.result(all_project_oef_task_id)
    return {
        "projecttype": rail.find_first_by_attr_and_get_attr(
            data,
            "name",
            "Project Type",
            "uri"),
        "itemcategory": rail.find_first_by_attr_and_get_attr(
            data,
            "name",
            "Item Category",
            "uri"),
        "serviceordertype": rail.find_first_by_attr_and_get_attr(
            data,
            "name",
            "Service Order Type",
            "uri"),
        "projectidentifier": rail.find_first_by_attr_and_get_attr(
            data,
            "name",
            "WBS Offering Group",
            "uri"),
        "psaflag": rail.find_first_by_attr_and_get_attr(
            data,
            "name",
            "PSA Flag",
            "uri"),
        "gsaptaskrequired": rail.find_first_by_attr_and_get_attr(data,"name","GSAP Task Required","uri"),
        "referencemandatory": rail.find_first_by_attr_and_get_attr(data, 'name', 'Reference Mandatory', 'uri'),
        "commentsmandatory": rail.find_first_by_attr_and_get_attr(data, 'name', 'Comments Mandatory', 'uri'),
        }


def validate_responsible_person_field():
    responsible_person_field = 'PersonResponsibleNumber' if request_payload.is_wbs_project(
    ) else 'SOPersonResponsible'
    applicant_field = 'WBSOwner2Number' if request_payload.is_wbs_project(
    ) else 'SOPartnerWBSOwner2'
    conf = request_payload.get_dag_run_conf()
    return bool(conf[responsible_person_field] and conf[applicant_field] and
                conf[applicant_field] == conf[responsible_person_field])


def validate_user_based_on_empid_method(
        get_user_based_on_empid_task_id, **_):
    user_info = rail.result(get_user_based_on_empid_task_id)
    responsible_person_field = 'PersonResponsibleNumber' if request_payload.is_wbs_project(
    ) else 'SOPersonResponsible'
    applicant_field = 'WBSOwner2Number' if request_payload.is_wbs_project(
    ) else 'SOPartnerWBSOwner2'
    errors = []
    conf = request_payload.get_dag_run_conf()
    can_assign_co_manager = True
    can_assign_manager = True
    if conf[responsible_person_field] and conf[applicant_field] and \
            conf[applicant_field] == conf[responsible_person_field]:
        can_assign_co_manager = False
        can_assign_manager = False
        errors.append({'status': 'Warning',
                       'message':
                       "Responsible Person field and Applicants field have same user"})

    if user_info:
        if not user_info['useruri']:
            can_assign_manager = False
            errors.append({'status': 'Warning',
                           'message':
                           f"Person Responsible {conf[responsible_person_field]} is not available in Replicon"})

        if not user_info['comanageruri']:
            can_assign_co_manager = False
            errors.append({'status': 'Warning',
                           'message':
                           f"Applicant number {conf[applicant_field]} is not available in Replicon"})

        if conf[responsible_person_field] and user_info['useruri'] == 'multiple-entry':
            can_assign_manager = False
            errors.append({'status': 'Exception',
                           'message':
                           f'Multiple users are available in Replicon with Person Responsible number {conf[responsible_person_field]}'})

        if conf[applicant_field] and user_info['comanageruri'] == 'multiple-entry':
            can_assign_co_manager = False
            errors.append({'status': 'Exception',
                           'message':
                           f'Multiple users are available in Replicon with Applicant number{conf[applicant_field]}'})

        if not user_info['comanagerstatus'] and user_info['comanagerenddate'] and datetime(
                **user_info['comanagerenddate']).date() < date.today():
            can_assign_co_manager = False
            errors.append({'status': 'Warning',
                           'message':
                           f"WBS Owner2 {conf[applicant_field]} is not added as project co-manager since the user is disabled and end date is in past"})

        if not user_info['userstatus'] and user_info['userenddate'] and datetime(
                **user_info['userenddate']).date() < date.today():
            can_assign_manager = False
            errors.append({'status': 'Warning',
                           'message':
                           # pylint: disable=line-too-long
                           f"Person Responsible {conf[responsible_person_field]} is not added as user since the project manager is disabled and end date is in past"})

        if user_info['comanageremployeegroup'] == 'Contractor':
            can_assign_co_manager = False
            errors.append({'status': 'Exception',
                           'message':
                           f"Applicant number {conf[applicant_field]} is a contractor in Replicon"})

        if user_info['useremployeegroup'] == 'Contractor':
            can_assign_manager = False
            errors.append({'status': 'Exception',
                           'message':
                           f"Person Responsible {conf[responsible_person_field]} is a contractor in Replicon"})

    rail.set_result(can_assign_manager, 'can_assign_manager')
    rail.set_result(can_assign_co_manager, 'can_assign_co_manager')

    return errors
