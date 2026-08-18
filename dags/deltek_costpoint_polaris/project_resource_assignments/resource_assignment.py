from datetime import datetime, timedelta
from airflow.models import Variable
import rail
# pylint:disable=undefined-loop-variable
# pylint:disable=inconsistent-return-statements
# pylint:disable=too-many-arguments
# pylint:disable=too-many-nested-blocks
# pylint:disable=too-many-statements
# Dummy
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_polaris_resource_sync_{config.instance}',
        description=f'deltek_costpoint_polaris_resource_sync_poc_{config.instance}',
        # schedule_interval=timedelta(seconds=config.master_dag_interval),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=[
            rail.WebhookConf(
                hmac_secret_var=config.cp_polaris_resource_assignment_webhook_secret),
            rail.WebhookConf(
                hmac_secret_var=config.cp_project_polaris_teamMember_allocation)
        ],
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='false').lower() == 'true',
            yes_task='batch_task',
            no_task='get_polaris_project_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_polaris_project_details',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        # log_to_sumo = rail.DagRunLogToSumoOperator(
        #     task_id='log_to_sumo',
        #     sumo_conn_id='sumologic-dagrunlogger',
        #     trigger_rule='all_done',
        # )

        get_polaris_project_details = rail.RepliconServiceOperator(
            task_id='get_polaris_project_details',
            endpoint='/services/projectService1.svc/BulkGetProjectDetails3',
            replicon_conn_id=config.replicon_conn_id,
            method='POST',
            data={
                    "projects": [
                        {
                            "uri": '{{ dag_run.conf.webhook.data.project.uri }}'
                        }
                    ]
            }
        )

        get_polaris_resource_assignment = rail.RepliconServiceOperator(  # rail.SimpleHttpOperator(
            task_id='get_polaris_resource_assignment',
            endpoint="graphql",
            replicon_conn_id=config.replicon_conn_id,
            method='POST',
            app='polaris',
            data=lambda: get_resource_assignment_query(rail.result(
                'get_polaris_project_details')[0]['projectDetails']['uri'])
            # {"operationName":null,"variables":{},"query":"{\n  projects2 {\n    name\n    code\n    startDate2\n    endDate2\n  }\n}\n"}
            # lambda: get_graphql_query()
        )

        get_costpoint_project_plcs = rail.DeltekCostPointServiceOperator(
            task_id='get_costpoint_project_plcs',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=lambda: get_project_company_key(rail.result(
                'get_polaris_project_details')[0]['projectDetails']),
            data=lambda: {
                "filter": {
                    "id": "polaris_exp_plc_prj",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "PJM_PROJLABCAT_HDR",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "PROJ_ID",
                                                "relation": "=",
                                                "value": get_project_code(rail.result('get_polaris_project_details'))
                                            }
                                        ]
                                    }
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            }

        )

        get_plc_modifcations = rail.PythonOperator(
            task_id="get_plc_modifcations",
            python_callable=lambda: get_project_plc_modifications(
                rail.result('get_costpoint_project_plcs')[0], rail.result('get_polaris_resource_assignment'), rail.result('get_project_role_details'))
        )

        is_missing_plc_assignment = rail.IfOperator(
            task_id='is_missing_plc_assignment',
            test=lambda: is_plc_assignment_required(
                rail.result('get_plc_modifcations')),
            yes_task='assign_plc_to_project',
            no_task='get_deltek_work_force'
        )

        assign_plc_to_project = rail.DeltekCostPointServiceOperator(
            task_id='assign_plc_to_project',
            endpoint='cpweb/cprestfulws/cpwwsgenericimport.cps',
            company=lambda: get_project_company_key(rail.result(
                'get_polaris_project_details')[0]['projectDetails']),
            data=lambda: get_plc_assignments(rail.result('get_polaris_project_details'), rail.result(
                'get_plc_modifcations'))
        )

        def get_plc_assignments(project_details, plc_modifications):
            return {
                "document": {
                    "id": "polaris_imp_plc_pj",
                    "rows": [
                        {
                            "row": {
                                "rsId": "PJM_PROJLABCAT_HDR",
                                "tranType": "INSERT",
                                "data": {
                                    "PROJ_ID": get_project_code(project_details)
                                },
                                "children": get_plc_children(plc_modifications)
                            }
                        }
                    ]
                }
            }

        def get_plc_children(plc_modifications):
            children = []
            if plc_modifications:
                for plc in plc_modifications:
                    children.append({
                        "row": {
                            "rsId": "PJM_PROJLABCAT_CTW",
                            "tranType": "INSERT",
                            "data": {
                                "BILL_LAB_CAT_CD": plc["code"],
                                "BILL_LAB_CAT_DESC": plc["name"]
                            }
                        }
                    })
            return children

        get_deltek_work_force = rail.DeltekCostPointServiceOperator(
            task_id='get_deltek_work_force',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=lambda: get_project_company_key(rail.result(
                'get_polaris_project_details')[0]['projectDetails']),
            data=lambda: {
                "filter": {
                    "id": "polaris_exp_pjm_work",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "PJM_PROJEMPL_HDR",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "PROJ_ID",
                                                "relation": "=",
                                                "value": get_project_code(rail.result('get_polaris_project_details'))
                                            }
                                        ]
                                    }
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            }
        )

        def is_plc_assignment_required(plc_modifications):
            if plc_modifications and len(plc_modifications) > 0:
                return True
            return False

        def get_project_company_key(projectDetails):
            company = rail.find_first_by_attr_and_get_attr(
                projectDetails['customFields'], 'customField.displayText', 'Company', 'text')
            return [company]

        def get_project_code(projectDetails):
            if projectDetails:
                return projectDetails[0]['projectDetails']['code']

        get_resource_modifcations = rail.PythonOperator(
            task_id="get_resource_modifcations",
            python_callable=lambda: get_project_resource_modifications(
                rail.result('get_polaris_resource_assignment'), rail.result('get_deltek_work_force')[0], rail.result('get_project_role_details'))
        )

        is_resource_update_required = rail.IfOperator(
            task_id='is_resource_update_required',
            test=lambda: is_project_resource_update_required(
                rail.result('get_resource_modifcations')),
            yes_task='is_resource_insert_required',
            no_task='end'
        )

        is_resource_insert_required = rail.IfOperator(
            task_id='is_resource_insert_required',
            test=lambda: is_resource_insert_required_in_cp(
                rail.result('get_resource_modifcations')),
            yes_task='push_insert_to_cost_point',
            no_task='push_update_to_cost_point'
        )

        get_project_role_details = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_project_role_details',
            items=lambda: get_replicon_project_roles(
                rail.result('get_polaris_resource_assignment')),
            endpoint="/services/ProjectRoleService1.svc/GetRoleDetails",
            replicon_conn_id=config.replicon_conn_id,
            data={
                "projectRoleUri": "{{ item }}",
                "asOfDate": null
            }
        )

        push_update_to_cost_point = rail.DeltekCostPointServiceOperator(
            task_id='push_update_to_cost_point',
            endpoint='cpweb/cprestfulws/cpwwsgenericimport.cps',
            company=lambda: get_project_company_key(rail.result(
                'get_polaris_project_details')[0]['projectDetails']),
            data=lambda: costpoint_modification_body(rail.result(
                    'get_resource_modifcations'),
                rail.result('get_polaris_project_details'), False
            )
        )

        push_insert_to_cost_point = rail.DeltekCostPointServiceOperator(
            task_id='push_insert_to_cost_point',
            endpoint='cpweb/cprestfulws/cpwwsgenericimport.cps',
            company=lambda: get_project_company_key(rail.result(
                'get_polaris_project_details')[0]['projectDetails']),
            data=lambda: costpoint_modification_body(rail.result(
                    'get_resource_modifcations'),
                rail.result('get_polaris_project_details'), False
            )
        )

        push_plcs_to_cost_point = rail.DeltekCostPointServiceOperator(
            task_id='push_plcs_to_cost_point',
            endpoint='cpweb/cprestfulws/cpwwsgenericimport.cps',
            company=lambda: get_project_company_key(rail.result(
                'get_polaris_project_details')[0]['projectDetails']),
            data=lambda: costpoint_modification_body(rail.result(
                    'get_resource_modifcations'),
                rail.result('get_polaris_project_details'), True
            )
        )

        end = rail.EmptyOperator(
            task_id='end'
        )

        def costpoint_modification_body(resourceModifications, projectDetails, isPlcOnly):
            return {
                "document": {
                    "id": "polaris_imp_pjmwork",
                    "rows": get_modification_rows(resourceModifications, projectDetails, isPlcOnly)
                }
            }

        def get_modification_rows(resourceModification, projectDetails, isPlcOnly):
            tranType = 'UPDATE' if resourceModification["allCostpointResources"] else 'INSERT'
            if isPlcOnly:
                tranType = 'UPDATE'
            return [
                {
                    "row": {
                        "rsId": "PJM_PROJEMPL_HDR",
                        "tranType": tranType,
                        "data": {
                            "OT_AUTH_FL": "N",
                            "PROJ_ID": get_project_code(projectDetails)
                        },
                        "children": get_resource_children(resourceModification, tranType, isPlcOnly)
                    }
                }
            ]

        def get_resource_children(resourceModifications, tranType, isPlcOnly):
            children = []
            emp_plc = []
            empl_default_list = []
            added_users = []
            if resourceModifications:
                if resourceModifications:
                    for resource in resourceModifications['userAssignmentsToAdd']:
                        if resource["user"]["slug"].upper() not in added_users:
                            added_users.append(
                                resource["user"]["slug"].upper())
                            children.append({
                                "row": {
                                    "rsId": "PJM_PROJEMPL_CHILDTO",
                                    "tranType": "INSERT",
                                    "data": {
                                        "EMPL_ID": resource["user"]["slug"].upper()
                                    }
                                }
                            })
                    for resource in resourceModifications["resourcesToAdd"]:
                        emp_plc.append({
                            "row": {
                                "rsId": "PJM_PROJEMPLLABCAT_PLCWK",
                                "tranType": "INSERT",
                                "data": {
                                    "BILL_LAB_CAT_CD": resource["role"]["code"],
                                    "DFLT_FL": get_default_flag(resource["user"]["slug"], empl_default_list,
                                                                resourceModifications["allCostpointResources"], resourceModifications['emp_plc_to_remove']),
                                    "PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID": resource["user"]["slug"].upper()
                                }
                            }
                        })
                        if resource["user"]["slug"] not in empl_default_list:
                            empl_default_list.append(resource["user"]["slug"])

                    for deltekResource in resourceModifications['resourcesToRemove']:
                        if deltekResource["row"]["rsId"] == "PJM_PROJEMPL_CHILDTO":
                            children.append({
                                "row": {
                                    "rsId": "PJM_PROJEMPL_CHILDTO",
                                    "tranType": "DELETE",
                                    "data": {
                                        "EMPL_ID": deltekResource["row"]["data"].get("PJM_PROJEMPL_CHILDTO_EMPL_ID") or deltekResource["row"]["data"].get("EMPL_ID")
                                    }
                                }
                            })
                        # elif deltekResource["row"]["rsId"] == "PJM_PROJEMPLLABCAT_PLCWK":
                        #     emp_plc.append({
                        #         "row": {
                        #             "rsId": "PJM_PROJEMPLLABCAT_PLCWK",
                        #             "tranType": "DELETE",
                        #             "data": {
                        #                 "BILL_LAB_CAT_CD": deltekResource["row"]["data"]["PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD"],
                        #                 "DFLT_FL": deltekResource["row"]["data"]["DFLT_FL"],
                        #                 "PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID": deltekResource["row"]["data"]["PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID"]
                        #             }
                        #         }
                        #     })
                    for wrk_frc_to_remove in resourceModifications['emp_plc_to_remove']:
                        emp_plc.append({
                            "row": {
                                "rsId": "PJM_PROJEMPLLABCAT_PLCWK",
                                "tranType": "DELETE",
                                "data": {
                                        "BILL_LAB_CAT_CD": wrk_frc_to_remove['plc'],
                                        "DFLT_FL": wrk_frc_to_remove["DFLT_FL"],
                                        "PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID": wrk_frc_to_remove["emp_id"]
                                }
                            }
                        })
                        if wrk_frc_to_remove["DFLT_FL"] == "Y" and wrk_frc_to_remove["emp_id"] not in empl_default_list:
                            # udapte default flag of one of the existing plcs for this user
                            emp_plc.append({
                                "row": {
                                    "rsId": "PJM_PROJEMPLLABCAT_PLCWK",
                                    "tranType": "MERGE",
                                    "data": {
                                        "BILL_LAB_CAT_CD": get_existing_plc_for_default(resourceModifications['emp_plc_to_remove'],
                                                                                        resourceModifications["allCostpointResources"], wrk_frc_to_remove),
                                        "DFLT_FL": "Y",
                                        "PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID": wrk_frc_to_remove["emp_id"]
                                    }
                                }
                            })

            if tranType == 'UPDATE':
                children.append({"row": {
                    "rsId": "PJM_PROJEMPL_LABCAT_PLCWKFRCE",
                    "tranType": "SELECT",
                    "data": {
                    },
                    "children": emp_plc}})

            if isPlcOnly:
                children = []
                children.append({"row": {
                    "rsId": "PJM_PROJEMPL_LABCAT_PLCWKFRCE",
                    "tranType": "SELECT",
                    "data": {
                    },
                    "children": emp_plc}})
            return children

        def get_existing_plc_for_default(modificationsToRemove, allCostpointResources, workforce_to_remove):
            if workforce_to_remove and modificationsToRemove and allCostpointResources:
                wrk_frc_assingments = rail.find_first_by_attr_and_get_attr(allCostpointResources, 'row.rsId',
                                                                           'PJM_PROJEMPL_LABCAT_PLCWKFRCE', 'row.children')
                if wrk_frc_assingments:
                    for assignment in wrk_frc_assingments:
                        if assignment['row']['rsId'] == 'PJM_PROJEMPLLABCAT_PLCWK' and \
                            assignment['row']['data']['PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID'] == workforce_to_remove['emp_id'] \
                            and assignment['row']['data']['PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD'].upper() \
                                != workforce_to_remove['plc'].upper():
                            is_plc_getting_removed = False
                            for modificationToRemove in modificationsToRemove:
                                if modificationToRemove['emp_id'] == workforce_to_remove['emp_id'] and \
                                    modificationToRemove['plc'].upper() \
                                        == assignment['row']['data']['PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD'].upper():
                                    is_plc_getting_removed = True

                            if not is_plc_getting_removed:
                                return assignment['row']['data']['PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD'].upper()

        def get_default_flag(user, empl_default_list, costPointAllocations, allocationToRemove):
            if user in empl_default_list:
                return "N"
            if costPointAllocations:
                for cpEmployeePlc in costPointAllocations:
                    if cpEmployeePlc["row"]["rsId"] == "PJM_PROJEMPL_LABCAT_PLCWKFRCE" and cpEmployeePlc["row"]["children"]:
                        for empChild in cpEmployeePlc["row"]["children"]:
                            if empChild and empChild["row"] and empChild["row"]["rsId"] \
                                    and empChild["row"]["rsId"] == 'PJM_PROJEMPLLABCAT_PLCWK':
                                if empChild["row"]["data"] and empChild["row"]["data"]["PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID"].lower() \
                                        == user.lower():
                                    if not is_default_emp_plc_removed(empChild["row"]["data"]["PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID"],
                                                                      empChild["row"]["data"]["PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD"], allocationToRemove):
                                        return "N"

            return "Y"

        def is_default_emp_plc_removed(emp_id, plc_id, allocationsToRemove):
            for allocation in allocationsToRemove:
                if allocation["plc"].lower() == plc_id.lower() and allocation["emp_id"].lower() == emp_id.lower() \
                        and allocation["DFLT_FL"] == "Y":
                    return True
            return False

        def get_replicon_project_roles(polaris_resources):
            roleUris = []
            if polaris_resources and polaris_resources['data'] and polaris_resources['data']['project']:
                if polaris_resources['data']['project']['resources'] and polaris_resources['data']['project']['resources']['items']:
                    for item in polaris_resources['data']['project']['resources']['items']:
                        for projectRole in item['projectRoles']:
                            if projectRole and projectRole['projectRole']['uri'] not in roleUris:
                                roleUris.append(
                                    projectRole['projectRole']['uri'])
            return roleUris

        def get_project_plc_modifications(cpPLCs, polaris_resources, role_details):
            roleCodes = []
            roleUris = []
            if polaris_resources and polaris_resources['data'] and polaris_resources['data']['project']:
                if polaris_resources['data']['project']['resources'] and polaris_resources['data']['project']['resources']['items']:
                    for item in polaris_resources['data']['project']['resources']['items']:
                        for projectRole in item['projectRoles']:
                            if projectRole and projectRole['projectRole']['uri'] not in roleUris:
                                roleUris.append(
                                    projectRole['projectRole']['uri'])
                                role_id = get_role_id(
                                    projectRole['projectRole']['uri'], role_details)
                                if role_id:
                                    if not is_plc_present_in_costPoint(role_id, cpPLCs):
                                        roleCodes.append({
                                            "code": role_id,
                                            "name": projectRole['projectRole']["name"]})
            return roleCodes

        def is_plc_present_in_costPoint(plc, projectPlcs):
            if projectPlcs and projectPlcs["document"] and projectPlcs["document"]["rows"] and projectPlcs["document"]["rows"][0] \
                    and projectPlcs["document"]["rows"][0]["row"] and projectPlcs["document"]["rows"][0]["row"]["children"]:
                for child in projectPlcs["document"]["rows"][0]["row"]["children"]:
                    if child["row"] and child["row"]["data"] and \
                            (child["row"]["data"].get("BILL_LAB_CAT_CD") == plc or child["row"]["data"].get("PJM_PROJLABCAT_CTW_BILL_LAB_CAT_CD") == plc):
                        return True
            return False

        def is_project_resource_update_required(resource_modifications):
            if (resource_modifications and (resource_modifications['resourcesToAdd'] or
                                            resource_modifications['resourcesToRemove']) or resource_modifications["emp_plc_to_remove"]):
                return True
            return False

        def is_resource_insert_required_in_cp(resource_modifications):
            if (resource_modifications and resource_modifications['allCostpointResources']):
                return False
            return True

        def get_project_resource_modifications(polaris_resources, costpoint_resources, role_details):
            assignmentToAdd = []
            userAssignmentsToAdd = []
            assignmentToRemove = []
            costpoint_assignments = []
            polaris_assignments = []
            emp_plc_to_remove = []
            if costpoint_resources and costpoint_resources['document'] and costpoint_resources['document']['rows']:
                if (costpoint_resources['document']['rows'][0]):
                    if costpoint_resources['document']['rows'][0]['row']:
                        if costpoint_resources['document']['rows'][0]['row']['children']:
                            for child in costpoint_resources['document']['rows'][0]['row']['children']:
                                if child['row']["rsId"] == "PJM_PROJEMPL_CHILDTO":
                                    costpoint_assignments.append(child)
                                    if not is_present_in_polaris(child, polaris_resources):
                                        assignmentToRemove.append(child)
                                elif child["row"]["rsId"] == "PJM_PROJEMPL_LABCAT_PLCWKFRCE":
                                    costpoint_assignments.append(child)
                                    if child["row"]["children"]:
                                        for plcChild in child["row"]["children"]:
                                            if plcChild["row"]["rsId"] == "PJM_PROJEMPLLABCAT_PLCWK" and \
                                                    is_employee_present_in_polaris(plcChild["row"]["data"]['PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID'], polaris_resources):
                                                if not is_employee_plc_present_in_polaris(
                                                    plcChild["row"]["data"]['PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID'],
                                                        polaris_resources, plcChild["row"]["data"][
                                                            'PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD'],
                                                        role_details):
                                                    emp_plc_to_remove.append({
                                                        'emp_id': plcChild["row"]["data"]['PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID'],
                                                        'plc': plcChild["row"]["data"]['PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD'],
                                                        'DFLT_FL': plcChild["row"]["data"]['DFLT_FL']
                                                    })
                                                # to do: add plc check
            if polaris_resources and polaris_resources['data'] and polaris_resources['data']['project']:
                if polaris_resources['data']['project']['resources'] and polaris_resources['data']['project']['resources']['items']:
                    for item in polaris_resources['data']['project']['resources']['items']:
                        for projectRole in item['projectRoles']:
                            if projectRole:
                                assignment = {
                                    'user': {
                                        'uri': item['uri'],
                                        'slug': item['slug']},
                                    'role': {
                                        'uri': projectRole['projectRole']['uri'],
                                        'name': projectRole['projectRole']['name'],
                                        'code': get_role_id(projectRole['projectRole']['uri'], role_details)
                                    }
                                }
                                polaris_assignments.append(assignment)
                                if not is_user_assignment_present_in_costPoint(assignment, costpoint_resources):
                                    userAssignmentsToAdd.append(assignment)
                                if not is_assignment_present_in_costPoint(assignment, costpoint_resources):
                                    assignmentToAdd.append(assignment)

            return {
                'resourcesToAdd': assignmentToAdd,
                'userAssignmentsToAdd': userAssignmentsToAdd,
                'resourcesToRemove': assignmentToRemove,
                'allCostpointResources': costpoint_assignments,
                'allPolarisAssignments': polaris_assignments,
                'emp_plc_to_remove': emp_plc_to_remove
            }

        def is_user_assignment_present_in_costPoint(polaris_resouce, costpoint_resources):
            # return False
            if polaris_resouce and costpoint_resources and costpoint_resources['document'] \
                    and costpoint_resources['document']['rows'] and costpoint_resources['document']['rows'][0] \
                    and costpoint_resources['document']['rows'][0]['row'] and costpoint_resources['document']['rows'][0]['row']['children']:
                for child in costpoint_resources['document']['rows'][0]['row']['children']:
                    if child and child['row'] and child['row']['rsId'] == 'PJM_PROJEMPL_CHILDTO' and \
                            child['row']['data']:
                        if ((child['row']['data'].get('PJM_PROJEMPL_CHILDTO_EMPL_ID') or child['row']['data'].get('EMPL_ID') or '').lower() ==
                                polaris_resouce['user']['slug'].lower()):
                            return True
            return False

        def is_assignment_present_in_costPoint(polaris_resouce, costpoint_resources):
            # return False
            if polaris_resouce and costpoint_resources and costpoint_resources['document'] \
                    and costpoint_resources['document']['rows'] and costpoint_resources['document']['rows'][0] \
                    and costpoint_resources['document']['rows'][0]['row'] and costpoint_resources['document']['rows'][0]['row']['children']:
                for child in costpoint_resources['document']['rows'][0]['row']['children']:
                    if child and child['row'] and child['row']['rsId'] == 'PJM_PROJEMPL_LABCAT_PLCWKFRCE' and \
                            child['row']['children']:
                        for plc_wrk in child['row']['children']:
                            if plc_wrk['row'] and plc_wrk['row']['rsId'] == 'PJM_PROJEMPLLABCAT_PLCWK' and \
                                    plc_wrk['row']['data'] and plc_wrk['row']['data']['PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD'] \
                                    and plc_wrk['row']['data']['PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID']:
                                if ((plc_wrk['row']['data']['PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID'].lower() ==
                                    polaris_resouce['user']['slug'].lower()) and
                                    (plc_wrk['row']['data']['PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD'].lower() ==
                                        polaris_resouce['role']['code'].lower())):
                                    return True
            return False

        def get_role_id(role_uri, role_details):
            role_id = None
            if role_uri and role_details:
                role_id = rail.find_first_by_attr_and_get_attr(
                    role_details, 'uri', role_uri, 'description')
            return role_id

        def is_present_in_polaris(cp_resource, polaris_resources):
            if cp_resource and cp_resource["row"] and cp_resource["row"]["data"] and \
                    (cp_resource["row"]["data"].get("PJM_PROJEMPL_CHILDTO_EMPL_ID") or cp_resource["row"]["data"].get("EMPL_ID")):
                return is_employee_present_in_polaris(cp_resource["row"]["data"].get("PJM_PROJEMPL_CHILDTO_EMPL_ID") or cp_resource["row"]["data"].get("EMPL_ID"), polaris_resources)
            return False

        def is_employee_present_in_polaris(empl_Id, polaris_resources):
            if empl_Id:
                if polaris_resources['data'] and polaris_resources['data']['project'] and \
                        polaris_resources['data']['project']['resources'] and polaris_resources['data']['project']['resources']['items']:
                    for item in polaris_resources['data']['project']['resources']['items']:
                        for projectRole in item['projectRoles']:
                            if projectRole and item["slug"].lower() == empl_Id.lower():
                                return True
            return False

        def is_employee_plc_present_in_polaris(empl_Id, polaris_resources, plc, role_details):
            if empl_Id:
                if polaris_resources['data'] and polaris_resources['data']['project'] and \
                        polaris_resources['data']['project']['resources'] and polaris_resources['data']['project']['resources']['items']:
                    for item in polaris_resources['data']['project']['resources']['items']:
                        for projectRole in item['projectRoles']:
                            if projectRole and item["slug"].lower() == empl_Id.lower():
                                if plc:
                                    if plc.lower() == (get_role_id(projectRole['projectRole']['uri'], role_details)).lower():
                                        return True
            return False

        def get_resource_assignment_query(projectId):
            return {
                "variables":
                    {
                        "projectId": projectId,
                        "searchPhrase": "",
                        "allocationStatusList": ["COMMITTED"]},
                "query": "query Eager_projectResourcesQuery($projectSlug: String, $projectId: String, $searchPhrase: String, $allocationStatusList: [ResourceAllocationStatus!]) {\n  project(projectSlug: $projectSlug, projectId: $projectId) {\n    ...ProjectResources\n    __typename\n  }\n}\n\nfragment ProjectResources on Project {\n  id\n  resources(\n    searchPhrase: $searchPhrase\n    allocationStatusList: $allocationStatusList\n  ) {\n    totalItems\n    items {\n      id\n      uri\n      slug\n      displayText\n      isEnabled\n      projectRoles {\n        isPrimary\n        projectRole {\n          uri\n          name\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n"}

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> end  # >> log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> get_polaris_project_details >> get_polaris_resource_assignment >> get_project_role_details >> \
            get_costpoint_project_plcs >> get_plc_modifcations >> is_missing_plc_assignment

        is_missing_plc_assignment >> rail.Label(
            'yes') >> assign_plc_to_project >> get_deltek_work_force

        is_missing_plc_assignment >> rail.Label(
            'no') >> get_deltek_work_force

        get_deltek_work_force >> \
            get_resource_modifcations >> is_resource_update_required

        is_resource_update_required >> rail.Label(
            'yes') >> is_resource_insert_required

        is_resource_update_required >> rail.Label('no') >> end

        is_resource_insert_required >> rail.Label(
            'no') >> push_update_to_cost_point >> end

        is_resource_insert_required >> rail.Label(
            'yes') >> push_insert_to_cost_point >> push_plcs_to_cost_point >> end

        return dag


rail.for_each_instance(create_dag)
