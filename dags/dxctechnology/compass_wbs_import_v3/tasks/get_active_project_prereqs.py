import rail


# safely index into a list or iterable, returning None if index out of bounds
def list_get(collection, index):
    return (list(collection)[index:index + 1] or [None])[0]


def get_active_project_prereqs():

    with rail.TaskGroup(group_id='get_active_project_prereqs', prefix_group_id=False):

        active_projects_prereqs = rail.EmptyOperator(
            task_id='active_projects_prereqs')

        def convert_company_codes(resp):
            rows = [row["cells"] for row in resp.json()['d']['rows']]

            def map_row(cells):
                full_path_names = [elem['textValue']
                                   for elem in cells[1]['cellCollection']]
                full_path_uris = [elem['uri']
                                  for elem in cells[1]['cellCollection']]
                return {
                    "name": cells[0]['textValue'],
                    "fullpath": " | ".join(full_path_names),
                    "uri": cells[0]['uri'],
                    "parent": list_get(full_path_names, -2),
                    "parenturi": list_get(full_path_uris, -2),
                }
            return [map_row(row) for row in rows]
        get_divisions_company_codes = rail.RepliconServiceOperator(
            task_id="get_divisions_company_codes",
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 100000000,
                "columnUris": [
                    "urn:replicon:division-list-column:division",
                    "urn:replicon:division-list-column:full-path"
                ],
                "filterExpression": {
                    "leftExpression": {"filterDefinitionUri": "urn:replicon:division-list-filter:effectively-enabled"},
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {"value": {"bool": True}},
                }
            },
            response_filter=convert_company_codes
        )

        get_gsap_company_codes = rail.CreateCollectionOperator(
            task_id='get_gsap_company_codes',
            source=lambda: list(filter(lambda x: "GSAP" in (x['name'],x['parent']), rail.result(
                'get_divisions_company_codes'))),
        )

        get_departments = rail.RepliconServiceOperator(
            task_id="get_departments",
            endpoint="/services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups",
        )

        get_permission_sets = rail.RepliconServiceOperator(
            task_id="get_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_filter_definitions = rail.RepliconServiceOperator(
            task_id="get_all_filter_definitions",
            endpoint="/services/ProjectListService1.svc/GetAllFilterDefinitions",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'name', 'Parent WBS', 'uri')
        )

        get_all_columns = rail.RepliconServiceOperator(
            task_id="get_all_columns",
            endpoint="/services/ProjectListService1.svc/GetAllColumns",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'][0]['columns'], 'displayText', 'Parent WBS', 'uri')
        )

        def convert_employee_types(resp):
            def map_row(et):
                return {
                    'name': et['displayText'],
                    'uri': et['uri'],
                    'status': 'No' if 'contractor' in et['displayText'].lower() else 'Yes',
                }
            employee_types = [map_row(et) for et in resp.json()['d']]
            return list(
                filter(lambda et: et['status'] == 'Yes', employee_types))
        get_employee_types = rail.RepliconServiceOperator(
            task_id="get_employee_types",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
            response_filter=convert_employee_types,
        )

        def convert_project_oefs(resp):
            oefs = resp.json()['d']

            def get_oef(name):
                return list_get([oef['uri'] for oef in filter(
                    lambda oef: oef['name'] == name, oefs)], 0)
            return {
                'timetrackingrequired': get_oef("Time Tracking Required Attribute"),
                'globalflag': get_oef("Global Flag (COMPASS)"),
                'iwoindicator': get_oef("IWO Indicator"),
                'compassprojecttype': get_oef("COMPASS Project Type"),
                'wbstype': get_oef("WBS Type"),
                'salesforceopportunityid': get_oef("Salesforce Opportunity ID"),
                'salesforceopportunityname': get_oef("Salesforce Opportunity Name"),
                'psaflag':get_oef("PSA Flag"),
                'projectofferinggroup':get_oef("Project Offering Group"),
                'wbsofferinggroup': get_oef("WBS Offering Group"),
                'serviceofferingid': get_oef("DXC Service Offering ID"),
                "tmwbsindicator": get_oef("COMPASS T&M Indicator")
            }
        get_project_oefs = rail.RepliconServiceOperator(
            task_id="get_project_oefs",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={"bindingContextUri": "urn:replicon:object-type:project"},
            response_filter=convert_project_oefs,
        )

        def get_oef_values(oef):
            op = rail.RepliconServiceOperator(
                task_id=f"get_{oef}_values",
                endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
                data={
                    "objectExtensionTagDefinitionUri": f"{{{{ result('get_project_oefs')['{oef}'] }}}}"},
                response_filter=lambda resp: resp.json()['d']['tags'],
            )
            get_project_oefs >> op
            return op
        get_timetrackingrequired_values = get_oef_values(
            'timetrackingrequired')
        get_globalflag_values = get_oef_values('globalflag')
        get_iwoindicator_values = get_oef_values('iwoindicator')
        get_compassprojecttype_values = get_oef_values('compassprojecttype')
        get_projectofferinggroup_values = get_oef_values('projectofferinggroup')
        get_wbsofferinggroup_values = get_oef_values('wbsofferinggroup')
        get_psaflag_values = get_oef_values('psaflag')
        get_tmwbsindicator_values = get_oef_values('tmwbsindicator')
        active_projects_prereqs_loaded = rail.EmptyOperator(
            task_id='active_projects_prereqs_loaded')

        get_divisions_company_codes >> get_gsap_company_codes
        active_projects_prereqs >> [
            get_divisions_company_codes,
            get_departments,
            get_permission_sets,
            get_employee_types,
            get_all_filter_definitions,
            get_all_columns,
            get_project_oefs]
        get_project_oefs >> [
            get_timetrackingrequired_values,
            get_globalflag_values,
            get_iwoindicator_values,
            get_compassprojecttype_values]
        [get_gsap_company_codes, get_departments, get_permission_sets, get_employee_types, get_all_filter_definitions,
        get_all_columns,get_timetrackingrequired_values,
        get_globalflag_values, get_iwoindicator_values, get_compassprojecttype_values,
        get_projectofferinggroup_values,get_wbsofferinggroup_values, get_psaflag_values, get_tmwbsindicator_values] >> active_projects_prereqs_loaded
    return active_projects_prereqs, active_projects_prereqs_loaded
