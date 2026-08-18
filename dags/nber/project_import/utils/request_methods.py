
import rail
from datetime import datetime
from uuid import uuid4

null = None

PROJECT_STATUS = {0: "In Progress", 1: "Completed"}

def _parse_ddmmyyyy_to_replicon_date(value):
    """
    STRICT: accepts ONLY DDMMYYYY (e.g. '01012026').
    Returns rail.parse_date('YYYY-MM-DD') on success, else None.
    """
    if not value:
        return None

    s = str(value).strip()
    if not (len(s) == 8 and s.isdigit()):
        return None

    try:
        dd = int(s[0:2])
        mm = int(s[2:4])
        yyyy = int(s[4:8])
        dt = datetime(yyyy, mm, dd)
        iso = dt.date().isoformat()
        return rail.parse_date(iso, "%Y-%m-%d")
    except:
        return None


def _date_to_apply_from_conf(dag_run, key: str):
    raw = dag_run.conf.get(key)
    parsed = _parse_ddmmyyyy_to_replicon_date(raw)
    return {"date": {**parsed}} if parsed else None


def _normalize_yyyy_mm_dd(raw):
    """
    Converts STRICT DDMMYYYY → YYYY-MM-DD for comparison only.
    """
    parsed = _parse_ddmmyyyy_to_replicon_date(raw)
    if not parsed:
        return None

    y = parsed.get("year")
    m = parsed.get("month")
    d = parsed.get("day")

    try:
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    except:
        return None


def _extract_existing_date(existing_value):
    if not existing_value:
        return None
    if isinstance(existing_value, str) and existing_value.strip():
        s = existing_value.strip()
        try:
            return datetime.strptime(s, "%Y-%m-%d").date().isoformat()
        except ValueError:
            try:
                s2 = s.replace("Z", "")
                return datetime.fromisoformat(s2).date().isoformat()
            except Exception:
                return s
    if isinstance(existing_value, dict):
        d = existing_value.get("date") or existing_value
        y = d.get("year"); m = d.get("month"); day = d.get("day")
        if y is not None and m is not None and day is not None:
            try:
                return f"{int(y):04d}-{int(m):02d}-{int(day):02d}"
            except Exception:
                return None
    return None

def _extract_status_name(existing_status):
    if existing_status is None:
        return None
    if isinstance(existing_status, str):
        s = existing_status.strip()
        return s if s else None
    if isinstance(existing_status, dict):
        name = existing_status.get("name")
        if isinstance(name, str):
            s = name.strip()
            return s if s else None
    return None

def _text_changed(current, incoming):
    cur = (str(current).strip() if current is not None else "")
    inc = (str(incoming).strip() if incoming is not None else "")
    return cur != inc

def _oef(name, value):
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    return {
        "definition": {"uri": None, "name": name},
        "tag": None,
        "numericValue": None,
        "textValue": s,
        "fileValue": None,
        "jsonValue": None,
    }

def _existing_oef_map(existing_project):
    result = {}
    fields = existing_project.get("objectExtensionFields") or []
    for item in fields:
        try:
            name = item.get("definition", {}).get("name")
            val  = item.get("textValue")
            if name:
                result[name] = (str(val).strip() if val is not None else "")
        except Exception:
            continue
    return result

def build_create_payload(dag_run):
    name = dag_run.conf.get("grant_name")
    code = dag_run.conf.get("grant_code")
    start_date_to_apply = _date_to_apply_from_conf(dag_run, "grant_start_date")
    end_date_to_apply   = _date_to_apply_from_conf(dag_run, "grant_end_date")
    raw_status = dag_run.conf.get("grant_status")
    status_name = None
    if raw_status is not None and str(raw_status).strip() != "":
        try:
            status_name = PROJECT_STATUS[int(str(raw_status).strip())]
        except Exception:
            status_name = None
    status_to_apply = {"uri": None, "name": status_name} if status_name else None
    object_extension_fields = []
    for field_name, conf_key in [
        ("CFDA Number", "cfda_number"),
        ("Award Number", "award_number"),
        ("Funding Source", "funding_source"),
        ("Cost category", "cost_category"),
        ("Misc Field 1", "misc_field_1"),
        ("Misc Field 2", "misc_field_2"),
    ]:
        item = _oef(field_name, dag_run.conf.get(conf_key))
        if item:
            object_extension_fields.append(item)
    modifications = {
        "nameToApply": {"value": name} if (name and str(name).strip()) else None,
        "codeToApply": {"value": code} if (code and str(code).strip()) else None,
        "descriptionToApply": None,
        "percentCompletedToApply": None,
        "startDateToApply": start_date_to_apply,
        "endDateToApply": end_date_to_apply,
        "billingTypeToApply": 
            {
                "value": "urn:replicon:billing-type:time-and-material"
            },
        "clientBillingAllocationMethodToApply": None,
        "clientAssignmentsSchedulesToApply": None,
        "statusToApply": status_to_apply,
        "projectWorkflowStateToApply": None,
        "clientRepresentativeToApply": None,
        "programToApply":{
            "program": {
                "uri": null,
                "name": dag_run.conf["program"]
            }
        } if dag_run.conf["program"] else None,
        "projectLeaderToApply": {
            "user": {
                "uri": rail.result("pm_assignment_details"),
                "loginName": null,
                "employeeId": null,
                "parameterCorrelationId": null
            }
            } if rail.result("pm_assignment_details") else None,
        "isProjectLeaderApprovalRequired": None,
        "costTypeToApply": None,
        "isTimeEntryAllowed": True,
        "expenseCodesToApply": None,
        "estimatedHoursToApply": None,
        "budgetedHoursToApply": None,
        "estimatedCostToApply": None,
        "budgetedCostToApply": None,
        "expenseBudgetedCostToApply": None,
        "totalEstimatedContractValueToApply": None,
        "defaultBillingCurrencyToApply": None,
        "timeAndMaterials": 
            {
                "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                "billingRateFrequency": 
                    {
                        "uri": None,
                        "name": "Hourly"
                    },
                "billingRateFrequencyDuration": None,
                "billingRates": []
            },
        "billingContractToApply": None,
        "fixedBid": None,
        "customFieldsToApply": [],
        "resourceAssignmentModifications": None,
        "resourceProjectAssignmentModifications": None,
        "billingContractModifications": None,
        "keyValuesToApply": [],
        "objectExtensionFieldsToApply": object_extension_fields,
        "portfolioToApply": None,
        "locationToApply": None,
        "divisionToApply": None,
        "serviceCenterToApply": None,
        "costCenterToApply": None,
        "departmentGroupToApply": None,
        "employeeTypeGroupToApply": None,
    }
    return {
        "target": None,
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid4()),
    }

def build_update_payload(dag_run, existing_project: dict):
    target = {
        "uri": existing_project.get("uri"),
        "parameterCorrelationId": None,
    }
    name_in  = dag_run.conf.get("grant_name")
    code_in  = dag_run.conf.get("grant_code")
    start_in = dag_run.conf.get("grant_start_date")
    end_in   = dag_run.conf.get("grant_end_date")
    stat_in  = dag_run.conf.get("grant_status")
    name_cur  = existing_project.get("name")
    code_cur  = existing_project.get("code")
    start_cur = _extract_existing_date(existing_project.get("startDate"))
    end_cur   = _extract_existing_date(existing_project.get("endDate"))
    stat_cur  = _extract_status_name(existing_project.get("status"))
    nameToApply = {"value": name_in} if (name_in and str(name_in).strip() and _text_changed(name_cur, name_in)) else None
    codeToApply = {"value": code_in} if (code_in and str(code_in).strip() and _text_changed(code_cur, code_in)) else None
    existing_program = existing_project["program"]["name"] if existing_project.get("program") else {}
    program_update = dag_run.conf.get("program") if dag_run.conf["program"] != existing_program else None
    start_in_norm = _normalize_yyyy_mm_dd(start_in)
    end_in_norm   = _normalize_yyyy_mm_dd(end_in)
    startDateToApply = _date_to_apply_from_conf(dag_run, "grant_start_date") if (
        start_in_norm and _text_changed(start_cur, start_in_norm)
    ) else None
    endDateToApply = _date_to_apply_from_conf(dag_run, "grant_end_date") if (
        end_in_norm and _text_changed(end_cur, end_in_norm)
    ) else None
    status_name_in = None
    if stat_in is not None and str(stat_in).strip() != "":
        try:
            status_name_in = PROJECT_STATUS[int(str(stat_in).strip())]
        except Exception:
            status_name_in = None
    statusToApply = {"uri": None, "name": status_name_in} if (
        status_name_in and _text_changed(stat_cur, status_name_in)
    ) else None
    existing_oef = _existing_oef_map(existing_project)
    changed_oefs = []
    for field_name, conf_key in [
        ("CFDA Number", "cfda_number"),
        ("Award Number", "award_number"),
        ("Funding Source", "funding_source"),
        ("Cost category", "cost_category"),
        ("Misc Field 1", "misc_field_1"),
        ("Misc Field 2", "misc_field_2"),
    ]:
        incoming = dag_run.conf.get(conf_key)
        current  = existing_oef.get(field_name, "")
        if _text_changed(current, incoming):
            item = _oef(field_name, incoming)
            if item:
                changed_oefs.append(item)
    modifications = {
        "nameToApply": nameToApply,
        "codeToApply": codeToApply,
        "descriptionToApply": None,
        "percentCompletedToApply": None,
        "startDateToApply": startDateToApply,
        "endDateToApply": endDateToApply,
        "billingTypeToApply": 
            {
                "value": "urn:replicon:billing-type:time-and-material"
            },
        "clientBillingAllocationMethodToApply": None,
        "clientAssignmentsSchedulesToApply": None,
        "statusToApply": statusToApply,
        "projectWorkflowStateToApply": None,
        "clientRepresentativeToApply": None,
        "programToApply": {
        "program": {
            "uri": null,
            "name": program_update
        }
        } if program_update else None,
        "projectLeaderToApply": {
            "user": {
                "uri": rail.result("pm_assignment_details"),
                "loginName": null,
                "employeeId": null,
                "parameterCorrelationId": null
            }
        } if rail.result("pm_assignment_details") else None,
        "isProjectLeaderApprovalRequired": None,
        "costTypeToApply": None,
        "isTimeEntryAllowed": True,
        "expenseCodesToApply": None,
        "estimatedHoursToApply": None,
        "budgetedHoursToApply": None,
        "estimatedCostToApply": None,
        "budgetedCostToApply": None,
        "expenseBudgetedCostToApply": None,
        "totalEstimatedContractValueToApply": None,
        "defaultBillingCurrencyToApply": None,
        "timeAndMaterials":None,
        "billingContractToApply": None,
        "fixedBid": None,
        "customFieldsToApply": [],
        "resourceAssignmentModifications": None,
        "resourceProjectAssignmentModifications": None,
        "billingContractModifications": None,
        "keyValuesToApply": [],
        "objectExtensionFieldsToApply": changed_oefs,
        "portfolioToApply": None,
        "locationToApply": None,
        "divisionToApply": None,
        "serviceCenterToApply": None,
        "costCenterToApply": None,
        "departmentGroupToApply": None,
        "employeeTypeGroupToApply": None,
    }
    return {
        "target": target,
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid4()),
    }

def get_project_manager(dag_run):
    return {
  "page": "1",
  "pagesize": "10",
  "columnUris": [
    "urn:replicon:user-list-column:user-name"
  ],
  "sort": [],
  "filterExpression": {
    "leftExpression": {
      "leftExpression": {
        "leftExpression": null,
        "operatorUri": null,
        "rightExpression": null,
        "value": null,
        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
      },
      "operatorUri": "urn:replicon:filter-operator:text-search",
      "rightExpression": {
        "leftExpression": null,
        "operatorUri": null,
        "rightExpression": null,
        "value": {
          "uri": null,
          "uris": [],
          "bool": null,
          "date": null,
          "money": null,
          "number": null,
          "text": dag_run.conf["grant_manager"],
          "time": null,
          "calendarDayDurationValue": null,
          "workdayDurationValue": null,
          "dateRange": null,
          "dateTimeUtc": null,
          "dateTimeUtcRange": null,
          "numberRange": null
        },
        "filterDefinitionUri": null
      },
      "value": null,
      "filterDefinitionUri": null
    },
    "operatorUri": "urn:replicon:filter-operator:and",
    "rightExpression": {
      "leftExpression": {
        "leftExpression": null,
        "operatorUri": null,
        "rightExpression": null,
        "value": null,
        "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
      },
      "operatorUri": "urn:replicon:filter-operator:equal",
      "rightExpression": {
        "leftExpression": null,
        "operatorUri": null,
        "rightExpression": null,
        "value": {
          "uri": null,
          "uris": [],
          "bool": "true",
          "date": null,
          "money": null,
          "number": null,
          "text": null,
          "time": null,
          "calendarDayDurationValue": null,
          "workdayDurationValue": null,
          "dateRange": null,
          "dateTimeUtc": null,
          "dateTimeUtcRange": null,
          "numberRange": null
        },
        "filterDefinitionUri": null
      },
      "value": null,
      "filterDefinitionUri": null
    },
    "value": null,
    "filterDefinitionUri": null
  }
}

def add_pm_permissions():
    user_uri = rail.result("get_project_manager_details")[0]["cells"][0]["uri"]
    return {
  "target": {
    "uri": user_uri,
    "loginName": null,
    "employeeId": null,
    "parameterCorrelationId": null
  },
  "template": null,
  "modifications": {
    "permissionSets": [
      {
        "modificationOptionUri": "urn:replicon:collection-modification-option:add",
        "items": [
          {
            "permissionSetPolicy": {
              "uri": null,
              "name": "Billing rates view only"
            },
            "groupAccessFilter": null
          }
        ]
      },
      {
        "modificationOptionUri": "urn:replicon:collection-modification-option:add",
        "items": [
          {
            "permissionSetPolicy": {
              "uri": null,
              "name": "Project Resource with Reports"
            },
            "groupAccessFilter": null
          }
        ]
      },
      {
        "modificationOptionUri": "urn:replicon:collection-modification-option:add",
        "items": [
          {
            "permissionSetPolicy": {
              "uri": null,
              "name": "Project Manager"
            },
            "groupAccessFilter": null
          }
        ]
      }
    ],
  },
  "userModificationOptionUri": "urn:replicon:user-modification-option:save",
  "unitOfWorkId": str(uuid4())
    }