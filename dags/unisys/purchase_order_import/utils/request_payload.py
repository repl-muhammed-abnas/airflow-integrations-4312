import uuid

null = None

def get_all_departments_payload():
    return {
        "page": 1,
        "pagesize": 10000,
        "columnUris": [
            "urn:replicon:department-group-list-column:department-group"
        ],
        "filterExpression": null,
        "hierarchyListDataOptionUris": []
    }

def create_department_group_payload(purchase_order_id, parent_department):
    return {
        "departmentGroup": {
            "uri": null,
            "parent": {
                "uri": parent_department,
                "parent": null,
                "name": null,
                "parameterCorrelationId": null
            },
            "name": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "name": purchase_order_id,
            "codeToApply": null,
            "descriptionToApply": null,
            "isEnabled": "1"
        },
        "unitOfWorkId": str(uuid.uuid4()),
    }