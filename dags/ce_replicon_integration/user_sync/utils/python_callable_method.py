import math


def getChunks(arrayof_obj):
    chunk_size = 50
    chunks = [arrayof_obj[i:i + chunk_size]
              for i in range(0, len(arrayof_obj), chunk_size)]
    return chunks


def getFilterExpression(employeeId):
    return {
        "leftExpression": {
            "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
        },
        "operatorUri": "urn:replicon:filter-operator:text-search",
        "rightExpression": {
            "value": {
                "text": employeeId
            }
        }
    }


def joinFilter(leftExpression, rightExpression, operatorUri):
    return {
        "leftExpression": leftExpression,
        "operatorUri": operatorUri,
        "rightExpression": rightExpression
    }


def combineLeaves(leaves):
    if not leaves:
        return None
    if len(leaves) == 1:
        return leaves[0]
    midpoint = math.ceil(len(leaves) / 2)
    return joinFilter(combineLeaves(leaves[:midpoint]), combineLeaves(leaves[midpoint:]), "urn:replicon:filter-operator:or")


def get_chunk_request(loginname_list, columnUris):
    leaves = []
    for loginname in loginname_list:
        filterExpression = getFilterExpression(loginname)
        leaves.append(filterExpression)
    finalFilterExpression = combineLeaves(leaves)
    return {
        "page": 1,
        "pagesize": 10000,
        "columnUris": columnUris,
        "sort": [],
        "filterExpression": finalFilterExpression
    }


def page_handler(request, result):
    if len(result['rows']) > 0:
        request['page'] += 1
        return request
    return None


def extract_user_details(users_response):
    if not users_response:
        return {}

    replicon_users = {}

    for user in users_response:
        user_details = user.get('userDetails', {})
        extension_fields = user_details.get('extensionFieldValues', [])
        login_name = user['securityConfiguration']['user'].get('loginName')

        worker_class = None
        for field in extension_fields:
            if field['definition'].get('displayText') == 'Worker Class' and field.get('tag'):
                worker_class = field['tag'].get('displayText')
                break

        department_group = None
        if user.get('departmentGroupSchedule'):
            department_group = user['departmentGroupSchedule'][0]['departmentGroup'].get(
                'displayText')

        user_uri = user.get('securityConfiguration', {}
                            ).get('user', {}).get('uri')
        replicon_users[login_name] = {
            'uri': user_uri,
            'loginName': login_name,
            'firstName': user_details.get('firstName'),
            'lastName': user_details.get('lastName'),
            'local': department_group,
            'workerClass': worker_class,
            'active': user.get('securityConfiguration', {}).get('isLoginEnabled', False)
        }

    return replicon_users


def get_dropdown_value_by_union_class(oef_id, user, dropdown_tags_data, exception_messages):
    emp_union = user['union']
    emp_class = user['class']
    if (not emp_union or emp_union.strip() == '') and (not emp_class or emp_class.strip() == ''):
        return None

    if emp_class and emp_class.strip() != '':
        if not emp_union or emp_union.strip() == '':
            emp_union = 'NON-UNION'

        combined_key = f"{emp_union} : {emp_class}".strip()

        oef_data = next(
            (item for item in dropdown_tags_data if item['id'] == oef_id), None)
        if not oef_data or not oef_data.get('tags'):
            exception_messages.append(
                f"Dropdown OEF '{oef_id}' tags not found in Replicon. ")
            return None

        matched_tag = next((tag for tag in oef_data['tags'] if tag['name'].upper(
        ) == combined_key.upper()), None)

        if not matched_tag:
            exception_messages.append(
                f"Worker Class value '{combined_key}' not found in Replicon dropdown options. ")
            return None

        return matched_tag['uri']

    return None
