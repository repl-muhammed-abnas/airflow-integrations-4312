import itertools
null = None


def get_task_resource(response):
    resource_assignments_resp = response.json()['d']
    return list(itertools.chain(*list(map(lambda resource: list(map(lambda assignment_data: {
        "resource_uri": assignment_data["resource"]["departmentGroup"]["uri"]
        if assignment_data is not null and assignment_data["resource"] is not null
                                and assignment_data["resource"]["departmentGroup"] is not null
                                and assignment_data["resource"]["departmentGroup"]["uri"] is not null else null,
                                "task_uri": resource["taskUri"] if resource and resource["taskUri"] is not null else null
                                }, resource["assignments"])), resource_assignments_resp))))
