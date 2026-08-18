POLARIS_TO_REPLICON = {
    'Initiate': 'Tentative',
    'Planning': 'Planning',
    'Execution': 'In Progress',
    'Closeout': 'Completed'
}


PROJECT_STATUS_URI = {
    'Tentative': 'urn:replicon:project-status-type:tentative',
    'In Progress': 'urn:replicon:project-status-type:in-progress',
    'Completed': 'urn:replicon:project-status-type:completed',
    'Deferred': 'urn:replicon:project-status-type:deferred',
    'Cancelled': 'urn:replicon:project-status-type:cancelled',
    'Archived': 'urn:replicon:project-status-type:archived'
}


def get_project_details(replicon_projects, opportunity: str):
    return next((x for x in replicon_projects if x['name'] == opportunity), None)


def get_project_status(conf):
    for item in conf['customSettings']['statusSyncCriteria']:
        start, end = map(float, item['key'].split(':'))
        if start <= float(conf['probability']) <= end:
            return item['value']
    return None
