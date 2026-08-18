# Time Off Project Mapping by Employee ID Prefix
# Maps Employee ID prefix (first 2 chars) to Costpoint leave project details
# Used to route sick leave entries to the correct leave project

timeoff_project_by_employee_prefix = {
    '03': {
        'project': 'LEAVE1.SIC',
        'pay_type': 'SIC',
        'company': 'Cape Fox Lodge (CFL)'
    },
    '04': {
        'project': 'LEAVE1.SIC',
        'pay_type': 'SIC',
        'company': 'Cape Fox (CFT)'
    },
    '08': {
        'project': 'LEAVE1.SIC',
        'pay_type': 'SIC',
        'company': 'Cape Fox (CFPM)'
    },
    '32': {
        'project': 'LEAVE3.SIC',
        'pay_type': 'SIC',
        'company': 'Cape Fox Nonprofit (CFCF)'
    },
}
