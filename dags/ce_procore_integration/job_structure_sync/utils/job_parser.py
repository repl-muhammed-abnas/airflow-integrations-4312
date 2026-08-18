import rail


def combine_address_lines(address1, address2, address3):
    full_address = ""
    address_lines = [
        (address1 or '').strip(),
        (address2 or '').strip(),
        (address3 or '').strip()
    ]

    for line in address_lines:
        if line and line.lower() not in full_address.lower():
            if full_address:
                full_address += " " + line
            else:
                full_address = line

    return full_address


def parse_job_data(job, department_lookup=None, project_template_udf_id=None):
    if not job:
        return None

    if department_lookup is None:
        department_lookup = {}

    full_address = combine_address_lines(
        job.get('address'),
        job.get('address2'),
        job.get('address3')
    )

    job_dept = job.get('job_dept', '')
    department_ids = [department_lookup[job_dept]
                      ] if job_dept and job_dept in department_lookup else []

    project_template_udf_value = None
    if project_template_udf_id and job.get('user_field_values'):
        project_template_udf_value = rail.find_first_by_attr_and_get_attr(
            job['user_field_values'],
            'id',
            project_template_udf_id,
            'value'
        )

    return {
        'code': job.get('code', ''),
        'description': job.get('description', ''),
        'status': job.get('status') == 'active',
        'address': full_address,
        'city': job.get('city', ''),
        'state': job.get('state', ''),
        'zipcode': job.get('zipcode', ''),
        'jobdate_open': job.get('jobdate_open', ''),
        'jobdate_due': job.get('jobdate_due', ''),
        'department_ids': department_ids,
        'wbs_type': job.get('wbs_type', ''),
        'project_template_udf_value': project_template_udf_value or '',
        'customer_name': job.get('customer_name', ''),
        'customer_code': job.get('customer_code', '')
    }


def parse_phase_data(phase):
    if not phase:
        return None

    return {
        'code': phase.get('code', ''),
        'description': phase.get('description', ''),
        'status': phase.get('status', ''),
        'job_code': phase.get('job_code', ''),
    }


def parse_category_data(category):
    if not category:
        return None

    return {
        'code': category.get('code', ''),
        'description': category.get('description', ''),
        'status': category.get('status', ''),
        'job_code': category.get('job_code', ''),
        'phase_code': category.get('phase_code', ''),
    }
