"""
Common utility methods for VP UKG Pro Employee Sync integration.
"""
import logging
import rail
import pycountry


def format_date_to_yyyy_mm_dd(date_value):
    """Format date to YYYY-MM-DD format"""
    if not date_value:
        return None
    if isinstance(date_value, str) and 'T' in date_value:
        return date_value.split('T')[0]
    return date_value


def get_supervisor_employee():
    """Get supervisor Employee ID from get_supervisor_from_vp task result"""
    supervisor_result = rail.result('get_supervisor_from_vp')
    if (supervisor_result and isinstance(supervisor_result, list) and
            len(supervisor_result) > 0):
        return supervisor_result[0].get('Employee')
    return None


def get_country_code(country_value):
    """Convert country name/code to 2-letter ISO code"""
    if not country_value:
        return None

    val = str(country_value).strip().upper()

    if len(val) == 2:
        country = pycountry.countries.get(alpha_2=val)
        return country.alpha_2 if country else None

    try:
        return pycountry.countries.search_fuzzy(val)[0].alpha_2
    except LookupError:
        return None


def get_billing_category():
    """Find billing category Code where Description matches job group code"""
    conf = rail.get_current_context()['dag_run'].conf
    job_group_code = conf.get('jobGroupCode', None)

    if not job_group_code:
        return None

    billing_categories = rail.result('get_billing_categories_from_vp')
    if not billing_categories or not isinstance(billing_categories, list):
        return None

    for category in billing_categories:
        if category.get('Description') == job_group_code:
            return category.get('Category')

    return None


def check_job_title_match():
    """Check if job title from UKG Pro exists in Vantagepoint"""
    job_titles = rail.result('get_job_titles_from_vp')
    target_job_title = (
        rail.get_current_context()['dag_run'].conf.get('jobTitle')
    )

    if not job_titles or not isinstance(job_titles, list):
        return False

    for job_title in job_titles:
        if job_title.get('Code') == target_job_title:
            return True

    return False


def check_required_fields_present():
    """Check if required organizational fields are present"""
    data = rail.result('combine_employee_data')
    company_code = data.get('companyCode')
    org_level2 = data.get('orgLevel2Code')
    org_level3 = data.get('orgLevel3Code')
    return all([company_code, org_level2, org_level3])


def check_organization_exists_in_vp():
    """Check if organization exists in Vantagepoint"""
    organizations = rail.result('get_organizations_from_vp')
    employee_data = rail.result('combine_employee_data')

    company_code = employee_data.get('companyCode', '')
    org_level3 = employee_data.get('orgLevel3Code', '')
    org_level2 = employee_data.get('orgLevel2Code', '')

    search_key = f"{company_code}:{org_level3}:{org_level2}"

    if organizations and isinstance(organizations, list):
        for org in organizations:
            if org.get('Org') == search_key:
                return org.get('Name') is not None

    return False


def check_employee_status_codes_match():
    """Check if both UKG Pro and VP statuses are Active"""
    ukgpro_status = (
        rail.result('combine_employee_data').get('employeeStatusCode')
    )
    vp_employees = rail.result('get_employee_from_vp')
    vp_status = vp_employees[0].get('Status') if vp_employees else None

    return ukgpro_status == 'A' and vp_status == 'A'


def check_rehire_status_method():
    """Check if UKG Pro is Active and VP is Terminated"""
    ukgpro_status = (
        rail.result('combine_employee_data').get('employeeStatusCode')
    )
    vp_employees = rail.result('get_employee_from_vp')
    vp_status = vp_employees[0].get('Status') if vp_employees else None

    return ukgpro_status == 'A' and vp_status == 'T'


def fail_missing_fields_for_update():
    """Fail when org fields are missing for an employee update"""
    data = rail.result('combine_employee_data')
    raise RuntimeError(
        f"Employee {data.get('employeeNumber', 'Unknown')} update "
        f"failed: company/region/department fields missing from UKG Pro "
        f"(companyCode={data.get('companyCode', 'Missing')}, "
        f"orgLevel2Code={data.get('orgLevel2Code', 'Missing')}, "
        f"orgLevel3Code={data.get('orgLevel3Code', 'Missing')})"
    )


def fail_organization_not_found_for_update():
    """Fail when organization is not found in VP for an employee update"""
    data = rail.result('combine_employee_data')
    raise RuntimeError(
        f"Employee {data.get('employeeNumber', 'Unknown')} update "
        f"failed: organization "
        f"{data.get('companyCode', '')}:{data.get('orgLevel3Code', '')}:"
        f"{data.get('orgLevel2Code', '')} "
        f"not found in Vantagepoint"
    )


def fail_missing_fields_for_rehire():
    """Fail when org fields are missing for an employee rehire"""
    data = rail.result('combine_employee_data')
    raise RuntimeError(
        f"Employee {data.get('employeeNumber', 'Unknown')} rehire "
        f"failed: company/region/department fields missing from UKG Pro "
        f"(companyCode={data.get('companyCode', 'Missing')}, "
        f"orgLevel2Code={data.get('orgLevel2Code', 'Missing')}, "
        f"orgLevel3Code={data.get('orgLevel3Code', 'Missing')})"
    )


def fail_organization_not_found_for_rehire():
    """Fail when organization is not found in VP for an employee rehire"""
    data = rail.result('combine_employee_data')
    raise RuntimeError(
        f"Employee {data.get('employeeNumber', 'Unknown')} rehire "
        f"failed: organization "
        f"{data.get('companyCode', '')}:{data.get('orgLevel3Code', '')}:"
        f"{data.get('orgLevel2Code', '')} "
        f"not found in Vantagepoint"
    )


def fail_status_conditions_not_met():
    """Fail when employee status combination does not match any sync scenario
    """
    data = rail.result('combine_employee_data')
    vp_employees = rail.result('get_employee_from_vp')
    vp_status = (
        vp_employees[0].get('Status', 'Unknown') if vp_employees else 'Unknown'
    )
    raise RuntimeError(
        f"Employee {data.get('employeeNumber', 'Unknown')} skipped: "
        f"status combination not valid for any sync scenario "
        f"(UKG Pro={data.get('employeeStatusCode', 'Unknown')}, "
        f"VP={vp_status})"
    )


def fail_both_systems_inactive():
    """Fail when employee is inactive in both UKG Pro and Vantagepoint"""
    data = rail.result('combine_employee_data')
    raise RuntimeError(
        f"Employee {data.get('employeeNumber', 'Unknown')} not processed: "
        f"inactive in both UKG Pro and Vantagepoint"
    )


def fail_multiple_employees_found():
    """Fail when more than one VP employee record is found for the same number
    """
    data = rail.result('combine_employee_data')
    vp_employees = rail.result('get_employee_from_vp')
    raise RuntimeError(
        f"Employee {data.get('employeeNumber', 'Unknown')} update failed: "
        f"found {len(vp_employees)} records in Vantagepoint, expected 1"
    )


def fail_inactive_employee():
    """Fail when employee is inactive in UKG Pro and does not exist in VP"""
    data = rail.result('combine_employee_data')
    raise RuntimeError(
        f"Employee {data.get('employeeNumber', 'Unknown')} skipped: "
        f"inactive in UKG Pro "
        f"(status={data.get('employeeStatusCode', 'Unknown')})"
    )


def fail_missing_fields():
    """Fail when org fields are missing for an employee create"""
    data = rail.result('combine_employee_data')
    raise RuntimeError(
        f"Employee {data.get('employeeNumber', 'Unknown')} create "
        f"failed: company/region/department fields missing from UKG Pro "
        f"(companyCode={data.get('companyCode', 'Missing')}, "
        f"orgLevel2Code={data.get('orgLevel2Code', 'Missing')}, "
        f"orgLevel3Code={data.get('orgLevel3Code', 'Missing')})"
    )


def fail_organization_not_found():
    """Fail when organization is not found in VP for an employee create"""
    data = rail.result('combine_employee_data')
    raise RuntimeError(
        f"Employee {data.get('employeeNumber', 'Unknown')} create "
        f"failed: organization "
        f"{data.get('companyCode', '')}:{data.get('orgLevel3Code', '')}:"
        f"{data.get('orgLevel2Code', '')} "
        f"not found in Vantagepoint"
    )


def collect_triggered_dagrun_ids():
    """Collect dag run(s) from whichever trigger executed (create or update)"""
    dag_runs = []
    for task_id in ['trigger_employee_create', 'trigger_employee_update']:
        try:
            result = rail.result(task_id)
            if result is not None:
                dag_runs.append(result)
        except Exception:  # pylint: disable=broad-exception-caught
            pass
    return dag_runs


def capture_router_dag_error(employee_number, fallback_error_message):
    """
    Check child dag errors first (specific VP API errors).
    Fall back to this dag's own error message when no child errors.
    Returns error dict so the scheduler can gather it, or None on success.
    Returning (not raising) keeps this dag run as SUCCESS so the
    scheduler's WaitForDagRunsSensor never sees a failed dag run.
    """
    child_errors = []
    try:
        gathered = rail.result('gather_employee_dag_errors')
        if gathered:
            child_errors = (
                gathered if isinstance(gathered, list) else [gathered]
            )
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    if child_errors:
        error_message = ' | '.join(
            e.get('error', str(e)) for e in child_errors if e
        )
    elif fallback_error_message:
        error_message = (
            f"Employee {employee_number} - sync failed: "
            f"{fallback_error_message}"
        )
    else:
        return None

    return {'error': error_message}


def capture_create_error(employee_number, error_message):
    """Capture employee create error and return it for scheduler collection"""
    return {
        'error': (
            f"Employee {employee_number} - create failed: "
            f"{error_message}"
        )
    }


def warn_supervisor_not_found_for_create():
    """Warn that supervisor was not found; employee will be created without one
    """
    conf = rail.get_current_context()['dag_run'].conf
    logging.warning(
        "Supervisor not assigned for employee %s - supervisor %s not "
        "found in Vantagepoint. Employee will be created without supervisor.",
        conf.get('employeeNumber'),
        conf.get('supervisorEmployeeNumber')
    )


def capture_update_error(employee_number, update_type, error_message):
    """Capture employee update/rehire/termination error for scheduler
    collection
    """
    return {
        'error': (
            f"Employee {employee_number} ({update_type}) - "
            f"update failed: {error_message}"
        )
    }


def warn_supervisor_not_found_for_update():
    """Warn that supervisor was not found; employee will be updated without one
    """
    conf = rail.get_current_context()['dag_run'].conf
    logging.warning(
        "Supervisor not assigned for employee %s - supervisor %s not "
        "found in Vantagepoint. Employee will be updated without supervisor.",
        conf.get('employeeNumber'),
        conf.get('supervisorEmployeeNumber')
    )


def check_termination_status_method():
    """Check if UKG Pro is Terminated with date"""
    ukgpro_status = (
        rail.result('combine_employee_data').get('employeeStatusCode')
    )
    date_of_termination = (
        rail.result('combine_employee_data').get('dateOfTermination')
    )

    return (ukgpro_status == 'T' and date_of_termination is not None and
            date_of_termination != '')
