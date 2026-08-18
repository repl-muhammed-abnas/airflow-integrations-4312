"""
Employee Create DAG for VP UKG Pro Employee Sync.
Creates new employees in Vantagepoint from UKG Pro data.
"""
from datetime import timedelta
import rail
from vp_ukgpro_integration.employee_sync.utils.python_callable_method import (
    format_date_to_yyyy_mm_dd,
    get_supervisor_employee,
    get_country_code,
    get_billing_category,
    check_job_title_match,
    warn_supervisor_not_found_for_create,
    capture_create_error
)


# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
def create_dag(config):
    """
    Create DAG for creating new employees in Vantagepoint.

    Args:
        config: Configuration object with instance settings
    """
    with rail.create_airflow_dag(
        dag_id=f'vp_ukgpro_employee_sync_create_{config.instance}',
        description='Sync employees from UKG Pro to Vantagepoint',
        company_key=config.company_key,
        integration_type='generic',
        multi_tenant=True,
        max_active_runs=config.max_active_runs,
        schedule_interval=None,
        tags=['vantagepoint_ukgpro', 'employee_sync', 'create_employee'],
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        get_billing_categories_from_vp = rail.VantagepointSettingsListOperator(
            task_id='get_billing_categories_from_vp',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/codeTable/BTLaborCats',
            request_method='GET'
        )

        is_job_title_exist_in_ukgpro = rail.IfOperator(
            task_id='is_job_title_exist_in_ukgpro',
            test=lambda dag_run: (
                dag_run.conf.get('jobTitle') is not None and
                dag_run.conf.get('jobTitle') != ''
            ),
            yes_task='get_job_titles_from_vp',
            no_task='is_supervisor_exist_in_ukgpro'
        )

        get_job_titles_from_vp = rail.VantagepointSettingsListOperator(
            task_id='get_job_titles_from_vp',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/codeTable/CFGEmployeeTitle',
            request_method='GET'
        )

        is_job_title_found_in_vp = rail.IfOperator(
            task_id='is_job_title_found_in_vp',
            test=check_job_title_match,
            yes_task='is_supervisor_exist_in_ukgpro',
            no_task='create_job_title_in_vp'
        )

        create_job_title_in_vp = rail.VantagepointSettingsListOperator(
            task_id='create_job_title_in_vp',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/codeTable/CFGEmployeeTitle',
            request_method='POST',
            request_body=lambda: {
                "Code": (
                    rail.get_current_context()['dag_run'].conf.get('jobTitle')
                ),
                "Description": (
                    rail.get_current_context()['dag_run'].conf.get(
                        'jobDescription'
                    )
                )
            }
        )

        is_supervisor_exist_in_ukgpro = rail.IfOperator(
            task_id='is_supervisor_exist_in_ukgpro',
            test=lambda dag_run: (
                dag_run.conf.get('supervisorEmployeeNumber') is not None and
                dag_run.conf.get('supervisorEmployeeNumber') != ''
            ),
            yes_task='get_supervisor_from_vp',
            no_task='create_employee_in_vp'
        )

        get_supervisor_from_vp = rail.VantagepointEmployeeOperator(
            task_id='get_supervisor_from_vp',
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/employee',
            request_method='GET',
            filters=lambda: (
                f'?filterHash[0][name]=ADPFileNumber&filterHash[0][value]='
                f'{rail.get_current_context()["dag_run"].conf.get("supervisorEmployeeNumber")}'  # noqa: E501
            )
        )

        is_supervisor_exist_in_vp = rail.IfOperator(
            task_id='is_supervisor_exist_in_vp',
            test=lambda: len(rail.result('get_supervisor_from_vp')) > 0,
            yes_task='create_employee_in_vp',
            no_task='log_supervisor_not_found'
        )

        log_supervisor_not_found = rail.PythonOperator(
            task_id='log_supervisor_not_found',
            python_callable=warn_supervisor_not_found_for_create
        )

        catch_employee_dag_error = rail.PythonOperator(
            task_id='catch_employee_dag_error',
            trigger_rule='one_failed',
            python_callable=capture_create_error,
            op_args=[
                '{{ dag_run.conf.employeeNumber }}',
                '{{ get_error_message() }}'
            ]
        )

        def build_create_employee_request_body():
            """Build request body for creating employee in Vantagepoint"""
            conf = rail.get_current_context()['dag_run'].conf

            company_code = conf.get('companyCode', '')
            org_level3 = conf.get('orgLevel3Code', '')
            org_level2 = conf.get('orgLevel2Code', '')
            org_key = f"{company_code}:{org_level3}:{org_level2}"

            request_body = {
                "Employee": "[AUTONUMBER]",
                "FirstName": conf.get('firstName'),
                "LastName": conf.get('lastName'),
                "MiddleName": conf.get('middleName'),
                "EMail": conf.get('emailAddress'),
                "WorkPhone": conf.get('workPhoneNumber'),
                "Status": conf.get('employeeStatusCode'),
                "HireDate": format_date_to_yyyy_mm_dd(
                    conf.get('originalHireDate')
                ),
                "EmployeeCompany": company_code,
                "Org": org_key,
                "HomeCompany": company_code,
                "Supervisor": get_supervisor_employee(),
                "Address1": conf.get('addressLine1'),
                "Address2": conf.get('addressLine2'),
                "City": conf.get('addressCity'),
                "State": conf.get('addressState'),
                "ZIP": conf.get('addressZipCode'),
                "Country": get_country_code(conf.get('addressCountry')),
                "PreferredName": conf.get('preferredName'),
                "Title": conf.get('jobTitle'),
                "Type": "E",
                "EmployeeCompanyName": conf.get('companyName'),
                "Location": conf.get('primaryWorkLocationCode'),
                "ReadyForProcessing": conf.get(
                    'approved_for_use_in_processing'
                ),
                "ReadyForApproval": conf.get('approved_for_accounting_users'),
                "BillingCategory": get_billing_category(),
                "AvailableForCRM": "N",
                "JobCostRate": conf.get('payRate'),
                "JCOvtPct": conf.get('jobCostOvertimePercent'),
                "JCSpecialOvtPct": conf.get('jobCostOvertime2Percent'),
                "JobCostType": conf.get('payPeriod'),
                "ADPFileNumber": conf.get('employeeNumber')
            }

            return {k: v for k, v in request_body.items() if v is not None}

        create_employee_in_vp = rail.VantagepointEmployeeOperator(
            task_id="create_employee_in_vp",
            vp_conn_id="{{ dag_run.conf.connections.vantagepoint }}",
            endpoint='/employee',
            request_method='POST',
            request_body=build_create_employee_request_body
        )

        get_billing_categories_from_vp >> is_job_title_exist_in_ukgpro
        (
            is_job_title_exist_in_ukgpro >>
            rail.Label('Job title exists in ukg pro') >>
            get_job_titles_from_vp
        )
        (
            is_job_title_exist_in_ukgpro >>
            rail.Label('Job title not found in ukg pro') >>
            is_supervisor_exist_in_ukgpro
        )

        get_job_titles_from_vp >> is_job_title_found_in_vp
        (
            is_job_title_found_in_vp >>
            rail.Label('Job title exists in vp') >>
            is_supervisor_exist_in_ukgpro
        )
        (
            is_job_title_found_in_vp >>
            rail.Label('Job title not found in vp') >>
            create_job_title_in_vp
        )

        create_job_title_in_vp >> is_supervisor_exist_in_ukgpro
        (
            is_supervisor_exist_in_ukgpro >>
            rail.Label('Supervisor exists in ukg pro') >>
            get_supervisor_from_vp
        )
        (
            is_supervisor_exist_in_ukgpro >>
            rail.Label('Supervisor not found in ukg pro') >>
            create_employee_in_vp
        )

        get_supervisor_from_vp >> is_supervisor_exist_in_vp
        (
            is_supervisor_exist_in_vp >>
            rail.Label('Supervisor exists in vp') >>
            create_employee_in_vp
        )
        (
            is_supervisor_exist_in_vp >>
            rail.Label('Supervisor not found in vp') >>
            log_supervisor_not_found
        )

        log_supervisor_not_found >> create_employee_in_vp

        create_employee_in_vp >> catch_employee_dag_error

        return dag


rail.for_each_instance(create_dag)
