
from datetime import timedelta
import rail
from adtalem.custom_email_notification.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'{config.company_key}_custom_email_notification_master_{config.instance}',
        description=f'Live|Adtalem_Custom Email Notification Master_10 AM CST {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval="0 14,17 * * *",
        max_active_runs=1,
    ) as dag:

        trigger_check = rail.IfOperator(
            task_id='trigger_check',
            test=python_callable.check_for_trigger_day,
            yes_task="get_holidays_in_date_range",
            no_task="finish",
        )

        get_holidays_in_date_range = rail.RepliconServiceOperator(
            task_id='get_holidays_in_date_range',
            endpoint="/services/HolidayCalendarService2.svc/GetHolidaysInDateRange",
            data=lambda: {
                "holidayCalendarUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":holiday-calendar:15",
                "dateRange": null
            }
        )

        date_today = rail.PythonOperator(
            task_id='date_today',
            python_callable=python_callable.get_date_today
        )

        get_holiday_list = rail.PythonOperator(
            task_id='get_holiday_list',
            python_callable=python_callable.get_currrent_or_future_days_list
        )
        get_coming_payroll_details = rail.PythonOperator(
            task_id='get_coming_payroll_details',
            python_callable=python_callable.get_coming_date
        )

        check_for_regular_timesheet_reminder = rail.IfOperator(
            task_id='check_for_regular_timesheet_reminder',
            test="{{ result('get_coming_payroll_details').date_difference == 3 and result('get_coming_payroll_details').name == 'Regular' }}",
            yes_task="custom_email_notification_regular_timesheet_reminder_child",
            no_task="get_accelerated_payroll_details",
        )

        custom_email_notification_regular_timesheet_reminder_child = rail.TriggerDagRunForEachItemOperator(
            task_id='custom_email_notification_regular_timesheet_reminder_child',
            retries=0,
            items=lambda: [rail.result('get_coming_payroll_details')],
            trigger_dag_id=f'{config.company_key}_custom_email_notification_timesheet_reminder_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "today": rail.result('date_today'),
                "type": rail.result('get_coming_payroll_details').get('name'),
                "companykey": rail.get_company_key(),
                "swimlane": rail.get_tenant_slug(),
                "payrolldate": rail.result('get_coming_payroll_details').get('date'),
                "slug": rail.get_tenant_slug()
            }
        )

        wait_for_regular_timesheet_reminder_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_regular_timesheet_reminder_child',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("custom_email_notification_regular_timesheet_reminder_child") }}'
        )
        get_accelerated_payroll_details = rail.PythonOperator(
            task_id='get_accelerated_payroll_details',
            python_callable=python_callable.get_accelerated_payroll_details
        )
        check_for_accelerated_timesheet_reminder = rail.IfOperator(
            task_id='check_for_accelerated_timesheet_reminder',
            test="{{ result('get_accelerated_payroll_details').reminder == result('get_coming_payroll_details').date_difference and result('get_coming_payroll_details').name == 'Accelerated' }}",
            yes_task="custom_email_notification_accelerated_timesheet_reminder_child",
            no_task="check_for_pay_at_risk_trigger",
        )

        custom_email_notification_accelerated_timesheet_reminder_child = rail.TriggerDagRunForEachItemOperator(
            task_id='custom_email_notification_accelerated_timesheet_reminder_child',
            retries=0,
            items=lambda: [rail.result('get_coming_payroll_details')],
            trigger_dag_id=f'{config.company_key}_custom_email_notification_timesheet_reminder_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "today": rail.result('date_today'),
                "type": rail.result('get_coming_payroll_details').get('name'),
                "companykey": rail.get_company_key(),
                "swimlane": rail.get_tenant_slug(),
                "payrolldate": rail.result('get_coming_payroll_details').get('date'),
                "slug": rail.get_tenant_slug()
            }
        )

        wait_for_accelerated_timesheet_reminder_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_accelerated_timesheet_reminder_child',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("custom_email_notification_accelerated_timesheet_reminder_child") }}'
        )

        check_for_pay_at_risk_trigger = rail.IfOperator(
            task_id='check_for_pay_at_risk_trigger',
            test='''{{ result('get_coming_payroll_details').date_difference == 0 }}''',
            yes_task="custom_email_notification_timesheet_reminder_pay_at_risk_child",
            no_task="finish",
        )

        custom_email_notification_timesheet_reminder_pay_at_risk_child = rail.TriggerDagRunForEachItemOperator(
            task_id='custom_email_notification_timesheet_reminder_pay_at_risk_child',
            retries=0,
            items=lambda: [rail.result('get_coming_payroll_details')],
            trigger_dag_id=f'{config.company_key}_custom_email_notification_for_pay_at_risk_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "today": rail.result('date_today'),
                "type": item.get('name'),
                "companykey": rail.get_company_key(),
                "swimlane": rail.get_tenant_slug(),
                "slug": rail.get_tenant_slug()
            }
        )

        wait_for_completion_timesheet_reminder_pay_at_risk_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_timesheet_reminder_pay_at_risk_child',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("custom_email_notification_timesheet_reminder_pay_at_risk_child") }}'
        )

        custom_email_notification_timesheet_reminder_paycheck_at_risk_child = rail.TriggerDagRunForEachItemOperator(
            task_id='custom_email_notification_timesheet_reminder_paycheck_at_risk_child',
            retries=0,
            items=lambda: [rail.result('get_coming_payroll_details')],
            trigger_dag_id=f'{config.company_key}_custom_email_notification_for_paycheck_at_risk_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "today": rail.result('date_today'),
                "type": item.get('name'),
                "companykey": rail.get_company_key(),
                "swimlane": rail.get_tenant_slug(),
                "slug": rail.get_tenant_slug()
            }
        )

        wait_for_completion_timesheet_reminder_paycheck_at_risk_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_timesheet_reminder_paycheck_at_risk_child',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("custom_email_notification_timesheet_reminder_paycheck_at_risk_child") }}'
        )
        finish = rail.EmptyOperator(
            task_id='finish',
        )
        trigger_check >> rail.Label(
            'Yes') >> get_holidays_in_date_range
        trigger_check >> rail.Label(
            'No') >> finish
        get_holidays_in_date_range >> date_today >> get_holiday_list >> get_coming_payroll_details >> check_for_regular_timesheet_reminder
        check_for_regular_timesheet_reminder >> rail.Label(
            'Yes') >> custom_email_notification_regular_timesheet_reminder_child >> wait_for_regular_timesheet_reminder_child >> get_accelerated_payroll_details >> check_for_accelerated_timesheet_reminder
        check_for_regular_timesheet_reminder >> rail.Label(
            'No') >> get_accelerated_payroll_details >> check_for_accelerated_timesheet_reminder
        check_for_accelerated_timesheet_reminder >> rail.Label(
            'Yes') >> custom_email_notification_accelerated_timesheet_reminder_child >> wait_for_accelerated_timesheet_reminder_child >> check_for_pay_at_risk_trigger
        check_for_accelerated_timesheet_reminder >> rail.Label(
            'No') >> check_for_pay_at_risk_trigger
        check_for_pay_at_risk_trigger >> rail.Label(
            'Yes') >> custom_email_notification_timesheet_reminder_pay_at_risk_child >> wait_for_completion_timesheet_reminder_pay_at_risk_child >> custom_email_notification_timesheet_reminder_paycheck_at_risk_child >> wait_for_completion_timesheet_reminder_paycheck_at_risk_child >> finish
        check_for_pay_at_risk_trigger >> rail.Label(
            'No') >> finish

    return dag


rail.for_each_instance(create_dag)
