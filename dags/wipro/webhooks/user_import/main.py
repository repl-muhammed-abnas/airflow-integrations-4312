from datetime import timedelta
from pendulum import datetime
from airflow.models import Variable
from wipro.webhooks.user_import.task.process_each_country import process_country
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"wipro_user_import_{config.instance}",
        description="Wipro User import master dag (Endpoint)",
        start_date=datetime(2023, 11, 1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        webhook_conf=[
            rail.WebhookConf(
                bearer_token_var=config.wipro_user_import_bearer_token_variable_trial)
        ]
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")


        can_process_payload = rail.IfOperator(
            task_id='can_process_payload',
            test=lambda: Variable.get(
                config.can_process_payload_var, default_var="true").lower() == 'true',
            yes_task='process_employee_data'
        )

        def get_employee_data(dag_run):
            if dag_run.conf["webhook"]["data"].get("ns0:Message1"):
                dag_run.conf["webhook"]["data"] = dag_run.conf["webhook"]["data"]["ns0:Message1"]["root"]["item"]
            if isinstance(dag_run.conf['webhook']['data']['item'], dict):
                dag_run.conf['webhook']['data']['item'] = [dag_run.conf['webhook']['data']['item']] 
            return dag_run.conf['webhook']['data']['item'] 

        process_employee_data = rail.PythonOperator(
            task_id="process_employee_data",
            python_callable=get_employee_data
        )

        create_user_record_collection = rail.CreateCollectionOperator(
            task_id="create_user_record_collection",
            source='{{dag_run.conf.webhook.data.item|to_json}}',
            columns={'EMPLOYEE_ID': 'employee_id', 'EMPLOYEE_FIRST_NAME': 'employee_first_name', 'EMPLOYEE_LAST_NAME': 'employee_last_name',
                    'EMPLOYEE_EMAIL_ID': 'employee_email_id', 'PRIMARY_SUPERVISOR_ID': 'primary_supervisor_id', 
                    'PRIMARY_SUPERVISOR_MAILID': 'primary_supervisor_mailid', 'PRIMARY_SUPERVISOR_ADID': 'primary_supervisor_adid', 
                    'PROJECT_SUPERVISOR_ID': 'project_supervisor_id', 'PROJECT_SUPERVISOR_MAILID': 'project_supervisor_mailid', 
                    'HR_MANAGER_ID': 'hr_manager_id', 'HR_MANAGER_MAILID': 'hr_manager_mailid', 'GENDER': 'gender', 'ACQUIRED': 'acquired', 
                    'ACQUIRED_COMPANY': 'acquired_company', 'ACQUIRED_DOJ': 'acquired_doj', 
                    'BILLABILITY_STATUS': 'billability_status', 'DATE_OF_JOINING': 'date_of_joining', 
                    'COUNTRY': 'country', 'HIRING_STATUS': 'hiring_status', 'MARITAL_STATUS': 'marital_status', 
                    'ACTION': 'action', 'ACTION_REASON': 'action_reason', 'ONSITE_DIRECT_RECRUIT': 'onsite_direct_recruit', 
                    'ONSITE_END_DATE': 'onsite_end_date', 'TRAVEL_END_DATE': 'travel_end_date', 
                    'ONSITE_START_DATE': 'onsite_start_date', 'SALES_IDENTIFIER': 'sales_identifier', 
                    'RESIGN_DATE': 'resign_date', 'REVERSAL_DATE': 'reversal_date', 
                    'PRIMARY_MANAGER_FLG': 'primary_manager_flg', 'PROJECT_MANAGER_FLG': 'project_manager_flg', 
                    'HR_MANAGER_FLG': 'hr_manager_flg', 'LOCATION': 'location', 'EMPLOYMENT_STATUS': 'employment_status', 
                    'ARD_LRD': 'ard_lrd', 'NO_OF_CHILDREN': 'no_of_children', 'COMPANY_CODE': 'company_code', 
                    'DATE_OF_BIRTH': 'date_of_birth', 'PERSONNEL_AREA_TEXT': 'personnel_area_text', 
                    'PERSONNEL_SUBAREA_TEXT': 'personnel_subarea_text', 'EMPLOYEE_GROUP_TEXT': 'employee_group_text', 
                    'COST_CENTER': 'cost_center', 'INSURANCE_TYPE': 'insurance_type', 
                    'GPO_ID': 'gpo_id', 'GPO_EMAIL_ID': 'gpo_email_id', 
                    'WORK_COUNCIL_TAGGING': 'work_council_tagging', 
                    'EXEMPT_NON_EXEMPT': 'exempt_non_exempt', 'EMPLOYEE_UTILISATION': 'employee_utilisation', 
                    'TRAVEL_START_DATE': 'travel_start_date', 'EMPLOYEE_BAND': 'employee_band', 'FLAG': 'flag',
                    'PAYROLL_START_DATE': 'payroll_start_date', 'PAYROLL_END_DATE': 'payroll_end_date', 
                    'EMPLOYMENT_PERCENTAGE': 'employment_percentage', 'FORFAIT_EMP_IDENTIFIER': 'forfait_emp_identifier', 
                    'WFH': 'wfh', 'ADID': 'adid', 'PAYMENT_MODEL_NET_OR_GROSS': 'payment_model_net_or_gross',
                    'HR_ADID': 'hr_adid', 
                    'GPO_ADID': 'gpo_adid', 'DEPARTMENT': 'department',
                    'PROJECT_SUPERVISOR_ADID': 'project_supervisor_adid', 'RELIGION': 'religion',
                    "1st Language": "_1st_language",
                    "2nd Language": "_2nd_language",
                    "Education": "education",
                    "Experience":"experience", "Degree Date":"degree_date",
                    "NIGHT_HOURS_ELIGIBILITY": "night_hours_eligibility",
                    "CAPABILITY":"capability"},
            name="userdeltarecords"
        )

        process_users_nl = process_country("process_users_netherlands",config,str("Netherlands").lower(),
                                                                       "Netherlands",config.trigger_dag_id["netherlands"])

        process_users_ksa = process_country("process_users_saudi_arabia",config,str("Saudi Arabia").replace(" ", "_").lower(),
                                                                          "Saudi Arabia",config.trigger_dag_id["saudi_arabia"])
        process_users_pol = process_country("process_users_poland",config,str("Poland").lower(),
                                                                         "Poland",config.trigger_dag_id["poland"] )

        process_users_ro = process_country("process_users_romania",config,str("Romania").replace(" ", "_").lower(),
                                                                       "Romania", config.trigger_dag_id["romania"])

        process_users_por = process_country("process_users_portugal",config,str("Portugal").lower(),
                                                                         "Portugal", config.trigger_dag_id["portugal"])

        process_users_ger = process_country("process_users_germany",config,str("Germany").lower(),
                                                                         "Germany", config.trigger_dag_id["germany"])

        process_users_ire = process_country("process_users_ireland",config,str("Ireland").lower(),
                                                                         "Ireland", config.trigger_dag_id["ireland"])

        process_users_bel = process_country("process_users_belgium",config,str("Belgium").lower(),
                                                                         "Belgium", config.trigger_dag_id["belgium"])

        process_users_spain = process_country("process_users_spain",config,str("Spain").lower(),
                                                                         "Spain", config.trigger_dag_id["spain"])

        process_users_uk = process_country("process_users_uk",config,str("united_kingdom").lower(),
                                                                         "United Kingdom", config.trigger_dag_id["united_kingdom"])


        process_users_switzerland = process_country("process_users_switzerland",config,"switzerland",
                                                                         "Switzerland", config.trigger_dag_id["switzerland"])

        process_users_austria = process_country("process_users_austria",config,"austria",
                                                                         "Austria", config.trigger_dag_id["austria"])

        process_users_france = process_country("process_users_france",config,"france",
                                                                         "France", config.trigger_dag_id["france"])


        can_process_payload >> rail.Label(
            "Yes") >> process_employee_data >>\
        create_user_record_collection >> process_users_nl
        create_user_record_collection >> process_users_ksa
        create_user_record_collection >> process_users_pol
        create_user_record_collection >> process_users_ro
        create_user_record_collection >> process_users_por
        create_user_record_collection >> process_users_ger
        create_user_record_collection >> process_users_ire
        create_user_record_collection >> process_users_spain
        create_user_record_collection >> process_users_uk
        create_user_record_collection >> process_users_bel
        create_user_record_collection >> process_users_switzerland
        create_user_record_collection >> process_users_austria
        create_user_record_collection >> process_users_france

    return dag


rail.for_each_instance(create_main_dag)