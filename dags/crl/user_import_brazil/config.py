region = "us-east-1"

environment = "pre-production"

execution_timeout_days = 14
gather_user_logs_timeout_hours = 12

max_active_runs_process_user_import_payload = 1
max_active_runs_process_groups = 1
max_active_runs_process_buisness_unit = 1
max_active_runs_process_company_code = 1
max_active_runs_process_cost_center = 1
max_active_runs_process_location = 1
max_active_runs_process_new_departments = 1

max_active_runs_process_users = 20
max_active_runs_process_new_users = 20
max_active_runs_process_update_users = 20
max_active_runs_process_disable_users = 20
max_active_runs_process_supervisor = 20
max_active_runs_process_log_generation = 1

max_active_runs_process_timeoff_type_no_accrual = 20
max_active_runs_process_vacation_new_user = 20
max_active_runs_process_time_off_type_assignment_new_user = 20
max_active_runs_process_time_off_type_assignment_update_rehire_user = 20

disable_user_master_dag_active_runs = 1
disable_user_child_dag_active_runs = 2

trigger_parallel_dagrun_count_process_users = 15

ACTIVE_STATUS = ['Active','Paid Leave','Furlough','Dormant']
DISABLE_STATUS = ['Terminated','Unpaid Leave','Suspended','Retired','Discarted','Deceased']

BATCH_COUNT = 3

IGNORE_STATUS_ZERO_ACCRUAL = ['Unpaid Leave', 'Suspended']

DEFAULT_TIME_OFF_TYPE = "[BRA] Férias"

APPLICABLE_TIME_OFF_TYPES = [
    "[BRA] Folga Aniversário CRL",
    "[BRA] Folga Aniversário Funcionário",
    "[BRA] Recesso de Estágio",
    "[BRA] Maternidade",
    "[BRA] Licença remunerada",
    "[BRA] Licença Paternidade",
    "[BRA] Atestado Médico",
    "[BRA] Auxílio Doença",
    "[BRA] Licença temporária devido a acidente de trabalho",
    "[BRA] Débito de Horas – B.H.",
    "[BRA] Ausência injustificada",
    "[BRA] Férias"
]

GLOBAL_TIME_OFF_TYPES = [
    "[BRA] Folga Aniversário CRL",
    "[BRA] Folga Aniversário Funcionário",
    "[BRA] Recesso de Estágio",
    "[BRA] Maternidade",
    "[BRA] Licença remunerada",
    "[BRA] Licença Paternidade",
    "[BRA] Atestado Médico",
    "[BRA] Auxílio Doença",
    "[BRA] Licença temporária devido a acidente de trabalho",
    "[BRA] Débito de Horas – B.H.",
    "[BRA] Ausência injustificada",
    "[BRA] Férias"
]

PLACEHOLDER_BASED_TIMEOFF_TYPES = []

TO_PLACEHOLDER_HIDDEN_OEF_NAMES = []

MANNUAL_TIMEOFF_TYPES = []

sumo_conn_id = 'sumologic-dagrunlogger'

END_DATE_STATUS = ['Terminated','Retired','Discarted','Deceased']
