"""
Configuration for TransparentBPO User Import Integration
"""
# Environment Configuration
region = 'us-east-1'
environment = 'pre-production'

# Scheduling Configuration
master_dag_interval = 60

# This is without Daylight saving, always constant at -07:00 (as per workato)
time_zone = 'America/Phoenix'

# DATE TIME FORMATS
EMAIL_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
# this is as per production bambooHR instance config
BAMBOO_DATE_FORMAT = "YYYY-MM-DDTHH:mm:ssZ"
# Same is defined in Custom methods. If this needs to be changed, change there as well
DATE_FORMAT = "%Y-%m-%d"
LOG_TIMESTAMP = "%Y-%m-%dT%H"

# DAG configuration
max_active_runs_master = 1
max_active_runs_process_each_user = 5
max_active_runs_add_update_user = 5
max_active_runs_process_log_generation= 1

# Timeout Parameters
execution_timeout_days = 14
gather_user_logs_timeout_hours = 4

process_each_changed_user_parallel_count = 5

# Custom fields to fetch from BambooHR (based on Workato recipe)
BAMBOO_CUSTOM_FIELDS = [
    'customAdjustedWPM', 'customBadgeID', 'customBelizeInternetProvider',
    'customCASTScore', 'customCBSTRawScore', 'customCLIKScore',
    'customEmailLicense', 'customEmailSignatures', 'customEmmersionScore',
    'customEPP%-CustomerService', 'customEPP%-DataEntry',
    'customEPP%-Leadership1', 'customEPP%-Sales', 'customGAMEScore',
    'customHomeDevelopmentMutualFund(HDMF)Number', 'customInternationalSSN',
    'customJamaicaInternetProvider', 'customLaborLevel', 'customLanguagesSpoken',
    'customLineofBusiness', 'customNIS', 'customPhilHealth(PHIC)Number',
    'customPhilippinesInternetProvider', 'customReferredBy', 'customRegion',
    'customSchedule', 'customSocialSecuritySystem(SSS)Number',
    'customTaxIdentificationNumber', 'customTelephonyID', 'customTelephonySystem',
    'customTrainingBillingType', 'customInternetProvider',
    'customWeddingAnniversary', 'customWhatsApp1', 'customZohoCID'
]

# Standard fields to fetch
BAMBOO_STANDARD_FIELDS = [
    'id', 'employeeNumber', 'firstName', 'middleName', 'lastName',
    'preferredName', 'displayName', 'jobTitle', 'workEmail', 'homeEmail',
    'mobilePhone', 'workPhone', 'workPhoneExtension', 'department',
    'division', 'location', 'supervisor', 'supervisorId', 'supervisorEId',
    'status', 'hireDate', 'terminationDate', 'dateOfBirth', 'gender',
    'address1', 'address2', 'city', 'state', 'stateCode', 'zipcode', 'country',
    'maritalStatus', 'ethnicity', 'ssn', 'nickname', 'payRate',
    'payRateEffectiveDate', 'payType', 'payGroup', 'payGroupId',
    'flsaCode', 'employmentHistoryStatus',
    'homePhone', 'fullName1', 'fullName2', 'fullName3', 'fullName4', 'fullName5',
    'age', 'bestEmail', 'birthday', 'workPhonePlusExtension', 'photoUploaded',
    'photoUrl', 'lastChanged', 'payChangeReason', 'flsaEmployeeExemption'
]

MANDATORY_FIELDS = {
    'employeeNumber': 'Employee number',
    'firstName': 'First name',
    'lastName': 'Last name',
    'workEmail': 'Work email',
    'hireDate': 'Hire date',
    'status': 'Status',
    'location': 'Location',
    'department': 'Department',
    'jobTitle': 'Job title'
}

SUPERVISOR_PAYRULE_MAPPER = {
    "Belmopan Belize": "Belize Payrule",
    "Coney Belize": "Belize Payrule",
    "Seaside Belize": "Belize Payrule",
    "WFH Belize": "Belize Payrule",
    "Jamaica - Angels": "Jamaica Payrule",
    "Jamaica - WFH Angels": "Jamaica Payrule"
}


CUSTOM_TABLE_NAME = "customProgramInformation"
