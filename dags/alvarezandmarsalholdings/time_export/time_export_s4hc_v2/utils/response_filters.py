from datetime import datetime
from functools import lru_cache
import rail

null = None

EXPORT_DATE_FORMAT = "%Y-%m-%d"

def convert_date_to_export_formate(date_string: str) -> str:
    return datetime.strptime(date_string, '%Y/%m/%d').strftime(EXPORT_DATE_FORMAT)

def get_activitytype(row,SBU_CHAR_CODE, JOB_CATEGORY_CHAR_CODE):
    if row['cost_center_code'] and row['job_category_name']:
        sbu_code = rail.find_first_by_attr_and_get_attr(SBU_CHAR_CODE, 'sbu_derived_from_costcenter', row['cost_center_code'][4:7], 'sbu_2_character_code','')
        job_cat_code = rail.find_first_by_attr_and_get_attr(JOB_CATEGORY_CHAR_CODE, 'job_category_name', row['job_category_name'], 'job_category_4_character_code','')
        if sbu_code and job_cat_code:
            return sbu_code + job_cat_code
    return ""

def get_wbs_element(row,TIME_OFF_TYPE_PROJECT_CODE):
    if (row['project_profile'] == 'P001') and (row['work_package_work_item_code']):
        return row['work_package_code'] if row['work_package_code'] else ""

    if (row['project_profile'] in ['YP03', 'YP04']) and (row['work_package_work_item_code']):
        return row['work_package_work_item_code'].split('-')[-1]
    if row['timeoff_type_name']:
        return rail.find_first_by_attr_and_get_attr(TIME_OFF_TYPE_PROJECT_CODE, 'time_off_type_name', row['timeoff_type_name'], 'project_code','')
    return ""

def translate_rows(row, TIME_OFF_TYPE_PROJECT_CODE, SBU_CHAR_CODE, JOB_CATEGORY_CHAR_CODE, PROJECT_PROFILE_VALUE):
    if row:
        return {
            'PersonWorkAgreement': row['employee_id'],
            'CompanyCode': row['cost_center_code'][0:4] if row['cost_center_code'] else '',
            'TimeSheetRecord': '',
            'TimeSheetDate': convert_date_to_export_formate(row['entry_date']),
            'TimeSheetOperation': '',
            'ControllingArea': row['controlling_area'] if row['controlling_area'] else '',
            'ReceiverCostCenter': row['cost_center_code'] if ((
                row['project_profile'] in PROJECT_PROFILE_VALUE) or row['timeoff_type_name']) and row['cost_center_code'] else '',
            'ActivityType': get_activitytype(row,SBU_CHAR_CODE, JOB_CATEGORY_CHAR_CODE),
            'WBSElement': get_wbs_element(row, TIME_OFF_TYPE_PROJECT_CODE),
            'WorkItem': row['work_package_work_item_code'].split('-')[-1] if ((row['project_profile'] == 'P001') and (len(row['work_package_work_item_full_path'].split('/')) > 1)) else '',
            'BillingControlCategory': row['billing_control_category'] if row['billing_control_category'] == 'NON_BILL' else '',
            'TimeSheetNote': row['comments'] if row['comments'] else '',
            'RecordedHours': row['hours'] if row['hours'] else '',
            'HoursUnitOfMeasure': 'H',
            'TimeSheetWrkLocCode': row['work_location_code'] if row['work_location_code'] else '',
            'TimeSheetStatus': '30',
            'RepliconUniqueNum': row['entry_id'] if row['timeoff_type_name'] else row['timeentry_id']
        }
    return []
