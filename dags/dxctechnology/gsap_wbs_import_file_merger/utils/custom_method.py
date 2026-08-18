import rail
from rail.lib.ecid import get_dagrun_ecid


def has_any_file(result_task_id, input_file_path):
    if not result_task_id or not input_file_path:
        raise Exception(
            "Task_id" if not result_task_id else "input path" + "is not provided")
    data = rail.result(result_task_id)
    if not data:
        return False
    return len(data[input_file_path]) > 0


def get_wbs_status(dag_run):
    current_file_time = rail.result('get_time_for_file')
    return {
        'WBS_Name': "{{item.WBS_Name}}",
        'WBS_Code': "{{item.WBS_Code}}",
        'Company_Code': "{{item.Company_Code}}",
        'Project_Type': "{{item.Project_Type}}",
        'Profit_Centre': "{{item.Profit_Centre}}",
        'Task_Indicator': "{{item.Task_Indicator}}",
        'Project_Start': "{{item.Project_Start}}",
        'Project_End': "{{item.Project_End}}",
        'Primary_Project_Manager_ID': "{{item.Primary_Project_Manager_ID}}",
        'Primary_Project_Manager_Name': "{{item.Primary_Project_Manager_Name}}",
        'WBS_Currency': "{{item.WBS_Currency}}",
        'Parent_Project': "{{item.Parent_Project}}",
        'WBS_Parent_Project': "{{item.WBS_Parent_Project}}",
        'Customer_Name': "{{item.Customer_Name}}",
        'PSA_Flag': "{{item.PSA_Flag}}",
        'filedatetime': "{{item.file_date_time}}",
        'sourcefilename': "{{item.file_name}}",
        'sourcefilerecordcount': "{{item.sourcefilerecordcount}}",
        'sequenceno': "{{item.sequance_number}}",
        'md5': "{{item.md5}}",
        'ignored': "{{item.ignored}}",
        'jobid': get_dagrun_ecid(dag_run),
        'mergedfilename': f"GSAP_WBS_Mergeddata_{current_file_time}.xml"
    }
