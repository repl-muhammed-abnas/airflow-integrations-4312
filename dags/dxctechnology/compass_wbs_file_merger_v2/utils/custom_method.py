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
        'WBS': "{{item.WBS}}",
        'Description': "{{item.WBSDescription}}",
        'Status': "{{item.Status}}",
        'CompanyCode': "{{item.CompanyCode}}",
        'ProjectType': "{{item.ProjectType}}",
        'ProjectStart': "{{item.ProjectStart}}",
        'ProjectEnd': "{{item.ProjectEnd}}",
        'PersonResponsible1': "{{item.PersonResponsible1}}",
        'PersonResponsible2': "{{item.PersonResponsible2}}",
        'TimeTrackingRequiredAttribute': "{{item.TimeTrackingRequiredAttribute}}",
        'GlobalWBSIndicator': "{{item.GlobalWBSIndicator}}",
        'IWOWBSIndicator': "{{item.IWOWBSIndicator}}",
        'filedatetime': "{{item.file_date_time}}",
        'sourcefilename': "{{item.file_name}}",
        'sourcefilerecordcount': "{{item.sourcefilerecordcount}}",
        'sequenceno': "{{item.sequance_number}}",
        'md5': "{{item.md5}}",
        'ignored': "{{item.ignored}}",
        'jobid': get_dagrun_ecid(dag_run),
        'mergedfilename': f"Compass_WBS_Mergeddata_{current_file_time}.xml"
    }
