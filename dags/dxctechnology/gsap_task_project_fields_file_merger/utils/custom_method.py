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


def get_tasks_status(dag_run):
    current_file_time = rail.result('get_time_for_file')
    return {
        'WBS_Name': "{{item.WBS_Name}}",
        'Task_Name': "{{item.Task_Name}}",
        'Task_Code': "{{item.Task_Code}}",
        'Task_Start_Date': "{{item.Task_Start_Date}}",
        'Task_End_Date': "{{item.Task_End_Date}}",
        'filedatetime': "{{item.file_date_time}}",
        'sourcefilename': "{{item.file_name}}",
        'sourcefilerecordcount': "{{item.sourcefilerecordcount}}",
        'sequenceno': "{{item.sequance_number}}",
        'md5': "{{item.md5}}",
        'ignored': "{{item.ignored}}",
        'jobid': get_dagrun_ecid(dag_run),
        'mergedfilename': f"GSAP_Tasks_Mergeddata_{current_file_time}.xml"
    }
