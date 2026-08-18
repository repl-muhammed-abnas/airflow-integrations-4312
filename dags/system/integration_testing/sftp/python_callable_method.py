import rail


def get_task_state(task_status, error_message):
    assert task_status == "success", error_message


def assert_file_download(error_message):
    expected_value = [{"value": "1", "id": "1"}, {"value": "2", "id": "2"}]
    actual_value = rail.load_all_records(rail.result("parse_csv"))
    assert expected_value == actual_value, error_message


def get_list_log_files(error_message):
    expected_value = "File1.csv"
    actual_value = rail.result("list_log_files")['/SystemTest/Log/'][0]['name']
    assert expected_value == actual_value, error_message
