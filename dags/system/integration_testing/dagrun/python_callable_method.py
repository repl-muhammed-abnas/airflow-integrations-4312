
def do_assert_dagrun_conf(test_string, test_integer, error_message):
    expected_dag_run_conf = {
        "test_string": "test dagrun for string props", "test_integer": '0'}
    actual_dag_run_conf = {"test_string": test_string,
                           "test_integer": test_integer}
    assert expected_dag_run_conf == actual_dag_run_conf, error_message


def do_assert_trigger_dagrun(trigger_dag_run_ids, count, error_message):
    assert len(trigger_dag_run_ids) != count, error_message


def do_assert_wait_for_dagruns(task_status, error_message):
    assert task_status == 'success', error_message


def assert_gathered_result(expected, received, err_message):
    assert expected == sorted(received), err_message
