import rail


def assert_response(error_message):
    expected_value = ['AY28-10', 'AY28-9']
    actual_value = [item['name'] for item in rail.result(
        'load_all_tasks_from_replicon') if 'name' in item]
    assert expected_value == actual_value, error_message
