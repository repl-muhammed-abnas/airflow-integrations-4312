import rail


def assert_decrypted_file(error_message, actual_data):
    expected_value = [{"value": "1"}]
    actual_value = rail.load_all_records(actual_data)
    assert expected_value == actual_value, error_message
