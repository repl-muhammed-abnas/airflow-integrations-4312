def assert_batch_state(expected_id, error_message, batch_state):
    expected_value = expected_id
    actual_value = batch_state
    assert expected_value == actual_value, error_message
