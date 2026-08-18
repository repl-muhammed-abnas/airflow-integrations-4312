import rail


def assert_true_value(actual_id, error_message, expected_id):
    assert expected_id == actual_id, error_message


def assert_data_adaptor_operator_1(error_message):
    actual_data = rail.load_all_records(rail.result('data_adaptor_operator_1'))
    expected_data = [
        {"taskname": "1Task", "fullname": "1FirstName1LastName"},
        {"taskname": "2Task", "fullname": "2FirstName2LastName"},
        {"taskname": "3Task", "fullname": "3FirstName3LastName"},
        {"taskname": "4Task", "fullname": "4FirstName4LastName"}
    ]
    assert expected_data == actual_data, error_message


def assert_data_adaptor_operator_2(error_message):
    actual_data = rail.result('data_adaptor_operator_2')
    expected_data = {'employeeid': '1EmployeeId',
                     'fullname': '1FirstName1LastName'}

    assert expected_data == actual_data, error_message
