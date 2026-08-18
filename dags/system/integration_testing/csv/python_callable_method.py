import rail


def assert_csv_data(error_message, actual_data):
    expected_entries = [{'JobId': '001', 'EmployeeName': 'emp01', 'EmployeeLocation': 'loc01'}, {
        'JobId': '002', 'EmployeeName': 'emp02', 'EmployeeLocation': 'loc02'}, {'JobId': '003', 'EmployeeName': 'emp03', 'EmployeeLocation': 'loc03'}]
    actual_entries = rail.load_all_records(actual_data)
    assert expected_entries == actual_entries, error_message


def assert_csv_data_with_footer(error_message, actual_data):
    expected_entries = [{'JobId': '001', 'EmployeeName': 'emp01', 'EmployeeLocation': 'loc01'}, {'JobId': '002', 'EmployeeName': 'emp02', 'EmployeeLocation': 'loc02'}, {
        'JobId': '003', 'EmployeeName': 'emp03', 'EmployeeLocation': 'loc03'}, {'JobId': 'Number of records found: 3', 'EmployeeName': 'Number of records processed: 3', 'EmployeeLocation': None}]
    actual_entries = rail.load_all_records(actual_data)
    assert expected_entries == actual_entries, error_message
