import rail


def assert_collection_creation(err_message):
    expected = [
        {
            'Project_Id': 'burlington-textiles',
            'Project_Name': 'Burlington Textiles',
            'Project_Type': 'New Customer'
        },
        {
            'Project_Id': 'edge-installation',
            'Project_Name': 'Edge Installation',
            'Project_Type': 'Existing'
        }
    ]
    received = rail.load_all_records(
        rail.result('query_created_collection')
    )

    assert expected == received, err_message


def assert_collection_query(err_message):
    expected = {
        'Project_Id': 'burlington-textiles',
        'Project_Name': 'Burlington Textiles',
        'Project_Type': 'New Customer'
    }
    received = rail.result('query_filtered_collection')

    assert expected == received, err_message
