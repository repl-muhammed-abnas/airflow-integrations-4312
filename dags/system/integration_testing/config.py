from system.integration_testing.utils import is_fips_environment

region = "us-west-2"
environment = "devops"
company_key = "system"
sftp_conn_id = "sftp_uswest2"
aws_conn_id = "systemtest_aws_conn_id"
# https://pwd.rplcn.co/pid=56837
replicon_conn_id = "airflowsandbox-replicon-admin"
# Here the Tenant Slug is hardcoded as we have decoupled the Identity class
# from calling GetMyIdentity API for System DAG(s).
tenant_slug = "a7427f9c8a1747fc81f7ef31746e293e"
write_csv_thread_pool_size = 10
pgp_conn_id = "systemtest_pgp_conn_id"
execution_timeout_hours = 1
http_conn_id = "systemtest_http_conn_id"
dag_operator_mapper = [
    *([{
        "dag_id": "system_integration_testing_pgp_operators",
        "category": "pgp",
        "conf": {
            "input_filepath": "/SystemTest/pgp/",
            "file_name": "pgp_file.csv"
        }
    }] if not is_fips_environment() else []),
    {
        "dag_id": "system_integration_testing_log_operators",
        "category": "log",
        "conf": {
            "test_string": "test log for string props",
            "test_integer": 0
        }
    },
    {
        "dag_id": "system_integration_testing_general_operators",
        "category": "general",
        "conf": {
            "test_string": "operator test"
        }
    },
    {
        "dag_id": "system_integration_testing_replicon_operators",
        "category": "replicon",
        "conf": {
            "employeeid": "1234",
            "loginname": "a1b1",
            "firstname": "a1",
            "lastname": "b1",
            "password": "@Adminen1018",
            "admin_loginname": "admin"
        }
    },
    {
        "dag_id": "system_integration_testing_sumo_operators",
        "category": "sumo",
        "conf": {
            "employeeDetail": {
                "JobId": "123",
                "EmployeeName": "abc",
                "EmployeeLocation": "xyz"
            },
            "employeeDetails": [
                {
                    "JobId": "001",
                    "EmployeeName": "emp01",
                    "EmployeeLocation": "loc01"
                },
                {
                    "JobId": "002",
                    "EmployeeName": "emp02",
                    "EmployeeLocation": "loc02"
                },
                {
                    "JobId": "003",
                    "EmployeeName": "emp03",
                    "EmployeeLocation": "loc03"
                }
            ],
            "sumo_conn_id": "sumologic-dagrunlogger"
        }
    },
    {
        "dag_id": "system_integration_testing_collection_operators",
        "category": "collection",
        "conf": {
            "records": [
                {
                    "Id": "burlington-textiles",
                    "Name": "Burlington Textiles",
                    "Type": "New Customer"
                },
                {
                    "Id": "edge-installation",
                    "Name": "Edge Installation",
                    "Type": "Existing"
                },
            ]
        }
    },
    {
        "dag_id": "system_integration_testing_dagrun_operators",
        "category": "dagrun",
        "conf": {
            "test_string": "test dagrun for string props",
            "test_integer": 0
        }
    },
    {
        "dag_id": "system_integration_testing_batch_taskrun_operators",
        "category": "batchtask",
        "conf": {
            "test_string": "test1",
            "test_string1": "test2"
        }
    },
    {
        "dag_id": "system_integration_testing_sftp_network_operators",
        "category": "sftp",
        "conf": {
            "log_filepath": "/SystemTest/Log/",
            "file_name": "File1.csv",
            "archive_filepath": "/SystemTest/Archive/"
        },
    },
    {
        "dag_id": "system_integration_testing_network_operators",
        "category": "network",
        "conf": {
            "aws_s3_bucket": "airflow-systemtest",
            "network_filepath": "SystemTest/NetworkS3/",
            "file_name": "S3File.csv",
            "new_network_filepath": "NewSystemTest/MoveNetworkS3/",
            "new_file_name": "NewS3File.csv"
        }
    },
    # {
    #     "dag_id": "system_integration_testing_file_format_operators",
    #     "category": "file_formats",
    #     "conf": {
    #         "input_filepath": "/SystemTest/File_formats/",
    #         "file_name": "File_Format1.csv",
    #         "xml_file_name": "dummyData.xml"
    #     }
    # },
    {
        "dag_id": "system_integration_testing_business_replicon_report_details_operators",
        "category": "business_replicon_report_details",
        "conf": {
            "report_name": "User Details"
        }
    },
    # {
    # "dag_id": "system_integration_testing_business_get_all_project_tasks_operators",
    # "category": "business_get_all_project_tasks",
    # "conf": {
    #     "project_name": "Integration Platform",
    #     "project_id": "IP2",
    #     "tasks": [
    #         {
    #             "key": "AY28-10",
    #             "issue_summary": "IP8: Test Issue10",
    #             "issue_id": "10101",
    #             "project_key": "AY28",
    #             "project_id": "10042",
    #             "created": "2024-12-18",
    #             "status": "Open"
    #         },
    #         {
    #             "key": "AY28-9",
    #             "issue_summary": "IP8: Test Issue 9",
    #             "issue_id": "10100",
    #             "project_key": "AY28",
    #             "project_id": "10042",
    #             "created": "2024-12-18",
    #             "status": "Open"
    #         }
    #     ]
    # }
    # },
    {
        "dag_id": "system_integration_testing_business_get_groups_matching_filter_operators",
        "category": "business_get_groups_matching_filter",
        "conf": {
        }
    },
    {
        "dag_id": "system_integration_testing_csv_operators",
        "category": "csv",
        "conf": {
            "employeeDetails": [
                {
                    "JobId": "001",
                    "EmployeeName": "emp01",
                    "EmployeeLocation": "loc01"
                },
                {
                    "JobId": "002",
                    "EmployeeName": "emp02",
                    "EmployeeLocation": "loc02"
                },
                {
                    "JobId": "003",
                    "EmployeeName": "emp03",
                    "EmployeeLocation": "loc03"
                }
            ]
        }
    },
]
