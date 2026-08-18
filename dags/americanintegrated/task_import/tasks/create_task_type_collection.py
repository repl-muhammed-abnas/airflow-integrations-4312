from hashlib import md5
import rail


def create_task_entry_collection(sftp_path, list_name):
    with rail.TaskGroup(
        group_id=f"create_collection_for_{list_name}",
        prefix_group_id=False
    ) as task_group:

        list_sftp_files = rail.SFTPListFilesOperator(
            task_id=f"list_sftp_files_for_{list_name}",
            paths=[sftp_path],
        )

        get_oldest_file = rail.PythonOperator(
            task_id=f"get_oldest_file_{list_name}",
            python_callable=lambda: list(reversed(rail.result(
                f"list_sftp_files_for_{list_name}").get(sftp_path)))
        )

        for_each_file = rail.ForEachOperator(
            task_id=f"for_each_file_{list_name}",
            items=lambda: rail.result(f"get_oldest_file_{list_name}"),
            start_task=f"if_txt_and_task_prefix_{list_name}",
            end_task=f"end_for_reference_{list_name}"
        )

        if_txt_and_task_prefix = rail.IfOperator(
            task_id=f"if_txt_and_task_prefix_{list_name}",
            test='{{result("for_each_file_'+list_name +
            '")["name"] | ends_with(".txt")}}',
            yes_task=f"download_file_{list_name}",
            no_task=f"end_for_reference_{list_name}"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id=f"download_file_{list_name}",
            remote_filepath=sftp_path +"/"
            '{{result("for_each_file_'+list_name+'").name}}'
        )

        parse_csv_list = rail.LoadCSVFileOperator(
            task_id=f"parse_csv_{list_name}",
            document='{{result("download_file_'+list_name+'")}}',
            delimiter="\t",
            headers=["taskcode","taskname"] if "basic_task" in list_name else ["taskcode","taskname","payrollclassification","rate1","rate2","rate3"]
        )

        if_basic_task_type = rail.IfOperator(
            task_id=f"if_basic_task_type_{list_name}",
            test=lambda: ("basic_task" in list_name),
            yes_task=f"create_content_csv_md5_{list_name}",
            no_task=f"append_wage_content_{list_name}"
        )

        create_content_csv_md5 = rail.DataAdaptorOperator(
            task_id=f"create_content_csv_md5_{list_name}",
            source='{{result("parse_csv_'+list_name+'")}}',
            columns=["taskname", "taskcode", "md_5"],
            data=lambda item: {
                    "taskname": item["taskname"],
                    "taskcode": item["taskcode"],
                    "md_5": md5((item["taskname"]+item["taskcode"]).encode()).hexdigest()
            } if item else None
        )

        append_basic_content_to_list = rail.CreateCollectionOperator(
            task_id=f"append_basic_content_{list_name}",
            name=f"file_content_{list_name}",
            source='{{result("create_content_csv_md5_'+list_name+'")}}'
        )

        append_wage_content_to_list = rail.CreateCollectionOperator(
            task_id=f"append_wage_content_{list_name}",
            name=f"file_content_{list_name}",
            source='{{result("parse_csv_'+list_name+'")}}'
        )

        end_for_reference = rail.EmptyOperator(
            task_id=f"end_for_reference_{list_name}")

        list_sftp_files >> get_oldest_file >>\
            for_each_file >> end_for_reference
        for_each_file >> if_txt_and_task_prefix >>\
            rail.Label("Yes") >> download_file >> parse_csv_list >>\
            if_basic_task_type >> rail.Label("Yes") >> create_content_csv_md5 >>\
            append_basic_content_to_list >> end_for_reference
        if_txt_and_task_prefix >> rail.Label("No") >> end_for_reference 
        if_basic_task_type >> rail.Label("No") >>\
            append_wage_content_to_list >> end_for_reference

    return task_group
