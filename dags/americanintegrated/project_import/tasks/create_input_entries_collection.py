import rail


def create_entry_collection(sftp_path, list_name, task_file_prefix):
    with rail.TaskGroup(
        group_id=f"create_collection_for_{list_name}",
        prefix_group_id=False
    ) as task_group:

        list_sftp_files = rail.SFTPListFilesOperator(
            task_id=f"list_sftp_files_for_{list_name}",
            paths=[sftp_path],
        )

        for_each_reference_file = rail.ForEachOperator(
            task_id=f"for_each_file_{list_name}",
            items=lambda: rail.result(f"list_sftp_files_for_{list_name}").get(
                sftp_path),
            start_task=f"download_file_{list_name}",
            end_task=f"end_for_reference_{list_name}"
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
        append_content_to_list = rail.CreateCollectionOperator(
            task_id=f"append_content_{list_name}",
            name=f"reference_file_content_{list_name}",
            source='{{result("parse_csv_'+list_name+'")}}'
        )
        end_for_reference = rail.EmptyOperator(
            task_id=f"end_for_reference_{list_name}")

        list_sftp_files >>\
            for_each_reference_file >> end_for_reference
        for_each_reference_file >> download_file >> parse_csv_list >>\
            append_content_to_list >> end_for_reference

        return task_group
