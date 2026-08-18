import rail

def move_to_processing_task_group(countryname,countryid, filename, filepath):
    with rail.TaskGroup(group_id=f'move_to_processing_{countryname}', prefix_group_id=False) as move_to_processing:

        query_records_each_country = rail.QueryCollectionOperator(
            task_id= f"query_records_{countryname}",
            query = f"""Select * from inputdatacollection where countryid ='{countryid}'""",
        )

        has_any_data = rail.IfOperator(
            task_id=f'has_any_data_{countryname}',
            test="{{ result('query_records_" + f'{countryname}'+"','length') > 0 }}",
            yes_task=f'create_csv_{countryname}',
        )

        create_csv_each_country = rail.WriteCSVFileOperator(
            task_id=f'create_csv_{countryname}',
            source=lambda: rail.result(f'query_records_{countryname}'),
            header=[
                'Country ID',
                'Login Name',
                'Employee ID',
                'Date of Birth',
                'Rehire',
                'Start Date',
                'LastName',
                'FirstName',
                'Last day worked',
                'EndDate',
                'Email ID',
                'Time Zone',
                'Language',
                'PN Flag',
                'ADP File#',
                'FTE%',
                'Employee Category',
                'Actual Working hours',
                'Statutory Limit',
                'Effective Date',
                'Employee Type Name',
                'Division Name',
                'Location Name',
                'Location Code',
                'Company Name',
                'Company Code',
                'Supervisor ID/Emp ID',
                'Supervisor First name',
                'Supervisor Last name',
                'Supervisor Email ID',
                'Job Title',
            ],
            row=[
                '{{ item.countryid }}',
                '{{ item.loginname }}',
                '{{ item.employeeid }}',
                '{{item.dateofbirth}}',
                '{{ item.rehire }}',
                '{{ item.startdate }}',
                '{{ item.lastname }}',
                '{{ item.firstname }}',
                '{{ item.lastdayworked }}',
                '{{ item.enddate }}',
                '{{ item.emailid }}',
                '{{ item.timezone }}',
                '{{ item.language }}',
                '{{ item.pnflag }}',
                '{{ item.adpfile }}',
                '{{ item.ftepercent }}',
                '{{ item.employeecategory }}',
                '{{ item.actualworkinghrs }}',
                '{{ item.statutorylimit }}',
                '{{ item.effectivedate }}',
                '{{ item.employeetypename }}',
                '{{ item.divisionname }}',
                '{{ item.locationname }}',
                '{{ item.locationcode }}',
                '{{ item.companyname }}',
                '{{ item.companycode }}',
                '{{ item.supervisorid }}',
                '{{ item.supervisorfirstname }}',
                '{{ item.supervisorlastname }}',
                '{{ item.supervisoremailid }}',
                '{{ item.jobtitle }}',
            ],
            delimiter='|'
        )

        upload_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id=f'upload_log_to_sftp_{countryname}',
            content="{{result('create_csv_"+f'{countryname}'+"')}}",
            remote_filepath=filepath +
            '/'+filename+'_'+countryname+'.csv',
        )

        query_records_each_country >> has_any_data >> rail.Label('Yes') >> create_csv_each_country >> upload_file_to_sftp

    return move_to_processing
