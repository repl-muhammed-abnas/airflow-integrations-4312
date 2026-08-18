

def get_send_payroll_email(division_name, config,
                           file_names, payrun_datestamp, records):
    email_content = f"""
                <p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello,
                <br /> <br /> The Payroll Iterms data export completed successfully for the company code {division_name}
                on {payrun_datestamp}. Please find the extract details for reference. </p>
                <ul>
                <li>File Name: {file_names['itemfilename']}.pgp </li>
                <li>File Path: {config.output_filepath}</li>
                <li>Company Code: {division_name}</li>
                <li>Number of Records: {records}</li>
                </ul>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>
                """

    email_subject = f'{config.company_key} | Payroll Items data export completed for the company code {division_name} on {payrun_datestamp}'

    return email_content, email_subject


def get_send_no_item_data_email(division_name, config):
    email_content = """<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
                The Payroll Item data export is skipped for the company code  on since there was no data to export .</p>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>
                """
    email_subject = f'{config.company_key} | Payroll Item data export is skipped for the company code {division_name}'

    return email_content, email_subject


def get_send_no_data_email(division_name, config):
    email_content = """<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
                The Payroll export is skipped for the company code  on since there was no data to export .</p>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>
                """
    email_subject = f'{config.company_key} | Payroll export is skipped for the company code {division_name}'

    return email_content, email_subject


def get_send_no_absense_data_email(division_name, config):
    email_content = """<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
                The absense data export is skipped for the company code  on since there was no data to export .</p>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>
                """
    email_subject = f'{config.company_key} | Absense data export is skipped for the company code {division_name}'

    return email_content, email_subject


def get_send_absense_data_email(
        division_name, config, file_names, payrun_datestamp, records):
    email_content = f"""
                <p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello,
                <br /> <br /> The Payroll Absense data export completed successfully for the company code {division_name}
                on {payrun_datestamp}. Please find the extract details for reference. </p>
                <ul>
                <li>File Name: {file_names['absesnsefilename']}.pgp </li>
                <li>File Path: {config.output_filepath}</li>
                <li>Company Code: {division_name}</li>
                <li>Number of Records: {records}</li>
                </ul>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>
                """

    email_subject = f'{config.company_key} | Payroll Absense data export completed for the company code {division_name}-{payrun_datestamp}'

    return email_content, email_subject


def get_send_no_vacation_data_email(division_name, config):
    email_content = """<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
                The vacation data export is skipped for the company code  on since there was no data to export .</p>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>
                """
    email_subject = f'{config.company_key} | Payroll Vacation data export is skipped for the company code {division_name}'
    return email_content, email_subject


def get_send_vacation_data_email(
        division_name, config, file_names, payrun_datestamp, records):
    email_content = f"""
                <p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello,
                <br /> <br /> The Payroll vacation data export completed successfully for the company code {division_name}
                on {payrun_datestamp}. Please find the extract details for reference. </p>
                <ul>
                <li>File Name: {file_names['vacationfilename']}.pgp </li>
                <li>File Path: {config.output_filepath}</li>
                <li>Company Code: {division_name}</li>
                <li>Number of Records: {records}</li>
                </ul>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>
                """

    email_subject = f'{config.company_key} | Payroll vacation data export completed for the company code  {division_name}-{payrun_datestamp}'
    return email_content, email_subject


def get_send_no_timeoff_data_email(division_name, config):
    email_content = f"""
                <p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
                The Payroll Vacation and Absences data export is skipped for the company code on {division_name}
                since there was no data to export .</p>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>
                """
    email_subject = f'{config.company_key} | Payroll Vacation and Absences data export is skipped for the company code {division_name}'

    return email_content, email_subject
