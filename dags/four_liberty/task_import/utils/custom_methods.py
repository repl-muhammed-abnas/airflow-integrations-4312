import hashlib
import rail

null = None


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_csv_rows(item):
    def get_hash_md5():
        return hashlib.md5(
            (str(item['Task_Name'] if item['Task_Name'] else null) + "_"
             + str(item['Budget_Code_Name']
                   if item['Budget_Code_Name'] else null) + "_"
             + str(item['Budget_Code'] if item['Budget_Code'] else null) + "_"
             + str(item['Work_Order'] if item['Work_Order'] else null) + "_"
             + str(item['Substation___Work_Order_Name']
                   if item['Substation___Work_Order_Name'] else null) + "_"
             + str(item['Internal_Order']
                   if item['Internal_Order'] else null) + "_"
             + str(item['FERC_Code'] if item['FERC_Code'] else null) + "_"
             + str(item['System_Status']
                   if item['System_Status'] else null) + "_"
             + str(item['Work_Order_Status']
                   if item['Work_Order_Status'] else null) + "_"
             + str(item['Open_Date'] if item['Open_Date'] else null) + "_"
             + str(item['TECO_Date'] if item['TECO_Date'] else null) + "_"
             + str(item['Close_Date'] if item['Close_Date'] else null) + "_"
             ).encode('utf-8')).hexdigest()

    row_data = [
        item['Task_Name'].strip() if item['Task_Name'] else null,
        item['Budget_Code_Name'],
        item['Budget_Code'],
        item['Work_Order'],
        item['Substation___Work_Order_Name'],
        item['Internal_Order'] if item['Internal_Order'] else null,
        item['FERC_Code'],
        item['System_Status'],
        item['Work_Order_Status'].strip().upper(
        ) if item['Work_Order_Status'] else null,
        item['Open_Date'],
        item['TECO_Date'],
        item['Close_Date'],
        get_hash_md5()

    ]
    return row_data
