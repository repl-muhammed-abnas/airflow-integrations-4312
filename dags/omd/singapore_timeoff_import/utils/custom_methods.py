import hashlib
import rail

null = None


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_csv_rows(item):
    def get_hash_md5():
        return hashlib.md5(
            (str(item['Employee_ID']) + ","
             + str(item['Leave_Code']) + ","
             + str(item['Start_Date']) + ","
             + str(item['Start_Day_Type']) + ","
             + str(item['Start_Day_Hours']) + ","
             + str(item['End_Date']) + ","
             + str(item['End_Day_Type']) + ","
             + str(item['End_Day_Hours']) + ","
             + str(item['ID']) + ","
             + str(item['Status'])
             ).encode('utf-8')).hexdigest()

    row_data = [
        item['Employee_ID'],
        item['Leave_Code'],
        item['Start_Date'],
        item['Start_Day_Type'],
        item['Start_Day_Hours'],
        item['End_Date'],
        item['End_Day_Type'],
        item['End_Day_Hours'],
        item['ID'],
        item['Status'],
        get_hash_md5()

    ]
    return row_data
