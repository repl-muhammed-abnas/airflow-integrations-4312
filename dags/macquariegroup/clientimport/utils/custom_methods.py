import hashlib
import rail

null = None


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_extra_info():
    # to handle all the failure scenarios
    if not rail.result('csv_files_list'):
        return null

    filenames = ' '.join([file['filename']
                         for file in rail.result('csv_files_list')])
    return {
        'filenames': filenames,
        'filecount': len(rail.result('csv_files_list')),
        'status': 'Not processed'
    }


def get_csv_rows(item):
    row_data = [
        item['clientname'],
        item['clientcode'],
        item['location'],
        hashlib.md5((str(item['clientname']) + "," + str(item['clientcode']) +
                    "," + str(item['location'])).encode('utf-8')).hexdigest()
    ]
    return row_data
