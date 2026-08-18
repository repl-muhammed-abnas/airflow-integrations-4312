import rail
from pimco.create_new_task_consultant.utils import custom_methods

def make_list():
    max_task_level = list(map(lambda item: item['MAX_tasklevel_'], custom_methods.read_collection(
        rail.result('query_max_level'))))
    return list(map(lambda i: i, range(1, int(max_task_level[0])+1)))
