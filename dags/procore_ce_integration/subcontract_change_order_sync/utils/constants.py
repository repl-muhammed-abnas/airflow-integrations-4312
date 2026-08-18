RESOURCE_CHANGE_ORDER_PACKAGE = 'Change Order Packages'

EVENT_TYPE_CREATE = 'create'
EVENT_TYPE_UPDATE = 'update'

RFC_FIELD_LIMITS = {
    #'jobnum': 10,
    #'rfcnum': 10,
    'description': 30,
    #'phasenum': 4,
    #'catnum': 6
}


class SyncStatus:
    SKIPPED = 'skipped'
    ERROR = 'error'


class SkipReason:
    INVALID_STATUS = 'invalid_status'
    NO_PCO_FOUND = 'no_pco_found'
    NO_LINE_ITEMS = 'no_line_items'
    ALL_AMOUNTS_ZERO = 'all_amounts_zero'
    INVALID_JOB_CODE = 'invalid_job_code'
    XML_TRANSFORM_FAILURE = 'xml_failed_to_transform'
    COST_TYPE_NOT_PRESENT = 'cost_type_not_present'
    JOB_NOT_PRESENT = 'job_not_present'
    WBS_TYPE_TM = 'wbstype_time_and_materials'
    WBS_TYPE_EMPTY = 'wbs_type_empty'
