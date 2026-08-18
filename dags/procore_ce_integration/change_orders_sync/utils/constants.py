class WBSType:
    JOB_CAT = 'Job/Cat'
    JOB_PHASE_CAT = 'Job/Phase/Cat'
    TIME_MATERIAL = 'T&M'
    
RESOURCE_CHANGE_EVENT = 'Change Events'
RESOURCE_BUDGET_CHANGES = 'Budget Changes'
RESOURCE_CHANGE_ORDER_PACKAGES = 'Change Order Packages'

CREATE = 'create'
APPROVED = 'approved'
CE_STATUS_OPEN = 'Open'
PENDING_APPROVAL = 'pending_approval'

RFC_FIELD_LIMITS = {
    'jobnum': 10,
    'rfcnum': 10,
    'description': 30,
    'phasenum': 4,
    'catnum': 6,
    'approvedby': 30,
    'changeordernum': 10
}
