from typing import TypedDict


class ProcoreEquipmentStatus(TypedDict):
    ACTIVE = 'Active'
    INACTIVE = 'Inactive'


class Operation(TypedDict):
    CREATE = 'create'
    UPDATE = 'update'
