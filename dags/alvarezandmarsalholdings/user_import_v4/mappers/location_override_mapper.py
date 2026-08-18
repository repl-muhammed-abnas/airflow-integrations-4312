## --- office location level override mapper v1.0
##
## Office location codes that get no time off types assigned and all user level
## notifications disabled. Matched case insensitively against Office_Location_Code.

NEVER_DELIVER = "urn:replicon:user-notification-delivery-option:never-deliver"
SHARED_DELIVERY_OPTION_URIS = [
    "urn:replicon:user-shared-delivery-preference-option:always-deliver"
]

ALL_NOTIFICATION_OBJECT_TYPES = [
    "expense-sheet",
    "holiday",
    "pay-rule-script",
    "project",
    "time-entry-revision-group",
    "time-off",
    "timesheet",
    "user",
    "time-punch"
]


location_override_mapper = ["ITSTP", "FRAVO"]


def is_location_excluded(office_location_code):
    if not office_location_code:
        return False

    code = str(office_location_code).strip().casefold()
    return code in [mapped_code.strip().casefold()
                    for mapped_code in location_override_mapper]


def get_disabled_notification_preferences():
    return {
        "notificationDeliveryPreferences": [
            {
                "notificationDeliveryOptionUri": NEVER_DELIVER,
                "objectTypeUri": f"urn:replicon:object-type:{object_type}"
            }
            for object_type in ALL_NOTIFICATION_OBJECT_TYPES
        ],
        "sharedDeliveryPreferenceOptionUris": SHARED_DELIVERY_OPTION_URIS
    }
