import re
from galaxyusopcoinc.workday_user_sync.user_import_v2.utils import request_payload
email_regex = re.compile(
    r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
required = True

field_config_add = {
    # entry = tuple ( isrequired=Boolean, (optional)custom message - str,method)

    "employeeid": (required, None),
    "legalfirstname": (required, None),
    "legallastname": (required, None),
    "businesstitle": (required, None),
    "hiredate": (required, lambda x: 'Incorrect date format received for HireDate' \
                 if x['hiredate'] and not request_payload.get_replicon_date(x['hiredate']) else False),
    "workemail": (required, lambda x: 'Email address not present in payload' if not x['workemail'] \
                  else 'Email not updated since email field received incorrect format' \
                  if x['workemail'] and not re.fullmatch(email_regex, x['workemail']) else False),
    "costcenterid": (required, None),
    "company": (required, None),
    "companycode": (required, None),
    "country": (required, None),
    "location": (not required, None),
    "locationtype": (required, None),
    "scheduledweeklyhours": (required, None),
    "defaultweeklyhours": (required, None),
    "employeetype": (required, None),
    "contracttype": (not required, 'Contract Type not present'),
    "manageremail": (required, None),
    "positionid": (required, None),
    "exempt": (required, None),
    "fte": (required, None),
    "jobprofile": (not required, 'Job Profile not present'),
    "jobprofilecode": (not required, 'Job Profile Code not present'),
    "jobfamily": (not required, 'Job Family not present'),
    "jobfamilygroup": (not required, 'Job Family Group not present'),
    "costcentername": (required, None),
    "contractenddate": (not required, 'Contract End Date not present'),
    "collectiveagreement": (not required, 'Collective Agreement not present')
}

field_config_update = {
    # entry = tuple ( isrequired=Boolean, (optional)custom message - str,method)

    "employeeid": (required, None),
    "legalfirstname": (required, None),
    "legallastname": (required, None),
    "businesstitle": (required, None),
    "hiredate": (required, lambda x: 'Incorrect date format received for HireDate' \
                 if x['hiredate'] and not request_payload.get_replicon_date(x['hiredate']) else False),
    "workemail": (required, lambda x: 'Email address not present in payload' if not x['workemail'] \
                  else 'Email not updated since email field received incorrect format' \
                  if x['workemail'] and not re.fullmatch(email_regex, x['workemail']) else False),
    "costcenterid": (required, None),
    "company": (required, None),
    "companycode": (required, None),
    "country": (required, None),
    "location": (not required, None),
    "locationtype": (required, None),
    "scheduledweeklyhours": (required, None),
    "defaultweeklyhours": (required, None),
    "employeetype": (required, None),
    "contracttype": (not required, 'Contract Type not present'),
    "manageremail": (required, None),
    "positionid": (required, None),
    "exempt": (required, None),
    "fte": (required, None),
    "jobprofile": (not required, 'Job Profile not present'),
    "jobprofilecode": (not required, 'Job Profile Code not present'),
    "jobfamily": (not required, 'Job Family not present'),
    "jobfamilygroup": (not required, 'Job Family Group not present'),
    "costcentername": (required, None),
    "contractenddate": (not required, 'Contract End Date not present'),
    "collectiveagreement": (not required, 'Collective Agreement not present')
}


def validate_field(field_config):
    data = request_payload.get_conf()
    errors = []
    for field_name in data:
        if field_name in field_config:
            (is_required, custom_message) = field_config[field_name]
            field_value = data[field_name]
            error = None
            if custom_message and callable(custom_message):
                error = custom_message(data)
            elif is_required and not field_value:
                error = f'{field_name} is not present in payload'
            if error:
                errors.append(
                    {'field_name': field_name, 'log_type': 'Exception' if is_required else 'Warning', 'message': error})

    return errors
