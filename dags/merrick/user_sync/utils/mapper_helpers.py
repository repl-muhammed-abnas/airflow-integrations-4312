def as_name_list(value):
    """Normalize a mapper config value to a list of names.

    Accepts a single string (legacy single-value config), a list of strings
    (multi-value config, per the User Configuration spec's ``+``-separated
    fields), or a falsy value. Blank/None entries are dropped, so a missing
    config still yields ``[]``.
    """
    if not value:
        return []
    values = value if isinstance(value, list) else [value]
    return [v for v in values if v]
