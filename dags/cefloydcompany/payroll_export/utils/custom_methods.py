def to_float(val):
    """Convert a value to float, removing commas from strings if present."""
    if isinstance(val, str):
        val = val.replace(",", "")
    return float(val)
