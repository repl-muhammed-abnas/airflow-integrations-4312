Time_off_mappper = [
    {
        "Time off type": "[UK] Annual leave",
        "Quote Type": "200H"
    }
]

# Extract time-off types from mapper for use in config
def get_termination_timeoff_types():
    """Returns list of time-off types configured in the mapper."""
    return tuple(mapping["Time off type"] for mapping in Time_off_mappper)
