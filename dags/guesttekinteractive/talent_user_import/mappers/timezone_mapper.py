"""
Timezone Mapper - GuestTek Talent User Import Integration

Maps location names to Replicon timezone display text values.
Used for assigning correct timezone when creating or updating users.

Usage:
    from guesttekinteractive.talent_user_import.mappers.timezone_mapper import TIMEZONE_MAPPER
    timezone = TIMEZONE_MAPPER.get(location_name, default_timezone)
"""

# Location Name -> Replicon Timezone Display Text
TIMEZONE_MAPPER = {
    # APAC
    "Australia": "(UTC+9:30) Cen.Australia Standard Time",
    "Hong Kong": "(UTC+8:00) China Standard Time",
    "India - Gurgaon": "(UTC+5:30) India Standard Time",
    "India - Mumbai": "(UTC+5:30) India Standard Time",
    "Indonesia": "(UTC+7:00) North Asia Standard Time",
    "Japan": "(UTC+9:00) Tokyo Standard Time",
    "Malaysia": "(UTC+8:00) China Standard Time",
    "Philippines": "(UTC+8:00) North Asia East Standard Time",
    "Singapore": "(UTC+8:00) Singapore Standard Time",
    "Thailand": "(UTC+7:00) North Asia Standard Time",

    # Americas
    "Aruba": "(UTC-7:00) Mountain Standard Time",
    "Brazil": "(UTC-3:00) SA Eastern Standard Time",
    "Canada": "(UTC-7:00) Mountain Standard Time",
    "Guatemala": "(UTC-6:00) Central Standard Time",
    "Mexico": "(UTC-8:00) Pacific Standard Time (Mexico)",
    "USA": "(UTC-7:00) Mountain Standard Time",

    # Europe
    "France": "(UTC+1:00) Central Europe Standard Time",
    "Germany": "(UTC+1:00) Central Europe Standard Time",
    "Malta": "(UTC+1:00) Central Europe Standard Time",
    "N'lands": "(UTC+1:00) Central Europe Standard Time",
    "Poland": "(UTC+1:00) Central Europe Standard Time",
    "Spain": "(UTC+1:00) Central Europe Standard Time",
    "UK": "(UTC+0:00) GMT Standard Time",

    # MEA
    "Dubai": "(UTC+4:00) Arabian Standard Time",
    "Egypt": "(UTC+2:00) Egypt Standard Time",
    "Turkey": "(UTC+3:00) E.Europe Standard Time",

    # Default
    "Default": "(UTC-7:00) Mountain Standard Time",
}

def get_timezone_for_location(location_name):
    """
    Get timezone display text for a given location.
    
    Args:
        location_name (str): The location name from Talent API
        
    Returns:
        str: Replicon timezone display text
    """
    if not location_name:
        return TIMEZONE_MAPPER.get("Default")
    
    # Try exact match first
    if location_name in TIMEZONE_MAPPER:
        return TIMEZONE_MAPPER[location_name]
    
    # Try case-insensitive match
    location_lower = location_name.lower()
    for key, value in TIMEZONE_MAPPER.items():
        if key.lower() == location_lower:
            return value
    
    # Return default if no match
    return TIMEZONE_MAPPER.get("Default")
