import os

def is_fips_environment() -> bool:
    return os.environ.get('AWS_USE_FIPS_ENDPOINT', 'false').lower() == 'true'
