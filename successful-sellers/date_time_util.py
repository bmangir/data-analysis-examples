from datetime import timedelta
from datetime import datetime
from decimal import Decimal


# Convert timedelta and Decimal to serializable formats
def convert_to_serializable(obj):
    if isinstance(obj, timedelta):
        return str(obj)
    elif isinstance(obj, Decimal):
        return float(obj)


# Convert the datetime to epoch seconds time
def datetime_to_epoch_converter(date_as_str):
    # Add 3600 * 3 seconds to avoid time zone for GMT
    epoch_time = int(__string_to_datetime(date_as_str).timestamp()) + (3600 * 3)

    return epoch_time


# Convert the string date to datetime type
def __string_to_datetime(date_as_str):
    return datetime.strptime(date_as_str, "%Y-%m-%d")
