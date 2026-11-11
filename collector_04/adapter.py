import json


def decode(content):
    value = json.loads(content)
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise ValueError("source must be an array of objects")
    return value
