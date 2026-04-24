import requests
import os
from datetime import datetime
import pytz

# API user credentials for Coordinate app
COORDINATE_USER = os.getenv("COORDINATE_USER")
COORDINATE_PASSWORD = os.getenv("COORDINATE_PASSWORD")
COORDINATE_BASE_URL = os.getenv("COORDINATE_BASE_URL")


def paginate_coordinate_request(next_url, headers, data):
    # handling pagination of 50 permits per request
    while next_url:
        response = requests.get(next_url, headers=headers, params={"type": "permit"})
        response.raise_for_status()
        data += response.json().get("results")
        next_url = response.json().get("next")
    return data


def get_activated_work_zones():
    """
    Checks Coordinate for a list of activated work zones.

    :return: a list of all the activated permits' folderrsns
    """
    # Authentication
    auth_url = f"{COORDINATE_BASE_URL}/api-token-auth/?format=json"
    auth_payload = {
        "username": COORDINATE_USER,
        "password": COORDINATE_PASSWORD,
    }
    auth_response = requests.post(auth_url, json=auth_payload)
    auth_response.raise_for_status()
    # Extract token
    token = auth_response.json().get("token")
    headers = {"Authorization": f"Token {token}"}

    # Get activated work zones
    url = f"{COORDINATE_BASE_URL}/api/map/?attrs__workzone_activated=true"
    response = requests.get(url, headers=headers, params={"type": "permit"})
    response.raise_for_status()
    data = response.json().get("results")
    next_url = response.json().get("next")

    data = paginate_coordinate_request(next_url, headers, data)
    activated_permits = [int(permit["external_id"]) for permit in data]

    # Get work zone start/end dates
    # This is a separate request as the above is filtered to only activated work zones
    url = f"{COORDINATE_BASE_URL}/api/map/"
    ct_date = datetime.now(pytz.timezone("America/Chicago")).date().isoformat()
    params = {
        "type": "permit",
        "attrs__workzone_end__gte": ct_date,
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    data = response.json().get("results")
    next_url = response.json().get("next")

    data = paginate_coordinate_request(next_url, headers, data)

    work_zone_dates = {}
    for rec in data:
        work_zone_dates[int(rec["external_id"])] = {
            "start": rec["workzone_start"],
            "end": rec["workzone_end"],
        }
    return activated_permits, work_zone_dates
