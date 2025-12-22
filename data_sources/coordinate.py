import requests
import os

# API user credentials for Coordinate app
COORDINATE_USER = os.getenv("COORDINATE_USER")
COORDINATE_PASSWORD = os.getenv("COORDINATE_PASSWORD")
COORDINATE_BASE_URL = os.getenv("COORDINATE_BASE_URL")


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

    # handling pagination of 50 permits per request
    while next_url:
        response = requests.get(next_url, headers=headers, params={"type": "permit"})
        response.raise_for_status()
        data += response.json().get("results")
        next_url = response.json().get("next")

    activated_permits = [int(permit["external_id"]) for permit in data]
    return activated_permits
