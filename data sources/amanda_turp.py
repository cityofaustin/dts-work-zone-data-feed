import pandas as pd
import datetime
import requests
from urllib.parse import urlencode
import uuid
import pytz

import json

from amanda import get_amanda_data
from config import turp_query
from workzone import WorkZone


def get_start_end_date(row):
    if not pd.isnull(row["EXTENSION_START_DATE"]) and not pd.isnull(
        row["EXTENSION_END_DATE"]
    ):
        row["START_DATE"] = row["EXTENSION_START_DATE"]
        row["END_DATE"] = row["EXTENSION_END_DATE"]
    return row


def chunk_list(input_list, chunk_size=25):
    """Chunk the input list into batches of chunk_size."""
    for i in range(0, len(input_list), chunk_size):
        yield input_list[i : i + chunk_size]


def get_geometry(segment_ids):
    chunks = chunk_list(segment_ids)
    base_url = (
        "https://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/TRANSPORTATION_street_segment"
        "/FeatureServer/0/query"
    )
    responses = []
    for c in chunks:
        segments = ",".join(c)
        params = {
            "where": f"SEGMENT_ID IN({segments})",
            "f": "pgeojson",
            "outFields": "*",
        }

        full_url = f"{base_url}?{urlencode(params)}"
        response = requests.get(full_url)
        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError as json_err:
            print("JSON decode error:", json_err)
            print(response.text)
            print(c)
        responses += data["features"]
    return responses


def create_feed_info(amanda_id, current_time):
    feed_info = {
        "publisher": "City of Austin",
        "version": "4.2",
        "license": "https://creativecommons.org/publicdomain/zero/1.0/",
        "data_sources": [
            {
                "data_source_id": amanda_id,
                "organization_name": "City of Austin",
                "update_date": current_time.astimezone(pytz.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "update_frequency": 3600,  # assuming 1 hr refresh rate
                "contact_name": "Transportation and Public Works Department",
                "contact_email": "transportation.data@austintexas.gov",
            }
        ],
        "update_date": current_time.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "update_frequency": 3600,  # assuming 1 hr refresh rate
        "contact_name": "Transportation and Public Works Department",
        "contact_email": "transportation.data@austintexas.gov",
    }

    return feed_info


def main():
    data = get_amanda_data(turp_query)
    df = pd.DataFrame(data)

    # df = pd.read_csv("closure_data_v2.csv")

    df = df.apply(get_start_end_date, axis=1)
    segments = (
        df[
            df["CLOSURE_TYPE"].isin(
                ["Closure : Full Road", "Traffic Lane : Dimensions"]
            )
        ]["SEGMENT_ID"]
        .astype(str)
        .unique()
    )
    segment_info = get_geometry(segments)
    segment_lookup = {}
    for s in segment_info:
        segment_lookup[int(s["properties"]["SEGMENT_ID"])] = s

    amanda_id = str(uuid.uuid5(uuid.NAMESPACE_OID, "COA_AMANDA_TURP"))

    central_time_zone = pytz.timezone("US/Central")
    current_time = datetime.datetime.now(central_time_zone)
    df["START_DATE"] = pd.to_datetime(df["START_DATE"]).dt.tz_localize(
        central_time_zone
    )
    df["END_DATE"] = pd.to_datetime(df["END_DATE"]).dt.tz_localize(central_time_zone)

    work_zones = []
    permits = df["FOLDERRSN"].unique()
    for p in permits:
        closures = df[df["FOLDERRSN"] == p]
        segments = closures["SEGMENT_ID"].unique()

        start_date = closures["START_DATE"].iloc[0]
        end_date = closures["END_DATE"].iloc[0]

        # checking if the closure is some time in the future.
        # adding one hour to the end time to help inform consumers that the work zone has officially ended.
        if end_date + datetime.timedelta(hours=1) > current_time:
            wz = WorkZone(
                data_source_id=amanda_id,
                folderrsn=p,
                description=closures["FOLDERDESCRIPTION"].iloc[0],
                start_date=start_date.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
                end_date=end_date.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
            for s in segments:
                seg = closures[closures["SEGMENT_ID"] == s]
                if "Closure : Full Road" in list(seg["CLOSURE_TYPE"]):
                    if s in segment_lookup:
                        wz.add_closure(s, "all-lanes-closed", segment_lookup[s])
                    else:
                        print(f"{s} not found in street segments feature layer")
                elif "Traffic Lane : Dimensions" in list(seg["CLOSURE_TYPE"]):
                    if s in segment_lookup:
                        wz.add_closure(s, "some-lanes-closed", segment_lookup[s])
                    else:
                        print(f"{s} not found in street segments feature layer")
            if wz.get_number_of_closures() > 0:
                work_zones.append(wz)

    feed_info = create_feed_info(amanda_id, current_time)

    features = []
    for wz in work_zones:
        features += wz.generate_json()

    output = {"feed_info": feed_info, "type": "FeatureCollection", "features": features}

    with open("test_export.json", "w") as f:
        json.dump(output, f, ensure_ascii=False)


if __name__ == "__main__":
    main()
