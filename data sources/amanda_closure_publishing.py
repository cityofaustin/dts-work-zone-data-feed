import pandas as pd
import datetime
import uuid
import pytz
from sodapy import Socrata

import json
import os

from amanda import get_amanda_data
from config import turp_query, excavation_permits
from workzone import AmandaWorkZone

# Socrata app token
SO_TOKEN = os.getenv("SO_TOKEN")


def get_start_end_date(row):
    if not pd.isnull(row["EXTENSION_START_DATE"]) and not pd.isnull(
            row["EXTENSION_END_DATE"]
    ):
        row["START_DATE"] = row["EXTENSION_START_DATE"]
        row["END_DATE"] = row["EXTENSION_END_DATE"]
    return row


def get_geometry(segment_ids):
    """
    Gets CTM segment geometry from the open data portal.
    :param segment_ids (list): a list of CTM segment IDs to fetch
    :return: the geometry of each segment
    """
    segment_ids = ", ".join(map(str, segment_ids))
    client = Socrata("data.austintexas.gov", app_token=SO_TOKEN)
    segments = client.get(
        "8hf2-pdmb", where=f"segment_id in ({segment_ids})", limit=999999
    )

    # socrata stores all segments as MultilineStrings, when they're single LineStrings
    for s in segments:
        if s["the_geom"]["type"] == "MultiLineString":
            s["the_geom"]["type"] = "LineString"
            s["the_geom"]["coordinates"] = s["the_geom"]["coordinates"][0]
    return segments


def create_feed_info(data_source_id, current_time):
    feed_info = {
        "publisher": "City of Austin",
        "version": "4.2",
        "license": "https://creativecommons.org/publicdomain/zero/1.0/",
        "data_sources": [
            {
                "data_source_id": data_source_id,
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
    # Getting AMANDA data
    data = get_amanda_data(turp_query)
    df = pd.DataFrame(data)
    data = get_amanda_data(excavation_permits)
    df = pd.concat([df, pd.DataFrame(data)])

    df = df.apply(get_start_end_date, axis=1)
    segments = df[
        df["CLOSURE_TYPE"].isin(["Closure : Full Road", "Traffic Lane : Dimensions", "Open Cuts : Street"])
    ]["SEGMENT_ID"].unique()
    segment_info = get_geometry(segments)
    segment_lookup = {}

    # Generating a dictionary of street segment IDs
    for s in segment_info:
        segment_lookup[int(s["segment_id"])] = s

    # Generating UUIDs data sources
    amanda_turp_id = str(uuid.uuid5(uuid.NAMESPACE_OID, "COA_AMANDA_TURP"))
    amanda_ex_id = str(uuid.uuid5(uuid.NAMESPACE_OID, "COA_AMANDA_EX"))

    central_time_zone = pytz.timezone("US/Central")
    current_time = datetime.datetime.now(central_time_zone)
    df["START_DATE"] = pd.to_datetime(df["START_DATE"]).dt.tz_localize(
        central_time_zone
    )
    df["END_DATE"] = pd.to_datetime(df["END_DATE"]).dt.tz_localize(central_time_zone)

    work_zones = []
    permits = df["FOLDERRSN"].unique()
    for p in permits:
        permit_type = df["FOLDERTYPE"].iloc[0]
        if permit_type == "RW":
            description = "Temporary use of Right of Way Permit has been issued for this location."
        elif permit_type == "EX":
            description = "Excavation Permit has been issued for this location."

        closures = df[df["FOLDERRSN"] == p]
        segments = closures["SEGMENT_ID"].unique()

        start_date = closures["START_DATE"].iloc[0]
        end_date = closures["END_DATE"].iloc[0]

        # checking if the closure is some time in the future.
        # adding one hour to the end time to help inform consumers that the work zone has officially ended.
        if end_date + datetime.timedelta(hours=1) > current_time:
            wz = AmandaWorkZone(
                data_source_id=amanda_turp_id,
                folderrsn=p,
                description=description,
                start_date=start_date.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
                end_date=end_date.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
            for s in segments:
                seg = closures[closures["SEGMENT_ID"] == s]
                if "Closure : Full Road" in list(seg["CLOSURE_TYPE"]):
                    if s in segment_lookup:
                        wz.add_closure(s, "all-lanes-closed", segment_lookup[s])
                    else:
                        print(f"{s} not found in street segments feature layer under folderrsn {p}")
                elif "Traffic Lane : Dimensions" in list(seg["CLOSURE_TYPE"]) or "Open Cuts : Street" in list(
                        seg["CLOSURE_TYPE"]):
                    if s in segment_lookup:
                        wz.add_closure(s, "some-lanes-closed", segment_lookup[s])
                    else:
                        print(f"{s} not found in street segments feature layer under folderrsn {p}")
            if wz.get_number_of_closures() > 0:
                work_zones.append(wz)

    feed_info = create_feed_info(amanda_turp_id, current_time)

    features = []
    for wz in work_zones:
        features += wz.generate_json()

    output = {"feed_info": feed_info, "type": "FeatureCollection", "features": features}

    with open("test_export.json", "w") as f:
        json.dump(output, f, ensure_ascii=False)

    # for exporting to AGOL:
    features = []
    for wz in work_zones:
        features += wz.generate_agol_export()

    with open("agol_export.geojson", "w") as f:
        json.dump(output, f, ensure_ascii=False)


if __name__ == "__main__":
    main()
