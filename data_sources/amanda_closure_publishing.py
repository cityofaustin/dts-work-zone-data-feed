import datetime
import logging
import pandas as pd
import pytz
import uuid
from sodapy import Socrata

import json
import os

from amanda import get_amanda_data
from config import amanda_closure_mapping, turp_query, excavation_permits
from utils import get_logger
from workzone import AmandaWorkZone

# Socrata app token
SO_TOKEN = os.getenv("SO_TOKEN")
CONTACT_EMAIL = os.getenv("CONTACT_EMAIL")

# Optional: Socrata credentials for publishing to a dataset
SO_WEB = os.getenv("SO_WEB")
SO_USER = os.getenv("SO_USER")
SO_PASS = os.getenv("SO_PASS")
FEED_DATASET = os.getenv("FEED_DATASET")
FLAT_DATASET = os.getenv("FLAT_DATASET")


def get_start_end_date(row):
    if not pd.isnull(row["EXTENSION_START_DATE"]) and not pd.isnull(
        row["EXTENSION_END_DATE"]
    ):
        row["START_DATE"] = row["EXTENSION_START_DATE"]
        row["END_DATE"] = row["EXTENSION_END_DATE"]
    return row


def batch_segments(data, batch_size=100):
    for i in range(0, len(data), batch_size):
        yield data[i : i + batch_size]


def get_geometry(segment_ids):
    """
    Gets CTM segment geometry from the open data portal.
    :param segment_ids (list): a list of CTM segment IDs to fetch
    :return: the geometry of each segment
    """
    # Batching our list of segments into groups of 100. This is to avoid potentially sending too large of a request.
    segment_batches = batch_segments(segment_ids)
    segment_data = []
    for segment_batch in segment_batches:
        segment_batch = ", ".join(map(str, segment_batch))
        client = Socrata("data.austintexas.gov", app_token=SO_TOKEN)
        segment_data += client.get(
            "8hf2-pdmb", where=f"segment_id in ({segment_batch})", limit=999999
        )

    # socrata stores all segments as MultilineStrings, when they're single LineStrings
    for s in segment_data:
        if s["the_geom"]["type"] == "MultiLineString":
            s["the_geom"]["type"] = "LineString"
            s["the_geom"]["coordinates"] = s["the_geom"]["coordinates"][0]
    return segment_data


def create_feed_info(turp_id, ex_id, current_time):
    feed_info = {
        "publisher": "City of Austin",
        "version": "4.2",
        "license": "https://creativecommons.org/publicdomain/zero/1.0/",
        "update_date": current_time.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "update_frequency": 3600,
        "data_sources": [
            {
                "data_source_id": turp_id,
                "organization_name": "City of Austin: AMANDA Right of Way Permits",
                "update_date": current_time.astimezone(pytz.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "update_frequency": 3600,
                "contact_name": "Transportation and Public Works Department",
                "contact_email": CONTACT_EMAIL,
            },
            {
                "data_source_id": ex_id,
                "organization_name": "City of Austin: AMANDA Excavation Permits",
                "update_date": current_time.astimezone(pytz.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "update_frequency": 3600,
                "contact_name": "Transportation and Public Works Department",
                "contact_email": CONTACT_EMAIL,
            },
        ],
        "update_date": current_time.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "update_frequency": 3600,  # assuming 1 hr refresh rate
        "contact_name": "Transportation and Public Works Department",
        "contact_email": CONTACT_EMAIL,
    }

    return feed_info


def main():
    # Getting AMANDA data
    # Temporary Use of Right of Way (TURP) permits:
    logger.info(f"Querying AMANDA for TURP permits")
    data = get_amanda_data(turp_query)
    closures = pd.DataFrame(data)
    logger.info(f"Downloaded {len(closures['FOLDERRSN'].unique())} TURP permits")

    # Excavation (EX) permits:
    logger.info(f"Querying AMANDA for EX permits")
    data = get_amanda_data(excavation_permits)
    closures = pd.concat([closures, pd.DataFrame(data)])
    logger.info(f"Downloaded {len(closures['FOLDERRSN'].unique())} EX permits")

    # Getting the list of unique street segments present in our data
    segments = closures[
        closures["CLOSURE_TYPE"].isin(
            ["Closure : Full Road", "Traffic Lane : Dimensions", "Open Cuts : Street"]
        )
    ]["SEGMENT_ID"].unique()
    logger.info(f"Retrieving CTM street segments from Socrata")
    segment_info = get_geometry(segments)
    segment_lookup = {}

    # Generating a lookup dict of street segment IDs for later
    for segment_id in segment_info:
        segment_lookup[int(segment_id["segment_id"])] = segment_id

    # Generating UUIDs data sources
    amanda_turp_id = str(uuid.uuid5(uuid.NAMESPACE_OID, "COA_AMANDA_TURP"))
    amanda_ex_id = str(uuid.uuid5(uuid.NAMESPACE_OID, "COA_AMANDA_EX"))

    # Creating start/end date including logic for extensions
    closures = closures.apply(get_start_end_date, axis=1)
    central_time_zone = pytz.timezone("US/Central")
    current_time = datetime.datetime.now(central_time_zone)
    closures["start_date_dt"] = pd.to_datetime(closures["START_DATE"]).dt.tz_localize(
        central_time_zone
    )
    closures["end_date_dt"] = pd.to_datetime(closures["END_DATE"]).dt.tz_localize(
        central_time_zone
    )

    work_zones = []

    # Iterating by permit number, our dataframe contains multiple closures per permit ID.
    permit_ids = closures["FOLDERRSN"].unique()
    for permit_id in permit_ids:
        # Filtering our closures dataframe to only the ones associated with the selected permit
        permit_closures = closures[closures["FOLDERRSN"] == permit_id]

        # Gathering permit metadata from the first row of our closures dataframe.
        # This is a consequence of how we've retrieved the data from AMANDA
        permit_type = permit_closures["FOLDERTYPE"].iloc[0]
        folderdesc = permit_closures["FOLDERDESCRIPTION"].iloc[0]
        foldername = permit_closures["FOLDERNAME"].iloc[0]
        subtype = permit_closures["SUBCODE"].iloc[0]
        workcode = permit_closures["WORKCODE"].iloc[0]
        start_date = permit_closures["start_date_dt"].iloc[0]
        end_date = permit_closures["end_date_dt"].iloc[0]

        # Naming and description logic
        if permit_type == "RW":
            # Filtering out details from franchise utilities.
            if subtype == 50500 and workcode in (50570, 50575, 50580):
                description = f"Temporary use of Right of Way Permit has been issued for this location."
                name = "WorkZone Event"
            else:
                description = f"Temporary use of Right of Way Permit has been issued for this location. \n Details: {folderdesc}"
                name = foldername
            data_source_id = amanda_turp_id
        elif permit_type == "EX":
            # Filtering out details from franchise utilities.
            if subtype == 50685:
                description = f"Excavation Permit has been issued for this location."
                name = "WorkZone Event"
            else:
                description = f"Excavation Permit has been issued for this location. \n Details: {folderdesc}"
                name = foldername

            data_source_id = amanda_ex_id

        # Gathering the list of unique segment IDs for iterating on below.
        segments = permit_closures["SEGMENT_ID"].unique()

        # Checking if the closure is some time in the future, if it's not we do not publish it to the feed.
        # Adding one hour to the end time to help inform consumers that the work zone has officially ended.
        if end_date + datetime.timedelta(hours=1) > current_time:
            wz = AmandaWorkZone(
                data_source_id=data_source_id,
                name=name,
                folderrsn=permit_id,
                description=description,
                start_date=start_date.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
                end_date=end_date.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
            # Closure type logic
            # This is how we convert AMANDA road closures into workzone closure types
            for segment_id in segments:
                # Filtering to the closure types that have been applied to this one segment ID
                seg = permit_closures[permit_closures["SEGMENT_ID"] == segment_id]
                for closure_type in amanda_closure_mapping:
                    if closure_type["amanda_closure"] in list(seg["CLOSURE_TYPE"]):
                        if segment_id in segment_lookup:
                            wz.add_closure(
                                segment_id,
                                veh_impact=closure_type["vehicle_impact"],
                                segment_info=segment_lookup[segment_id],
                            )
                            # If we find a closure type, we break out of the loop. This makes the order of
                            # amanda_closure_mapping important.
                            break
                        else:
                            logger.info(
                                f"{segment_id} not found in street segments feature layer under folderrsn {permit_id}"
                            )
            if wz.get_number_of_closures() > 0:
                work_zones.append(wz)

    # Generates a json blob of feed metadata
    feed_info = create_feed_info(amanda_turp_id, amanda_ex_id, current_time)

    # generate all closure feature's json blobs
    features = []
    for wz in work_zones:
        wz.reduce_closure_geometry()
        features += wz.generate_json()

    # Stitching everything together
    output = {"feed_info": feed_info, "type": "FeatureCollection", "features": features}

    # Output to Socrata feed/dataset
    if SO_USER and SO_PASS:
        logger.info("Uploading data to Socrata")
        # logging in with sodapy
        soda = Socrata(
            SO_WEB,
            SO_TOKEN,
            username=SO_USER,
            password=SO_PASS,
            timeout=500,
        )
        logger.info("uploading geojson file to Socrata")
        files = {"file": ("wzdx_atx.geojson", json.dumps(output))}
        response = soda.replace_non_data_file(FEED_DATASET, {}, files)
        logger.info(response)

        # for flat exporting to socrata:
        features = []
        for wz in work_zones:
            features += wz.generate_socrata_export()
        logger.info("uploading flat dataset to Socrata")
        response = soda.replace(FLAT_DATASET, features)
        logger.info(response)


if __name__ == "__main__":
    logger = get_logger(
        __name__,
        level=logging.INFO,
    )

    main()
