import datetime
import logging
import pandas as pd
import pytz
import uuid
from sodapy import Socrata

import argparse
import json
import os

from amanda import get_amanda_data
from config import (
    amanda_query,
    work_zone_type_mapping,
)
from coordinate import get_activated_work_zones
from utils import get_logger
from workzone import AmandaWorkZone

# Socrata app token
SO_TOKEN = os.getenv("SO_TOKEN")
CONTACT_EMAIL = os.getenv("CONTACT_EMAIL")
SEGMENT_DATASET = os.getenv("SEGMENT_DATASET")

# Optional: Socrata credentials for publishing to a dataset
SO_WEB = os.getenv("SO_WEB")
SO_USER = os.getenv("SO_USER")
SO_PASS = os.getenv("SO_PASS")
FEED_DATASET = os.getenv("FEED_DATASET")
CRITICAL_DATASET = os.getenv("CRITICAL_DATASET")
FLAT_DATASET = os.getenv("FLAT_DATASET")


def get_start_end_date(row):
    """
    Determines the start and end date of a closure using the following logic:
    - If dates are provided by COORDINATE, use those dates.
    - If an extension is present, use the extension dates.
    - Other wise, use the start and end dates of the permit.

    Only coordinate dates are considered "verified".
    """
    if "WORK_ZONE_DATES" in row:
        row["START_DATE"] = row["WORK_ZONE_DATES"]["start"] + " 00:00"
        row["END_DATE"] = row["WORK_ZONE_DATES"]["end"] + " 23:59"
        row["is_start_date_verified"] = True
        row["is_end_date_verified"] = True
        row = convert_date_to_datetime(row)
        return row
    if row["EXTENSION_START_DATE"] and row["EXTENSION_END_DATE"]:
        row["START_DATE"] = row["EXTENSION_START_DATE"]
        row["END_DATE"] = row["EXTENSION_END_DATE"]
    row["is_start_date_verified"] = False
    row["is_end_date_verified"] = False
    row = convert_date_to_datetime(row)
    return row


def convert_date_to_datetime(row: dict, tz="US/Central") -> dict:
    """Parse START_DATE and END_DATE and localize to US/Central, in place."""
    parsed = pd.to_datetime(row.get("START_DATE"), errors="coerce")
    row["start_date_dt"] = (
        None if pd.isna(parsed) else parsed.tz_localize(pytz.timezone(tz))
    )

    parsed = pd.to_datetime(row.get("END_DATE"), errors="coerce")
    row["end_date_dt"] = (
        None if pd.isna(parsed) else parsed.tz_localize(pytz.timezone(tz))
    )
    return row


def log_invalid_dates(permits: dict) -> None:
    """Log rows where START_DATE or END_DATE failed to parse (as opposed to being blank)."""
    for rsn in permits.keys():
        row = permits[rsn]
        if row["start_date_dt"] is None and row["START_DATE"] is not None:
            logger.info(
                f"Invalid start date ({row['START_DATE']}) for folderRSN: {row['FOLDERRSN']} and segment "
                f"ID: {row['SEGMENT_ID']}"
            )
        if row["end_date_dt"] is None and row["END_DATE"] is not None:
            logger.info(
                f"Invalid end date ({row['END_DATE']}) for folderRSN: {row['FOLDERRSN']} and segment "
                f"ID: {row['SEGMENT_ID']}"
            )


def batch_segments(data, batch_size=100):
    for i in range(0, len(data), batch_size):
        yield data[i : i + batch_size]


def get_geometry(segment_ids, client):
    """
    Gets segment geometry from the open data portal.
    :param segment_ids (list): a list of CTM segment IDs to fetch
    :param client (Socrata): Socrata client object
    :return: the geometry of each segment
    """
    # Batching our list of segments into groups of 100. This is to avoid potentially sending too large of a request.
    segment_batches = batch_segments(segment_ids)
    segment_data = []
    for segment_batch in segment_batches:
        segment_batch = ", ".join(map(str, segment_batch))
        segment_data += client.get(
            SEGMENT_DATASET, where=f"segment_id in ({segment_batch})", limit=999999
        )
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


def main(local_file=None):
    # Getting current time for later
    central_time_zone = pytz.timezone("US/Central")
    current_time = datetime.datetime.now(central_time_zone)

    # Getting AMANDA data
    logger.info("Querying AMANDA for TURP/EX permits")
    data = get_amanda_data(amanda_query)
    logger.info(f"Downloaded {len(data)} potential closures.")

    segment_closures = {}
    for rec in data:
        direction = rec["DIRECTION"]
        if direction is None:
            direction = "No Direction"
        if rec["FOLDERRSN"] not in segment_closures:
            segment_closures[rec["FOLDERRSN"]] = {
                rec["SEGMENT_ID"]: {rec["CLOSURE_TYPE"]: {direction.lower()}}
            }
        elif rec["SEGMENT_ID"] not in segment_closures[rec["FOLDERRSN"]]:
            segment_closures[rec["FOLDERRSN"]][rec["SEGMENT_ID"]] = {
                rec["CLOSURE_TYPE"]: {direction.lower()}
            }
        elif direction not in segment_closures[rec["FOLDERRSN"]][rec["SEGMENT_ID"]]:
            segment_closures[rec["FOLDERRSN"]][rec["SEGMENT_ID"]][
                rec["CLOSURE_TYPE"]
            ] = {direction.lower()}
        else:
            segment_closures[rec["FOLDERRSN"]][rec["SEGMENT_ID"]][
                rec["CLOSURE_TYPE"]
            ].add(direction.lower())

    permit_details = {}
    excluded_keys = [
        "CLOSURE_TYPE",
        "SEGMENT_ID",
        "LENGTH",
        "WIDTH",
        "NUM_LANES",
        "DIRECTION",
    ]
    for rec in data:
        permit_details[rec["FOLDERRSN"]] = {
            k: v for k, v in rec.items() if k not in excluded_keys
        }

    # Get activated Work Zones from Coordinate
    logger.info("Retrieving activated work zones from Coordinate")
    active_folder_rsns, work_zone_dates = get_activated_work_zones()
    logger.info(f"{len(active_folder_rsns)} Activated Work Zones retrieved")

    for rsn in permit_details.keys():
        if rsn in active_folder_rsns:
            permit_details[rsn]["activated_permit"] = True
        else:
            permit_details[rsn]["activated_permit"] = False
        if rsn in work_zone_dates:
            permit_details[rsn]["WORK_ZONE_DATES"] = work_zone_dates[rsn]

    # Getting the list of unique street segments present in our data
    segments = []
    for rsn in segment_closures.keys():
        segments += segment_closures[rsn].keys()
    segments = list(set(segments))

    logger.info(f"Retrieving street segment geometry from Socrata")
    soda_client = Socrata(
        SO_WEB,
        SO_TOKEN,
        username=SO_USER,
        password=SO_PASS,
        timeout=500,
    )
    segment_info = get_geometry(segments, soda_client)
    segment_details = {}
    for record in segment_info:
        if record["segment_id"] not in segment_details:
            segment_details[record["segment_id"]] = {
                record["bearing_dir"].lower(): record
            }
        else:
            segment_details[record["segment_id"]][
                record["bearing_dir"].lower()
            ] = record

    # Generating UUIDs for our data sources
    amanda_turp_id = str(uuid.uuid5(uuid.NAMESPACE_OID, "COA_AMANDA_TURP"))
    amanda_ex_id = str(uuid.uuid5(uuid.NAMESPACE_OID, "COA_AMANDA_EX"))

    # Creating start/end date including logic for extensions
    for rsn in permit_details.keys():
        permit_details[rsn] = get_start_end_date(permit_details[rsn])

    # Logging of invalid dates
    log_invalid_dates(permit_details)

    # Ignores permits with invalid dates, this can happen as the extension date is just a string input with no form validation.
    rsns_to_remove = []
    for rsn in permit_details.keys():
        if (
            not permit_details[rsn]["start_date_dt"]
            or not permit_details[rsn]["end_date_dt"]
        ):
            rsns_to_remove.append(rsn)
    for rsn in rsns_to_remove:
        permit_details.pop(rsn, None)

    work_zones = []
    for permit_id in permit_details.keys():
        if permit_id not in segment_closures:
            continue
        permit_closures = segment_closures[permit_id]
        details = permit_details[permit_id]

        # Checking if this is an activated work zone in Coordinate
        if details["activated_permit"]:
            worker_presence = True
        else:
            worker_presence = False

        # Naming and description logic
        if details["FOLDERTYPE"] == "RW":
            # Filtering out details from franchise utilities.
            if details["SUBCODE"] == 50500 and details["WORKCODE"] in (
                50570,
                50575,
                50580,
            ):
                description = "Temporary use of Right of Way Permit has been issued for this location."
                name = "WorkZone Event"
            else:
                description = f"Temporary use of Right of Way Permit has been issued for this location. \n Details: {details['FOLDERDESCRIPTION']}"
                name = details["FOLDERNAME"]
            data_source_id = amanda_turp_id
        elif details["FOLDERTYPE"] == "EX":
            # Filtering out details from franchise utilities.
            if details["SUBCODE"] == 50685:
                description = "Excavation Permit has been issued for this location."
                name = "WorkZone Event"
            else:
                description = f"Excavation Permit has been issued for this location. \n Details: {details['FOLDERDESCRIPTION']}"
                name = details["FOLDERNAME"]

            data_source_id = amanda_ex_id

        if details["WORK_ZONE_TYPE"]:
            work_zone_type = work_zone_type_mapping.get(
                details["WORK_ZONE_TYPE"].lower()
            )
        else:
            work_zone_type = "static"

        # Checking if the closure is some time in the future, if it's not we do not publish it to the feed.
        # Adding one hour to the end time to help inform consumers that the work zone has officially ended.
        if details["end_date_dt"] + datetime.timedelta(hours=1) > current_time:
            wz = AmandaWorkZone(
                data_source_id=data_source_id,
                name=name,
                folderrsn=permit_id,
                description=description,
                start_date=details["start_date_dt"]
                .tz_convert("UTC")
                .strftime("%Y-%m-%dT%H:%M:%SZ"),
                end_date=details["end_date_dt"]
                .tz_convert("UTC")
                .strftime("%Y-%m-%dT%H:%M:%SZ"),
                work_zone_type=work_zone_type,
                workers_present=worker_presence,
                start_date_verified=details["is_start_date_verified"],
                end_date_verified=details["is_end_date_verified"],
            )
            # Closure type logic
            # This is how we convert AMANDA road closures into workzone closure types
            for segment_id in permit_closures.keys():
                closures = permit_closures[segment_id]
                if str(segment_id) not in segment_details:
                    logger.info(
                        f"{segment_id} not found in street segments feature layer under folderrsn {permit_id}"
                    )
                    continue
                directions_details = segment_details[str(segment_id)]
                possible_directions = [
                    k for k in directions_details if k != "centerline"
                ]
                # 1. First thing to check is if there is a full directional of all directions.
                if "Closure : Full Road" in closures:
                    # Close all directions here
                    for direction in possible_directions:
                        wz.add_closure(
                            segment_id,
                            veh_impact="all-lanes-closed",
                            segment_info=directions_details[direction],
                            direction=direction,
                        )
                    continue
                closed_dir = None
                if (
                    "Closure : Does this result in a full directional closure?"
                    in closures
                ):
                    direction = closures[
                        "Closure : Does this result in a full directional closure?"
                    ]
                    closed_dir = next(iter(direction))
                    if closed_dir != "both directions" and closed_dir != "no direction":
                        wz.add_closure(
                            segment_id,
                            veh_impact="all-lanes-closed",
                            segment_info=directions_details[closed_dir],
                            direction=closed_dir,
                        )
                if (
                    "Traffic Lane : Dimensions" in closures
                    or "Open Cuts : Street" in closures
                ):
                    directions_affected = set()
                    if "Traffic Lane : Dimensions" in closures:
                        directions_affected = (
                            directions_affected | closures["Traffic Lane : Dimensions"]
                        )
                    if "Open Cuts : Street" in closures:
                        directions_affected = (
                            directions_affected | closures["Open Cuts : Street"]
                        )

                    if "both directions" in directions_affected:
                        # add a partial closure in both directions, unless it is already closed by the earlier step
                        for direction in possible_directions:
                            if direction != closed_dir:
                                wz.add_closure(
                                    segment_id,
                                    veh_impact="some-lanes-closed",
                                    segment_info=directions_details[direction],
                                    direction=direction,
                                )
                        continue
                    # Directional partial closures
                    directional_partial_closure_published = False
                    for direction in directions_affected:
                        if direction in directions_details.keys():
                            # Close partial direction if not closed by the earlier step
                            if direction != closed_dir:
                                wz.add_closure(
                                    segment_id,
                                    veh_impact="some-lanes-closed",
                                    segment_info=directions_details[direction],
                                    direction=direction,
                                )
                                directional_partial_closure_published = True
                    if (
                        "no direction" in directions_affected
                        and not directional_partial_closure_published
                        and not closed_dir
                    ):
                        # If we got no idea what to do just make an unknown partial closure.
                        # Check if anything as been closed, if not then do it.
                        wz.add_closure(
                            segment_id,
                            veh_impact="some-lanes-closed",
                            segment_info=directions_details["centerline"],
                            direction="unknown",
                        )
            if wz.get_number_of_closures() > 0:
                work_zones.append(wz)

    # Generates a json blob of feed metadata
    feed_info = create_feed_info(amanda_turp_id, amanda_ex_id, current_time)

    # generate all closure feature's json blobs
    features = []
    critical_features = []
    for wz in work_zones:
        wz.reduce_closure_geometry()
        features += wz.generate_json()
        critical_wz = wz.generate_critical_corridor_json()
        if critical_wz:
            critical_features += critical_wz

    # Stitching everything together
    output = {"feed_info": feed_info, "type": "FeatureCollection", "features": features}
    output_critical = {
        "feed_info": feed_info,
        "type": "FeatureCollection",
        "features": critical_features,
    }

    if local_file:
        logger.info(
            f"Writing output to local file: {local_file}.geojson and {local_file}.csv"
        )
        with open(f"{local_file}.geojson", "w") as f:
            json.dump(output, f, indent=2)
        # for flat exporting to socrata:
        features = []
        for wz in work_zones:
            features += wz.generate_socrata_export()
        pd.DataFrame(features).to_csv(f"{local_file}.csv", index=False)

    # Output to Socrata feed/dataset
    if SO_USER and SO_PASS:
        logger.info("Uploading data to Socrata")
        # logging in with sodapy
        logger.info("uploading geojson files to Socrata")
        files = {"file": ("wzdx_atx.geojson", json.dumps(output, indent=2))}
        response = soda_client.replace_non_data_file(FEED_DATASET, {}, files)
        logger.info(response)
        files_critical = {
            "file": ("wzdx_atx_critical.geojson", json.dumps(output_critical, indent=2))
        }
        response = soda_client.replace_non_data_file(
            CRITICAL_DATASET, {}, files_critical
        )
        logger.info(response)

        # for flat exporting to socrata:
        features = []
        for wz in work_zones:
            features += wz.generate_socrata_export()
        logger.info("uploading flat dataset to Socrata")
        response = soda_client.replace(FLAT_DATASET, features)
        logger.info(response)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate WZDx feed")
    parser.add_argument(
        "--local-file",
        help="Optional path to write the output GeoJSON feed locally. For debugging or validation.",
        required=False,
    )
    args = parser.parse_args()

    logger = get_logger(
        __name__,
        level=logging.INFO,
    )

    main(local_file=args.local_file)
