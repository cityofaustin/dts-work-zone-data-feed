import os
import logging
import warnings
import requests

import geopandas as gpd
import pandas as pd
import pyproj
from sodapy import Socrata
from shapely.geometry import LineString, shape, mapping
from shapely.ops import linemerge

from utils import get_logger

SO_TOKEN = os.getenv("SO_TOKEN")
SO_WEB = os.getenv("SO_WEB")
SO_USER = os.getenv("SO_USER")
SO_PASS = os.getenv("SO_PASS")
SOURCE_DATASET = os.getenv("SOURCE_SEGMENT_DATASET")
SEGMENT_DATASET = os.getenv("SEGMENT_DATASET")
AGOL_USERNAME = os.getenv("AGOL_USERNAME")
AGOL_PASSWORD = os.getenv("AGOL_PASSWORD")


def unwrap_multiline(geom, on_discontinuous="warn"):
    """
    Converts a MultiLineString geometry to a single LineString.

    Before merging, checks whether the component lines actually form a
    continuous path (i.e. each line's endpoint connects to the next
    line's start/end, with no gaps or branching). If they do, the merge
    is exact and order-independent. If they don't, the function falls
    back to naive point concatenation and warns/raises
    so you know the result may not represent a real continuous path.

    Parameters
    ----------
    geom : shapely.geometry.MultiLineString or shapely.geometry.LineString
    on_discontinuous : str, {"warn", "raise", "ignore"}
        What to do if the MultiLineString is NOT a continuous path.
        - "warn":   merge anyway via point concatenation, emit a warning
        - "raise":  raise a ValueError instead of merging
        - "ignore": merge anyway via point concatenation, silently

    Returns
    -------
    shapely.geometry.LineString
    """

    if geom.geom_type == "MultiLineString":
        # If there's only one LineString inside, extract it
        if len(geom.geoms) == 1:
            return geom.geoms[0]

        # linemerge stitches lines together based on shared endpoints,
        # regardless of input order or direction. If the result is a
        # single LineString, the parts were truly continuous.
        merged = linemerge(geom)

        if merged.geom_type == "LineString":
            return merged

        # merged is still a MultiLineString -> parts don't connect
        # (gap between segments, or a branching/forked structure)
        msg = (
            f"MultiLineString with {len(geom.geoms)} parts is not a continuous "
            "path (gap or branch detected between segments). Falling back to "
            "naive point concatenation in original order, which may produce "
            "a misleading line."
        )
        if on_discontinuous == "raise":
            raise ValueError(msg)
        elif on_discontinuous == "warn":
            warnings.warn(msg)
        return LineString([pt for line in geom.geoms for pt in line.coords])

    return geom

def tagging_critical_corridor_segments(df):
    critical = get_critical_corridor_segments()
    df = df.to_crs(2277)
    df["critical_corridor"] = df.geometry.intersects(critical)
    df = df.to_crs(4326)
    return df


def get_critical_corridor_segments():
    token = get_token(AGOL_USERNAME, AGOL_PASSWORD)
    layer_url = "https://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/CCO_Critical_Corridor_Network/FeatureServer/4"
    geojson_features = query_all_features(layer_url, token)
    critical = gpd.GeoDataFrame.from_features(geojson_features, crs="EPSG:4326")
    # Reproject to a feet-based CRS so buffer distance is in feet
    critical = critical.to_crs("EPSG:2277")
    # Buffer 100 ft
    critical["geometry"] = critical.buffer(100)
    # merging them all into one shape
    critical = critical.unary_union
    return critical

def get_token(username: str, password: str) -> str:
    """Generate a short-lived ArcGIS Online token."""
    token_url = "https://www.arcgis.com/sharing/rest/generateToken"
    resp = requests.post(
        token_url,
        data={
            "username": username,
            "password": password,
            "referer": "https://www.arcgis.com",
            "f": "json",
            "expiration": 60,  # minutes
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "token" not in data:
        raise RuntimeError(f"Token generation failed: {data}")
    return data["token"]

def query_all_features(layer_url: str, token: str) -> list[dict]:
    """Page through a layer's features (handles maxRecordCount) and return GeoJSON features."""
    features = []
    offset = 0
    page_size = 1000  # will be clipped to server's actual max automatically by most services

    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": page_size,
            "token": token,
        }
        resp = requests.get(f"{layer_url}/query", params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            raise RuntimeError(f"Query error on {layer_url}: {data['error']}")

        page_features = data.get("features", [])
        if not page_features:
            break

        features.extend(page_features)

        # Stop if server says there are no more, or if we got fewer than requested
        if not data.get("properties", {}).get("exceededTransferLimit", False) and len(page_features) < page_size:
            break

        offset += len(page_features)

    return features

def bearing(line):
    """
    Calculate the bearing of a line segment.
    Parameters
    ----------
    line (shapely.geometry.linestring.LineString): The line segment to calculate the bearing of.

    Returns
    -------
    The bearing of the line segment as a float from 0.0-360.0
    """
    geodesic = pyproj.Geod(ellps="WGS84")
    x1, y1 = line.coords[0]
    x2, y2 = line.coords[-1]
    fwd_azimuth, back_azimuth, distance = geodesic.inv(x1, y1, x2, y2)
    return fwd_azimuth % 360


def direction_label(row):
    """
    Generates a directionality label for a line segment based on the bearing of the line (0-360 degrees)
    and the road name's prefix direction (N, S, E, W)

    Parameters
    ----------
    row: Operates on a single row of a geopandas dataframe containing: bearing, prefix_direction

    Returns
    -------
    "Eastbound", "Westbound", "Northbound", or "Southbound" as appropriate.

    """
    bearing = row["bearing"]
    orientation = row["prefix_direction"]
    # Default to compass heading if no prefix direction is provided
    if pd.isna(orientation) or orientation is None:
        if 45 <= bearing < 135:
            return "Eastbound"
        elif 135 <= bearing < 225:
            return "Southbound"
        elif 225 <= bearing < 315:
            return "Westbound"
        else:
            return "Northbound"
    # Road is considered running north-south
    if orientation in ["N", "S"]:
        if 0 <= bearing < 90:
            return "Northbound"
        elif 90 <= bearing < 270:
            return "Southbound"
        else:
            return "Northbound"
    # Road is considered running east-west
    if orientation in ["E", "W"]:
        if 0 <= bearing < 180:
            return "Eastbound"
        else:
            return "Westbound"

    # If we get here, something unexpected happened, raise an error.
    raise ValueError(
        f"direction_label() could not determine direction for row: "
        f"bearing={bearing}, prefix_direction={orientation}"
    )


def offset_line(geometry, offset_dist=2):
    """
    Moves a line segment by an offset distance to the right. Right is defined as to the right facing the heading of the line.

    Parameters
    ----------
    geometry: shapely.geometry.linestring.LineString
    offset_dist: unit matches whatever coordinate system you applied to geometry. Defaults to 2.

    Returns
    -------
    The geometry shifted offset_dist units to the right.
    """
    # note that "right" will cause the line to reverse in directionality, so we flip it back
    return geometry.parallel_offset(offset_dist, "right").reverse()


def keep_row(row):
    """
    Logic for handling one-way streets. We want to remove segments from one way streets that are not in the proper
    direction of travel. one_way is provided from the CTM street segment data.
    Keep forward if one_way == "FT" (From → To)
    Keep reverse if one_way == "TF" (To → From)
    Keep both if one_way == "B" or undefined

    Parameters
    ----------
    row: Operates on a single row of a geopandas dataframe containing: one_way, direction

    Returns
    -------
    True or False. We will remove rows from the dataframe which contain False after.

    """
    ow = row["one_way"]
    dirn = row["direction"]
    if ow == "FT" and dirn == "Forward":
        return True
    elif ow == "TF" and dirn == "Reverse":
        return True
    elif ow in ("B", "", "NONE", "NAN", "NULL") or pd.isna(ow):
        return True
    else:
        return False


def gdf_to_payload_with_geojson(gdf):
    out = pd.DataFrame(gdf)
    # Convert to geoJSON output, which is what socrata expects
    out["geometry"] = out["geometry"].apply(mapping)

    # Replace NaN/NaT with None for all columns
    out = out.where(out.notna(), None)
    return out.to_dict(orient="records")


def main():
    # Logging into Socrata for retrieving and publishing data.
    soda_client = Socrata(
        SO_WEB,
        SO_TOKEN,
        username=SO_USER,
        password=SO_PASS,
        timeout=10*60,
    )

    # Downloading street segments from socrata
    logger.info("Downloading street segments from Socrata")
    segment_data = soda_client.get(SOURCE_DATASET, limit=999999)
    df = pd.DataFrame(segment_data)

    logger.info(f"Transforming {len(df)} street segments")
    # Converting to geopandas dataframe
    df["geometry"] = df["the_geom"].apply(lambda x: shape(x))
    df.drop(["the_geom"], axis=1, inplace=True)
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

    # Cleaning up data converting to single line geometry. Socrata is stored as multiline but in my testing all of
    # them are actually single lines.
    gdf["geometry"] = gdf.geometry.apply(unwrap_multiline)

    # Tagging segments if they are a part of the critical coordinator network
    gdf = tagging_critical_corridor_segments(gdf)

    # Calculate bearing for the line
    gdf["bearing"] = gdf.geometry.apply(bearing)

    gdf_fwd = gdf.copy()
    gdf_fwd["direction"] = "Forward"

    # Making a copy to contain reversed line geometries
    gdf_rev = gdf.copy()
    gdf_rev["geometry"] = gdf_rev.geometry.reverse()
    gdf_rev["direction"] = "Reverse"

    # Combine forward + reverse
    gdf_dir = pd.concat([gdf_fwd, gdf_rev], ignore_index=True)

    # Calculate directionality label ex: "Northbound"
    gdf_dir["bearing_dir"] = gdf_dir.apply(direction_label, axis=1)

    # For reversed geometries, make the label to the opposite direction.
    flip = {
        "Northbound": "Southbound",
        "Southbound": "Northbound",
        "Eastbound": "Westbound",
        "Westbound": "Eastbound",
    }
    gdf_dir.loc[gdf_dir["direction"] == "Reverse", "bearing_dir"] = gdf_dir.loc[
        gdf_dir["direction"] == "Reverse", "bearing_dir"
    ].replace(flip)

    # Convert to projected coordinates: NAD83 / Texas Central (ftUS)
    gdf_dir = gdf_dir.to_crs(2277)
    # Move all lines 2 feet to the right, so they're not on top of each other.
    gdf_dir["geometry"] = gdf_dir.geometry.apply(offset_line)
    # Convert back to WGS84
    gdf_dir = gdf_dir.to_crs(4326)

    # A rare bug with parallel_offset can cause some single lines to become multilines.
    gdf_dir["geometry"] = gdf_dir.geometry.apply(unwrap_multiline)

    # Making sure one_way is always upper case and no spaces.
    gdf_dir["one_way"] = gdf_dir["one_way"].astype(str).str.upper().str.strip()

    # Handling one way streets logic
    gdf_dir = gdf_dir[gdf_dir.apply(keep_row, axis=1)].copy()

    # Adding centerline geometry back into the dataset
    gdf["bearing_dir"] = "centerline"
    output = pd.concat([gdf, gdf_dir], ignore_index=True)

    # Sending data to socrata open data portal
    logger.info(f"Sending {len(output)} records to socrata open data portal")
    data = gdf_to_payload_with_geojson(output)
    response = soda_client.replace(SEGMENT_DATASET, payload=data)
    logger.info(response)


if __name__ == "__main__":
    logger = get_logger(
        __name__,
        level=logging.INFO,
    )

    main()
