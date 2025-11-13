from sodapy import Socrata
import os
import pyproj
import geopandas as gpd
from shapely.geometry import LineString, shape
import numpy as np

import pandas as pd

SO_TOKEN = os.getenv("SO_TOKEN")
SO_WEB = os.getenv("SO_WEB")
SO_USER = os.getenv("SO_USER")
SO_PASS = os.getenv("SO_PASS")


def bearing(line):
    geodesic = pyproj.Geod(ellps="WGS84")
    x1, y1 = line.coords[0]
    x2, y2 = line.coords[-1]
    fwd_azimuth, back_azimuth, distance = geodesic.inv(x1, y1, x2, y2)
    return fwd_azimuth % 360


def unwrap_multiline(geom):
    if geom.geom_type == "MultiLineString":
        # If there's only one LineString inside, extract it
        if len(geom.geoms) == 1:
            return geom.geoms[0]
        else:
            # If there are multiple, merge them into one continuous line
            return LineString([pt for line in geom.geoms for pt in line.coords])
    return geom


# gdf = gpd.read_file("Street_Centerline.geojson").to_crs(4326)

soda_client = Socrata(
    SO_WEB,
    SO_TOKEN,
    username=SO_USER,
    password=SO_PASS,
    timeout=500,
)
segment_data = soda_client.get("8hf2-pdmb", limit=999999)
df = pd.DataFrame(segment_data)
df["geometry"] = df["the_geom"].apply(lambda x: shape(x))
df.drop(["the_geom"], axis=1, inplace=True)
gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
# gdf = gdf[gdf["objectid"]=="57499"]

# Apply to your GeoDataFrame
gdf["geometry"] = gdf.geometry.apply(unwrap_multiline)

gdf["bearing"] = gdf.geometry.apply(bearing)

gdf_fwd = gdf.copy()
gdf_rev = gdf.copy()
gdf_rev["geometry"] = gdf_rev.geometry.reverse()
gdf_fwd["direction"] = "Forward"
gdf_rev["direction"] = "Reverse"
gdf_dir = pd.concat([gdf_fwd, gdf_rev], ignore_index=True)


def direction_label(row):
    bearing = row["bearing"]
    orientation = row["prefix_direction"]
    # Default to compass heading if no prefix direction
    if orientation is np.nan or orientation is None:
        if 45 <= bearing < 135:
            return "Eastbound"
        elif 135 <= bearing < 225:
            return "Southbound"
        elif 225 <= bearing < 315:
            return "Westbound"
        else:
            return "Northbound"
    # Road is considered north-south
    if orientation in ["N", "S"]:
        if 45 <= bearing < 135:
            return "Northbound"
        elif 135 <= bearing < 225:
            return "Southbound"
        elif 225 <= bearing < 315:
            return "Southbound"
        else:
            return "Northbound"
    # Road is considered east-west
    if orientation in ["E", "W"]:
        if 45 <= bearing < 135:
            return "Eastbound"
        elif 135 <= bearing < 225:
            return "Eastbound"
        elif 225 <= bearing < 315:
            return "Westbound"
        else:
            return "Westbound"


gdf_dir["bearing_dir"] = gdf_dir.apply(direction_label, axis=1)

# Flip for reversed lines
flip = {
    "Northbound": "Southbound",
    "Southbound": "Northbound",
    "Eastbound": "Westbound",
    "Westbound": "Eastbound",
}
gdf_dir.loc[gdf_dir["direction"] == "Reverse", "bearing_dir"] = gdf_dir.loc[
    gdf_dir["direction"] == "Reverse", "bearing_dir"
].replace(flip)

gdf_dir = gdf_dir.to_crs(2277)

offset_dist = 2  # meters


def offset_line(row):
    geom = row.geometry
    # note that "right" will cause the line to reverse in directionality, so we flip it back
    return geom.parallel_offset(offset_dist, "right").reverse()


gdf_dir["geometry"] = gdf_dir.apply(offset_line, axis=1)

gdf_dir = gdf_dir.to_crs(4326)

# Normalize the one_way field (sometimes it's lowercase or has spaces)
gdf_dir["one_way"] = gdf_dir["one_way"].astype(str).str.upper().str.strip()


# Keep forward if ONEWAY == "FT" (From → To)
# Keep reverse if ONEWAY == "TF" (To → From)
# Keep both if ONEWAY == "B" or undefined
def keep_row(row):
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


gdf_dir = gdf_dir[gdf_dir.apply(keep_row, axis=1)].copy()

gdf_dir.to_file("Austin_StreetCenterline_Directional_Offset.geojson", driver="GeoJSON")
