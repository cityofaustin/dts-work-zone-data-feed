import uuid
from shapely.ops import linemerge
import geopandas as gpd
from shapely.geometry import shape, mapping

class WorkZone:
    def __init__(self, data_source_id, name, description, start_date, end_date):
        self.data_source_id = data_source_id
        self.name = name
        self.start_date = start_date
        self.end_date = end_date
        self.description = description

        self.segments = []

    def add_closure(self, segment_id, veh_impact, segment_info, direction="unknown"):
        self.segments.append(
            {
                "segment_id": segment_id,
                "vehicle_impact": veh_impact,
                "geometry": segment_info["the_geom"],
                "feature_data": segment_info,
                "direction": direction,
                "street_place_id": segment_info["street_place_id"],
            }
        )

    def get_number_of_closures(self):
        return len(self.segments)

    def generate_closure_id(self, segment_id):
        return str(
            uuid.uuid5(
                uuid.NAMESPACE_OID, f"{self.start_date}-{self.end_date}-{segment_id}"
            )
        )

    def reduce_closure_geometry(self):
        """
        Takes the current list of roadway segments and combines them if they are continuous segments on the same road.
        """
        if len(self.segments) > 1:
            for i in self.segments:
                i["geometry"] = shape(i["geometry"])

            segment_df = gpd.GeoDataFrame(self.segments, geometry="geometry")

            # Get list of road names
            places = segment_df["street_place_id"].unique()
            # Get types of closures
            closure_types = segment_df["vehicle_impact"].unique()

            # For each road/closure type try to reduce the geometry
            reduced_segments = []
            for type in closure_types:
                for place in places:
                    place_df = segment_df[segment_df["vehicle_impact"] == type]
                    place_df = place_df[place_df["street_place_id"] == place]
                    if len(place_df) > 1:   # We need more than 1 segment to reduce

                        # Attempt to merge the list of line geometries.
                        merged_segments = linemerge(list(place_df["geometry"]))

                        # If a single linestring is returned, we know we have successfully combined all segments
                        if merged_segments.geom_type == "LineString":
                            edited_segment = dict(place_df.iloc[0])
                            edited_segment["geometry"] = merged_segments
                            reduced_segments.append(edited_segment)

                        # If a multiline is returned, we failed to combine as the segments are likely disjointed
                        elif merged_segments.geom_type == "MultiLineString":
                            reduced_segments += place_df.to_dict('records')
                    else:
                        reduced_segments += place_df.to_dict('records')

            # Converting from WKT back to geojson
            for segment in reduced_segments:
                segment["geometry"] = mapping(segment["geometry"])
            self.segments = reduced_segments



    def generate_json(self):
        data = []
        for segment in self.segments:
            core_details = {
                "name": self.name,
                "event_type": "work-zone",
                "data_source_id": self.data_source_id,
                "road_names": [segment["feature_data"]["full_street_name"]],
                "direction": "unknown",
                "description": self.description,
            }
            properties = {
                "core_details": core_details,
                "start_date": self.start_date,  # need to be careful about formatting these
                "end_date": self.end_date,  # need to require start/end dates,
                "is_start_date_verified": False,
                "is_end_date_verified": False,
                "is_start_position_verified": False,
                "is_end_position_verified": False,
                "location_method": "other",
                "work_zone_type": "static",
                "vehicle_impact": segment["vehicle_impact"],
            }
            event_object = {
                "id": self.generate_closure_id(segment["segment_id"]),
                "type": "Feature",
                "properties": properties,
                "geometry": segment["geometry"],
            }
            data.append(event_object)
        return data


class AmandaWorkZone(WorkZone):
    def __init__(self, data_source_id, name, description, start_date, end_date, folderrsn):
        super().__init__(data_source_id, name, description, start_date, end_date)
        self.folderrsn = folderrsn

    def generate_closure_id(self, segment_id):
        return str(uuid.uuid5(uuid.NAMESPACE_OID, f"{self.folderrsn}-{segment_id}"))

    def generate_socrata_export(self):
        """
        Generates flattened export for Socrata to help with debugging
        :return:
        """
        data = []
        for segment in self.segments:
            properties = {
                "id": self.generate_closure_id(segment["segment_id"]),
                "name": self.name,
                "type": "Feature",
                "geometry": segment["geometry"],
                "event_type": "work-zone",
                "data_source_id": self.data_source_id,
                "road_names": segment["feature_data"]["full_street_name"],
                "direction": "unknown",
                "description": self.description,
                "start_date": self.start_date,
                "end_date": self.end_date,
                "is_start_date_verified": False,
                "is_end_date_verified": False,
                "is_start_position_verified": False,
                "is_end_position_verified": False,
                "location_method": "other",
                "work_zone_type": "static",
                "vehicle_impact": segment["vehicle_impact"],
                "folderrsn": str(self.folderrsn),
            }
            data.append(properties)
        return data
