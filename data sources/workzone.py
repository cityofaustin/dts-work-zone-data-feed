import uuid


class WorkZone:
    def __init__(self, data_source_id, folderrsn, description, start_date, end_date):
        self.data_source_id = data_source_id
        self.folderrsn = folderrsn
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
            }
        )

    def get_number_of_closures(self):
        return len(self.segments)

    def generate_json(self):
        data = []
        for segment in self.segments:
            core_details = {
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
                # not sure see: https://github.com/usdot-jpo-ode/wzdx/blob/main/spec-content/enumerated-types/LocationMethod.md
                "vehicle_impact": segment["vehicle_impact"],
            }
            event_object = {
                "id": str(
                    uuid.uuid5(
                        uuid.NAMESPACE_OID, f"{self.folderrsn}-{segment['segment_id']}"
                    )
                ),
                "type": "Feature",
                "properties": properties,
                "geometry": segment["geometry"],
            }
            data.append(event_object)
        return data
