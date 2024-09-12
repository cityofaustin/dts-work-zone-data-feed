import uuid


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
