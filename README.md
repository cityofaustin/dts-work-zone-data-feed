# City of Austin Work Zone Data Exchange (WZDx) Feed 

Scripts that support publishing work zone location data to a geojson feed specified by USDOT called [WZDx](https://github.com/usdot-jpo-ode/wzdx/tree/main).

The json feed is available [here](https://data.austintexas.gov/Transportation-and-Mobility/Work-Zone-Data-Exchange-WZDx-JSON-Feed/d9mm-cjw9/about_data).

A tabular version is also available [here](https://data.austintexas.gov/Transportation-and-Mobility/Roadway-Work-Zones/qyfh-gwei/about_data).

These work zones are currently being shown on the [CTXGO website](https://ctxgo.com/workzones) (managed by TxDOT).

***

## Data Sources

### AMANDA Temporary Use of Right of Way (TURP) Permits

AMANDA is software used by the city to manage permitting. This data source is only for the TURP permits that are issued.

TURP permits can close entire roadways or a few lanes. 

### AMANDA Excavation Permits

Excavation permits frequently require a lane closure in at least one direction. This feed only includes those excavation permits
issued for the roadway or those next to the roadway.

### Coordinate

City permits are tracked in a software platform called [Coordinate](https://austin.dotmapsapp.com/). One feature of this 
platform allows for work zones to be "activated" by permittees. This information is included in the work zone data 
feed through the `worker_presence` object.

***

## Validation

The `schema_validation.py` script will check a small subset of work zones in local copy of work zone datafeed's geojson against the schema.

To validate:
1. Run the feed, with the `local-file` argument: `python data_sources/amanda_closure_publishing.py --local-file wzdx_output.geojson`
2. Then run the validator `python validation/schema_validation.py`
3. You should get `✅ data sample passes schema validation` or an error message if something is wrong.

***

## Deployment

### Docker

It is recommended to run this script using the docker container. You can build it using:

Note, if you are on Apple Silicon you may need to add `--platform linux/amd64` to get GDAL to install correctly.  
```
docker build . -t atddocker/dts-work-zone-data-feed:local
```

Then, run it with an env_file created using the env_template.

```
docker run -it --env-file .env atddocker/dts-work-zone-data-feed:local /bin/bash
python data_sources/amanda_closure_publishing.py
```

