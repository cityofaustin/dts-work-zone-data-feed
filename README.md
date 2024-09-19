# City of Austin Work Zone Data Exchange (WZDx) Feed 

Scripts that support publishing work zone location data to a geojson feed specified by USDOT called [WZDx](https://github.com/usdot-jpo-ode/wzdx/tree/main).

## Data Sources

### AMANDA Temporary Use of Right of Way (TURP) Permits

AMANDA is software used by the city to manage permitting. This data source is only for the TURP permits that are issued.

TURP permits can close entire roadways or a few lanes. 

### AMANDA Excavation Permits

Excavation permits frequently require a lane closure in at least one direction. This feed only includes those excavation permits
issued for the roadway or those next to the roadway.

## Deployment

### Docker

It is recommended to run this script using the docker container. You can build it using:

Note, if you are on Apple Silicon you may need to add `--platform linux/amd64` to get GDAL to install correctly.  
```
$ docker build . -t dts-work-zone-data-feed:production
```

Then, run it with an env_file created using the env_template.

```
$ docker run -it --env-file env_file dts-work-zone-data-feed /bin/bash
$ python data_sources/amanda_closure_publishing.py
```

