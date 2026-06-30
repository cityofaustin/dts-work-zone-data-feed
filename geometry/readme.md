# Street Segment Directionality Processing

This directory contains code for converting a centerline street segment layer into directional vectors.

![A diagram showing the splitting on a centerline to directional vectors.](geometry/diagram.png)

This conversion is a necessary step to supply directional closures and to make the feed compliant with the WZDX specification.

[From the specification:](https://github.com/usdot-jpo-ode/wzdx/blob/develop/Creating_a_WZDx_Feed.md)
> The order of coordinates is meaningful: the first coordinate is the first (furthest upstream) point a road user encounters when traveling through the road event.

## Source Layer

The source for this conversion is the [citywide centerline layer](https://data.austintexas.gov/dataset/Street-Centerline/8hf2-pdmb/about_data) maintained by Austin Technology Services.

This layer is used in applications such as AMANDA and Maximo.

## High-Level Process 

1. Calculate the centerline's bearing
2. Create a copy of the centerline drawn in the opposite direction
3. Add a direction label for each directional segment based on:
   - The bearing of the segment
   - If there is a prefix to the street name (e.g. the "N" in "N Lamar Blvd"), use that to determine directionality.
N Lamar Blvd would be considered north-south running, so even if there are segments that appear east-west 
running the whole corridor will be considered north-south running.
4. For one-way roads, only keep the segment for which the direction of travel is allowed.
5. Separate the directional segments by 2 feet from each other so they are not too close together. 

## Running the Code

### Docker

Follow these steps from the main README to get set up with docker:

It is recommended to run this script using the docker container. You can build it using:

Note, if you are on Apple Silicon you may need to add `--platform linux/amd64` to get GDAL to install correctly.  
```
docker build . -t atddocker/dts-work-zone-data-feed:local
```

Then, run it with an env_file created using the env_template. If you are wanting to only run this step, all you will need 
is `SOURCE_SEGMENT_DATASET` and the destination dataset `SEGMENT_DATASET` filled out.

```
docker run -it --env-file .env atddocker/dts-work-zone-data-feed:local /bin/bash
python geometry/street_segment_directionality.py
```
