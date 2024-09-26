"""
Mapping AMANDA closure types to WZDX vehicle impact types.
Note that this list is prioritized so the top value will be checked first and so on.
"""
amanda_closure_mapping = [
    {
        "amanda_closure": "Closure : Full Road",
        "vehicle_impact": "all-lanes-closed",
    },
    {
        "amanda_closure": "Traffic Lane : Dimensions",
        "vehicle_impact": "some-lanes-closed",
    },
    {
        "amanda_closure": "Open Cuts : Street",
        "vehicle_impact": "some-lanes-closed",
    },
]

"""
Temporary use of right of way (TURP) permits query. Gets the road closure info from the freeform tab (FOLDERFREEFORM),
along with permit details stored in FOLDERINFO and FOLDER. Ignores emergency permits, secondary permits and those
created prior to 2018. Only retrieves active permits.

Closure types:
- 'Traffic Lane : Dimensions'
- 'Closure : Full Road'
- 'Closure : Alley',
- 'Closure : Sidewalk'
- 'Parking Lane : Dimensions'
"""

turp_query = """
    SELECT f.FOLDERRSN,
           f.FOLDERTYPE,
           f.SUBCODE,
           f.WORKCODE,
           f.FOLDERNAME,
           f.INDATE,
           f.ISSUEDATE,
           f.FOLDERDESCRIPTION,
           f.FOLDERCONDITION,
           f.CUSTOMFOLDERNUMBER,
           TO_CHAR(fi.START_DATE, 'YYYY-MM-DD HH24:MI')           AS START_DATE,
           TO_CHAR(fi.END_DATE, 'YYYY-MM-DD HH24:MI')             AS END_DATE,
           TO_CHAR(fi.EXTENSION_START_DATE, 'YYYY-MM-DD HH24:MI') AS EXTENSION_START_DATE,
           TO_CHAR(fi.EXTENSION_END_DATE, 'YYYY-MM-DD HH24:MI')   AS EXTENSION_END_DATE,
           ff.LOCATION_NAME,
           ff.CLOSURE_TYPE,
           ff.SEGMENT_ID,
           ff.LENGTH,
           ff.WIDTH,
           ff.NUM_LANES
    FROM folder f
             LEFT OUTER JOIN (SELECT FOLDERRSN,
                                     MAX(
                                             CASE
                                                 WHEN INFOCODE = 75980 THEN
                                                     INFOVALUEDATETIME
                                                 END) AS START_DATE,
                                     MAX(
                                             CASE
                                                 WHEN INFOCODE = 75985 THEN
                                                     INFOVALUEDATETIME
                                                 END) AS end_date,
                                     MAX(
                                             CASE
                                                 WHEN INFOCODE = 75993 THEN
                                                     INFOVALUEDATETIME
                                                 END) AS extension_start_date,
                                     MAX(
                                             CASE
                                                 WHEN INFOCODE = 75994 THEN
                                                     INFOVALUEDATETIME
                                                 END) AS extension_end_date,
                                     MAX(
                                             CASE
                                                 WHEN INFOCODE = 72101 THEN
                                                     INFOVALUE
                                                 END) AS secondary_permit,
                                    MAX(
                                             CASE
                                                 WHEN INFOCODE = 79490 THEN
                                                     INFOVALUE
                                                 END) AS emergency_permit
    
                              FROM FOLDERINFO
                              GROUP BY FOLDERRSN) fi ON f.FOLDERRSN = fi.FOLDERRSN
             LEFT OUTER JOIN (SELECT FOLDERRSN,
                                     C01 AS location_name,
                                     C02 AS closure_type,
                                     N01 AS segment_id,
                                     N02 AS length,
                                     N03 AS width,
                                     N04 AS num_lanes
                              FROM FOLDERFREEFORM
                              WHERE FREEFORMCODE in (1010, 1015)
                                AND C02 in ('Traffic Lane : Dimensions', 'Closure : Full Road', 'Closure : Alley',
                                            'Closure : Sidewalk', 'Parking Lane : Dimensions') and C03 = 'Yes')
                                ff
                             ON ff.FOLDERRSN = f.FOLDERRSN
    WHERE f.FOLDERTYPE = 'RW' AND f.SUBCODE = 50500                            -- Temporary use of ROW permits (TURPs)
      AND f.STATUSCODE = 50010                           -- active permits
      AND f.INDATE > TO_DATE('2017-12-31', 'yyyy-mm-dd') -- this filters out old 'LA' permits
      AND ff.segment_id IS NOT NULL
      AND fi.secondary_permit = 'No'
      AND fi.emergency_permit = 'No'

"""


"""
Excavation permits query. Gets the road closure info from the freeform tab (FOLDERFREEFORM), along with permit details stored
in FOLDERINFO and FOLDER. Ignores emergency permits, secondary permits and those created prior to 2018. Only retrives active
permits.

Closure types:
- 'Traffic Lane : Dimensions'
- 'Closure : Full Road'
- 'Closure : Alley',
- 'Closure : Sidewalk'
- 'Parking Lane : Dimensions'
- 'Open Cuts : Street' 

'Open Cuts : Street' is treated as a partial road closure.
"""
excavation_permits = """
    SELECT f.FOLDERRSN,
           f.FOLDERTYPE,
           f.SUBCODE,
           f.WORKCODE,
           f.FOLDERNAME,
           f.INDATE,
           f.ISSUEDATE,
           f.FOLDERDESCRIPTION,
           f.FOLDERCONDITION,
           f.CUSTOMFOLDERNUMBER,
           TO_CHAR(fi.START_DATE, 'YYYY-MM-DD HH24:MI')           AS START_DATE,
           TO_CHAR(fi.END_DATE, 'YYYY-MM-DD HH24:MI')             AS END_DATE,
           TO_CHAR(fi.EXTENSION_START_DATE, 'YYYY-MM-DD HH24:MI') AS EXTENSION_START_DATE,
           TO_CHAR(fi.EXTENSION_END_DATE, 'YYYY-MM-DD HH24:MI')   AS EXTENSION_END_DATE,
           ff.LOCATION_NAME,
           ff.CLOSURE_TYPE,
           ff.SEGMENT_ID,
           ff.LENGTH,
           ff.WIDTH,
           ff.NUM_LANES
    FROM folder f
             LEFT OUTER JOIN (SELECT FOLDERRSN,
                                     MAX(
                                             CASE
                                                 WHEN INFOCODE = 76110 THEN
                                                     INFOVALUEDATETIME
                                                 END) AS START_DATE,
                                     MAX(
                                             CASE
                                                 WHEN INFOCODE = 76115 THEN
                                                     INFOVALUEDATETIME
                                                 END) AS end_date,
                                     MAX(
                                             CASE
                                                 WHEN INFOCODE = 75993 THEN
                                                     INFOVALUEDATETIME
                                                 END) AS extension_start_date,
                                     MAX(
                                             CASE
                                                 WHEN INFOCODE = 75994 THEN
                                                     INFOVALUEDATETIME
                                                 END) AS extension_end_date,
                                     MAX(
                                             CASE
                                                 WHEN INFOCODE = 72101 THEN
                                                     INFOVALUE
                                                 END) AS secondary_permit,
                                    MAX(
                                             CASE
                                                 WHEN INFOCODE = 79490 THEN
                                                     INFOVALUE
                                                 END) AS emergency_permit
    
                              FROM FOLDERINFO
                              GROUP BY FOLDERRSN) fi ON f.FOLDERRSN = fi.FOLDERRSN
             LEFT OUTER JOIN (SELECT FOLDERRSN,
                                     C01 AS location_name,
                                     C02 AS closure_type,
                                     N01 AS segment_id,
                                     N02 AS length,
                                     N03 AS width,
                                     N04 AS num_lanes
                              FROM FOLDERFREEFORM
                              WHERE FREEFORMCODE in (1010, 1015)
                                AND C02 in ('Traffic Lane : Dimensions', 'Closure : Full Road', 'Closure : Alley',
                                            'Closure : Sidewalk', 'Parking Lane : Dimensions', 'Open Cuts : Street') and C03 = 'Yes')
                                ff
                             ON ff.FOLDERRSN = f.FOLDERRSN
    WHERE f.FOLDERTYPE = 'EX'                            -- EX permits only
      AND f.STATUSCODE = 50010                           -- active permits
      AND f.INDATE > TO_DATE('2017-12-31', 'yyyy-mm-dd') 
      AND ff.segment_id IS NOT NULL
      AND fi.secondary_permit = 'No'
      AND fi.emergency_permit = 'No'
    """
