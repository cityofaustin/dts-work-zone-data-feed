turp_query = """SELECT f.FOLDERRSN,
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
                                             END) AS extension_end_date
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
                          WHERE FREEFORMCODE = 1010
                            AND C02 in ('Traffic Lane : Dimensions', 'Closure : Full Road', 'Closure : Alley',
                                        'Closure : Sidewalk', 'Parking Lane : Dimensions')) ff
                         ON ff.FOLDERRSN = f.FOLDERRSN
WHERE f.FOLDERTYPE = 'RW'
  AND f.SUBCODE = 50500                              -- Temporary use of ROW permits (TURPs)
  AND f.STATUSCODE = 50010                           -- active permits
  AND f.INDATE > TO_DATE('2017-12-31', 'yyyy-mm-dd') -- this filters out old 'LA' permits
  AND ff.segment_id IS NOT NULL
"""
