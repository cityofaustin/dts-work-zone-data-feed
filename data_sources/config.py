"""
Mapping AMANDA work zone types to WZDX work zone types
"""

work_zone_type_mapping = {
    "mobile": "planned-moving-area",
    "daily": "static",
    "24/7": "static",
}

"""
Get permits for Temporary use of right of way (TURP) and Excavation (EX) permits. 
Gets the road closure info from the freeform tab (FOLDERFREEFORM),
along with permit details stored in FOLDERINFO and FOLDER. Ignores emergency permits, secondary permits and those
created prior to 2018. Only retrieves active permits.

Closure types:
- 'Traffic Lane : Dimensions'
- 'Closure : Full Road'
- 'Closure : Does this result in a full directional closure?'
"""

amanda_query = """
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
       TO_CHAR(coa_folder.f_get_info_date(f.folderrsn, 76110), 'YYYY-MM-DD HH24:MI') AS START_DATE,
       TO_CHAR(coa_folder.f_get_info_date(f.folderrsn, 76115), 'YYYY-MM-DD HH24:MI') AS END_DATE,
       TO_CHAR(coa_folder.f_get_info_date(f.folderrsn, 75993), 'YYYY-MM-DD HH24:MI') AS EXTENSION_START_DATE,
       TO_CHAR(coa_folder.f_get_info_date(f.folderrsn, 75994), 'YYYY-MM-DD HH24:MI') AS EXTENSION_END_DATE,
       coa_folder.f_get_info_string(f.folderrsn, 50395) AS WORK_ZONE_TYPE,
       ff.LOCATION_NAME,
       ff.CLOSURE_TYPE,
       ff.SEGMENT_ID,
       ff.LENGTH,
       ff.WIDTH,
       ff.NUM_LANES,
       ff.DIRECTION
FROM folder f
       LEFT OUTER JOIN (SELECT FOLDERRSN,
                               C01 AS location_name,
                               C02 AS closure_type,
                               C11 AS direction,
                               N01 AS segment_id,
                               N02 AS length,
                               N03 AS width,
                               N04 AS num_lanes
                        FROM FOLDERFREEFORM
                        WHERE FREEFORMCODE IN (1010, 1015)
                          AND (
                          (C02 = 'Traffic Lane : Dimensions' AND (C03 = 'Yes' OR C03 IS NULL))
                            OR
                          (C02 IN ('Closure : Full Road',
                                   'Closure : Does this result in a full directional closure?')
                            AND C03 = 'Yes')
                          )) ff
                       ON ff.FOLDERRSN = f.FOLDERRSN
WHERE ((f.FOLDERTYPE = 'EX')
  OR (f.FOLDERTYPE = 'RW' AND f.SUBCODE = 50500)) -- EX  or RW TURP permits only
  AND f.STATUSCODE = 50010                        -- active permits
  AND f.INDATE > TO_DATE('2017-12-31', 'yyyy-mm-dd')
  AND ff.segment_id IS NOT NULL
  AND coa_folder.f_get_info_string(f.folderrsn, 72101) = 'No'   -- No secondary permits
  AND coa_folder.f_get_info_string(f.folderrsn, 79490) = 'No'   -- No emergency permits
"""
