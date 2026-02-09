MERGE INTO %(catalog)s.%(database)s.hail_county AS target
USING (
    SELECT h.ZTIME, h.LON, h.LAT, h.WSR_ID, h.CELL_ID, h.RANGE, 
            h.AZIMUTH, h.SEVPROB, h.PROB, h.MAXSIZE, h.geometry,
            p.postcode
    FROM %(catalog)s.%(database)s.hail_state h
    JOIN %(catalog)s.%(database)s.postcode_areas p
        ON ST_Contains(p.area, h.geometry)
    WHERE p.postcode = '%(us_postcode)s'
) AS source
ON target.ZTIME = source.ZTIME 
    AND target.LON = source.LON 
    AND target.LAT = source.LAT
    AND target.postcode = source.postcode
WHEN MATCHED THEN UPDATE SET
    SEVPROB = source.SEVPROB,
    PROB = source.PROB,
    MAXSIZE = source.MAXSIZE
WHEN NOT MATCHED THEN INSERT *
;