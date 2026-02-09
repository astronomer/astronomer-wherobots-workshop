MERGE INTO %(catalog)s.%(database)s.hail_state AS target
USING (
    SELECT h.*, s.state_code
    FROM %(catalog)s.%(database)s.hail_raw h
    JOIN %(catalog)s.%(database)s.state_boundary s
        ON ST_Contains(s.geometry, h.geometry)
) AS source
ON target.ZTIME = source.ZTIME 
    AND target.LON = source.LON 
    AND target.LAT = source.LAT
    AND target.WSR_ID = source.WSR_ID
    AND target.CELL_ID = source.CELL_ID
WHEN MATCHED THEN UPDATE SET
    SEVPROB = source.SEVPROB,
    PROB = source.PROB,
    MAXSIZE = source.MAXSIZE
WHEN NOT MATCHED THEN INSERT *
;