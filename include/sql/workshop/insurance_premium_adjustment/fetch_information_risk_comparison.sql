SELECT r.* 
FROM %(catalog)s.%(database)s.risk_comparison r
WHERE (r.area_type = 'county' AND r.area_id = '%(us_postcode)s')
   OR (r.area_type = 'state' AND r.area_id = (
       SELECT COALESCE(p.state_code, (SELECT state_code FROM %(catalog)s.%(database)s.state_boundary LIMIT 1))
       FROM %(catalog)s.%(database)s.postcode_areas p
       WHERE p.postcode = '%(us_postcode)s'
   ));