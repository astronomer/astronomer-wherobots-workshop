    SELECT *
    FROM %(catalog)s.%(database)s.risk_comparison
    WHERE area_id IN ('%(us_postcode)s', (SELECT state_code FROM %(catalog)s.%(database)s.state_boundary LIMIT 1));