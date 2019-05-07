--tables: cust_year_parts
select custkey, json_format(cast(year_parts as json))
from cust_year_parts where
 array_position(map_keys(year_parts), 1995) > 0
and array_position(map_keys(year_parts), 1996) > 0
 and year_parts[1995][1].pk < 1000000
 and year_parts[1996][1].pk < 2000000

;

