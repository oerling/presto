--tables: cust_year_parts
select custkey, year_parts[1995][1].pk, year_parts[1996][1].pk 
from cust_year_parts where
 array_position(map_keys(year_parts), 1995) > 0
and array_position(map_keys(year_parts), 1996) > 0
 and year_parts[1995][1].pk < 1000
 and year_parts[1996][1].pk < 1200
;


select custkey, json_format(cast(year_parts as json)) 
from cust_year_parts where
 array_position(map_keys(year_parts), 1995) > 0
and array_position(map_keys(year_parts), 1996) > 0
 and year_parts[1995][1].pk < 1000
 and year_parts[1996][1].pk < 1200
;

select count(*) 
from cust_year_parts where
 array_position(map_keys(year_parts), 1995) > 0
and array_position(map_keys(year_parts), 1996) > 0
 and year_parts[1995][1].pk < 1000
 and year_parts[1996][1].pk < 1200
;

--tables: lineitem_struct_list
select orderkey, partkey_list[1].comment, suppkey_quantity_list[2].quantity_comment
from lineitem_struct_list
where 
cardinality(partkey_list) > 4
and partkey_list[1] is not null and partkey_list[2].comment > 'f'
and partkey_list[1].partkey_comment > '13'
;

