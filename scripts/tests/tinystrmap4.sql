use hive.tpch;
select orderkey, linenumber, string_map, order_part_supp_map
from lineitem_aria_nulls
where linenumber = 1
and string_map['shipmode'] in ('AIR', 'REG AIR')
and order_part_supp_map[2] < 1000 
;
