use hive.tpch;
select orderkey, linenumber, order_part_supp_map
from lineitem_aria_nulls
where cardinality(order_part_supp_map) > 3
and order_part_supp_map[2] between 10 and 500
and order_part_supp_map[3] between 10 and 1000
and order_part_supp_map[4] = 2
;
