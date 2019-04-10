use hive.tpch;
select orderkey, linenumber, order_part_supp_array
from lineitem_aria_nulls
where order_part_supp_array[2] between 10 and 500
and order_part_supp_array[3] between 10 and 1000
;
