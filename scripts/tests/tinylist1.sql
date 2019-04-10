use hive.tpch;
select orderkey, linenumber, order_part_supp_array
from lineitem_aria_nulls
;
