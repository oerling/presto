use hive.tpch;
select orderkey, linenumber, order_part_supp_map
from lineitem_aria_nulls
;
