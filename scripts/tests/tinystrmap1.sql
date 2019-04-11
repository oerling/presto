use hive.tpch;
select orderkey, linenumber, string_map
from lineitem_aria_nulls
;
