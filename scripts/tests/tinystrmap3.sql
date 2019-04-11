use hive.tpch;
select orderkey, linenumber, string_map
from lineitem_aria_nulls
where linenumber = 1
and string_map['shipmode'] in ('AIR', 'REG AIR')
and string_map['comment'] between 'f' and 'h'
;
