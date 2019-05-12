--tables: lineitem_aria_nulls
select 
    orderkey,
    linenumber,
    partkey,
    suppkey,
    quantity,
    comment
FROM lineitem_aria_nulls
WHERE (
        partkey IS NULL
        OR partkey BETWEEN 100 AND 200
        OR partkey BETWEEN 1000 AND 1100
    )
    AND (
        suppkey IS NULL
        OR suppkey BETWEEN 10 AND 20
        OR suppkey BETWEEN 30 AND 40
    )
    AND (
        quantity IS NULL
        OR quantity BETWEEN 5 AND 10
        OR quantity BETWEEN 20 AND 40
    )
;

select json_format(cast(order_part_supp_array as json))
from lineitem_aria_nulls
where linenumber = 1
;

select count(*) from lineitem_aria_nulls
where order_part_supp_array is null
;
select order_part_supp_array from lineitem_aria_nulls
where order_part_supp_array is null
;

select count(*) from lineitem_aria_nulls
where order_part_supp_array is not null
;

select json_format(cast(order_part_supp_array as json))
from lineitem_aria_nulls
where order_part_supp_array is not null
;
select json_format(cast(order_part_supp_map as json))
from lineitem_aria_nulls
where linenumber = 1
;

select count(*) from lineitem_aria_nulls
where order_part_supp_map is null
;

select count(*) from lineitem_aria_nulls
where order_part_supp_map is not null
;

select json_format(cast(order_part_supp_map as json))
from lineitem_aria_nulls
where order_part_supp_map is not null
;

select orderkey, linenumber, order_part_supp_map[1]
from lineitem_aria_nulls
where order_part_supp_map is null
;

select order_part_supp_map[1] from lineitem_aria_nulls
where cardinality(order_part_supp_map) > 1
;

