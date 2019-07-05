
--tables: lineitem_aria_nulls

SELECT orderkey, linenumber, order_part_supp_array
FROM lineitem_aria_nulls
;

SELECT orderkey, linenumber, order_part_supp_array
FROM lineitem_aria_nulls
WHERE CARDINALITY(order_part_supp_array) > 1 and order_part_supp_array[2] between 10 AND 500
AND order_part_supp_array[3] between 10 AND 45
;

SELECT orderkey, linenumber, order_part_supp_array
FROM lineitem_aria_nulls
WHERE cardinality(order_part_supp_array) > 3
AND order_part_supp_array[2] between 10 AND 500
AND order_part_supp_array[3] between 10 AND 1000
AND order_part_supp_array[4] = 2
;


SELECT orderkey, linenumber, order_part_supp_array
FROM lineitem_aria_nulls
WHERE linenumber = 1
AND cardinality(order_part_supp_array) > 3
AND order_part_supp_array[2] between 10 AND 500
AND order_part_supp_array[3] between 10 AND 1000
AND order_part_supp_array[4] = 2
;

SELECT orderkey, linenumber, order_part_supp_map[1]
FROM lineitem_aria_nulls
;


SELECT orderkey, linenumber, order_part_supp_map[1]
FROM lineitem_aria_nulls
WHERE order_part_supp_map[2] between 10 AND 500
AND order_part_supp_map[3] between 10 AND 50
;

SELECT orderkey, linenumber, order_part_supp_map_empty[1]
FROM lineitem_aria_nulls
WHERE cardinality(order_part_supp_map_empty) > 3
AND order_part_supp_map_empty[2] between 10 AND 500
AND order_part_supp_map_empty[3] between 10 AND 50
AND order_part_supp_map_empty[4] = 2
;

SELECT orderkey, linenumber, order_part_supp_map_empty[1]
FROM lineitem_aria_nulls
WHERE linenumber = 1
AND cardinality(order_part_supp_map_empty) > 3
AND order_part_supp_map_empty[2] between 10 AND 500
AND order_part_supp_map_empty[3] between 10 AND 45
AND order_part_supp_map_empty[4] = 2
;


SELECT orderkey, linenumber, string_map['comment']
FROM lineitem_aria_nulls
where linenumber > 3
;

SELECT orderkey, linenumber, string_map['comment']
FROM lineitem_aria_nulls
WHERE string_map['shipmode'] in ('AIR', 'REG AIR')
AND string_map['comment'] between 'f' AND 'h'
;

SELECT orderkey, linenumber, string_map['comment'], string_map['shipinstruct']
FROM lineitem_aria_nulls
WHERE linenumber = 1
AND string_map['shipmode'] in ('AIR', 'REG AIR')
AND string_map['comment'] between 'f' AND 'h'
;

SELECT orderkey, linenumber, string_map['comment'], order_part_supp_map[1]
FROM lineitem_aria_nulls
WHERE linenumber = 1
AND string_map['shipmode'] in ('AIR', 'REG AIR')
AND order_part_supp_map[2] < 1000
;

SELECT orderkey, linenumber, string_map['comment'], order_part_supp_map[2]
FROM lineitem_aria_nulls
WHERE linenumber = 1
;
select count(*) from lineitem_aria where order_part_supp_map[2] < 1000 and  order_part_supp_map[3] < 10
;
-- Tests use of a region of a map in a filter function
select order_part_supp_map[2], string_map['shipinstruct']
from lineitem_aria_nulls
where (length(string_map['comment']) > 20 or order_part_supp_map[2] > 300)
and order_part_supp_map[3] > 30 and string_map['comment'] > 'c'
limit 10
;
