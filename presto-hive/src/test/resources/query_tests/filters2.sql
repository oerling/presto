--tables: lineitem_aria_nulls
-- Dictionary input to filter function
select shipmode, shipinstruct, comment
from lineitem_aria_nulls where
(shipmode like 'TR%' or shipmode like '%REG%')
and shipinstruct > 'DE'
and comment > 'f'
and comment > concat(shipmode, shipinstruct)
;


select count(*) from lineitem_aria_nulls where float_quantity in (1, 2, 4)
;
select count(*) from lineitem_aria where quantity in (1, 2, 4)
;

select count(*) from lineitem_aria where float_quantity in (1, 2, 4)
;

select count(*) from lineitem_aria where long_decimal_discount in (0.01, 0.02)
;

select count(*) from lineitem_aria where short_decimal_discount in (0.01, 0.02)
;

select is_returned from lineitem_aria where is_returned in (true, false)
;
select is_returned from lineitem_aria where is_returned <> false
;
select is_returned from lineitem_aria where is_returned <> true
;
select is_returned from lineitem_aria where is_returned <> true or is_returned is null
;
select partkey, suppkey, quantity, comment, extendedprice, returnflag, float_quantity, shipinstruct,
  json_format(cast(order_part_supp_array as json)),  json_format(cast(order_part_supp_map as json)), json_format(cast(string_map as json))
from lineitem_aria_nulls where linenumber = 1 and comment like '%f%';


