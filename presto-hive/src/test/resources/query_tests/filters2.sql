--tables: lineitem_aria_nulls
-- Dictionary input to filter function
select shipmode, shipinstruct, comment
from lineitem_aria_nulls where
(shipmode like 'TR%' or shipmode like '%REG%')
and shipinstruct > 'DE'
and comment > 'f'
and comment > concat(shipmode, shipinstruct)
;

