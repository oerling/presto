--table: cust_year_parts
SELECT
    custkey,
    map_agg(y, parts) AS year_parts,
    map_agg(y, total_cost) AS year_cost
FROM (
    SELECT
        c.custkey,
        YEAR(shipdate) AS y,
        ARRAY_AGG(
            CAST(ROW (partkey, extendedprice, quantity) AS ROW (pk BIGINT, ep DOUBLE, qt DOUBLE))
        ) AS parts,
        SUM(extendedprice) AS total_cost
    FROM tpch.tiny.lineitem l,
        tpch.tiny.orders o,
        tpch.tiny.customer c
    WHERE
        l.orderkey = o.orderkey
        AND o.custkey = c.custkey
        AND c.nationkey < 10
    GROUP BY
        c.custkey,
        YEAR(shipdate)
)
GROUP BY
    custkey
;
--table: lineitem_aria
SELECT
    orderkey,
    partkey,
    suppkey,
    linenumber,
    quantity,
    extendedprice,
    shipmode,
    shipdate,
    comment,
    returnflag = 'R' AS is_returned,
    CAST(quantity + 1 AS REAL) AS float_quantity,
    CAST(discount AS decimal(5, 2)) AS short_decimal_discount,
    CAST(discount AS decimal(38, 2)) AS long_decimal_discount,
    CAST(array_position(array['SHIP','REG AIR','AIR','FOB','MAIL','RAIL','TRUCK'], shipmode) AS tinyint) AS tinyint_shipmode,
    date_add('second', suppkey, cast(shipdate as timestamp)) AS timestamp_shipdate,
    MAP(
        ARRAY[1, 2, 3],
        ARRAY[orderkey, partkey, suppkey]
    ) AS order_part_supp_map,
    ARRAY[ARRAY[orderkey, partkey, suppkey]] AS order_part_supp_array
FROM tpch.tiny.lineitem
;

--table: lineitem_aria_nulls
SELECT *,
    case when have_complex_nulls then null
    when mod(orderkey, 7) = 0 then map(array[], array[])
    when returnflag = 'N' then  
      map(array[1, 2, 3], array[orderkey, partkey, suppkey])
      else map(array[1, 2, 3, 4], array[orderkey, partkey, suppkey, if (returnflag = 'A', 1, 2)]) end
  as order_part_supp_map_empty,
    case when have_complex_nulls then null
    when returnflag = 'N' then  
      map(array[1, 2, 3], array[orderkey, partkey, suppkey])
      else map(array[1, 2, 3, 4], array[orderkey, partkey, suppkey, if (returnflag = 'A', 1, 2)]) end
  as order_part_supp_map,

case when have_complex_nulls then null
    when mod(orderkey, 6) = 0 then array[]
when returnflag = 'N' then 
      array[orderkey, partkey, suppkey]
    else array[orderkey, partkey, suppkey, if (returnflag = 'A', 1, 2)] end
  as order_part_supp_array,
  IF(have_complex_nulls, null, IF (returnflag = 'N', 
    map(array['shipmode', 'shipinstruct', 'comment'],
    array[shipmode, shipinstruct, comment])))
  as string_map
FROM (
  SELECT 
       orderkey,
       linenumber,
       have_simple_nulls and mod(orderkey + linenumber, 5) = 0 AS have_complex_nulls,
       IF(have_simple_nulls and mod(orderkey + linenumber, 11) = 0, null, partkey) as partkey,
       IF(have_simple_nulls and mod(orderkey + linenumber, 13) = 0, null, suppkey) as suppkey,
       IF(have_simple_nulls and mod (orderkey + linenumber, 17) = 0, null, quantity) as quantity,
       IF(have_simple_nulls and mod (orderkey + linenumber, 19) = 0, null, extendedprice) as extendedprice,
       IF(have_simple_nulls and mod (orderkey + linenumber, 23) = 0, null, shipmode) as shipmode,
       IF(have_simple_nulls and mod (orderkey + linenumber, 7) = 0, null, comment) as comment,
       IF(have_simple_nulls and mod(orderkey + linenumber, 31) = 0, null, returnflag = 'R') as is_returned,
       IF(have_simple_nulls and mod(orderkey + linenumber, 11) = 0, null, returnflag) as returnflag,
       IF(have_simple_nulls and mod(orderkey + linenumber, 37) = 0, null, CAST(quantity + 1 as real)) as float_quantity,
       IF(have_simple_nulls and mod (orderkey + linenumber, 23) = 0, null, shipinstruct) as shipinstruct
  FROM (SELECT mod(orderkey, 32000) > 16000 as have_simple_nulls, * from tpch.tiny.lineitem))
;
  
--table: lineitem_aria_string_structs_with_nulls
SELECT
    orderkey,
    partkey,
    suppkey,
    linenumber,
    CAST(
        IF(mod(partkey, 5) = 0, NULL,
            ROW(
comment,
IF (mod(partkey, 13) = 0, NULL,
    CONCAT(CAST(partkey AS VARCHAR), comment)
)
            )
        ) AS ROW(
            comment VARCHAR,
            partkey_comment VARCHAR
        )
    ) AS partkey_struct,
    CAST(
        IF(mod(suppkey, 7) = 0, NULL,
            ROW(
IF (mod(suppkey, 17) = 0, NULL,
    CONCAT(CAST(suppkey AS VARCHAR), comment)
),
CONCAT(CAST(quantity AS VARCHAR), comment)
            )
        ) AS ROW(
            suppkey_comment VARCHAR,
            quantity_comment varchar
        )
    ) AS suppkey_quantity_struct
FROM tpch.tiny.lineitem
WHERE orderkey < 100000
;
--table: lineitem_struct_list
SELECT
    orderkey,
    array_agg(CAST(
        IF(mod(partkey, 5) = 0, NULL,
            ROW(
linenumber, comment,
IF (mod(partkey, 13) = 0, NULL,
    CONCAT(CAST(partkey AS VARCHAR), comment)
)
            )
        ) AS ROW(
            linenumber int,
comment VARCHAR,
            partkey_comment VARCHAR
        )
    )) AS partkey_list,
    array_agg(CAST(
        IF(mod(suppkey, 7) = 0, NULL,
            ROW(
IF (mod(suppkey, 17) = 0, NULL,
    CONCAT(CAST(suppkey AS VARCHAR), comment)
),
CONCAT(CAST(quantity AS VARCHAR), comment)
            )
        ) AS ROW(
            suppkey_comment VARCHAR,
            quantity_comment varchar
        )
    )) AS suppkey_quantity_list
FROM tpch.tiny.lineitem
WHERE orderkey < 100000
group by orderkey
;

--table: lineitem_aria_strings
SELECT
    orderkey,
    partkey,
    suppkey,
    linenumber,
    comment,
    CONCAT(CAST(partkey AS VARCHAR), comment) AS partkey_comment,
    CONCAT(CAST(suppkey AS VARCHAR), comment) AS suppkey_comment,
    CONCAT(CAST(quantity AS VARCHAR), comment) AS quantity_comment
FROM tpch.tiny.lineitem
WHERE orderkey < 100000
;

--table: lineitem_aria_string_structs
SELECT
    orderkey,
    partkey,
    suppkey,
    linenumber,
    CAST(
        ROW(
            comment,
            CONCAT(CAST(partkey AS VARCHAR), comment)
        ) AS ROW(
            comment VARCHAR,
            partkey_comment VARCHAR
        )
    ) AS partkey_struct,
    CAST(
        ROW(
            CONCAT(CAST(suppkey AS VARCHAR), comment),
            CONCAT(CAST(quantity AS VARCHAR), comment)
        ) AS ROW(
            suppkey_comment VARCHAR,
            quantity_comment VARCHAR
        )
    ) AS suppkey_quantity_struct
FROM tpch.tiny.lineitem
WHERE orderkey < 100000
;
