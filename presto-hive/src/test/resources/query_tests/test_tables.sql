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
--table: cust_order_line 
SELECT
    c_custkey,
    MAX(c_name) AS c_name,
    MAX(c_address) AS c_address,
    MAX(c_nationkey) AS c_nationkey,
    MAX(c_phone) AS c_phone,
    MAX(c_acctbal) AS c_acctbal,
    MAX(c_mktsegment) AS c_mktsegment,
    MAX(c_comment) AS c_comment,
    ARRAY_AGG(
        CAST(
            ROW (
                o_orderkey,
                o_orderstatus,
                o_totalprice,
                o_orderdate,
                o_orderpriority,
                o_shippriority,
                o_clerk,
                o_comment,
                LINES
            ) AS ROW(
                o_orderkey BIGINT,
                o_orderstatus VARCHAR,
                o_totalprice DOUBLE,
                o_orderdate DATE,
                o_orderpriority VARCHAR,
                o_shippriority VARCHAR,
                o_clerk VARCHAR,
                o_comment VARCHAR,
                o_lines ARRAY (
                    ROW (
                        l_partkey BIGINT,
                        l_suppkey BIGINT,
                        l_linenumber INTEGER,
                        l_quantity DOUBLE,
                        l_extendedprice DOUBLE,
                        l_discount DOUBLE,
                        l_tax DOUBLE,
                        l_returnflag VARCHAR(1),
                        l_linestatus VARCHAR(1),
                        l_shipdate DATE,
                        l_commitdate DATE,
                        l_receiptdate DATE,
                        l_shipinstruct VARCHAR(25),
                        l_shipmode VARCHAR(10),
                        l_comment VARCHAR(44)
                    )
                )
            )
        )
    ) AS c_orders
FROM (
    SELECT
        c_custkey AS c_custkey,
        o_orderkey,
        MAX(c_name) AS c_name,
        MAX(c_address) AS c_address,
        MAX(c_nationkey) AS c_nationkey,
        MAX(c_phone) AS c_phone,
        MAX(c_acctbal) AS c_acctbal,
        MAX(c_mktsegment) AS c_mktsegment,
        MAX(c_comment) AS c_comment,
        MAX(o_orderstatus) AS o_orderstatus,
        MAX(o_totalprice) AS o_totalprice,
        MAX(o_orderdate) AS o_orderdate,
        MAX(o_orderpriority) AS o_orderpriority,
        MAX(o_clerk) AS o_clerk,
        MAX(o_shippriority) AS o_shippriority,
        MAX(o_comment) AS o_comment,
        ARRAY_AGG(
            CAST(
                ROW(
                    l.partkey,
                    l.suppkey,
                    l.linenumber,
                    l.quantity,
                    l.extendedprice,
                    l.discount,
                    l.tax,
                    l.returnflag,
                    l.linestatus,
                    l.shipdate,
                    l.commitdate,
                    l.receiptdate,
                    l.shipinstruct,
                    l.shipmode,
                    l.comment
                ) AS ROW (
                    l_partkey BIGINT,
                    l_suppkey BIGINT,
                    l_linenumber INTEGER,
                    l_quantity DOUBLE,
                    l_extendedprice DOUBLE,
                    l_discount DOUBLE,
                    l_tax DOUBLE,
                    l_returnflag VARCHAR(1),
                    l_linestatus VARCHAR(1),
                    l_shipdate DATE,
                    l_commitdate DATE,
                    l_receiptdate DATE,
                    sl_hipinstruct VARCHAR(25),
                    l_shipmode VARCHAR(10),
                    l_comment VARCHAR(44)
                )
            )
        ) AS LINES
    FROM tpch.tiny.lineitem l,
        (
        SELECT
            c.custkey AS c_custkey,
            name AS c_name,
            address AS c_address,
            nationkey AS c_nationkey,
            phone AS c_phone,
            acctbal AS c_acctbal,
            mktsegment AS c_mktsegment,
            c.comment AS c_comment,
            orderkey AS o_orderkey,
            orderstatus AS o_orderstatus,
            totalprice AS o_totalprice,
            orderdate AS o_orderdate,
            orderpriority AS o_orderpriority,
            clerk AS o_clerk,
            shippriority AS o_shippriority,
            o.comment AS o_comment
        FROM tpch.tiny.orders o,
            tpch.tiny.customer c
        WHERE
            o.custkey = c.custkey
            AND c.custkey BETWEEN 0 AND 2000000
    )
    WHERE
        o_orderkey = l.orderkey
    GROUP BY
        c_custkey,
        o_orderkey
)
GROUP BY
    c_custkey
    ;
