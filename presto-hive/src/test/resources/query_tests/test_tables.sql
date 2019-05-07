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
