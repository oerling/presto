

-- custkey with map from year to an array of purchases that year.


create table hive.tpch.cust_year_parts as select custkey, map_agg(y, parts) as year_parts, map_agg(y, total_cost) as year_cost
  from (select c.custkey, year(shipdate) as y, array_agg(cast (row (partkey, extendedprice, quantity) as row (pk bigint, ep double, qt double))) as parts, sum (extendedprice) as total_cost
  from hive.tpch.lineitem_s l, hive.tpch.orders o, hive.tpch.customer c where l.orderkey = o.orderkey and o.custkey = c.custkey and c.nationkey = 1 and quantity < 10
  group by c.custkey, year(shipdate))
  group by custkey;



  

create table exportinfo as
select l.orderkey, linenumber, l.partkey, l.suppkey, extendedprice, discount, quantity, shipdate, receiptdate, commitdate, l.comment,
  CASE WHEN S.nationkey = C.nationkey THEN NULL ELSE 
CAST (row(
S.NATIONKEY, C.NATIONKEY,
CASE WHEN (S.NATIONKEY IN (6, 7, 19) AND C.NATIONKEY IN (6,7,19)) THEN 1 ELSE 0 END,
case when s.nationkey = 24 and c.nationkey = 10 then 1 else 0 end,
 case when p.comment like '%fur%' or p.comment like '%care%'
 then row(o.orderdate, l.shipdate, l.partkey + l.suppkey, concat(p.comment, l.comment))
   else null end
)
AS ROW (
S_NATION BIGINT, C_NATION BIGINT,
IS_INSIDE_EU INT,
IS_RESTRICTED INT,
LICENSE ROW (APPLYDATE DATE, GRANTDATE DATE, FILING_NO BIGINT, COMMENT VARCHAR)))
END AS EXPORT
FROM LINEITEM L, ORDERs O, CUSTOMER C, SUPPLIER S, PART P
WHERE L.ORDERKEY = O.ORDERKEY AND L.PARTKEY = P.PARTKEY AND L.SUPPKEY = S.SUPPKEY AND C.CUSTKEY = O.CUSTKEY 
AND L.ORDERKEY < 1000000;


select l.applydate from (select e.license as l from (select export as e from hive.tpch.exportinfo where orderkey < 5));

select orderkey, linenumber, s_nation, l.applydate from (select orderkey, linenumber, e.s_nation, e.license as l from (select orderkey, linenumber, export as e from hive.tpch.exportinfo where orderkey < 15));





create table hive.tpch.cust_order_line as
select c_custkey, max(c_name) as c_name, max(c_address) as c_address, max(c_nationkey) as c_nationkey, max(c_phone) as c_phone, max(c_acctbal) as c_acctbal, max(c_mktsegment) as c_mktsegment, max(c_comment) as c_comment,
array_agg(
  cast (row (o_orderkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_shippriority,
 o_clerk, o_comment, lines)
 as row(
o_orderkey bigint, o_orderstatus varchar, o_totalprice double, o_orderdate date, o_orderpriority varchar, o_shippriority varchar,
 o_clerk varchar, o_comment varchar,
 o_lines array (
row (
 partkey  bigint   ,
 suppkey  bigint   ,
 linenumber   integer   ,
 quantity double   ,
 extendedprice  double   ,
 discount double   ,
 tax double   ,
 returnflag   varchar(1) ,
 linestatus   varchar(1) ,
 shipdate date ,
 commitdate   date ,
 receiptdate   date ,
 shipinstruct  varchar(25) ,
 shipmode varchar(10), 
 comment  varchar(44) )
)
))) as c_orders
 from (
select c.custkey as c_custkey, o.orderkey as o_orderkey, max(c.name) as c_name, max(c.address) as c_address, max (c.nationkey) as c_nationkey, max(c.phone) as c_phone, max(c.acctbal) as c_acctbal, max(c.mktsegment) as c_mktsegment, max(c.comment) as c_comment,
max (o. orderstatus) as o_orderstatus, max(o.totalprice) as o_totalprice,   max(o.orderdate) as o_orderdate,
  max(orderpriority) as o_orderpriority, 
 max (clerk) as o_clerk,
 max(shippriority) as o_shippriority,
 max (o.comment) as o_comment, 
  array_agg(cast (row(
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
)
  as row (
 l_partkey  bigint   ,
 l_suppkey  bigint   ,
 l_linenumber   integer   ,
 l_quantity double   ,
 l_extendedprice  double   ,
 l_discount double   ,
 l_tax double   ,
 l_returnflag   varchar(1) ,
 l_linestatus   varchar(1) ,
 l_shipdate date ,
 l_commitdate   date ,
 l_receiptdate   date ,
 sl_hipinstruct  varchar(25) ,
 l_shipmode varchar(10), 
 l_comment  varchar(44) ))) as lines
from hive.tpch.lineitem l, hive.tpch.orders o, hive.tpch.customer c
where o.orderkey = l.orderkey and o.custkey = c.custkey and l.orderkey < 100
group by c.custkey, o.orderkey)
group by c_custkey;


select o_orderkey from    hive.tpch.cust_order_line cross join unnest c_orders where c_custkey = 4448479;


select export.s_nation from export_info where export.s_nation = 2;
