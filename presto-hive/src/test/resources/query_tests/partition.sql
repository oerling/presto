--tables: lineitem_partitioned_bucketed
select year, quarter, shipdate, orderkey from lineitem_partitioned_bucketed
where linenumber = 2 and year = '1996'
;

select orderkey, year, quarter from lineitem_partitioned_bucketed
where year <> cast(year(receiptdate) as varchar)
;

select orderkey, year, quarter from lineitem_partitioned_bucketed
where partkey < 1000 and suppkey < 10
;
