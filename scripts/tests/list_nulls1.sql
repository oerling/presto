select l_orderkey,  l_linenumber, l_partkey, l_array from hive.tpch.lineitem1_nulls where l_orderkey < 200000;
