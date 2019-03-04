select l_orderkey, l_linenumber, l_array   from hive.tpch.lineitem1_nulls where l_array[2] < 20000 and l_array[3] < 1000 and l_orderkey > 150000 and l_orderkey < 250000;
