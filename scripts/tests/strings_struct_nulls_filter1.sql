select orderkey, linenumber, s1.s1, s1.s2, s3.s3, s3.s4 from hive.tpch.strings_struct_nulls where
 s1.s1 > 'f'
 and s1.s2 > '1'
 and s3.s3 > '1'
 and s3.s4 > '2';
 
