--tables: lineitem_aria_string_structs_with_nulls
SELECT count (*) FROM lineitem_aria_string_structs_with_nulls
WHERE orderkey > 11 AND partkey_struct.comment > 'f'
;


SELECT orderkey, partkey_struct.comment FROM lineitem_aria_string_structs_with_nulls
WHERE orderkey > 11 AND partkey_struct.comment > 'f'
;

SELECT count(*) from lineitem_aria_string_structs_with_nulls
WHERE partkey_struct.comment IS NULL
;


SELECT count(*) from lineitem_aria_string_structs_with_nulls
WHERE partkey_struct IS NULL
;

SELECT orderkey, linenumber, partkey_struct.comment from lineitem_aria_string_structs_with_nulls
WHERE partkey_struct IS NOT NULL
;

SELECT orderkey, linenumber, partkey_struct.comment from lineitem_aria_string_structs_with_nulls
WHERE partkey_struct IS NOT NULL and partkey_struct.comment IS NOT NULL
;

SELECT orderkey, linenumber, partkey_struct.comment from lineitem_aria_string_structs_with_nulls
WHERE partkey_struct IS NULL and partkey_struct.comment IS NOT NULL
;

