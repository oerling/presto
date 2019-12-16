ln -sf VectorizedHashTable.java VectorizedHashTable.c
gcc -E  -o t.java VectorizedHashTable.c
sed --in-place "s/^#.*//" t.java
/home/oerling/format_java_file.sh t.java

cp t.java ../../java/com/facebook/presto/operator/aggregation/VectorizedHashTable.java
cd /home/oerling/presto-m
./bld.sh


