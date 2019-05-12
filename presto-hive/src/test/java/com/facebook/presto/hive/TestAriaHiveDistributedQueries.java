/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.ARIA_REUSE_PAGES;
import static com.facebook.presto.SystemSessionProperties.ARIA_SCAN;
import static com.facebook.presto.SystemSessionProperties.PUSHDOWN_SUBFIELDS;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.io.Resources.getResource;
import static io.airlift.tpch.TpchTable.getTables;
import static io.airlift.units.Duration.nanosSince;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestAriaHiveDistributedQueries
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestAriaHiveDistributedQueries.class);
    Map<String, String> tables = new HashMap();
    private Set<String> createdTables = new HashSet();

    protected TestAriaHiveDistributedQueries()
    {
        super(() -> createQueryRunner());
    }

    private static QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = HiveQueryRunner.createQueryRunner(ImmutableList.of());

        Session noAria = Session.builder(queryRunner.getDefaultSession())
                .setSystemProperty(ARIA_SCAN, "false")
                .build();

        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, noAria, getTables());
        return queryRunner;
    }

    private static void createTable(QueryRunner queryRunner, Session session, String tableName, String sql)
    {
        log.info("Creating %s table", tableName);
        long start = System.nanoTime();
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.info("Created %s rows for %s in %s", rows, tableName, nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private Session noAriaSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
            .setSystemProperty(ARIA_SCAN, "false")
            .setCatalogSessionProperty(HiveQueryRunner.HIVE_CATALOG, HiveSessionProperties.ARIA_SCAN_ENABLED, "false")
            .build();
    }

    private Session ariaSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(ARIA_SCAN, "true")
                .setSystemProperty(ARIA_REUSE_PAGES, "true")
                .setSystemProperty(PUSHDOWN_SUBFIELDS, "true")
                .setCatalogSessionProperty(HIVE_CATALOG, HiveSessionProperties.ARIA_SCAN_ENABLED, "true")
                .setCatalogSessionProperty(HIVE_CATALOG, HiveSessionProperties.FILTER_REORDERING_ENABLED, "true")
                .setCatalogSessionProperty(HIVE_CATALOG, HiveSessionProperties.READER_BUDGET_ENFORCEMENT_ENABLED, "true")
                .build();
    }

    // filters1.sql
    // filters1_nulls.sql
    @Test
    public void testFilters()
    {
        requireTestTable("lineitem_aria");
        requireTestTable("lineitem_aria_nulls");
        assertQuery(ariaSession(), "SELECT\n" +
                "    orderkey,\n" +
                "    linenumber,\n" +
                "    partkey,\n" +
                "    suppkey,\n" +
                "    comment\n" +
                "FROM lineitem\n" +
                "WHERE\n" +
                "    orderkey BETWEEN 10000 AND 20000\n" +
                "    AND partkey BETWEEN 10 AND 50\n" +
                "    AND suppkey BETWEEN 10 AND 50\n" +
                "    AND comment > 'f'");

        assertQuery(ariaSession(), "SELECT\n" +
                "    orderkey,\n" +
                "    linenumber,\n" +
                "    partkey,\n" +
                "    suppkey,\n" +
                "    comment\n" +
                "FROM lineitem_aria_nulls\n" +
                "WHERE\n" +
                "    orderkey BETWEEN 10000 AND 40000\n" +
                "    AND partkey BETWEEN 1000 AND 3000\n" +
                "    AND suppkey BETWEEN 10 AND 50\n" +
                    "    AND comment > 'f'", noAriaSession());

        // SliceDictionaryStreamReader for shipinstruct
        assertQuery(ariaSession(), "SELECT\n" +
                "    linenumber,\n" +
                "    orderkey,\n" +
                "    shipinstruct\n" +
                "FROM lineitem\n" +
                "WHERE\n" +
                "    shipinstruct = 'TAKE BACK RETURN'");

        // SliceDictionaryStreamReader for shipinstruct and shipmode
        assertQuery(ariaSession(), "SELECT\n" +
                "    linenumber,\n" +
                "    orderkey,\n" +
                "    shipinstruct,\n" +
                "    shipmode\n" +
                "FROM lineitem\n" +
                "WHERE\n" +
                "    shipinstruct = 'TAKE BACK RETURN' AND shipmode = 'AIR'");

        assertQuery(ariaSession(), "SELECT\n" +
                "    linenumber,\n" +
                "    orderkey\n" +
                "FROM lineitem\n" +
                "WHERE\n" +
                "    shipinstruct = 'TAKE BACK RETURN' AND orderkey < 10");

        // BooleanStreamReader
        assertQuery(ariaSession(), "SELECT linenumber FROM lineitem_aria WHERE is_returned = TRUE",
                "SELECT linenumber FROM lineitem WHERE returnflag = 'R'");

        assertQuery(ariaSession(), "SELECT linenumber, is_returned FROM lineitem_aria WHERE is_returned = TRUE",
                "SELECT linenumber, true FROM lineitem WHERE returnflag = 'R'");

        // Filter on a float column
        assertQuery(ariaSession(),
                "SELECT float_quantity FROM lineitem_aria WHERE float_quantity < 5",
                "SELECT quantity + 1 FROM lineitem WHERE quantity < 4");

        assertQuery(ariaSession(),
                "SELECT orderkey, float_quantity FROM lineitem_aria WHERE float_quantity = 4",
                "SELECT orderkey, quantity + 1 FROM lineitem WHERE quantity = 3");

        // Filter on a tinyint column
        assertQuery(ariaSession(),
                "SELECT linenumber, shipmode, tinyint_shipmode FROM lineitem_aria WHERE tinyint_shipmode in (3, 5, 6)",
                "SELECT linenumber, shipmode, CASE shipmode WHEN 'AIR' THEN 3 WHEN 'MAIL' THEN 5 WHEN 'RAIL' THEN 6 END FROM lineitem WHERE shipmode in ('AIR','MAIL','RAIL')");

        assertQuery(ariaSession(),
                "SELECT linenumber, shipmode FROM lineitem_aria WHERE tinyint_shipmode in (3, 5, 6)",
                "SELECT linenumber, shipmode FROM lineitem WHERE shipmode in ('AIR','MAIL','RAIL')");

        // Filter on a timestamp column
        assertQuery(ariaSession(), "SELECT shipdate, timestamp_shipdate FROM lineitem_aria WHERE timestamp_shipdate < date '1997-11-29'",
                "SELECT shipdate, dateadd('second', suppkey, shipdate) FROM lineitem where shipdate < date '1997-11-29'");

        assertQuery(ariaSession(), "SELECT shipdate FROM lineitem_aria WHERE timestamp_shipdate < date '1997-11-29'",
                "SELECT shipdate FROM lineitem where shipdate < date '1997-11-29'");

        // Filter on a decimal column
        assertQuery(ariaSession(), "SELECT shipdate, short_decimal_discount FROM lineitem_aria WHERE short_decimal_discount < decimal '0.3'",
                "SELECT shipdate, discount FROM lineitem where discount < 0.3");

        assertQuery(ariaSession(), "SELECT shipdate FROM lineitem_aria WHERE short_decimal_discount < decimal '0.3'",
                "SELECT shipdate FROM lineitem where discount < 0.3");

        assertQuery(ariaSession(), "SELECT shipdate, long_decimal_discount FROM lineitem_aria WHERE long_decimal_discount < decimal '0.3'",
                "SELECT shipdate, discount FROM lineitem where discount < 0.3");

        assertQuery(ariaSession(), "SELECT shipdate FROM lineitem_aria WHERE long_decimal_discount < decimal '0.3'",
                "SELECT shipdate FROM lineitem where discount < 0.3");
        // Select different scalars after a filter.
        assertQuery(ariaSession(), "select partkey, shipmode, comment,\n" +
                "  long_decimal_discount, short_decimal_discount, is_returned, float_quantity, timestamp_shipdate, tinyint_shipmode\n" +
                "FROM lineitem_aria WHERE linenumber = 4", noAriaSession());
    }

    // nulls1.sql
    // nulls2_sql
    @Test
    public void testNulls()
    {
        assertQuery(ariaSession(), "SELECT\n" +
                "    orderkey,\n" +
                "    linenumber,\n" +
                "    partkey,\n" +
                "    suppkey\n" +
                "FROM lineitem\n" +
                "WHERE\n" +
                "    partkey IS NULL\n" +
                "    AND suppkey IS NULL");

        assertQuery(ariaSession(), "SELECT\n" +
                "    orderkey,\n" +
                "    linenumber,\n" +
                "    partkey,\n" +
                "    suppkey,\n" +
                "    quantity,\n" +
                "    comment\n" +
                "FROM lineitem\n" +
                "WHERE (\n" +
                "        partkey IS NULL\n" +
                "        OR partkey BETWEEN 1000 AND 2000\n" +
                "        OR partkey BETWEEN 10000 AND 11000\n" +
                "    )\n" +
                "    AND (\n" +
                "        suppkey IS NULL\n" +
                "        OR suppkey BETWEEN 1000 AND 2000\n" +
                "        OR suppkey BETWEEN 3000 AND 4000\n" +
                "    )\n" +
                "    AND (\n" +
                "        quantity IS NULL\n" +
                "        OR quantity BETWEEN 5 AND 10\n" +
                "        OR quantity BETWEEN 20 AND 40\n" +
                "    )");
    }

    @Test
    public void testStrings()
    {
        requireTestTable("lineitem_aria_string_structs_with_nulls");
        requireTestTable("lineitem_aria_strings");
        requireTestTable("lineitem_aria_string_structs");
        assertQuery(ariaSession(), "SELECT\n" +
                "    orderkey,\n" +
                "    linenumber,\n" +
                "    comment,\n" +
                "    partkey_comment,\n" +
                "    suppkey_comment,\n" +
                "    quantity_comment\n" +
                "FROM lineitem_aria_strings\n" +
                "WHERE\n" +
                "    comment > 'f'\n" +
                "    AND partkey_comment > '1'\n" +
                "    AND suppkey_comment > '1'\n" +
                "    AND quantity_comment > '2'");

        assertQuery(ariaSession(), "SELECT\n" +
                "    orderkey,\n" +
                "    linenumber,\n" +
                "    partkey_struct.comment,\n" +
                "    partkey_struct.partkey_comment,\n" +
                "    suppkey_quantity_struct.suppkey_comment,\n" +
                "    suppkey_quantity_struct.quantity_comment\n" +
                "FROM lineitem_aria_string_structs",
                "SELECT\n" +
                        "    orderkey,\n" +
                        "    linenumber,\n" +
                        "    comment,\n" +
                        "    partkey_comment,\n" +
                        "    suppkey_comment,\n" +
                        "    quantity_comment\n" +
                        "FROM lineitem_aria_strings");

        assertQuery(ariaSession(), "SELECT\n" +
                        "    orderkey,\n" +
                        "    linenumber,\n" +
                        "    partkey_struct.comment,\n" +
                        "    partkey_struct.partkey_comment,\n" +
                        "    suppkey_quantity_struct.suppkey_comment,\n" +
                        "    suppkey_quantity_struct.quantity_comment\n" +
                        "FROM lineitem_aria_string_structs_with_nulls",
                "SELECT\n" +
                        "    orderkey,\n" +
                        "    linenumber,\n" +
                        "    CASEWHEN(mod(partkey, 5) = 0, null, comment),\n" +
                        "    CASEWHEN(mod(partkey, 5) = 0 OR mod(partkey, 13) = 0, null, partkey_comment),\n" +
                        "    CASEWHEN(mod(suppkey, 7) = 0 OR mod(suppkey, 17) = 0, null, suppkey_comment),\n" +
                        "    CASEWHEN(mod(suppkey, 7) = 0, null, quantity_comment)\n" +
                        "FROM lineitem_aria_strings");

        assertQuery(ariaSession(), "SELECT\n" +
                "    orderkey,\n" +
                "    linenumber,\n" +
                "    partkey_struct.comment,\n" +
                "    partkey_struct.partkey_comment,\n" +
                "    suppkey_quantity_struct.suppkey_comment,\n" +
                "    suppkey_quantity_struct.quantity_comment\n" +
                "FROM lineitem_aria_string_structs_with_nulls\n" +
                "WHERE\n" +
                "    partkey_struct.comment > 'f'\n" +
                "    AND partkey_struct.partkey_comment > '1'\n" +
                "    AND suppkey_quantity_struct.suppkey_comment > '1'\n" +
                "    AND suppkey_quantity_struct.quantity_comment > '2'",
                "SELECT\n" +
                        "    orderkey,\n" +
                        "    linenumber,\n" +
                        "    comment,\n" +
                        "    partkey_comment,\n" +
                        "    suppkey_comment,\n" +
                        "    quantity_comment\n" +
                        "FROM lineitem_aria_strings\n" +
                        "WHERE\n" +
                        "    mod(partkey, 5) <> 0 AND mod(partkey, 13) <> 0" +
                        "    AND mod(suppkey, 7) <> 0 AND mod(suppkey, 17) <> 0" +
                        "    AND comment > 'f'\n" +
                        "    AND partkey_comment > '1'\n" +
                        "    AND suppkey_comment > '1'\n" +
                        "    AND quantity_comment > '2'");
    }

    @Test
    public void testQueenOfTheNight()
    {
        assertQuery(ariaSession(), "SELECT\n" +
                "    COUNT(*),\n" +
                "    SUM(l.extendedprice * (1 - l.discount) - l.quantity * p.supplycost)\n" +
                "FROM lineitem l, partsupp p\n" +
                "WHERE\n" +
                "    l.partkey = p.partkey\n" +
                "    AND l.suppkey = p.suppkey\n" +
                "    AND p.availqty < 1000");
    }

    @Test
    public void testSchemaEvolution()
    {
        assertUpdate("CREATE TABLE test_schema_evolution AS SELECT * FROM nation", 25);
        assertUpdate("ALTER TABLE test_schema_evolution ADD COLUMN nation_plus_region BIGINT");
        assertUpdate("INSERT INTO test_schema_evolution SELECT *, nationkey + regionkey FROM nation", 25);
        assertUpdate("ALTER TABLE test_schema_evolution ADD COLUMN nation_minus_region BIGINT");
        assertUpdate("INSERT INTO test_schema_evolution SELECT *, nationkey + regionkey, nationkey - regionkey FROM nation", 25);

        String cte = "WITH test_schema_evolution AS (" +
                "SELECT *, null AS nation_plus_region, null AS nation_minus_region FROM nation " +
                "UNION ALL SELECT *, nationkey + regionkey, null FROM nation " +
                "UNION ALL SELECT *, nationkey + regionkey, nationkey - regionkey FROM nation)";

        assertQueryUsingH2Cte("SELECT * FROM test_schema_evolution", cte);
        assertQueryUsingH2Cte("SELECT * FROM test_schema_evolution WHERE nation_plus_region IS NULL", cte);
        assertQueryUsingH2Cte("SELECT * FROM test_schema_evolution WHERE nation_plus_region > 10", cte);
        assertQueryUsingH2Cte("SELECT * FROM test_schema_evolution WHERE nation_plus_region + 1 > 10", cte);
        assertQueryUsingH2Cte("SELECT * FROM test_schema_evolution WHERE nation_plus_region + nation_minus_region > 20", cte);

        assertUpdate("DROP TABLE test_schema_evolution");
    }

    private void assertQueryUsingH2Cte(String query, String cte)
    {
        assertQuery(ariaSession(), query, cte + " " + query);
    }

    @Test
    public void testNonDeterministicConstantFilters()
    {
        int count = computeActual(ariaSession(), "SELECT * FROM nation WHERE random(10) < 5").getRowCount();
        assertTrue(count > 0 && count < 25);

        count = computeActual(ariaSession(), "SELECT * FROM nation WHERE random(10) < 0").getRowCount();
        assertEquals(count, 0);

        count = computeActual(ariaSession(), "SELECT * FROM nation WHERE random(10) < 10").getRowCount();
        assertEquals(count, 25);
    }

    @Test
    public void testJoins()
    {
        assertQuery(ariaSession(), "SELECT\n" +
                "    p.supplycost\n" +
                "FROM lineitem l\n" +
                "JOIN partsupp p\n" +
                "    ON p.suppkey = l.suppkey AND p.partkey = l.partkey");
    }

    @Test
    public void testRepeated()
    {
        runTestFile("query_tests/repeated.sql");
    }

    private void readTables()
    {
        synchronized (getClass()) {
            if (tables.size() > 0) {
                return;
            }
            try {
                Path path = Paths.get(getResource("query_tests/test_tables.sql").getFile());
                List<String> lines = Files.readAllLines(path);
                String name = null;
                String query = null;
                for (int i = 0; i < lines.size(); i++) {
                    String line = lines.get(i);
                    if (name == null) {
                        if (line.startsWith("--table: ")) {
                            name = line.split(" ")[1].trim();
                        }
                    }
                    else if (name != null) {
                        if (line.trim().startsWith(";")) {
                            tables.put(name, query);
                            name = null;
                            query = null;
                            continue;
                        }
                        if (query == null) {
                            query = line;
                        }
                        else {
                            query = query + "\n" + line;
                        }
                    }
                }
            }
            catch (IOException e) {
                fail("Could not read test_tables.sql: " + e.getMessage());
            }
        }}

    private void testQuery(String[] tables, String query)
            throws IOException
    {
        for (String table : tables) {
            requireTestTable(table);
        }
        assertQuery(ariaSession(), query, noAriaSession());
    }

    private void requireTestTable(String name)
    {
        readTables();
        synchronized (getClass()) {
            if (createdTables.contains(name)) {
                return;
            }
            String query = tables.get(name);
            assertTrue(query != null, "No query defined for creating " + name);
            createTable(getQueryRunner(), noAriaSession(), name, "CREATE TABLE " + name + " AS\n" + query);
            createdTables.add(name);
        }
    }

    private void runTestFile(String name)
    {
        Path path = Paths.get(getResource(name).getFile());
        try {
            List<String> lines = Files.readAllLines(path);
            String[] names = null;
            String query = null;
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);
                if (line.startsWith("--tables: ")) {
                    names = line.substring(9).trim().split(" ,");
                    continue;
                }
                if (line.trim().startsWith(";")) {
                    if (names == null || query == null) {
                        fail("No query or --tables: declared before line " + i);
                    }
                    testQuery(names, query);
                    query = null;
                    continue;
                }
                if (query == null) {
                    query = line;
                }
                else {
                    query = query + "\n" + line;
                }
            }
        }
        catch (IOException e) {
            fail("Failed to read file: " + e.getMessage());
        }
    }

    @Test
    public void testNested()
    {
        runTestFile("query_tests/nested.sql");
    }

    @Test
    public void testNulls2()
    {
        runTestFile("query_tests/nulls.sql");
    }

    public void testFilters2()
    {
        runTestFile("query_tests/filters2.sql");
    }

    public void testStruct()
    {
        runTestFile("query_tests/struct.sql");
    }
}
