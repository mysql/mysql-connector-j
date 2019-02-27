/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package testsuite.x.devapi;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Test;

import com.mysql.cj.CoreSession;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.DataConversionException;
import com.mysql.cj.protocol.x.XProtocol;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.Column;
import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.SelectStatement;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;
import com.mysql.cj.xdevapi.SessionImpl;
import com.mysql.cj.xdevapi.SqlResult;
import com.mysql.cj.xdevapi.Statement;
import com.mysql.cj.xdevapi.Table;
import com.mysql.cj.xdevapi.Type;

/**
 * @todo
 */
public class TableSelectTest extends BaseTableTestCase {

    @Test
    public void basicQuery() {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop table if exists basicQuery");
        sqlUpdate("create table basicQuery (_id varchar(32), name varchar(20), birthday date, age int)");
        sqlUpdate("insert into basicQuery values ('some long UUID', 'Sakila', '2000-05-27', 14)");
        Table table = this.schema.getTable("basicQuery");
        Map<String, Object> params = new HashMap<>();
        params.put("name", "Saki%");
        params.put("age", 20);
        RowResult rows = table.select("birthday, `_id`, name").where("name like :name AND age < :age").bind(params).execute();

        // verify metadata
        List<String> columnNames = rows.getColumnNames();
        assertEquals("birthday", columnNames.get(0));
        assertEquals("_id", columnNames.get(1));
        assertEquals("name", columnNames.get(2));

        Row row = rows.next();
        assertEquals("2000-05-27", row.getString(0));
        assertEquals("2000-05-27", row.getString("birthday"));
        assertEquals("Sakila", row.getString(2));
        assertEquals("Sakila", row.getString("name"));
        assertEquals("some long UUID", row.getString(1));
        assertEquals("some long UUID", row.getString("_id"));

        // select with multiple projection params
        rows = table.select("`_id`", "name", "birthday").where("name like :name AND age < :age").bind(params).execute();
        // verify metadata
        columnNames = rows.getColumnNames();
        assertEquals("_id", columnNames.get(0));
        assertEquals("name", columnNames.get(1));
        assertEquals("birthday", columnNames.get(2));
    }

    @Test
    public void testComplexQuery() {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop table if exists complexQuery");
        sqlUpdate("create table complexQuery (name varchar(32), age int, something int)");
        sqlUpdate("insert into complexQuery values ('Mamie', 11, 0)");
        sqlUpdate("insert into complexQuery values ('Eulalia', 11, 0)");
        sqlUpdate("insert into complexQuery values ('Polly', 12, 0)");
        sqlUpdate("insert into complexQuery values ('Rufus', 12, 0)");
        sqlUpdate("insert into complexQuery values ('Cassidy', 13, 0)");
        sqlUpdate("insert into complexQuery values ('Olympia', 14, 0)");
        sqlUpdate("insert into complexQuery values ('Lev', 14, 0)");
        sqlUpdate("insert into complexQuery values ('Tierney', 15, 0)");
        sqlUpdate("insert into complexQuery values ('Octavia', 15, 0)");
        sqlUpdate("insert into complexQuery values ('Vesper', 16, 0)");
        sqlUpdate("insert into complexQuery values ('Caspian', 17, 0)");
        sqlUpdate("insert into complexQuery values ('Romy', 17, 0)");
        Table table = this.schema.getTable("complexQuery");
        // Result:
        // age_group | cnt
        // 11        | 2   <-- filtered out by where
        // 12        | 2   <-- filtered out by limit
        // 13        | 1   <-- filtered out by having
        // 14        | 2   * second row in result
        // 15        | 2   * first row in result
        // 16        | 1   <-- filtered out by having
        // 17        | 2   <-- filtered out by offset
        SelectStatement stmt = table.select("age as age_group, count(name) as cnt, something");
        stmt.where("age > 11 and 1 < 2 and 40 between 30 and 900");
        stmt.groupBy("something", "age_group");
        stmt.having("cnt > 1");
        stmt.orderBy("age_group desc");
        RowResult rows = stmt.limit(2).offset(1).execute();
        Row row = rows.next();
        assertEquals(15, row.getInt(0));
        assertEquals(2, row.getInt(1));
        assertEquals(2, row.getByte(1));
        assertEquals(2, row.getLong(1));
        assertEquals(new BigDecimal("2"), row.getBigDecimal(1));
        assertEquals(true, row.getBoolean(1));
        row = rows.next();
        assertEquals(14, row.getInt(0));
        assertEquals(2, row.getInt(1));
        assertFalse(rows.hasNext());
    }

    @Test
    public void allColumns() {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop table if exists allColumns");
        sqlUpdate("create table allColumns (x int, y int, z int)");
        sqlUpdate("insert into allColumns values (1,2,3)");
        Table table = this.schema.getTable("allColumns");
        // * must come first, as with SQL
        SelectStatement stmt = table.select("*, 42 as a_number, '43' as a_string");
        Row row = stmt.execute().next();
        assertEquals(42, row.getInt("a_number"));
        assertEquals(1, row.getInt("x"));
        assertEquals(2, row.getInt("y"));
        assertEquals(3, row.getInt("z"));
        assertEquals("43", row.getString("a_string"));
    }

    @Test
    public void countAllColumns() {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop table if exists countAllColumns");
        sqlUpdate("create table countAllColumns(x int, y int)");
        sqlUpdate("insert into countAllColumns values (1,1), (2,2), (3,3), (4,4)");
        Table table = this.schema.getTable("countAllColumns");
        Row row = table.select("count(*) + 10").execute().next();
        assertEquals(14, row.getInt(0));
    }

    /**
     * Tests fix for Bug#22931433, GETTING VALUE OF BIT COLUMN RESULTS IN EXCEPTION.
     * 
     * @throws Exception
     *             if the test fails.
     */
    @Test
    public void testBug22931433() {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop table if exists testBug22931433");
        sqlUpdate(
                "create table testBug22931433(c1 bit(8), c2 bit(16), c3 bit(24), c4 bit(32), c5 bit(40), c6 bit(48), c7 bit(56), c8 bit(64), cb1 bit(1), cb2 bit(64))");

        Table table = this.schema.getTable("testBug22931433");
        table.insert("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "cb1", "cb2")
                .values("a", "ba", "cba", "dcba", "edcba", "fedcba", "gfedcba", "hgfedcba", 0x01, -1).execute();
        table.insert("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "cb1", "cb2")
                .values(0xcc, 0xcccc, 0xcccccc, 0xccccccccL, 0xccccccccccL, 0xccccccccccccL, 0xccccccccccccccL, 0xccccccccccccccccL, 0x00, -2).execute();

        String testUrl = this.baseUrl + (this.baseUrl.contains("?") ? "&" : "?") + makeParam(PropertyKey.jdbcCompliantTruncation, "false", true);
        Session s1 = this.fact.getSession(testUrl);
        table = s1.getDefaultSchema().getTable("testBug22931433");

        RowResult rows = table.select("c1, c2, c3, c4, c5, c6, c7, c8, cb1, cb2").execute();

        Row row = rows.next();

        assertEquals('a', row.getByte("c1"));
        assertEquals('a', row.getByte("c2"));
        assertEquals('a', row.getByte("c3"));
        assertEquals('a', row.getByte("c4"));
        assertEquals('a', row.getByte("c5"));
        assertEquals('a', row.getByte("c6"));
        assertEquals('a', row.getByte("c7"));
        assertEquals('a', row.getByte("c8"));

        assertEquals(97, row.getInt("c1"));
        assertEquals(25185, row.getInt("c2"));
        assertEquals(6513249, row.getInt("c3"));
        assertEquals(1684234849, row.getInt("c4"));
        assertEquals(1684234849, row.getInt("c5")); // truncated to 4 bytes
        assertEquals(1684234849, row.getInt("c6")); // truncated to 4 bytes
        assertEquals(1684234849, row.getInt("c7")); // truncated to 4 bytes
        assertEquals(1684234849, row.getInt("c8")); // truncated to 4 bytes

        assertEquals(97, row.getLong("c1"));
        assertEquals(25185, row.getLong("c2"));
        assertEquals(6513249, row.getLong("c3"));
        assertEquals(1684234849, row.getLong("c4"));
        assertEquals(435475931745L, row.getLong("c5"));
        assertEquals(112585661964897L, row.getLong("c6"));
        assertEquals(29104508263162465L, row.getLong("c7"));
        assertEquals(7523094288207667809L, row.getLong("c8"));

        assertEquals(BigDecimal.valueOf(97), row.getBigDecimal("c1"));
        assertEquals(BigDecimal.valueOf(25185), row.getBigDecimal("c2"));
        assertEquals(BigDecimal.valueOf(6513249), row.getBigDecimal("c3"));
        assertEquals(BigDecimal.valueOf(1684234849), row.getBigDecimal("c4"));
        assertEquals(BigDecimal.valueOf(435475931745L), row.getBigDecimal("c5"));
        assertEquals(BigDecimal.valueOf(112585661964897L), row.getBigDecimal("c6"));
        assertEquals(BigDecimal.valueOf(29104508263162465L), row.getBigDecimal("c7"));
        assertEquals(BigDecimal.valueOf(7523094288207667809L), row.getBigDecimal("c8"));

        assertEquals(Double.valueOf(97), Double.valueOf(row.getDouble("c1")));
        assertEquals(Double.valueOf(25185), Double.valueOf(row.getDouble("c2")));
        assertEquals(Double.valueOf(6513249), Double.valueOf(row.getDouble("c3")));
        assertEquals(Double.valueOf(1684234849), Double.valueOf(row.getDouble("c4")));
        assertEquals(Double.valueOf(435475931745L), Double.valueOf(row.getDouble("c5")));
        assertEquals(Double.valueOf(112585661964897L), Double.valueOf(row.getDouble("c6")));
        assertEquals(Double.valueOf(29104508263162465L), Double.valueOf(row.getDouble("c7")));
        assertEquals(Double.valueOf(7523094288207667809L), Double.valueOf(row.getDouble("c8")));

        assertEquals(true, row.getBoolean("c1"));
        assertEquals(true, row.getBoolean("cb1"));
        assertEquals(true, row.getBoolean("cb2"));

        assertEquals(BigDecimal.valueOf(97).toString(), row.getString("c1"));
        assertEquals(BigDecimal.valueOf(25185).toString(), row.getString("c2"));
        assertEquals(BigDecimal.valueOf(6513249).toString(), row.getString("c3"));
        assertEquals(BigDecimal.valueOf(1684234849).toString(), row.getString("c4"));
        assertEquals(BigDecimal.valueOf(435475931745L).toString(), row.getString("c5"));
        assertEquals(BigDecimal.valueOf(112585661964897L).toString(), row.getString("c6"));
        assertEquals(BigDecimal.valueOf(29104508263162465L).toString(), row.getString("c7"));
        assertEquals(BigDecimal.valueOf(7523094288207667809L).toString(), row.getString("c8"));

        assertThrows(DataConversionException.class, "Unsupported conversion from BIT to java.sql.Date", new Callable<Void>() {
            public Void call() throws Exception {
                row.getDate("c1");
                return null;
            }
        });

        assertThrows(DataConversionException.class, "Unsupported conversion from BIT to com.mysql.cj.xdevapi.DbDoc", new Callable<Void>() {
            public Void call() throws Exception {
                row.getDbDoc("c1");
                return null;
            }
        });

        assertThrows(DataConversionException.class, "Unsupported conversion from BIT to java.sql.Time", new Callable<Void>() {
            public Void call() throws Exception {
                row.getTime("c1");
                return null;
            }
        });

        assertThrows(DataConversionException.class, "Unsupported conversion from BIT to java.sql.Timestamp", new Callable<Void>() {
            public Void call() throws Exception {
                row.getTimestamp("c1");
                return null;
            }
        });

        // test negative values
        Row row2 = rows.next();

        assertEquals(-52, row2.getByte("c1"));
        assertEquals(-52, row2.getByte("c2"));
        assertEquals(-52, row2.getByte("c3"));
        assertEquals(-52, row2.getByte("c4"));
        assertEquals(-52, row2.getByte("c5"));
        assertEquals(-52, row2.getByte("c6"));
        assertEquals(-52, row2.getByte("c7"));
        assertEquals(-52, row2.getByte("c8"));

        assertEquals(204, row2.getInt("c1"));
        assertEquals(52428, row2.getInt("c2"));
        assertEquals(13421772, row2.getInt("c3"));
        assertEquals(-858993460, row2.getInt("c4"));
        assertEquals(-858993460, row2.getInt("c5")); // truncated to 4 bytes
        assertEquals(-858993460, row2.getInt("c6")); // truncated to 4 bytes
        assertEquals(-858993460, row2.getInt("c7")); // truncated to 4 bytes
        assertEquals(-858993460, row2.getInt("c8")); // truncated to 4 bytes

        assertEquals(204, row2.getLong("c1"));
        assertEquals(52428, row2.getLong("c2"));
        assertEquals(13421772, row2.getLong("c3"));
        assertEquals(3435973836L, row2.getLong("c4"));
        assertEquals(879609302220L, row2.getLong("c5"));
        assertEquals(225179981368524L, row2.getLong("c6"));
        assertEquals(57646075230342348L, row2.getLong("c7"));
        assertEquals(-3689348814741910324L, row2.getLong("c8"));

        assertEquals(BigDecimal.valueOf(204), row2.getBigDecimal("c1"));
        assertEquals(BigDecimal.valueOf(52428), row2.getBigDecimal("c2"));
        assertEquals(BigDecimal.valueOf(13421772), row2.getBigDecimal("c3"));
        assertEquals(BigDecimal.valueOf(3435973836L), row2.getBigDecimal("c4"));
        assertEquals(BigDecimal.valueOf(879609302220L), row2.getBigDecimal("c5"));
        assertEquals(BigDecimal.valueOf(225179981368524L), row2.getBigDecimal("c6"));
        assertEquals(BigDecimal.valueOf(57646075230342348L), row2.getBigDecimal("c7"));
        assertEquals(new BigDecimal(new BigInteger("14757395258967641292")), row2.getBigDecimal("c8"));

        assertEquals(Double.valueOf(204), Double.valueOf(row2.getDouble("c1")));
        assertEquals(Double.valueOf(52428), Double.valueOf(row2.getDouble("c2")));
        assertEquals(Double.valueOf(13421772), Double.valueOf(row2.getDouble("c3")));
        assertEquals(Double.valueOf(3435973836L), Double.valueOf(row2.getDouble("c4")));
        assertEquals(Double.valueOf(879609302220L), Double.valueOf(row2.getDouble("c5")));
        assertEquals(Double.valueOf(225179981368524L), Double.valueOf(row2.getDouble("c6")));
        assertEquals(Double.valueOf(57646075230342348L), Double.valueOf(row2.getDouble("c7")));
        assertEquals(Double.valueOf(new BigInteger("14757395258967641292").doubleValue()), Double.valueOf(row2.getDouble("c8")));

        assertEquals(false, row2.getBoolean("c8"));
        assertEquals(false, row2.getBoolean("cb1"));
        assertEquals(false, row2.getBoolean("cb2"));

        sqlUpdate("drop table if exists testBug22931433");
    }

    @Test
    public void basicViewQuery() {
        if (!this.isSetForXTests) {
            return;
        }
        try {
            sqlUpdate("drop table if exists basicTable1");
            sqlUpdate("drop table if exists basicTable2");
            sqlUpdate("drop view if exists basicView");

            sqlUpdate("create table basicTable1 (_id varchar(32), name varchar(20))");
            sqlUpdate("create table basicTable2 (_id varchar(32), birthday date, age int)");
            sqlUpdate(
                    "create view basicView as select basicTable1._id, name, birthday, age from basicTable1 join basicTable2 on basicTable1._id=basicTable2._id");

            sqlUpdate("insert into basicTable1 values ('some long UUID', 'Sakila')");
            sqlUpdate("insert into basicTable2 values ('some long UUID', '2000-05-27', 14)");

            Table view = this.schema.getTable("basicView");
            Map<String, Object> params = new HashMap<>();
            params.put("name", "Saki%");
            params.put("age", 20);
            RowResult rows = view.select("birthday, `_id`, name").where("name like :name AND age < :age").bind(params).execute();

            // verify metadata
            List<String> columnNames = rows.getColumnNames();
            assertEquals("birthday", columnNames.get(0));
            assertEquals("_id", columnNames.get(1));
            assertEquals("name", columnNames.get(2));

            Row row = rows.next();
            assertEquals("2000-05-27", row.getString(0));
            assertEquals("2000-05-27", row.getString("birthday"));
            assertEquals("Sakila", row.getString(2));
            assertEquals("Sakila", row.getString("name"));
            assertEquals("some long UUID", row.getString(1));
            assertEquals("some long UUID", row.getString("_id"));
        } finally {
            sqlUpdate("drop table if exists basicTable1");
            sqlUpdate("drop table if exists basicTable2");
            sqlUpdate("drop view if exists basicView");
        }
    }

    @Test
    public void testOrderBy() {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop table if exists testOrderBy");
        sqlUpdate("create table testOrderBy (_id int, x int, y int)");
        sqlUpdate("insert into testOrderBy values (2,20,21), (1,20,22), (4,10,40), (3,10,50)");
        Table table = this.schema.getTable("testOrderBy");

        RowResult rows = table.select("_id").orderBy("x desc, y desc").execute();
        int i = 1;
        while (rows.hasNext()) {
            assertEquals(i++, rows.next().getInt("_id"));
        }
        assertEquals(5, i);

        // multiple SortExprStr
        rows = table.select("_id").orderBy("x desc", "y desc").execute();
        i = 1;
        while (rows.hasNext()) {
            assertEquals(i++, rows.next().getInt("_id"));
        }
        assertEquals(5, i);
    }

    @Test
    public void testBug22988922() {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop table if exists testBug22988922");
        sqlUpdate("create table testBug22988922 (g point,l longblob,t longtext)");

        Table table = this.schema.getTable("testBug22988922");

        RowResult rows = table.select("*").execute();
        List<Column> metadata = rows.getColumns();
        // assertEquals(4294967295L, metadata.get(0).getLength()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(4294967295L, metadata.get(1).getLength());
        assertEquals(4294967295L, metadata.get(2).getLength());

        sqlUpdate("drop table if exists testBug22988922");
    }

    /**
     * Tests fix for BUG#22931277, COLUMN.GETTYPE() RETURNS ERROR FOR VALID DATATYPES.
     */
    @Test
    public void testBug22931277() {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop table if exists testBug22931277");
        sqlUpdate("create table testBug22931277 (j year,k datetime(3))");
        Table table = this.schema.getTable("testBug22931277");

        table = this.schema.getTable("testBug22931277");
        table.insert("j", "k").values(2000, "2016-03-15 12:13:14").execute();

        RowResult rows = table.select("1,j,k").execute();
        List<Column> metadata = rows.getColumns();

        Column myCol = metadata.get(0);
        assertEquals(Type.TINYINT, myCol.getType());

        myCol = metadata.get(1);
        assertEquals(Type.SMALLINT, myCol.getType());

        myCol = metadata.get(2);
        assertEquals(Type.DATETIME, myCol.getType());
    }

    @Test
    public void testTableRowLocks() throws Exception {
        if (!this.isSetForXTests || !mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3"))) {
            return;
        }

        sqlUpdate("drop table if exists testTableRowLocks");
        sqlUpdate("create table testTableRowLocks (_id varchar(32), a varchar(20))");
        sqlUpdate("CREATE UNIQUE INDEX myIndex ON testTableRowLocks (_id)"); // index is required to enable row locking
        sqlUpdate("insert into testTableRowLocks values ('1', '1')");
        sqlUpdate("insert into testTableRowLocks values ('2', '1')");
        sqlUpdate("insert into testTableRowLocks values ('3', '1')");

        Session session1 = null;
        Session session2 = null;

        try {
            session1 = new SessionFactory().getSession(this.testProperties);
            Table table1 = session1.getDefaultSchema().getTable("testTableRowLocks");
            session2 = new SessionFactory().getSession(this.testProperties);
            Table table2 = session2.getDefaultSchema().getTable("testTableRowLocks");

            // test1: Shared Lock
            session1.startTransaction();
            table1.select("_id").where("_id = '1'").lockShared().execute();

            session2.startTransaction();
            table2.select("_id").where("_id = '2'").lockShared().execute(); // should return immediately

            CompletableFuture<RowResult> res1 = table2.select("_id").where("_id = '1'").lockShared().executeAsync(); // should return immediately
            res1.get(5, TimeUnit.SECONDS);
            assertTrue(res1.isDone());

            session1.rollback();
            session2.rollback();

            // test2: Shared Lock after Exclusive
            session1.startTransaction();
            table1.select("_id").where("_id = '1'").lockExclusive().execute();

            session2.startTransaction();
            table2.select("_id").where("_id = '2'").lockShared().execute(); // should return immediately
            CompletableFuture<RowResult> res2 = table2.select("_id").where("_id = '1'").lockShared().executeAsync(); // session2 blocks
            assertThrows(TimeoutException.class, new Callable<Void>() {
                public Void call() throws Exception {
                    res2.get(5, TimeUnit.SECONDS);
                    return null;
                }
            });

            session1.rollback(); // session2 should unblock now
            res2.get(5, TimeUnit.SECONDS);
            assertTrue(res2.isDone());
            session2.rollback();

            // test3: Exclusive after Shared
            session1.startTransaction();
            table1.select("_id").where("_id = '1'").lockShared().execute();
            table1.select("_id").where("_id = '3'").lockShared().execute();

            session2.startTransaction();
            table2.select("_id").where("_id = '2'").lockExclusive().execute(); // should return immediately
            table2.select("_id").where("_id = '3'").lockShared().execute(); // should return immediately
            CompletableFuture<RowResult> res3 = table2.select("_id").where("_id = '1'").lockExclusive().executeAsync(); // session2 blocks
            assertThrows(TimeoutException.class, new Callable<Void>() {
                public Void call() throws Exception {
                    res3.get(5, TimeUnit.SECONDS);
                    return null;
                }
            });

            session1.rollback(); // session2 should unblock now
            res3.get(5, TimeUnit.SECONDS);
            assertTrue(res3.isDone());
            session2.rollback();

            // test4: Exclusive after Exclusive
            session1.startTransaction();
            table1.select("_id").where("_id = '1'").lockExclusive().execute();

            session2.startTransaction();
            table2.select("_id").where("_id = '2'").lockExclusive().execute(); // should return immediately
            CompletableFuture<RowResult> res4 = table2.select("_id").where("_id = '1'").lockExclusive().executeAsync(); // session2 blocks
            assertThrows(TimeoutException.class, new Callable<Void>() {
                public Void call() throws Exception {
                    res4.get(5, TimeUnit.SECONDS);
                    return null;
                }
            });

            session1.rollback(); // session2 should unblock now
            res4.get(5, TimeUnit.SECONDS);
            assertTrue(res4.isDone());
            session2.rollback();

        } finally {
            if (session1 != null) {
                session1.close();
            }
            if (session2 != null) {
                session2.close();
            }
            sqlUpdate("drop table if exists testTableRowLocks");
        }

    }

    @Test
    public void testTableRowLockOptions() throws Exception {
        if (!this.isSetForXTests || !mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            return;
        }

        Function<RowResult, List<String>> asStringList = rr -> rr.fetchAll().stream().map(r -> r.getString(0)).collect(Collectors.toList());

        sqlUpdate("DROP TABLE IF EXISTS testTableRowLockOptions");
        sqlUpdate("CREATE TABLE testTableRowLockOptions (_id VARCHAR(32), a VARCHAR(20))");
        sqlUpdate("CREATE UNIQUE INDEX myIndex ON testTableRowLockOptions (_id)"); // index is required to enable row locking
        sqlUpdate("INSERT INTO testTableRowLockOptions VALUES ('1', '1'), ('2', '1'), ('3', '1')");

        Session session1 = null;
        Session session2 = null;

        try {
            session1 = new SessionFactory().getSession(this.testProperties);
            Table table1 = session1.getDefaultSchema().getTable("testTableRowLockOptions");
            session2 = new SessionFactory().getSession(this.testProperties);
            Table table2 = session2.getDefaultSchema().getTable("testTableRowLockOptions");
            RowResult res;
            CompletableFuture<RowResult> futRes;

            /*
             * 1. Shared Lock in both sessions.
             */

            // session2.lockShared() returns data immediately.
            session1.startTransaction();
            table1.select("_id").where("_id = '1'").lockShared().execute();

            session2.startTransaction();
            res = table2.select("_id").where("_id < '3'").lockShared().execute();
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), hasItems("1", "2"));
            session2.rollback();

            session2.startTransaction();
            futRes = table2.select("_id").where("_id < '3'").lockShared().executeAsync();
            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), hasItems("1", "2"));
            session2.rollback();

            session1.rollback();

            // session2.lockShared(NOWAIT) returns data immediately.
            session1.startTransaction();
            table1.select("_id").where("_id = '1'").lockShared().execute();

            session2.startTransaction();
            res = table2.select("_id").where("_id < '3'").lockShared(Statement.LockContention.NOWAIT).execute();
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), hasItems("1", "2"));
            session2.rollback();

            session2.startTransaction();
            futRes = table2.select("_id").where("_id < '3'").lockShared(Statement.LockContention.NOWAIT).executeAsync();
            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), hasItems("1", "2"));
            session2.rollback();

            session1.rollback();

            // session2.lockShared(SKIP_LOCK) returns data immediately.
            session1.startTransaction();
            table1.select("_id").where("_id = '1'").lockShared().execute();

            session2.startTransaction();
            res = table2.select("_id").where("_id < '3'").lockShared(Statement.LockContention.SKIP_LOCKED).execute();
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), hasItems("1", "2"));
            session2.rollback();

            session2.startTransaction();
            futRes = table2.select("_id").where("_id < '3'").lockShared(Statement.LockContention.SKIP_LOCKED).executeAsync();
            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), hasItems("1", "2"));
            session2.rollback();

            session1.rollback();

            /*
             * 2. Shared Lock in first session and exclusive lock in second.
             */

            // session2.lockExclusive() blocks until session1 ends.
            session1.startTransaction();
            table1.select("_id").where("_id = '1'").lockShared().execute();

            // session2.startTransaction();
            // res = table2.select("_id").where("_id < '3'").lockExclusive().execute(); (Can't test)
            // session2.rollback();

            session2.startTransaction();
            futRes = table2.select("_id").where("_id < '3'").lockExclusive().executeAsync();
            final CompletableFuture<RowResult> fr1 = futRes;
            assertThrows(TimeoutException.class, () -> fr1.get(3, TimeUnit.SECONDS));

            session1.rollback(); // Unlocks session2.

            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), hasItems("1", "2"));
            session2.rollback();

            // session2.lockExclusive(NOWAIT) should return locking error.
            session1.startTransaction();
            table1.select("_id").where("_id = '1'").lockShared().execute();

            session2.startTransaction();
            assertThrows(XProtocolError.class,
                    "ERROR 3572 \\(HY000\\) Statement aborted because lock\\(s\\) could not be acquired immediately and NOWAIT is set\\.",
                    () -> table2.select("_id").where("_id < '3'").lockExclusive(Statement.LockContention.NOWAIT).execute());
            session2.rollback();

            session2.startTransaction();
            futRes = table2.select("_id").where("_id < '3'").lockExclusive(Statement.LockContention.NOWAIT).executeAsync();
            final CompletableFuture<RowResult> fr2 = futRes;
            assertThrows(ExecutionException.class,
                    ".*XProtocolError: ERROR 3572 \\(HY000\\) Statement aborted because lock\\(s\\) could not be acquired immediately and NOWAIT is set\\.",
                    () -> fr2.get(3, TimeUnit.SECONDS));
            session2.rollback();

            session1.rollback();

            // session2.lockExclusive(SKIP_LOCK) should return (unlocked) data immediately.
            session1.startTransaction();
            table1.select("_id").where("_id = '1'").lockShared().execute();

            session2.startTransaction();
            res = table2.select("_id").where("_id < '3'").lockExclusive(Statement.LockContention.SKIP_LOCKED).execute();
            assertEquals(1, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), hasItems("2"));
            session2.rollback();

            session2.startTransaction();
            futRes = table2.select("_id").where("_id < '3'").lockExclusive(Statement.LockContention.SKIP_LOCKED).executeAsync();
            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(1, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), hasItems("2"));
            session2.rollback();

            session1.rollback();

            /*
             * 3. Exclusive Lock in first session and shared lock in second.
             */

            // session2.lockShared() blocks until session1 ends.
            session1.startTransaction();
            table1.select("_id").where("_id = '1'").lockExclusive().execute();

            // session2.startTransaction();
            // res = table2.select("_id").where("_id < '3'").lockShared().execute(); (Can't test)
            // session2.rollback();

            session2.startTransaction();
            futRes = table2.select("_id").where("_id < '3'").lockShared().executeAsync();
            final CompletableFuture<RowResult> fr3 = futRes;
            assertThrows(TimeoutException.class, () -> fr3.get(3, TimeUnit.SECONDS));

            session1.rollback(); // Unlocks session2.

            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), hasItems("1", "2"));
            session2.rollback();

            // session2.lockShared(NOWAIT) should return locking error.
            session1.startTransaction();
            table1.select("_id").where("_id = '1'").lockExclusive().execute();

            session2.startTransaction();
            assertThrows(XProtocolError.class,
                    "ERROR 3572 \\(HY000\\) Statement aborted because lock\\(s\\) could not be acquired immediately and NOWAIT is set\\.",
                    () -> table2.select("_id").where("_id < '3'").lockShared(Statement.LockContention.NOWAIT).execute());
            session2.rollback();

            session2.startTransaction();
            futRes = table2.select("_id").where("_id < '3'").lockShared(Statement.LockContention.NOWAIT).executeAsync();
            final CompletableFuture<RowResult> fr4 = futRes;
            assertThrows(ExecutionException.class,
                    ".*XProtocolError: ERROR 3572 \\(HY000\\) Statement aborted because lock\\(s\\) could not be acquired immediately and NOWAIT is set\\.",
                    () -> fr4.get(3, TimeUnit.SECONDS));
            session2.rollback();

            session1.rollback();

            // session2.lockShared(SKIP_LOCK) should return (unlocked) data immediately.
            session1.startTransaction();
            table1.select("_id").where("_id = '1'").lockExclusive().execute();

            session2.startTransaction();
            res = table2.select("_id").where("_id < '3'").lockShared(Statement.LockContention.SKIP_LOCKED).execute();
            assertEquals(1, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), hasItems("2"));
            session2.rollback();

            session2.startTransaction();
            futRes = table2.select("_id").where("_id < '3'").lockShared(Statement.LockContention.SKIP_LOCKED).executeAsync();
            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(1, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), hasItems("2"));
            session2.rollback();

            session1.rollback();

            /*
             * 4. Exclusive Lock in both sessions.
             */

            // session2.lockExclusive() blocks until session1 ends.
            session1.startTransaction();
            table1.select("_id").where("_id = '1'").lockExclusive().execute();

            // session2.startTransaction();
            // res = table2.select("_id").where("_id < '3'").lockExclusive().execute(); (Can't test)
            // session2.rollback();

            session2.startTransaction();
            futRes = table2.select("_id").where("_id < '3'").lockExclusive().executeAsync();
            final CompletableFuture<RowResult> fr5 = futRes;
            assertThrows(TimeoutException.class, () -> fr5.get(3, TimeUnit.SECONDS));

            session1.rollback(); // Unlocks session2.

            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), hasItems("1", "2"));
            session2.rollback();

            // session2.lockExclusive(NOWAIT) should return locking error.
            session1.startTransaction();
            table1.select("_id").where("_id = '1'").lockExclusive().execute();

            session2.startTransaction();
            assertThrows(XProtocolError.class,
                    "ERROR 3572 \\(HY000\\) Statement aborted because lock\\(s\\) could not be acquired immediately and NOWAIT is set\\.",
                    () -> table2.select("_id").where("_id < '3'").lockExclusive(Statement.LockContention.NOWAIT).execute());
            session2.rollback();

            session2.startTransaction();
            futRes = table2.select("_id").where("_id < '3'").lockExclusive(Statement.LockContention.NOWAIT).executeAsync();
            final CompletableFuture<RowResult> fr6 = futRes;
            assertThrows(ExecutionException.class,
                    ".*XProtocolError: ERROR 3572 \\(HY000\\) Statement aborted because lock\\(s\\) could not be acquired immediately and NOWAIT is set\\.",
                    () -> fr6.get(3, TimeUnit.SECONDS));
            session2.rollback();

            session1.rollback();

            // session2.lockExclusive(SKIP_LOCK) should return (unlocked) data immediately.
            session1.startTransaction();
            table1.select("_id").where("_id = '1'").lockExclusive().execute();

            session2.startTransaction();
            res = table2.select("_id").where("_id < '3'").lockExclusive(Statement.LockContention.SKIP_LOCKED).execute();
            assertEquals(1, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), hasItems("2"));
            session2.rollback();

            session2.startTransaction();
            futRes = table2.select("_id").where("_id < '3'").lockExclusive(Statement.LockContention.SKIP_LOCKED).executeAsync();
            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(1, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), hasItems("2"));
            session2.rollback();

            session1.rollback();
        } finally {
            if (session1 != null) {
                session1.close();
            }
            if (session2 != null) {
                session2.close();
            }
            sqlUpdate("DROP TABLE IF EXISTS testTableRowLockOptions");
        }
    }

    /**
     * Tests fix for Bug#22038729, X DEVAPI: ANY API CALL AFTER A FAILED CALL PROC() RESULTS IN HANG
     * and for duplicate Bug#25575010, X DEVAPI: ANY API CALL AFTER A FAILED SELECT RESULTS IN HANG
     */
    @Test
    public void testBug22038729() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        final Field pf = CoreSession.class.getDeclaredField("protocol");
        pf.setAccessible(true);

        try {
            sqlUpdate("drop table if exists testBug22038729");
            sqlUpdate("create table testBug22038729 (c1 int, c2 int unsigned, id bigint)");
            sqlUpdate("insert into testBug22038729 values(10, 100, -9223372036854775808)");
            sqlUpdate("insert into testBug22038729 values(11, 11, 9223372036854775806)");

            sqlUpdate("drop procedure if exists testBug22038729p");
            sqlUpdate("create procedure testBug22038729p (in p1 int,IN p2 char(20)) begin select -10;select id+1000 from testBug22038729; end;");

            // XProtocol.readRowOrNull()
            Session sess = new SessionFactory().getSession(this.testProperties);
            Table t1 = sess.getDefaultSchema().getTable("testBug22038729");
            RowResult rows = t1.select("c1-c2").orderBy("c1 DESC").execute();
            assertTrue(rows.hasNext());
            Row r = rows.next();
            assertEquals(0, r.getInt(0));
            assertThrows(XProtocolError.class, "ERROR 1690 \\(22003\\) BIGINT UNSIGNED value is out of range .*", () -> rows.hasNext());
            sess.close(); // It was hanging

            // XProtocol.readRowOrNull()
            sess = new SessionFactory().getSession(this.testProperties);
            SqlResult rs1 = sess.sql("select c1-c2 from testBug22038729 order by c1 desc").execute();
            assertEquals(0, rs1.fetchOne().getInt(0));
            assertThrows(XProtocolError.class, "ERROR 1690 \\(22003\\) BIGINT UNSIGNED value is out of range .*", () -> rs1.fetchOne());
            sess.close(); // It was hanging

            // XProtocol.drainRows()
            sess = new SessionFactory().getSession(this.testProperties);
            sess.sql("select c1-c2 from testBug22038729 order by c1 desc").execute();
            XProtocol xp = (XProtocol) pf.get(((SessionImpl) sess).getSession());
            assertThrows(XProtocolError.class, "ERROR 1690 \\(22003\\) BIGINT UNSIGNED value is out of range .*", () -> {
                xp.drainRows();
                return xp;
            });
            sess.close(); // It was hanging

            sess = new SessionFactory().getSession(this.testProperties);
            SqlResult rs2 = sess.sql("call testBug22038729p(?, ?)").bind(10).bind("X").execute();
            assertTrue(rs2.hasData());
            assertTrue(rs2.hasNext());
            r = rs2.next();
            assertEquals(-10, r.getInt(0));
            assertFalse(rs2.hasNext());
            assertTrue(rs2.nextResult());
            assertTrue(rs2.hasData());
            assertTrue(rs2.hasNext());
            r = rs2.next();
            assertEquals(-9223372036854774808L, r.getLong(0));
            assertThrows(XProtocolError.class, "ERROR 1690 \\(22003\\) BIGINT value is out of range .*", () -> rs2.hasNext());
            sess.close(); // It was hanging

        } finally {
            sqlUpdate("drop table if exists testBug22038729");
            sqlUpdate("drop procedure if exists testBug22038729p");
        }
    }

    @Test
    public void testPreparedStatements() {
        if (!this.isSetForXTests || !mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
            return;
        }

        try {
            // Prepare test data.
            sqlUpdate("DROP TABLE IF EXISTS testPrepareSelect");
            sqlUpdate("CREATE TABLE testPrepareSelect (id INT PRIMARY KEY, ord INT)");
            sqlUpdate("INSERT INTO testPrepareSelect VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8)");

            SessionFactory sf = new SessionFactory();
            /*
             * Test common usage.
             */
            Session testSession = sf.getSession(this.testProperties);

            int sessionThreadId = getThreadId(testSession);
            assertPreparedStatementsCount(sessionThreadId, 0, 1);
            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);

            Table testTbl = testSession.getDefaultSchema().getTable("testPrepareSelect");

            // Initialize several SelectStatement objects.
            SelectStatement testSelect1 = testTbl.select("ord"); // Select all.
            SelectStatement testSelect2 = testTbl.select("ord").where("ord >= :n"); // Criteria with one placeholder.
            SelectStatement testSelect3 = testTbl.select("ord").where("ord >= :n AND ord <= :n + 3"); // Criteria with same placeholder repeated.
            SelectStatement testSelect4 = testTbl.select("ord").where("ord >= :n AND ord <= :m"); // Criteria with multiple placeholders.

            assertPreparedStatementsCountsAndId(testSession, 0, testSelect1, 0, -1);
            assertPreparedStatementsCountsAndId(testSession, 0, testSelect2, 0, -1);
            assertPreparedStatementsCountsAndId(testSession, 0, testSelect3, 0, -1);
            assertPreparedStatementsCountsAndId(testSession, 0, testSelect4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);

            // A. Set binds: 1st execute -> non-prepared.
            assertTestPreparedStatementsResult(testSelect1.execute(), 1, 8);
            assertPreparedStatementsCountsAndId(testSession, 0, testSelect1, 0, -1);
            assertTestPreparedStatementsResult(testSelect2.bind("n", 2).execute(), 2, 8);
            assertPreparedStatementsCountsAndId(testSession, 0, testSelect2, 0, -1);
            assertTestPreparedStatementsResult(testSelect3.bind("n", 2).execute(), 2, 5);
            assertPreparedStatementsCountsAndId(testSession, 0, testSelect3, 0, -1);
            assertTestPreparedStatementsResult(testSelect4.bind("n", 2).bind("m", 5).execute(), 2, 5);
            assertPreparedStatementsCountsAndId(testSession, 0, testSelect4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);

            // B. Set orderBy resets execution count: 1st execute -> non-prepared.
            assertTestPreparedStatementsResult(testSelect1.orderBy("id").execute(), 1, 8);
            assertPreparedStatementsCountsAndId(testSession, 0, testSelect1, 0, -1);
            assertTestPreparedStatementsResult(testSelect2.orderBy("id").execute(), 2, 8);
            assertPreparedStatementsCountsAndId(testSession, 0, testSelect2, 0, -1);
            assertTestPreparedStatementsResult(testSelect3.orderBy("id").execute(), 2, 5);
            assertPreparedStatementsCountsAndId(testSession, 0, testSelect3, 0, -1);
            assertTestPreparedStatementsResult(testSelect4.orderBy("id").execute(), 2, 5);
            assertPreparedStatementsCountsAndId(testSession, 0, testSelect4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);

            // C. Set binds reuse statement: 2nd execute -> prepare + execute.
            assertTestPreparedStatementsResult(testSelect1.execute(), 1, 8);
            assertPreparedStatementsCountsAndId(testSession, 1, testSelect1, 1, 1);
            assertTestPreparedStatementsResult(testSelect2.bind("n", 3).execute(), 3, 8);
            assertPreparedStatementsCountsAndId(testSession, 2, testSelect2, 2, 1);
            assertTestPreparedStatementsResult(testSelect3.bind("n", 3).execute(), 3, 6);
            assertPreparedStatementsCountsAndId(testSession, 3, testSelect3, 3, 1);
            assertTestPreparedStatementsResult(testSelect4.bind("m", 6).execute(), 2, 6);
            assertPreparedStatementsCountsAndId(testSession, 4, testSelect4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 4, 4, 0);

            // D. Set binds reuse statement: 3rd execute -> execute.
            assertTestPreparedStatementsResult(testSelect1.execute(), 1, 8);
            assertPreparedStatementsCountsAndId(testSession, 4, testSelect1, 1, 2);
            assertTestPreparedStatementsResult(testSelect2.bind("n", 4).execute(), 4, 8);
            assertPreparedStatementsCountsAndId(testSession, 4, testSelect2, 2, 2);
            assertTestPreparedStatementsResult(testSelect3.bind("n", 4).execute(), 4, 7);
            assertPreparedStatementsCountsAndId(testSession, 4, testSelect3, 3, 2);
            assertTestPreparedStatementsResult(testSelect4.bind("n", 3).bind("m", 7).execute(), 3, 7);
            assertPreparedStatementsCountsAndId(testSession, 4, testSelect4, 4, 2);

            assertPreparedStatementsStatusCounts(testSession, 4, 8, 0);

            // E. Set where deallocates and resets execution count: 1st execute -> deallocate + non-prepared.
            assertTestPreparedStatementsResult(testSelect1.where("true").execute(), 1, 8);
            assertPreparedStatementsCountsAndId(testSession, 3, testSelect1, 0, -1);
            assertTestPreparedStatementsResult(testSelect2.where("true AND ord >= :n").bind("n", 4).execute(), 4, 8);
            assertPreparedStatementsCountsAndId(testSession, 2, testSelect2, 0, -1);
            assertTestPreparedStatementsResult(testSelect3.where("true AND ord >= :n AND ord <= :n + 3").bind("n", 4).execute(), 4, 7);
            assertPreparedStatementsCountsAndId(testSession, 1, testSelect3, 0, -1);
            assertTestPreparedStatementsResult(testSelect4.where("true AND ord >= :n AND ord <= :m").bind("n", 3).bind("m", 7).execute(), 3, 7);
            assertPreparedStatementsCountsAndId(testSession, 0, testSelect4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 4, 8, 4);

            // F. No Changes: 2nd execute -> prepare + execute.
            assertTestPreparedStatementsResult(testSelect1.execute(), 1, 8);
            assertPreparedStatementsCountsAndId(testSession, 1, testSelect1, 1, 1);
            assertTestPreparedStatementsResult(testSelect2.bind("n", 4).execute(), 4, 8);
            assertPreparedStatementsCountsAndId(testSession, 2, testSelect2, 2, 1);
            assertTestPreparedStatementsResult(testSelect3.bind("n", 4).execute(), 4, 7);
            assertPreparedStatementsCountsAndId(testSession, 3, testSelect3, 3, 1);
            assertTestPreparedStatementsResult(testSelect4.bind("n", 3).bind("m", 7).execute(), 3, 7);
            assertPreparedStatementsCountsAndId(testSession, 4, testSelect4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 8, 12, 4);

            // G. Set limit for the first time deallocates and re-prepares: 1st execute -> re-prepare + execute.
            assertTestPreparedStatementsResult(testSelect1.limit(2).execute(), 1, 2);
            assertPreparedStatementsCountsAndId(testSession, 4, testSelect1, 1, 1);
            assertTestPreparedStatementsResult(testSelect2.limit(2).execute(), 4, 5);
            assertPreparedStatementsCountsAndId(testSession, 4, testSelect2, 2, 1);
            assertTestPreparedStatementsResult(testSelect3.limit(2).execute(), 4, 5);
            assertPreparedStatementsCountsAndId(testSession, 4, testSelect3, 3, 1);
            assertTestPreparedStatementsResult(testSelect4.limit(2).execute(), 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testSelect4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 12, 16, 8);

            // H. Set limit and offset reuse prepared statement: 2nd execute -> execute.
            assertTestPreparedStatementsResult(testSelect1.limit(1).offset(1).execute(), 2, 2);
            assertPreparedStatementsCountsAndId(testSession, 4, testSelect1, 1, 2);
            assertTestPreparedStatementsResult(testSelect2.limit(1).offset(1).execute(), 5, 5);
            assertPreparedStatementsCountsAndId(testSession, 4, testSelect2, 2, 2);
            assertTestPreparedStatementsResult(testSelect3.limit(1).offset(1).execute(), 5, 5);
            assertPreparedStatementsCountsAndId(testSession, 4, testSelect3, 3, 2);
            assertTestPreparedStatementsResult(testSelect4.limit(1).offset(1).execute(), 4, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testSelect4, 4, 2);

            assertPreparedStatementsStatusCounts(testSession, 12, 20, 8);

            // I. Set orderBy deallocates and resets execution count, set limit and bind has no effect: 1st execute -> deallocate + non-prepared.
            assertTestPreparedStatementsResult(testSelect1.orderBy("id").limit(2).execute(), 2, 3);
            assertPreparedStatementsCountsAndId(testSession, 3, testSelect1, 0, -1);
            assertTestPreparedStatementsResult(testSelect2.orderBy("id").limit(2).bind("n", 4).execute(), 5, 6);
            assertPreparedStatementsCountsAndId(testSession, 2, testSelect2, 0, -1);
            assertTestPreparedStatementsResult(testSelect3.orderBy("id").limit(2).bind("n", 4).execute(), 5, 6);
            assertPreparedStatementsCountsAndId(testSession, 1, testSelect3, 0, -1);
            assertTestPreparedStatementsResult(testSelect4.orderBy("id").limit(2).bind("m", 7).execute(), 4, 5);
            assertPreparedStatementsCountsAndId(testSession, 0, testSelect4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 12, 20, 12);

            // J. Set offset reuse statement: 2nd execute -> prepare + execute.
            assertTestPreparedStatementsResult(testSelect1.offset(0).execute(), 1, 2);
            assertPreparedStatementsCountsAndId(testSession, 1, testSelect1, 1, 1);
            assertTestPreparedStatementsResult(testSelect2.offset(0).execute(), 4, 5);
            assertPreparedStatementsCountsAndId(testSession, 2, testSelect2, 2, 1);
            assertTestPreparedStatementsResult(testSelect3.offset(0).execute(), 4, 5);
            assertPreparedStatementsCountsAndId(testSession, 3, testSelect3, 3, 1);
            assertTestPreparedStatementsResult(testSelect4.offset(0).execute(), 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testSelect4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 16, 24, 12);

            testSession.close();
            assertPreparedStatementsCount(sessionThreadId, 0, 10); // Prepared statements won't live past the closing of the session.

            /*
             * Test falling back onto non-prepared statements.
             */
            testSession = sf.getSession(this.testProperties);
            int origMaxPrepStmtCount = this.session.sql("SELECT @@max_prepared_stmt_count").execute().fetchOne().getInt(0);

            try {
                // Allow preparing only one more statement.
                this.session.sql("SET GLOBAL max_prepared_stmt_count = ?").bind(getPreparedStatementsCount() + 1).execute();

                sessionThreadId = getThreadId(testSession);
                assertPreparedStatementsCount(sessionThreadId, 0, 1);
                assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);

                testTbl = testSession.getDefaultSchema().getTable("testPrepareSelect");

                testSelect1 = testTbl.select("ord");
                testSelect2 = testTbl.select("ord");

                // 1st execute -> don't prepare.
                assertTestPreparedStatementsResult(testSelect1.execute(), 1, 8);
                assertPreparedStatementsCountsAndId(testSession, 0, testSelect1, 0, -1);
                assertTestPreparedStatementsResult(testSelect2.execute(), 1, 8);
                assertPreparedStatementsCountsAndId(testSession, 0, testSelect2, 0, -1);

                assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);

                // 2nd execute -> prepare + execute.
                assertTestPreparedStatementsResult(testSelect1.execute(), 1, 8);
                assertPreparedStatementsCountsAndId(testSession, 1, testSelect1, 1, 1);
                assertTestPreparedStatementsResult(testSelect2.execute(), 1, 8); // Fails preparing, execute as non-prepared.
                assertPreparedStatementsCountsAndId(testSession, 1, testSelect2, 0, -1);

                assertPreparedStatementsStatusCounts(testSession, 2, 1, 0); // Failed prepare also counts.

                // 3rd execute -> execute.
                assertTestPreparedStatementsResult(testSelect1.execute(), 1, 8);
                assertPreparedStatementsCountsAndId(testSession, 1, testSelect1, 1, 2);
                assertTestPreparedStatementsResult(testSelect2.execute(), 1, 8); // Execute as non-prepared.
                assertPreparedStatementsCountsAndId(testSession, 1, testSelect2, 0, -1);

                assertPreparedStatementsStatusCounts(testSession, 2, 2, 0);

                testSession.close();
                assertPreparedStatementsCount(sessionThreadId, 0, 10); // Prepared statements won't live past the closing of the session.
            } finally {
                this.session.sql("SET GLOBAL max_prepared_stmt_count = ?").bind(origMaxPrepStmtCount).execute();
            }
        } finally {
            sqlUpdate("DROP TABLE IF EXISTS testPrepareSelect");
        }
    }

    private void assertTestPreparedStatementsResult(RowResult res, int expectedMin, int expectedMax) {
        for (Row r : res.fetchAll()) {
            assertEquals(expectedMin++, r.getInt("ord"));
        }
        assertEquals(expectedMax, expectedMin - 1);
    }
}
