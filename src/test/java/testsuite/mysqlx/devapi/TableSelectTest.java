/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package testsuite.mysqlx.devapi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.x.Column;
import com.mysql.cj.api.x.Row;
import com.mysql.cj.api.x.RowResult;
import com.mysql.cj.api.x.SelectStatement;
import com.mysql.cj.api.x.Table;
import com.mysql.cj.core.exceptions.DataConversionException;

/**
 * @todo
 */
public class TableSelectTest extends TableTest {
    @Before
    @Override
    public void setupTableTest() {
        super.setupTableTest();
    }

    @After
    @Override
    public void teardownTableTest() {
        super.teardownTableTest();
    }

    @Test
    public void basicQuery() {
        if (!this.isSetForMySQLxTests) {
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
        if (!this.isSetForMySQLxTests) {
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
        if (!this.isSetForMySQLxTests) {
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
        if (!this.isSetForMySQLxTests) {
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
        if (!this.isSetForMySQLxTests) {
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

        assertThrows(DataConversionException.class, "Unsupported conversion from BIT to com.mysql.cj.x.json.DbDoc", new Callable<Void>() {
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
        if (!this.isSetForMySQLxTests) {
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
        if (!this.isSetForMySQLxTests) {
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
        if (!this.isSetForMySQLxTests) {
            return;
        }
        sqlUpdate("drop table if exists testBug22988922");
        sqlUpdate("create table testBug22988922 (g point,l longblob,t longtext)");

        Table table = this.schema.getTable("testBug22988922");

        RowResult rows = table.select("*").execute();
        List<Column> metadata = rows.getColumns();
        assertEquals(4294967295L, metadata.get(0).getLength());
        assertEquals(4294967295L, metadata.get(1).getLength());
        assertEquals(4294967295L, metadata.get(2).getLength());

        sqlUpdate("drop table if exists testBug22988922");
    }
}
