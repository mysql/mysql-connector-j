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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.x.Row;
import com.mysql.cj.api.x.RowResult;
import com.mysql.cj.api.x.SelectStatement;
import com.mysql.cj.api.x.Table;

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
    }

    @Test
    public void testComplexQuery() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        sqlUpdate("drop table if exists complexQuery");
        sqlUpdate("create table complexQuery (name varchar(32), age int)");
        sqlUpdate("insert into complexQuery values ('Mamie', 11)");
        sqlUpdate("insert into complexQuery values ('Eulalia', 11)");
        sqlUpdate("insert into complexQuery values ('Polly', 12)");
        sqlUpdate("insert into complexQuery values ('Rufus', 12)");
        sqlUpdate("insert into complexQuery values ('Cassidy', 13)");
        sqlUpdate("insert into complexQuery values ('Olympia', 14)");
        sqlUpdate("insert into complexQuery values ('Lev', 14)");
        sqlUpdate("insert into complexQuery values ('Tierney', 15)");
        sqlUpdate("insert into complexQuery values ('Octavia', 15)");
        sqlUpdate("insert into complexQuery values ('Vesper', 16)");
        sqlUpdate("insert into complexQuery values ('Caspian', 17)");
        sqlUpdate("insert into complexQuery values ('Romy', 17)");
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
        SelectStatement stmt = table.select("age as age_group, count(name) as cnt");
        stmt.where("age > 11 and 1 < 2 and 40 between 30 and 900");
        stmt.groupBy("age_group");
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
}
