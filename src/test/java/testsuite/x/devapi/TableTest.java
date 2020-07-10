/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.Test;

import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.DatabaseObject.DbObjectStatus;
import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.Table;

/**
 * @todo
 */
public class TableTest extends BaseTableTestCase {
    @Test
    public void tableBasics() {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop table if exists tableBasics");
        Table table = this.schema.getTable("tableBasics");
        assertEquals(DbObjectStatus.NOT_EXISTS, table.existsInDatabase());
        sqlUpdate("create table tableBasics (name varchar(32), age int)");
        assertEquals(DbObjectStatus.EXISTS, table.existsInDatabase());
        assertEquals("Table(" + getTestDatabase() + ".tableBasics)", table.toString());
        assertEquals(this.session, table.getSession());
        Table table2 = this.schema.getTable("tableBasics");
        assertFalse(table == table2);
        assertTrue(table.equals(table2));
    }

    @Test
    public void viewBasics() {
        if (!this.isSetForXTests) {
            return;
        }

        try {
            sqlUpdate("drop table if exists tableBasics");
            sqlUpdate("drop view if exists viewBasics");

            Table table = this.schema.getTable("tableBasics");
            assertEquals(DbObjectStatus.NOT_EXISTS, table.existsInDatabase());

            Table view = this.schema.getTable("viewBasics");
            assertEquals(DbObjectStatus.NOT_EXISTS, view.existsInDatabase());

            // all objects return false for isView() if they don't exist in database 
            assertFalse(table.isView());
            assertFalse(view.isView());

            sqlUpdate("create table tableBasics (name varchar(32), age int, role int)");
            sqlUpdate("create view viewBasics as select name, age from tableBasics");

            assertEquals(DbObjectStatus.EXISTS, table.existsInDatabase());
            assertEquals(DbObjectStatus.EXISTS, view.existsInDatabase());

            assertEquals("Table(" + getTestDatabase() + ".tableBasics)", table.toString());
            assertEquals("Table(" + getTestDatabase() + ".viewBasics)", view.toString());

            assertEquals(this.session, table.getSession());
            assertEquals(this.session, view.getSession());

            assertFalse(table.isView());
            assertTrue(view.isView());

            Table table2 = this.schema.getTable("tableBasics", true);
            assertFalse(table == table2);
            assertTrue(table.equals(table2));

            Table view2 = this.schema.getTable("viewBasics", true);
            assertFalse(view == view2);
            assertTrue(view.equals(view2));

            assertFalse(table2.isView());
            assertTrue(view2.isView());

        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            sqlUpdate("drop table if exists tableBasics");
            sqlUpdate("drop view if exists viewBasics");
        }
    }

    @Test
    public void testCount() {
        if (!this.isSetForXTests) {
            return;
        }
        try {
            sqlUpdate("drop table if exists testCount");
            sqlUpdate("create table testCount (_id varchar(32), name varchar(20), birthday date, age int)");
            sqlUpdate("insert into testCount values ('1', 'Sakila', '2000-05-27', 14)");
            sqlUpdate("insert into testCount values ('2', 'Shakila', '2001-06-26', 13)");

            Table table = this.schema.getTable("testCount");
            assertEquals(2, table.count());
            assertEquals(2, table.select("count(*)").execute().fetchOne().getInt(0));

        } finally {
            sqlUpdate("drop table if exists testCount");
        }

        // test "not exists" message
        String tableName = "testExists";
        dropCollection(tableName);
        Table t = this.schema.getTable(tableName);
        assertThrows(XProtocolError.class, "Table '" + tableName + "' does not exist in schema '" + this.schema.getName() + "'", new Callable<Void>() {
            public Void call() throws Exception {
                t.count();
                return null;
            }
        });
    }

    @Test
    public void testBug25650912() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        try {
            sqlUpdate("drop table if exists testBug25650912");
            sqlUpdate("create table testBug25650912 (x bigint,y char(220))");

            Table table = this.schema.getTable("testBug25650912", false);

            table.insert("x", "y").values(1, 'a').executeAsync().get();
            RowResult rows = table.select("x, y").execute();
            Row row = rows.next();
            assertEquals(1, row.getInt("x"));
            assertEquals("a", row.getString("y"));

            assertThrows(XProtocolError.class, "ERROR 1366 \\(HY000\\) Incorrect integer value: 's' for column 'x' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    table.update().set("x", 's').execute();
                    return null;
                }
            });

            table.update().set("x", (byte) 2).set("y", 's').execute();
            rows = table.select("x, y").execute();
            row = rows.next();
            assertEquals(2, row.getInt("x"));
            assertEquals("s", row.getString("y"));

            table.update().set("x", BigInteger.valueOf(3)).execute();
            rows = table.select("x").execute();
            row = rows.next();
            assertEquals(3, row.getInt("x"));

            table.update().set("x", BigDecimal.valueOf(4.123)).execute();
            rows = table.select("x").execute();
            row = rows.next();
            assertEquals(4, row.getInt("x"));

        } finally {
            sqlUpdate("drop table if exists testBug25650912");
        }
    }
}
