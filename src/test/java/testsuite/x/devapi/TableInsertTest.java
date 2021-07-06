/*
 * Copyright (c) 2015, 2021, Oracle and/or its affiliates.
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

import static com.mysql.cj.xdevapi.Expression.expr;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DbDocImpl;
import com.mysql.cj.xdevapi.InsertResult;
import com.mysql.cj.xdevapi.JsonParser;
import com.mysql.cj.xdevapi.JsonString;
import com.mysql.cj.xdevapi.Result;
import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.SqlResult;
import com.mysql.cj.xdevapi.Table;

public class TableInsertTest extends BaseTableTestCase {
    @Test
    public void lastInsertId() {
        try {
            sqlUpdate("drop table if exists lastInsertId");
            sqlUpdate("create table lastInsertId (id int not null primary key auto_increment, name varchar(20) not null)");
            Table table = this.schema.getTable("lastInsertId");
            InsertResult res = table.insert("name").values("a").values("b").values("c").execute();
            assertEquals(3, res.getAffectedItemsCount());
            // the *first* ID
            assertEquals(new Long(1), res.getAutoIncrementValue());
        } finally {
            sqlUpdate("drop table if exists lastInsertId");
        }
    }

    @Test
    public void basicInsert() {
        try {
            sqlUpdate("drop table if exists basicInsert");
            sqlUpdate("drop view if exists basicInsertView");
            sqlUpdate("create table basicInsert (_id varchar(32), name varchar(20) not null default 'unknown', birthday date, age int)");
            sqlUpdate("create view basicInsertView as select * from basicInsert");

            Table table = this.schema.getTable("basicInsert");
            // insert with fields and values separately
            table.insert("_id", "birthday", "age").values(1, "2015-01-01", 1).execute();
            // insert all fields with values
            table.insert().values(2, "Orlando", "2014-01-01", 2).execute();
            Map<String, Object> row = new HashMap<>();
            row.put("_id", 3);
            row.put("age", 3);
            // insert a row in k/v pair form
            table.insert(row).execute();

            Table view = this.schema.getTable("basicInsertView");
            // insert with fields and values separately
            view.insert("_id", "birthday", "age").values(4, "2015-01-01", 1).execute();
            // insert all fields with values
            view.insert().values(5, "Orlando", "2014-01-01", 2).execute();
            row = new HashMap<>();
            row.put("_id", 6);
            row.put("age", 3);
            // insert a row in k/v pair form
            view.insert(row).execute();

            RowResult rows = table.select("_id, name, birthday, age").orderBy("_id").execute();
            Row r = rows.next();
            assertEquals("1", r.getString("_id"));
            assertEquals("unknown", r.getString("name"));
            assertEquals("2015-01-01", r.getString("birthday"));
            assertEquals(1, r.getInt("age"));
            r = rows.next();
            assertEquals("2", r.getString("_id"));
            assertEquals("Orlando", r.getString("name"));
            assertEquals("2014-01-01", r.getString("birthday"));
            assertEquals(2, r.getInt("age"));
            r = rows.next();
            assertEquals("3", r.getString("_id"));
            assertEquals("unknown", r.getString("name"));
            assertEquals(null, r.getString("birthday"));
            assertEquals(3, r.getInt("age"));
            r = rows.next();
            assertEquals("4", r.getString("_id"));
            assertEquals("unknown", r.getString("name"));
            assertEquals("2015-01-01", r.getString("birthday"));
            assertEquals(1, r.getInt("age"));
            r = rows.next();
            assertEquals("5", r.getString("_id"));
            assertEquals("Orlando", r.getString("name"));
            assertEquals("2014-01-01", r.getString("birthday"));
            assertEquals(2, r.getInt("age"));
            r = rows.next();
            assertEquals("6", r.getString("_id"));
            assertEquals("unknown", r.getString("name"));
            assertEquals(null, r.getString("birthday"));
            assertEquals(3, r.getInt("age"));
            assertFalse(rows.hasNext());
        } finally {
            sqlUpdate("drop table if exists basicInsert");
            sqlUpdate("drop view if exists basicInsertView");
        }
    }

    @Test
    public void jsonInsert() throws IOException {
        try {
            sqlUpdate("drop table if exists jsonInsert");
            sqlUpdate("create table jsonInsert (_id varchar(32), doc JSON)");

            Table table = this.schema.getTable("jsonInsert");

            table.insert("_id", "doc").values(1, "{\"x\":\"1\"}").execute();
            table.insert().values(2, "{\"x\":\"2\"}").execute();

            Map<String, Object> row = new HashMap<>();
            row.put("_id", 3);
            row.put("doc", "{\"x\":\"3\"}");
            table.insert(row).execute();

            DbDoc doc = new DbDocImpl().add("firstName", new JsonString().setValue("Georgia"));
            doc.add("middleName", new JsonString().setValue("Totto"));
            doc.add("lastName", new JsonString().setValue("O'Keeffe"));
            table.insert("_id", "doc").values(4, doc).execute();

            RowResult rows = table.select("_id, doc").orderBy("_id").execute();
            Row r = rows.next();
            assertEquals("1", r.getString("_id"));
            assertEquals(JsonParser.parseDoc(new StringReader("{\"x\":\"1\"}")).toString(), r.getDbDoc("doc").toString());

            r = rows.next();
            assertEquals("2", r.getString("_id"));
            assertEquals("{\"x\": \"2\"}", r.getString("doc"));

            r = rows.next();
            assertEquals("3", r.getString("_id"));
            assertEquals("{\"x\": \"3\"}", r.getString("doc"));

            r = rows.next();
            assertEquals("4", r.getString("_id"));
            assertEquals(doc.toString(), r.getDbDoc("doc").toString());

        } finally {
            sqlUpdate("drop table if exists jsonInsert");
        }
    }

    @Test
    public void testGetAutoIncrementValueAsync() throws Exception {
        try {
            SqlResult res = this.session.sql("drop table if exists mytab").executeAsync().get();
            res = this.session.sql("create table mytab (x bigint auto_increment primary key,y int)").executeAsync().get();
            res = this.session.sql("drop table if exists mytabtmp").executeAsync().get();
            res = this.session.sql("create table mytabtmp (x bigint,y int)").executeAsync().get();
            res = this.session.sql("insert into mytabtmp values(NULL,8)").executeAsync().get();
            res = this.session.sql("insert into mytabtmp values(1111,9)").executeAsync().get();

            res = this.session.sql("ALTER TABLE mytab AUTO_INCREMENT = 111").executeAsync().get();
            res = this.session.sql("insert into mytab values(NULL,1)").executeAsync().get();
            assertEquals((long) 111, (long) res.getAutoIncrementValue());

            res = this.session.sql("insert into mytab values(-100,2)").executeAsync().get();
            assertEquals((long) -100, (long) res.getAutoIncrementValue());

            res = this.session.sql("insert into mytab (y)values(3)").executeAsync().get();
            assertEquals((long) 112, (long) res.getAutoIncrementValue());

            res = this.session.sql("insert into mytab values(NULL,4),(NULL,5),(887,6),(NULL,7)").executeAsync().get();
            assertEquals((long) 113, (long) res.getAutoIncrementValue());

            res = this.session.sql("insert into mytab select * from mytabtmp").executeAsync().get();
            assertEquals((long) 889, (long) res.getAutoIncrementValue());

            res = this.session.sql("insert into mytab (y) select (y+1) from mytabtmp").executeAsync().get();
            assertEquals((long) 1112, (long) res.getAutoIncrementValue());

            //Ignore duplicate
            res = this.session.sql("insert IGNORE  mytab select * from mytabtmp").executeAsync().get();
            assertEquals((long) 1115, (long) res.getAutoIncrementValue());

            // ON DUPLICATE KEY
            res = this.session.sql("insert into mytab values(-100,2) ON DUPLICATE KEY UPDATE Y=Y*-1").executeAsync().get();
            assertEquals((long) -100, (long) res.getAutoIncrementValue());

            // ON DUPLICATE KEY
            res = this.session.sql("insert into mytab values(-100,2) ON DUPLICATE KEY UPDATE X=X*-2").executeAsync().get();
            assertEquals((long) 200, (long) res.getAutoIncrementValue());

            //Replace
            res = this.session.sql("Replace into mytab (y)values(100000)").executeAsync().get();
            assertEquals((long) 1116, (long) res.getAutoIncrementValue());
        } finally {
            sqlUpdate("drop table if exists mytabtmp");
            sqlUpdate("drop table if exists mytab");
        }
    }

    @Test
    public void testExprInInsert() {
        try {
            sqlUpdate("drop table if exists qatablex");
            sqlUpdate("create table qatablex (x char(100),y bigint,z int)");

            Table table = this.schema.getTable("qatablex");
            Result res = table.insert("x", "y", "z").values(expr("concat('A','-1')"), expr("concat('1','100')"), 1).execute();
            assertEquals(1L, res.getAffectedItemsCount());

            res = table.insert("x", "y", "z").values("expr(\"concat('A','-1)\")", expr("length(concat('1','000'))+100"), 2).execute();
            assertEquals(1L, res.getAffectedItemsCount());

            res = table.insert("x", "y", "z").values(expr("STR_TO_DATE('1,1,2014','%d,%m,%Y')"), expr("length(concat('1','000'))+101"), 3).execute();
            assertEquals(1L, res.getAffectedItemsCount());

            res = table.insert("x", "y", "z").values("expr(\"concat('A','-2)\")", expr("length(concat('1','000'))+101"), 4)
                    .values("expr(\"concat('A','-3)\")", expr("length(concat('1','000'))+102"), 5).execute();
            assertEquals(2L, res.getAffectedItemsCount());

            res = table.insert("x", "y", "z").values(expr("concat('A',length('abcd'))"), expr("length(concat('1','000'))+103"), 3).execute();
            assertEquals(1L, res.getAffectedItemsCount());

            Map<String, Object> row = new HashMap<>();
            row.put("x", expr("concat('A','''\n-10\"')"));
            row.put("y", expr("concat('1','000')+103"));
            row.put("z", 6);
            res = table.insert(row).execute();
            assertEquals(1, res.getAffectedItemsCount());
            row.clear();
            row.put("x", "expr(\"concat('A','\n''-10\")\")");
            row.put("y", expr("concat('1','000')+104"));
            row.put("z", 7);
            res = table.insert(row).execute();
            assertEquals(1L, res.getAffectedItemsCount());
        } finally {
            sqlUpdate("drop table if exists qatablex");
        }
    }

    @Test
    public void testGetAutoIncrementValue() {
        Table table = null;
        InsertResult res = null;
        try {
            sqlUpdate("drop table if exists qatable1");
            sqlUpdate("drop table if exists qatable2");
            sqlUpdate("create table qatable1 (x bigint auto_increment primary key,y double)");
            sqlUpdate("create table qatable2 (x double auto_increment primary key,y bigint)");

            table = this.schema.getTable("qatable1", true);
            res = table.insert("y").values(101.1).execute();
            assertEquals(1L, (long) res.getAutoIncrementValue());
            assertEquals(1L, res.getAffectedItemsCount());

            res = table.insert("y").values(102.1).values(103.1).values(104.1).execute();
            assertEquals(2L, (long) res.getAutoIncrementValue());
            assertEquals(3L, res.getAffectedItemsCount());

            Map<String, Object> row = new HashMap<>();
            row.put("y", expr("concat('1','05.1')"));

            res = table.insert(row).execute();
            assertEquals(5L, (long) res.getAutoIncrementValue());
            assertEquals(1L, res.getAffectedItemsCount());

            res = table.insert("y").values(102.1).values(103.1).values(104.1).execute();
            assertEquals(6L, (long) res.getAutoIncrementValue());
            assertEquals(3L, res.getAffectedItemsCount());

            res = table.insert("y").values(102.1).values(103.1).values(104.1).execute();
            assertEquals(9L, (long) res.getAutoIncrementValue());
            assertEquals(3L, res.getAffectedItemsCount());

            this.session.sql("ALTER TABLE qatable1 AUTO_INCREMENT = 9223372036854775800").execute();
            res = table.insert("y").values(102.1).values(103.1).values(104.1).execute();
            assertEquals(9223372036854775800L, (long) res.getAutoIncrementValue());
            assertEquals(3L, res.getAffectedItemsCount());

            res = table.insert(row).execute();
            assertEquals(9223372036854775803L, (long) res.getAutoIncrementValue());
            assertEquals(1L, res.getAffectedItemsCount());

            table = this.schema.getTable("qatable2");
            res = table.insert("y").values(101.1).execute();
            assertEquals(1L, (long) res.getAutoIncrementValue());
            assertEquals(1L, res.getAffectedItemsCount());

            res = table.insert("y").values(102.1).values(103.1).values(104.1).execute();
            assertEquals(2L, (long) res.getAutoIncrementValue());
            assertEquals(3L, res.getAffectedItemsCount());

            row = new HashMap<>();
            row.put("y", expr("concat('1','05.1')"));

            res = table.insert(row).execute();
            assertEquals(5L, (long) res.getAutoIncrementValue());
            assertEquals(1L, res.getAffectedItemsCount());

            this.session.sql("ALTER TABLE qatable2 AUTO_INCREMENT = 4294967299000000").execute();
            res = table.insert("y").values(102.1).values(103.1).values(104.1).execute();
            assertEquals(4294967299000000L, (long) res.getAutoIncrementValue());
            assertEquals(3L, res.getAffectedItemsCount());

            this.session.sql("ALTER TABLE qatable2 AUTO_INCREMENT = 4294967299000000").execute();
            res = table.insert(row).execute();
            assertEquals(4294967299000003L, (long) res.getAutoIncrementValue());
            assertEquals(1L, res.getAffectedItemsCount());
        } finally {
            sqlUpdate("drop table if exists qatable1");
            sqlUpdate("drop table if exists qatable2");
        }
    }
}
