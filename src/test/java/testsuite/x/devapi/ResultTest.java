/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.exceptions.DataReadException;
import com.mysql.cj.util.TimeUtil;
import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.Table;
import com.mysql.cj.xdevapi.Warning;

public class ResultTest extends DevApiBaseTestCase {
    @Before
    public void setupTableTest() {
        super.setupTestSession();
    }

    @After
    public void teardownTableTest() {
        super.destroyTestSession();
    }

    @Test
    public void testForceBuffering() {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop table if exists testx");
        sqlUpdate("create table testx (x int)");
        sqlUpdate("insert into testx values (1), (2), (3)");
        Table table = this.schema.getTable("testx");
        RowResult rows = table.select("x/0 as bad_x").execute();
        // get warnings IMMEDIATELY
        assertEquals(3, rows.getWarningsCount());
        Iterator<Warning> warnings = rows.getWarnings();
        assertEquals(1365, warnings.next().getCode());
        assertEquals(1365, warnings.next().getCode());
        assertEquals(1365, warnings.next().getCode());
        Row r = rows.next();
        assertEquals(null, r.getString("bad_x"));
        r = rows.next();
        assertEquals(null, r.getString("bad_x"));
        r = rows.next();
        assertEquals(null, r.getString("bad_x"));
        try {
            rows.next();
            fail("should throw");
        } catch (NoSuchElementException ex) {
            // expected, end of results
        }
    }

    @Test
    public void testMars() {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop table if exists testx");
        sqlUpdate("create table testx (x int)");
        sqlUpdate("insert into testx values (1), (2), (3)");
        Table table = this.schema.getTable("testx");
        RowResult rows = table.select("x").orderBy("x").execute();
        int i = 1;
        while (rows.hasNext()) {
            assertEquals(String.valueOf(i++), rows.next().getString("x"));
            RowResult rows2 = table.select("x").orderBy("x").execute();
            assertEquals("1", rows2.next().getString("x"));
        }
    }

    @Test
    public void exceptionForNonExistingColumns() {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop table if exists testx");
        sqlUpdate("create table testx (x int)");
        sqlUpdate("insert into testx values (1), (2), (3)");
        Table table = this.schema.getTable("testx");
        RowResult rows = table.select("x").orderBy("x").execute();
        Row r = rows.next();
        r.getString("x");
        try {
            r.getString("non_existing");
        } catch (DataReadException ex) {
            assertTrue(ex.getMessage().contains("Invalid column"));
        }
        r.getString(0);
        try {
            r.getString(1);
        } catch (DataReadException ex) {
            assertTrue(ex.getMessage().contains("Invalid column"));
        }
    }

    @Test
    public void testDateTimeTypes() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop table if exists testx");
        sqlUpdate("create table testx (w date, x datetime(6), y timestamp(6), z time)");
        Table table = this.schema.getTable("testx");
        SimpleDateFormat df = TimeUtil.getSimpleDateFormat(null, "yyyy-MM-dd'T'HH:mm:ss.S", null, null);
        java.util.Date theDate = df.parse("2015-09-22T12:31:16.136");
        Date w = new Date(theDate.getTime());
        Timestamp y = new Timestamp(theDate.getTime());
        Time z = new Time(theDate.getTime());
        table.insert().values(w, theDate, y, z).execute();
        RowResult rows = table.select("w, x, y, z").execute();
        Row r = rows.next();
        assertEquals("2015-09-22", r.getString("w"));
        // use string comparison for java.sql.Date objects
        assertEquals(w.toString(), r.getDate("w").toString());
        assertEquals("2015-09-22 12:31:16.136000000", r.getString("x"));
        assertEquals(theDate, r.getTimestamp("x"));
        assertEquals("2015-09-22 12:31:16.136000000", r.getString("y"));
        assertEquals(y.toString(), r.getTimestamp("y").toString());
        assertEquals("12:31:16", r.getString("z"));
        assertEquals(z.toString(), r.getTime("z").toString());
    }
}
