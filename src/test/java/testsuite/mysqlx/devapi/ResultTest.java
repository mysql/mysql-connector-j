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

import com.mysql.cj.api.x.Row;
import com.mysql.cj.api.x.RowResult;
import com.mysql.cj.api.x.Table;
import com.mysql.cj.api.x.Warning;
import com.mysql.cj.core.exceptions.DataReadException;

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
        if (!this.isSetForMySQLxTests) {
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
        if (!this.isSetForMySQLxTests) {
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
        if (!this.isSetForMySQLxTests) {
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
        if (!this.isSetForMySQLxTests) {
            return;
        }
        sqlUpdate("drop table if exists testx");
        sqlUpdate("create table testx (w date, x datetime(6), y timestamp(6), z time)");
        Table table = this.schema.getTable("testx");
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S");
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
