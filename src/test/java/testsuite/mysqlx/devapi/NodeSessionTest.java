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
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.x.Row;
import com.mysql.cj.api.x.SqlResult;
import com.mysql.cj.api.x.SqlStatement;

public class NodeSessionTest extends DevApiBaseTestCase {
    @Before
    public void setupCollectionTest() {
        setupTestSession();
    }

    @After
    public void teardownCollectionTest() {
        destroyTestSession();
    }

    @Test
    public void basicSql() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        SqlStatement stmt = this.session.sql("select 1,2,3 from dual");
        SqlResult res = stmt.execute();
        assertTrue(res.hasData());
        Row r = res.next();
        assertEquals("1", r.getString(0));
        assertEquals("2", r.getString(1));
        assertEquals("3", r.getString(2));
        assertEquals("1", r.getString("1"));
        assertEquals("2", r.getString("2"));
        assertEquals("3", r.getString("3"));
        assertFalse(res.hasNext());
    }

    @Test
    public void sqlUpdate() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        SqlStatement stmt = this.session.sql("set @cjTestVar = 1");
        SqlResult res = stmt.execute();
        assertFalse(res.hasData());
        assertEquals(0, res.getAffectedItemsCount());
        assertEquals(null, res.getAutoIncrementValue());
        assertEquals(0, res.getWarningsCount());
        assertFalse(res.getWarnings().hasNext());
    }

    @Test
    public void sqlArguments() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        SqlStatement stmt = this.session.sql("select ? as a, 40 + ? as b, ? as c");
        SqlResult res = stmt.bind(1).bind(2).bind(3).execute();
        Row r = res.next();
        assertEquals("1", r.getString("a"));
        assertEquals("42", r.getString("b"));
        assertEquals("3", r.getString("c"));
    }

    @Test
    public void basicMultipleResults() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        sqlUpdate("drop procedure if exists basicMultipleResults");
        sqlUpdate("create procedure basicMultipleResults() begin explain select 1; explain select 2; end");
        SqlStatement stmt = this.session.sql("call basicMultipleResults()");
        SqlResult res = stmt.execute();
        assertTrue(res.hasData());
        /* Row r = */ res.next();
        assertFalse(res.hasNext());
        assertTrue(res.nextResult());
        assertTrue(res.hasData());
        assertFalse(res.nextResult());
        assertFalse(res.nextResult());
        assertFalse(res.nextResult());
    }

    @Test
    public void smartBufferMultipleResults() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        sqlUpdate("drop procedure if exists basicMultipleResults");
        sqlUpdate("create procedure basicMultipleResults() begin explain select 1; explain select 2; end");
        SqlStatement stmt = this.session.sql("call basicMultipleResults()");
        /* SqlResult res = */ stmt.execute();
        // execute another statement, should work fine
        this.session.sql("call basicMultipleResults()");
        this.session.sql("call basicMultipleResults()");
        this.session.sql("call basicMultipleResults()");
    }
}
