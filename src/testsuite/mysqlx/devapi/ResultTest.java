/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.mysql.cj.api.x.FetchedRows;
import com.mysql.cj.api.x.Row;
import com.mysql.cj.api.x.Table;
import com.mysql.cj.api.x.Warning;

public class ResultTest extends BaseDevApiTest {
    @Before
    public void setupTableTest() {
        super.setupTestSession();
    }

    @After
    public void teardownTableTest() {
        super.destroyTestSession();
    }

    @Test
    @Ignore("xplugin not reporting warnings for Find queries, reported MYP-155")
    public void testForceBuffering() {
        sqlUpdate("drop table if exists testx");
        sqlUpdate("create table testx (x int)");
        sqlUpdate("insert into testx values (1), (2), (3)");
        Table table = this.schema.getTable("testx");
        FetchedRows rows = table.select("x/0 as bad_x").execute();
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
    public void testTypes() {
        // TODO: !
    }
}
