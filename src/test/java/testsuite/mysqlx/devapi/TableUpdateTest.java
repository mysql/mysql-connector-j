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

import static com.mysql.cj.api.x.Expression.expr;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.x.Result;
import com.mysql.cj.api.x.Row;
import com.mysql.cj.api.x.RowResult;
import com.mysql.cj.api.x.Table;

/**
 * @todo
 */
public class TableUpdateTest extends TableTest {
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
    public void testUpdates() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        try {
            sqlUpdate("drop table if exists updates");
            sqlUpdate("drop view if exists updatesView");
            sqlUpdate("create table updates (_id varchar(32), name varchar(20), birthday date, age int)");
            sqlUpdate("create view updatesView as select _id, name, age from updates");

            sqlUpdate("insert into updates values ('1', 'Sakila', '2000-05-27', 14)");
            sqlUpdate("insert into updates values ('2', 'Shakila', '2001-06-26', 13)");

            Table table = this.schema.getTable("updates");
            Result res = table.update().set("name", expr("concat(name, '-updated')")).set("age", expr("age + 1")).where("name == 'Sakila'").execute();
            assertEquals(null, res.getAutoIncrementValue());

            Table view = this.schema.getTable("updatesView");
            res = view.update().set("name", expr("concat(name, '-updated')")).set("age", expr("age + 3")).where("name == 'Shakila'").orderBy("age", "name")
                    .execute();
            assertEquals(null, res.getAutoIncrementValue());

            RowResult rows = table.select("name, age").where("_id == :theId").bind("theId", 1).execute();
            Row r = rows.next();
            assertEquals("Sakila-updated", r.getString(0));
            assertEquals(15, r.getInt(1));
            assertFalse(rows.hasNext());

            rows = table.select("name, age").where("_id == :theId").bind("theId", 2).execute();
            r = rows.next();
            assertEquals("Shakila-updated", r.getString(0));
            assertEquals(16, r.getInt(1));
            assertFalse(rows.hasNext());
        } finally {
            sqlUpdate("drop table if exists updates");
            sqlUpdate("drop view if exists updatesView");
        }
    }

    // TODO: there could be more tests, but I expect this API and implementation to change to better accommodate some "normal" use cases
}
