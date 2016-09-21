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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.x.Result;
import com.mysql.cj.api.x.Table;

/**
 * @todo
 */
public class TableDeleteTest extends TableTest {
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
    public void testDelete() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        try {
            sqlUpdate("drop table if exists testDelete");
            sqlUpdate("drop view if exists testDeleteView");
            sqlUpdate("create table testDelete (_id varchar(32), name varchar(20), birthday date, age int)");
            sqlUpdate("create view testDeleteView as select _id, age from testDelete");

            sqlUpdate("insert into testDelete values ('1', 'Sakila', '2000-05-27', 14)");
            sqlUpdate("insert into testDelete values ('2', 'Shakila', '2001-06-26', 13)");
            sqlUpdate("insert into testDelete values ('3', 'Shakila', '2002-06-26', 12)");

            Table table = this.schema.getTable("testDelete");
            assertEquals(3, table.count());
            Result res = table.delete().orderBy("age", "name").where("age == 13").execute();
            assertEquals(null, res.getAutoIncrementValue());
            assertEquals(2, table.count());

            Table view = this.schema.getTable("testDeleteView");
            assertEquals(2, view.count());
            res = view.delete().where("age == 12").execute();
            assertEquals(null, res.getAutoIncrementValue());
            assertEquals(1, view.count());

            table.delete().where("age = :age").bind("age", 14).execute();
            assertEquals(0, table.count());
        } finally {
            sqlUpdate("drop table if exists testDelete");
            sqlUpdate("drop view if exists testDeleteView");
        }
    }

    // TODO: there could be more tests, incl limit?
}
