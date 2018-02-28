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

import static com.mysql.cj.xdevapi.Expression.expr;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.Table;

/**
 * @todo
 */
public class TableUpdateTest extends BaseTableTestCase {

    @Test
    public void testUpdates() {
        if (!this.isSetForXTests) {
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
            table.update().set("name", expr("concat(name, '-updated')")).set("age", expr("age + 1")).where("name == 'Sakila'").execute();

            Table view = this.schema.getTable("updatesView");
            view.update().set("name", expr("concat(name, '-updated')")).set("age", expr("age + 3")).where("name == 'Shakila'").orderBy("age", "name").execute();

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
