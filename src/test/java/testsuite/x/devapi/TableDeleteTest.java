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

import org.junit.Test;

import com.mysql.cj.xdevapi.Table;

/**
 * @todo
 */
public class TableDeleteTest extends BaseTableTestCase {

    @Test
    public void testDelete() {
        if (!this.isSetForXTests) {
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
            table.delete().orderBy("age", "name").where("age == 13").execute();
            assertEquals(2, table.count());

            Table view = this.schema.getTable("testDeleteView");
            assertEquals(2, view.count());
            view.delete().where("age == 12").execute();
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
