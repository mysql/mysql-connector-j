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

package com.mysql.cj.api.x;

/**
 * In some cases access to the full SQL language is needed, e.g. to directly connect to a specific X Plugin node in a sharded environment and do operations
 * specifically on this node. In those cases a direct, low-level connections needs to be opened. This is performed by using the mysqlx.getNodeSession() function
 * which will return a NodeSession object.
 * <p>
 * When using literal/verbatim SQL the common API patterns are mostly the same compared to using DML and CRUD operations on Tables and Collections. Two
 * differences exist: setting the current schema and escaping names.
 * <p>
 * You cannot call mysqlx.getSchema() or mysqlx.getDefaultSchema() to obtain a Schema object against which you can issue verbatin SQL statements. The Schema
 * object does not feature a executeSql() function.
 * <p>
 * The executeSql() function is a method of the NodeSession class. Use NodeSession.executeSql() and the SQL command USE to change the current schema
 * <p>
 * NodeSession session = XSessionFactory.getNodeSession("root:s3kr3t@localhost");<br>
 * session.executeSql("USE test");
 * <p>
 * If a NodeSession has been established using a data source file the name of the default schema can be obtained to change the current database.
 * <p>
 * Properties p = new Properties();<br>
 * p.setProperty("dataSourceFile", "/home/app_instance50/mysqlxconfig.json");<br>
 * NodeSession session = MysqlxSessionFactory.getSession(p);<br>
 * String defaultSchema = session.getDefaultSchema().getName();<br>
 * session.executeSql("USE ?", defaultSchema);<br>
 * <p>
 * A quoting function exists to escape SQL names/identifiers. NodeSession.quoteName() will escape the identifier given in accordance to the settings of the
 * current connection. The escape function must not be used to escape values. Use the value bind syntax of NodeSession.executeSql() instead.
 * <p>
 * // use bind syntax for values<br>
 * session.executeSql("DROP TABLE IF EXISTS ?", name);<br>
 * <br>
 * // use escape function to quote names/identifier<br>
 * var create = "CREATE TABLE ";<br>
 * create += session.quoteName(name);<br>
 * create += "(id INT NOT NULL PRIMARY KEY AUTO_INCREMENT");<br>
 * <br>
 * session.executeSql(create);
 * <p>
 * Users of the CRUD API do not need to escape identifiers. This is true for working with collections and for working with relational tables.
 */
public interface NodeSession extends BaseSession {
    /**
     * Create a native SQL command. Placeholders are supported using the native "?" syntax.
     */
    SqlStatement sql(String sql);
}
