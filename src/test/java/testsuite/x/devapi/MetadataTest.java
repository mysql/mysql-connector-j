/*
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
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

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.x.protobuf.MysqlxResultset.ColumnMetaData;
import com.mysql.cj.xdevapi.Column;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.Table;
import com.mysql.cj.xdevapi.Type;

/**
 * Tests for "Column" table metadata API.
 */
public class MetadataTest extends BaseTableTestCase {
    @Before
    @Override
    public void setupTableTest() {
        super.setupTableTest();
        if (this.isSetForXTests) {
            sqlUpdate("drop table if exists example_metadata");
            sqlUpdate("create table example_metadata (_id varchar(32), name varchar(20), birthday date, age int)");
        }
    }

    @Test
    public void example_metadata() {
        if (!this.isSetForXTests) {
            return;
        }
        Table table = this.schema.getTable("example_metadata");
        RowResult rows = table.select("_id, name, birthday, age").execute();
        List<Column> metadata = rows.getColumns();
        assertEquals(4, metadata.size());
        Column idCol = metadata.get(0);

        assertEquals(this.schema.getName(), idCol.getSchemaName());
        assertEquals("example_metadata", idCol.getTableName());
        assertEquals("example_metadata", idCol.getTableLabel());
        assertEquals("_id", idCol.getColumnName());
        assertEquals("_id", idCol.getColumnLabel());
        assertEquals(Type.STRING, idCol.getType());
        if ("utf8mb4_0900_ai_ci".equals(this.dbCollation)) {
            assertEquals(128, idCol.getLength()); // TODO is it an xplugin bug after changing default charset to utf8mb4?
        } else {
            assertEquals(32, idCol.getLength());
        }
        assertEquals(0, idCol.getFractionalDigits());
        assertEquals(false, idCol.isNumberSigned()); // odd default
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
            // after Bug#28180155 fix
            assertEquals("utf8mb4_general_ci", idCol.getCollationName());
            assertEquals("utf8mb4", idCol.getCharacterSetName());
        } else {
            assertEquals(this.dbCollation, idCol.getCollationName());
            assertEquals(this.dbCharset, idCol.getCharacterSetName());
        }
        assertEquals(false, idCol.isPadded());
        assertEquals(true, idCol.isNullable());
        assertEquals(false, idCol.isAutoIncrement());
        assertEquals(false, idCol.isPrimaryKey());
        assertEquals(false, idCol.isUniqueKey());
        assertEquals(false, idCol.isPartKey());

        Column nameCol = metadata.get(1);
        assertEquals(this.schema.getName(), nameCol.getSchemaName());
        assertEquals("example_metadata", nameCol.getTableName());
        assertEquals("example_metadata", nameCol.getTableLabel());
        assertEquals("name", nameCol.getColumnName());
        assertEquals("name", nameCol.getColumnLabel());
        assertEquals(Type.STRING, nameCol.getType());
        if ("utf8mb4_0900_ai_ci".equals(this.dbCollation)) {
            assertEquals(80, nameCol.getLength()); // TODO is it an xplugin bug after changing default charset to utf8mb4?
        } else {
            assertEquals(20, nameCol.getLength());
        }
        assertEquals(0, nameCol.getFractionalDigits());
        assertEquals(false, nameCol.isNumberSigned());
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
            // after Bug#28180155 fix
            assertEquals("utf8mb4_general_ci", nameCol.getCollationName());
            assertEquals("utf8mb4", nameCol.getCharacterSetName());
        } else {
            assertEquals(this.dbCollation, nameCol.getCollationName());
            assertEquals(this.dbCharset, nameCol.getCharacterSetName());
        }
        assertEquals(false, nameCol.isPadded());
        assertEquals(true, nameCol.isNullable());
        assertEquals(false, nameCol.isAutoIncrement());
        assertEquals(false, nameCol.isPrimaryKey());
        assertEquals(false, nameCol.isUniqueKey());
        assertEquals(false, nameCol.isPartKey());

        Column birthdayCol = metadata.get(2);
        assertEquals(this.schema.getName(), birthdayCol.getSchemaName());
        assertEquals("example_metadata", birthdayCol.getTableName());
        assertEquals("example_metadata", birthdayCol.getTableLabel());
        assertEquals("birthday", birthdayCol.getColumnName());
        assertEquals("birthday", birthdayCol.getColumnLabel());
        assertEquals(Type.DATE, birthdayCol.getType());
        assertEquals(10, birthdayCol.getLength());
        assertEquals(0, birthdayCol.getFractionalDigits());
        assertEquals(false, birthdayCol.isNumberSigned());
        assertEquals(null, birthdayCol.getCollationName());
        assertEquals(null, birthdayCol.getCharacterSetName());
        assertEquals(false, birthdayCol.isPadded());
        assertEquals(true, birthdayCol.isNullable());
        assertEquals(false, birthdayCol.isAutoIncrement());
        assertEquals(false, birthdayCol.isPrimaryKey());
        assertEquals(false, birthdayCol.isUniqueKey());
        assertEquals(false, birthdayCol.isPartKey());

        Column ageCol = metadata.get(3);
        assertEquals(this.schema.getName(), ageCol.getSchemaName());
        assertEquals("example_metadata", ageCol.getTableName());
        assertEquals("example_metadata", ageCol.getTableLabel());
        assertEquals("age", ageCol.getColumnName());
        assertEquals("age", ageCol.getColumnLabel());
        assertEquals(Type.INT, ageCol.getType());
        assertEquals(11, ageCol.getLength());
        assertEquals(0, ageCol.getFractionalDigits());
        assertEquals(true, ageCol.isNumberSigned());
        assertEquals(null, ageCol.getCollationName());
        assertEquals(null, ageCol.getCharacterSetName());
        assertEquals(false, ageCol.isPadded());
        assertEquals(true, ageCol.isNullable());
        assertEquals(false, ageCol.isAutoIncrement());
        assertEquals(false, ageCol.isPrimaryKey());
        assertEquals(false, ageCol.isUniqueKey());
        assertEquals(false, ageCol.isPartKey());
    }

    @Test
    public void renameCol() {
        if (!this.isSetForXTests) {
            return;
        }
        Table table = this.schema.getTable("example_metadata");
        RowResult rows = table.select("_id as TheId").execute();
        List<Column> metadata = rows.getColumns();
        assertEquals(1, metadata.size());
        Column idCol = metadata.get(0);

        assertEquals(this.schema.getName(), idCol.getSchemaName());
        assertEquals("example_metadata", idCol.getTableName());
        assertEquals("example_metadata", idCol.getTableLabel());
        assertEquals("_id", idCol.getColumnName());
        assertEquals("TheId", idCol.getColumnLabel());
        assertEquals(Type.STRING, idCol.getType());
        if ("utf8mb4_0900_ai_ci".equals(this.dbCollation)) {
            assertEquals(128, idCol.getLength()); // TODO is it an xplugin bug after changing default charset to utf8mb4?
        } else {
            assertEquals(32, idCol.getLength());
        }
        assertEquals(0, idCol.getFractionalDigits());
        assertEquals(false, idCol.isNumberSigned());
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
            // after Bug#28180155 fix
            assertEquals("utf8mb4_general_ci", idCol.getCollationName());
            assertEquals("utf8mb4", idCol.getCharacterSetName());
        } else {
            assertEquals(this.dbCollation, idCol.getCollationName());
            assertEquals(this.dbCharset, idCol.getCharacterSetName());
        }
        assertEquals(false, idCol.isPadded());
        assertEquals(true, idCol.isNullable());
        assertEquals(false, idCol.isAutoIncrement());
        assertEquals(false, idCol.isPrimaryKey());
        assertEquals(false, idCol.isUniqueKey());
        assertEquals(false, idCol.isPartKey());
    }

    @Test
    public void derivedCol() {
        if (!this.isSetForXTests) {
            return;
        }
        Table table = this.schema.getTable("example_metadata");
        RowResult rows = table.select("_id + 1 as TheId").execute();
        List<Column> metadata = rows.getColumns();
        assertEquals(1, metadata.size());
        Column idCol = metadata.get(0);

        assertEquals("", idCol.getSchemaName());
        assertEquals("", idCol.getTableName());
        assertEquals("", idCol.getTableLabel());
        assertEquals("", idCol.getColumnName());
        assertEquals("TheId", idCol.getColumnLabel());
        assertEquals(Type.DOUBLE, idCol.getType());
        assertEquals(23, idCol.getLength());
        assertEquals(31, idCol.getFractionalDigits());
        assertEquals(true, idCol.isNumberSigned());
        assertEquals(null, idCol.getCollationName());
        assertEquals(null, idCol.getCharacterSetName());
        assertEquals(false, idCol.isPadded());
        assertEquals(true, idCol.isNullable());
        assertEquals(false, idCol.isAutoIncrement());
        assertEquals(false, idCol.isPrimaryKey());
        assertEquals(false, idCol.isUniqueKey());
        assertEquals(false, idCol.isPartKey());
    }

    @Test
    public void docAsTableIsJSON() {
        if (!this.isSetForXTests) {
            return;
        }
        String collName = "doc_as_table";
        dropCollection(collName);
        this.schema.createCollection(collName);
        Table table = this.schema.getCollectionAsTable(collName);
        RowResult rows = table.select("_id, doc").execute();
        List<Column> metadata = rows.getColumns();
        assertEquals(2, metadata.size());

        Column idCol = metadata.get(0);
        assertEquals(this.schema.getName(), idCol.getSchemaName());
        assertEquals(collName, idCol.getTableName());
        assertEquals(collName, idCol.getTableLabel());
        assertEquals("_id", idCol.getColumnName());
        assertEquals("_id", idCol.getColumnLabel());
        assertEquals(Type.STRING, idCol.getType());
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            assertEquals(32, idCol.getLength());
        } else {
            assertEquals(128, idCol.getLength());
        }
        assertEquals(0, idCol.getFractionalDigits());
        assertEquals(false, idCol.isNumberSigned());

        // Unlike ordinary tables, collections are always created in uft8mb4 charset, but collation was changed in 8.0.1.
        // Since MySQL 8.0.5 the _id column has collation 'binary'.
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            assertEquals("binary", idCol.getCollationName());
            assertEquals("binary", idCol.getCharacterSetName());
        } else if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.1"))) {
            assertEquals("utf8mb4_0900_ai_ci", idCol.getCollationName());
            assertEquals("utf8mb4", idCol.getCharacterSetName());
        } else {
            assertEquals("utf8mb4_general_ci", idCol.getCollationName());
            assertEquals("utf8mb4", idCol.getCharacterSetName());
        }
        assertEquals(false, idCol.isPadded());
        assertEquals(false, idCol.isNullable());
        assertEquals(false, idCol.isAutoIncrement());
        assertEquals(true, idCol.isPrimaryKey());
        assertEquals(false, idCol.isUniqueKey());
        assertEquals(false, idCol.isPartKey());

        Column docCol = metadata.get(1);
        assertEquals(this.schema.getName(), docCol.getSchemaName());
        assertEquals(collName, docCol.getTableName());
        assertEquals(collName, docCol.getTableLabel());
        assertEquals("doc", docCol.getColumnName());
        assertEquals("doc", docCol.getColumnLabel());
        assertEquals(Type.JSON, docCol.getType());
        assertEquals(4294967295L, docCol.getLength());
        assertEquals(0, docCol.getFractionalDigits());
        assertEquals(false, docCol.isNumberSigned());
        assertEquals("binary", docCol.getCollationName());
        assertEquals("binary", docCol.getCharacterSetName());
        assertEquals(false, docCol.isPadded());
        assertEquals(true, docCol.isNullable());
        assertEquals(false, docCol.isAutoIncrement());
        assertEquals(false, docCol.isPrimaryKey());
        assertEquals(false, docCol.isUniqueKey());
        assertEquals(false, docCol.isPartKey());
    }

    /**
     * Some metadata fields have no sense with concrete SQL data type. The following table from {@link ColumnMetaData}
     * describes which fields are relevant to each type:
     * 
     * <pre>
     *     ================= ============ ======= ========== ====== ========
     *     SQL Type          .type        .length .frac_dig  .flags .charset
     *     ================= ============ ======= ========== ====== ========
     *     TINY              SINT         x
     *     TINY UNSIGNED     UINT         x                  x
     *     SHORT             SINT         x
     *     SHORT UNSIGNED    UINT         x                  x
     *     INT24             SINT         x
     *     INT24 UNSIGNED    UINT         x                  x
     *     INT               SINT         x
     *     INT UNSIGNED      UINT         x                  x
     *     LONGLONG          SINT         x
     *     LONGLONG UNSIGNED UINT         x                  x
     *     DOUBLE            DOUBLE       x       x          x
     *     FLOAT             FLOAT        x       x          x
     *     DECIMAL           DECIMAL      x       x          x
     *     VARCHAR,CHAR,...  BYTES        x                  x      x
     *     GEOMETRY          BYTES
     *     TIME              TIME         x
     *     DATE              DATETIME     x
     *     DATETIME          DATETIME     x
     *     YEAR              UINT         x                  x
     *     TIMESTAMP         DATETIME     x
     *     SET               SET                                    x
     *     ENUM              ENUM                                   x
     *     NULL              BYTES
     *     BIT               BIT          x
     *     ================= ============ ======= ========== ====== ========
     * </pre>
     */
    @Test
    public void exhaustTypes() {
        if (!this.isSetForXTests) {
            return;
        }
        String tableName = "exhaust_types";
        sqlUpdate("drop table if exists " + tableName);
        sqlUpdate("create table exhaust_types (a bit, b char(20) not null, c int, d tinyint unsigned primary key, e bigint, "
                + "f double, g decimal(20, 3), h time, i datetime, j timestamp, k date, l set('1','2'), m enum('1','2'), unique (a), key(b, c))");
        Table table = this.schema.getTable(tableName);
        RowResult rows = table.select("a,b,c,d,e,f,g,h,i,j,k,l,m").execute();
        List<Column> metadata = rows.getColumns();
        assertEquals(13, metadata.size());

        Column c;

        c = metadata.get(0);
        assertEquals(this.schema.getName(), c.getSchemaName());
        assertEquals(tableName, c.getTableName());
        assertEquals(tableName, c.getTableLabel());
        assertEquals("a", c.getColumnName());
        assertEquals("a", c.getColumnLabel());
        assertEquals(Type.BIT, c.getType());
        // assertEquals(1, c.getLength()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(0, c.getFractionalDigits()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isNumberSigned()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(null, c.getCollationName()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(null, c.getCharacterSetName()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPadded()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(true, c.isNullable()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isAutoIncrement()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPrimaryKey()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(true, c.isUniqueKey()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPartKey()); // irrelevant, we shouldn't expect any concrete value

        c = metadata.get(1);
        assertEquals(this.schema.getName(), c.getSchemaName());
        assertEquals(tableName, c.getTableName());
        assertEquals(tableName, c.getTableLabel());
        assertEquals("b", c.getColumnName());
        assertEquals("b", c.getColumnLabel());
        assertEquals(Type.STRING, c.getType());
        if ("utf8mb4_0900_ai_ci".equals(this.dbCollation)) {
            assertEquals(80, c.getLength()); // TODO is it an xplugin bug after changing default charset to utf8mb4?
        } else {
            assertEquals(20, c.getLength());
        }
        // assertEquals(0, c.getFractionalDigits()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(false, c.isNumberSigned());
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
            // after Bug#28180155 fix
            assertEquals("utf8mb4_general_ci", c.getCollationName());
            assertEquals("utf8mb4", c.getCharacterSetName());
        } else {
            assertEquals(this.dbCollation, c.getCollationName());
            assertEquals(this.dbCharset, c.getCharacterSetName());
        }
        assertEquals(true, c.isPadded());
        assertEquals(false, c.isNullable());
        assertEquals(false, c.isAutoIncrement());
        assertEquals(false, c.isPrimaryKey());
        assertEquals(false, c.isUniqueKey());
        assertEquals(true, c.isPartKey());

        c = metadata.get(2);
        assertEquals(this.schema.getName(), c.getSchemaName());
        assertEquals(tableName, c.getTableName());
        assertEquals(tableName, c.getTableLabel());
        assertEquals("c", c.getColumnName());
        assertEquals("c", c.getColumnLabel());
        assertEquals(Type.INT, c.getType());
        assertEquals(11, c.getLength());
        // assertEquals(0, c.getFractionalDigits()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(true, c.isNumberSigned());
        // assertEquals(null, c.getCollationName()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(null, c.getCharacterSetName()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(false, c.isPadded()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(true, c.isNullable()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(false, c.isAutoIncrement()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(false, c.isPrimaryKey()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(false, c.isUniqueKey()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(false, c.isPartKey()); // irrelevant, we shouldn't expect any concrete value

        c = metadata.get(3);
        assertEquals(this.schema.getName(), c.getSchemaName());
        assertEquals(tableName, c.getTableName());
        assertEquals(tableName, c.getTableLabel());
        assertEquals("d", c.getColumnName());
        assertEquals("d", c.getColumnLabel());
        assertEquals(Type.TINYINT, c.getType());
        assertEquals(3, c.getLength());
        // assertEquals(0, c.getFractionalDigits()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(false, c.isNumberSigned());
        // assertEquals(null, c.getCollationName()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(null, c.getCharacterSetName()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(false, c.isPadded());
        assertEquals(false, c.isNullable());
        assertEquals(false, c.isAutoIncrement());
        assertEquals(true, c.isPrimaryKey());
        assertEquals(false, c.isUniqueKey());
        assertEquals(false, c.isPartKey());

        c = metadata.get(4);
        assertEquals(this.schema.getName(), c.getSchemaName());
        assertEquals(tableName, c.getTableName());
        assertEquals(tableName, c.getTableLabel());
        assertEquals("e", c.getColumnName());
        assertEquals("e", c.getColumnLabel());
        assertEquals(Type.BIGINT, c.getType());
        assertEquals(20, c.getLength());
        // assertEquals(0, c.getFractionalDigits()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(true, c.isNumberSigned());
        // assertEquals(null, c.getCollationName()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(null, c.getCharacterSetName()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(false, c.isPadded()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(true, c.isNullable()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(false, c.isAutoIncrement()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(false, c.isPrimaryKey()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(false, c.isUniqueKey()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(false, c.isPartKey()); // irrelevant, we shouldn't expect any concrete value

        c = metadata.get(5);
        assertEquals(this.schema.getName(), c.getSchemaName());
        assertEquals(tableName, c.getTableName());
        assertEquals(tableName, c.getTableLabel());
        assertEquals("f", c.getColumnName());
        assertEquals("f", c.getColumnLabel());
        assertEquals(Type.DOUBLE, c.getType());
        assertEquals(22, c.getLength());
        assertEquals(31, c.getFractionalDigits());
        assertEquals(true, c.isNumberSigned());
        // assertEquals(null, c.getCollationName()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(null, c.getCharacterSetName()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(false, c.isPadded());
        assertEquals(true, c.isNullable());
        assertEquals(false, c.isAutoIncrement());
        assertEquals(false, c.isPrimaryKey());
        assertEquals(false, c.isUniqueKey());
        assertEquals(false, c.isPartKey());

        c = metadata.get(6);
        assertEquals(this.schema.getName(), c.getSchemaName());
        assertEquals(tableName, c.getTableName());
        assertEquals(tableName, c.getTableLabel());
        assertEquals("g", c.getColumnName());
        assertEquals("g", c.getColumnLabel());
        assertEquals(Type.DECIMAL, c.getType());
        assertEquals(22, c.getLength());
        assertEquals(3, c.getFractionalDigits());
        assertEquals(true, c.isNumberSigned());
        // assertEquals(null, c.getCollationName()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(null, c.getCharacterSetName()); // irrelevant, we shouldn't expect any concrete value
        assertEquals(false, c.isPadded());
        assertEquals(true, c.isNullable());
        assertEquals(false, c.isAutoIncrement());
        assertEquals(false, c.isPrimaryKey());
        assertEquals(false, c.isUniqueKey());
        assertEquals(false, c.isPartKey());

        c = metadata.get(7);
        assertEquals(this.schema.getName(), c.getSchemaName());
        assertEquals(tableName, c.getTableName());
        assertEquals(tableName, c.getTableLabel());
        assertEquals("h", c.getColumnName());
        assertEquals("h", c.getColumnLabel());
        assertEquals(Type.TIME, c.getType());
        assertEquals(10, c.getLength());
        // assertEquals(0, c.getFractionalDigits()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isNumberSigned()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(null, c.getCollationName()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(null, c.getCharacterSetName()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPadded()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(true, c.isNullable()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isAutoIncrement()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPrimaryKey()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isUniqueKey()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPartKey()); // irrelevant, we shouldn't expect any concrete value

        c = metadata.get(8);
        assertEquals(this.schema.getName(), c.getSchemaName());
        assertEquals(tableName, c.getTableName());
        assertEquals(tableName, c.getTableLabel());
        assertEquals("i", c.getColumnName());
        assertEquals("i", c.getColumnLabel());
        assertEquals(Type.DATETIME, c.getType());
        assertEquals(19, c.getLength());
        // assertEquals(0, c.getFractionalDigits()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isNumberSigned()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(null, c.getCollationName()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(null, c.getCharacterSetName()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPadded()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(true, c.isNullable()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isAutoIncrement()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPrimaryKey()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isUniqueKey()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPartKey()); // irrelevant, we shouldn't expect any concrete value

        c = metadata.get(9);
        assertEquals(this.schema.getName(), c.getSchemaName());
        assertEquals(tableName, c.getTableName());
        assertEquals(tableName, c.getTableLabel());
        assertEquals("j", c.getColumnName());
        assertEquals("j", c.getColumnLabel());
        assertEquals(Type.TIMESTAMP, c.getType());
        assertEquals(19, c.getLength());
        // assertEquals(0, c.getFractionalDigits()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isNumberSigned()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(null, c.getCollationName()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(null, c.getCharacterSetName()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPadded()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(true, c.isNullable()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isAutoIncrement()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPrimaryKey()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isUniqueKey()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPartKey()); // irrelevant, we shouldn't expect any concrete value

        c = metadata.get(10);
        assertEquals(this.schema.getName(), c.getSchemaName());
        assertEquals(tableName, c.getTableName());
        assertEquals(tableName, c.getTableLabel());
        assertEquals("k", c.getColumnName());
        assertEquals("k", c.getColumnLabel());
        assertEquals(Type.DATE, c.getType());
        assertEquals(10, c.getLength());
        // assertEquals(0, c.getFractionalDigits()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isNumberSigned()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(null, c.getCollationName()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(null, c.getCharacterSetName()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPadded()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(true, c.isNullable()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isAutoIncrement()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPrimaryKey()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isUniqueKey()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPartKey()); // irrelevant, we shouldn't expect any concrete value

        c = metadata.get(11);
        assertEquals(this.schema.getName(), c.getSchemaName());
        assertEquals(tableName, c.getTableName());
        assertEquals(tableName, c.getTableLabel());
        assertEquals("l", c.getColumnName());
        assertEquals("l", c.getColumnLabel());
        assertEquals(Type.SET, c.getType());
        // assertEquals(3, c.getLength()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(0, c.getFractionalDigits()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isNumberSigned()); // irrelevant, we shouldn't expect any concrete value
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
            // after Bug#28180155 fix
            assertEquals("utf8mb4_general_ci", c.getCollationName());
            assertEquals("utf8mb4", c.getCharacterSetName());
        } else {
            assertEquals(this.dbCollation, c.getCollationName());
            assertEquals(this.dbCharset, c.getCharacterSetName());
        }
        // assertEquals(false, c.isPadded()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(true, c.isNullable()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isAutoIncrement()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPrimaryKey()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isUniqueKey()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPartKey()); // irrelevant, we shouldn't expect any concrete value

        c = metadata.get(12);
        assertEquals(this.schema.getName(), c.getSchemaName());
        assertEquals(tableName, c.getTableName());
        assertEquals(tableName, c.getTableLabel());
        assertEquals("m", c.getColumnName());
        assertEquals("m", c.getColumnLabel());
        assertEquals(Type.ENUM, c.getType());
        // assertEquals(1, c.getLength()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(0, c.getFractionalDigits()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isNumberSigned()); // irrelevant, we shouldn't expect any concrete value
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
            // after Bug#28180155 fix
            assertEquals("utf8mb4_general_ci", c.getCollationName());
            assertEquals("utf8mb4", c.getCharacterSetName());
        } else {
            assertEquals(this.dbCollation, c.getCollationName());
            assertEquals(this.dbCharset, c.getCharacterSetName());
        }
        // assertEquals(false, c.isPadded()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(true, c.isNullable()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isAutoIncrement()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPrimaryKey()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isUniqueKey()); // irrelevant, we shouldn't expect any concrete value
        // assertEquals(false, c.isPartKey()); // irrelevant, we shouldn't expect any concrete value

    }
}
