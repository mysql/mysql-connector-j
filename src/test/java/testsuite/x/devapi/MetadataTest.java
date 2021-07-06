/*
 * Copyright (c) 2016, 2021, Oracle and/or its affiliates.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.x.protobuf.MysqlxResultset.ColumnMetaData;
import com.mysql.cj.xdevapi.Column;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.SqlResult;
import com.mysql.cj.xdevapi.Table;
import com.mysql.cj.xdevapi.Type;

/**
 * Tests for "Column" table metadata API.
 */
public class MetadataTest extends BaseTableTestCase {
    @BeforeEach
    public void setupTableTest() {
        if (this.isSetForXTests) {
            sqlUpdate("drop table if exists example_metadata");
            sqlUpdate("create table example_metadata (_id varchar(32), name varchar(20), birthday date, age int)");
        }
    }

    @AfterEach
    public void teardownTableTest() {
        if (this.isSetForXTests) {
            sqlUpdate("drop table if exists example_metadata");
        }
    }

    @Test
    public void example_metadata() {
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
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.20"))) {
            // after Bug#30516849 fix
            assertEquals("utf8mb4_0900_ai_ci", idCol.getCollationName());
            assertEquals("utf8mb4", idCol.getCharacterSetName());
        } else if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
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
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.20"))) {
            // after Bug#30516849 fix
            assertEquals("utf8mb4_0900_ai_ci", nameCol.getCollationName());
            assertEquals("utf8mb4", nameCol.getCharacterSetName());
        } else if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
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
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.20"))) {
            // after Bug#30516849 fix
            assertEquals("utf8mb4_0900_ai_ci", idCol.getCollationName());
            assertEquals("utf8mb4", idCol.getCharacterSetName());
        } else if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
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
        assertEquals(0, idCol.getFractionalDigits());
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
        String collName = "doc_as_table";
        try {
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
        } finally {
            dropCollection(collName);
        }
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
        String tableName = "exhaust_types";
        try {
            sqlUpdate("drop table if exists " + tableName);
            sqlUpdate("create table " + tableName + " (a bit, b char(20) not null, c int, d tinyint unsigned primary key, e bigint, "
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
            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.20"))) {
                // after Bug#30516849 fix
                assertEquals("utf8mb4_0900_ai_ci", c.getCollationName());
                assertEquals("utf8mb4", c.getCharacterSetName());
            } else if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
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
            assertEquals(0, c.getFractionalDigits());
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
            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.20"))) {
                // after Bug#30516849 fix
                assertEquals("utf8mb4_0900_ai_ci", c.getCollationName());
                assertEquals("utf8mb4", c.getCharacterSetName());
            } else if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
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
            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.20"))) {
                // after Bug#30516849 fix
                assertEquals("utf8mb4_0900_ai_ci", c.getCollationName());
                assertEquals("utf8mb4", c.getCharacterSetName());
            } else if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
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
        } finally {
            sqlUpdate("drop table if exists " + tableName);
        }
    }

    @Test
    public void testGetColumnInfoFromnSession() throws Exception {
        Column myCol = null;
        List<Column> metadata = null;
        try {
            sqlUpdate("drop table if exists xyz");
            sqlUpdate(
                    "create table xyz (i int auto_increment primary key,j bigint not null,k tinyint unsigned unique,l char(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish2_ci,m decimal(16,10),key(l))");
            sqlUpdate("insert into xyz (j,k,l,m) values (1000,1,'a',10.12)");

            sqlUpdate("drop database if exists qadatabase");
            sqlUpdate("create database qadatabase");
            sqlUpdate("create table qadatabase.xyz (d date)");
            sqlUpdate("insert into qadatabase.xyz values ('2016-03-07')");

            SqlResult sRes = this.session.sql("select * from  " + this.schema.getName() + ".xyz , qadatabase.xyz mytable").execute();
            metadata = sRes.getColumns();

            assertEquals(6, metadata.size());
            for (int i = 0; i < metadata.size(); i++) {
                myCol = metadata.get(i);
            }

            myCol = metadata.get(0);
            assertEquals("i", myCol.getColumnName());
            assertEquals("i", myCol.getColumnLabel());
            assertEquals(this.schema.getName(), myCol.getSchemaName());
            assertEquals("xyz", myCol.getTableName());
            assertEquals("xyz", myCol.getTableLabel());
            assertEquals(Type.INT, myCol.getType());
            assertEquals(11, myCol.getLength());
            assertEquals(0, myCol.getFractionalDigits());
            assertEquals(true, myCol.isNumberSigned());
            assertEquals(null, myCol.getCollationName());
            assertEquals(null, myCol.getCharacterSetName());
            assertEquals(false, myCol.isPadded());
            assertEquals(false, myCol.isNullable());
            assertEquals(true, myCol.isAutoIncrement());
            assertEquals(true, myCol.isPrimaryKey());
            assertEquals(false, myCol.isUniqueKey());
            assertEquals(false, myCol.isPartKey());

            myCol = metadata.get(1);
            assertEquals("j", myCol.getColumnName());
            assertEquals("j", myCol.getColumnLabel());
            assertEquals(this.schema.getName(), myCol.getSchemaName());
            assertEquals("xyz", myCol.getTableName());
            assertEquals("xyz", myCol.getTableLabel());
            assertEquals(Type.BIGINT, myCol.getType());
            assertEquals(20, myCol.getLength());
            assertEquals(0, myCol.getFractionalDigits());
            assertEquals(true, myCol.isNumberSigned());
            assertEquals(null, myCol.getCollationName());
            assertEquals(null, myCol.getCharacterSetName());
            assertEquals(false, myCol.isPadded());
            assertEquals(false, myCol.isNullable());
            assertEquals(false, myCol.isAutoIncrement());
            assertEquals(false, myCol.isPrimaryKey());
            assertEquals(false, myCol.isUniqueKey());
            assertEquals(false, myCol.isPartKey());

            myCol = metadata.get(2);
            assertEquals("k", myCol.getColumnName());
            assertEquals("k", myCol.getColumnLabel());
            assertEquals(this.schema.getName(), myCol.getSchemaName());
            assertEquals("xyz", myCol.getTableName());
            assertEquals("xyz", myCol.getTableLabel());
            assertEquals(Type.TINYINT, myCol.getType());
            assertEquals(3, myCol.getLength());
            assertEquals(0, myCol.getFractionalDigits());
            assertEquals(false, myCol.isNumberSigned());
            assertEquals(null, myCol.getCollationName());
            assertEquals(null, myCol.getCharacterSetName());
            assertEquals(false, myCol.isPadded());
            assertEquals(true, myCol.isNullable());
            assertEquals(false, myCol.isAutoIncrement());
            assertEquals(false, myCol.isPrimaryKey());
            assertEquals(true, myCol.isUniqueKey());
            assertEquals(false, myCol.isPartKey());

            myCol = metadata.get(3);
            assertEquals("l", myCol.getColumnName());
            assertEquals("l", myCol.getColumnLabel());
            assertEquals(this.schema.getName(), myCol.getSchemaName());
            assertEquals("xyz", myCol.getTableName());
            assertEquals("xyz", myCol.getTableLabel());
            assertEquals(Type.STRING, myCol.getType());
            assertEquals(800, myCol.getLength());
            assertEquals(0, myCol.getFractionalDigits());
            assertEquals(false, myCol.isNumberSigned());
            assertEquals("utf8mb4_spanish2_ci", myCol.getCollationName());
            assertEquals("utf8mb4", myCol.getCharacterSetName());
            assertEquals(true, myCol.isPadded());
            assertEquals(true, myCol.isNullable());
            assertEquals(false, myCol.isAutoIncrement());
            assertEquals(false, myCol.isPrimaryKey());
            assertEquals(false, myCol.isUniqueKey());
            assertEquals(true, myCol.isPartKey());

            myCol = metadata.get(4);
            assertEquals("m", myCol.getColumnName());
            assertEquals("m", myCol.getColumnLabel());
            assertEquals(this.schema.getName(), myCol.getSchemaName());
            assertEquals("xyz", myCol.getTableName());
            assertEquals("xyz", myCol.getTableLabel());
            assertEquals(Type.DECIMAL, myCol.getType());
            assertEquals(18, myCol.getLength());
            assertEquals(10, myCol.getFractionalDigits());
            assertEquals(true, myCol.isNumberSigned());
            assertEquals(null, myCol.getCollationName());
            assertEquals(null, myCol.getCharacterSetName());
            assertEquals(false, myCol.isPadded());
            assertEquals(true, myCol.isNullable());
            assertEquals(false, myCol.isAutoIncrement());
            assertEquals(false, myCol.isPrimaryKey());
            assertEquals(false, myCol.isUniqueKey());
            assertEquals(false, myCol.isPartKey());

            myCol = metadata.get(5);
            assertEquals("d", myCol.getColumnName());
            assertEquals("d", myCol.getColumnLabel());
            assertEquals("qadatabase", myCol.getSchemaName());
            assertEquals("xyz", myCol.getTableName());
            assertEquals("mytable", myCol.getTableLabel());
            assertEquals(Type.DATE, myCol.getType());
            assertEquals(10, myCol.getLength());
            assertEquals(0, myCol.getFractionalDigits());
            assertEquals(false, myCol.isNumberSigned());
            assertEquals(null, myCol.getCollationName());
            assertEquals(null, myCol.getCharacterSetName());
            assertEquals(false, myCol.isPadded());
            assertEquals(true, myCol.isNullable());
            assertEquals(false, myCol.isAutoIncrement());
            assertEquals(false, myCol.isPrimaryKey());
            assertEquals(false, myCol.isUniqueKey());
            assertEquals(false, myCol.isPartKey());
        } finally {
            sqlUpdate("drop table if exists xyz");
            sqlUpdate("drop database if exists qadatabase");
        }
    }

    @Test
    public void testGetSchemaName() throws Exception {
        RowResult rows = null;
        Table table = null;
        try {
            sqlUpdate("drop table if exists qatable");
            sqlUpdate("create table qatable (_id varchar(32), a varchar(20), b date, c int)");
            sqlUpdate("insert into qatable values ('X', 'Abcd', '2016-03-07',10)");
            sqlUpdate("drop database if exists qadatabase");
            sqlUpdate("create database qadatabase");
            sqlUpdate("create table qadatabase.qatable (_id varchar(32), a varchar(20), b date, c int)");
            sqlUpdate("insert into qadatabase.qatable values ('X', 'Abcd', '2016-03-07',10)");

            table = this.schema.getTable("qatable");
            rows = table.select("_id, a, b, c").execute();
            List<Column> metadata = rows.getColumns();
            assertEquals(4, metadata.size());

            Column idCol = metadata.get(0);
            assertEquals(this.schema.getName(), idCol.getSchemaName());

            Column aCol = metadata.get(1);
            assertEquals(this.schema.getName(), aCol.getSchemaName());

            Column bCol = metadata.get(2);
            assertEquals(this.schema.getName(), bCol.getSchemaName());

            Column cCol = metadata.get(3);
            assertEquals(this.schema.getName(), cCol.getSchemaName());

            SqlResult sRes = this.session.sql("select * from  qadatabase.qatable").execute();
            metadata = sRes.getColumns();
            assertEquals(4, metadata.size());

            idCol = metadata.get(0);
            assertEquals("qadatabase", idCol.getSchemaName());

            aCol = metadata.get(1);
            assertEquals("qadatabase", aCol.getSchemaName());

            bCol = metadata.get(2);
            assertEquals("qadatabase", bCol.getSchemaName());

            cCol = metadata.get(3);
            assertEquals("qadatabase", cCol.getSchemaName());
        } finally {
            sqlUpdate("drop database if exists qadatabase");
        }
    }

    @Test
    public void testGetTableName() throws Exception {
        RowResult rows = null;
        Table table = null;
        try {
            sqlUpdate("drop table if exists qatable");
            sqlUpdate("create table qatable (_id varchar(32), a varchar(20), b date, c int)");
            sqlUpdate("insert into qatable values ('X', 'Abcd', '2016-03-07',10)");

            table = this.schema.getTable("qatable");
            rows = table.select("_id, a, b, c").execute();
            List<Column> metadata = rows.getColumns();
            assertEquals(4, metadata.size());

            Column idCol = metadata.get(0);
            assertEquals("qatable", idCol.getTableName());

            Column aCol = metadata.get(1);
            assertEquals("qatable", aCol.getTableName());

            Column bCol = metadata.get(2);
            assertEquals("qatable", bCol.getTableName());

            Column cCol = metadata.get(3);
            assertEquals("qatable", cCol.getTableName());
            assertEquals(table.getName(), cCol.getTableName());

        } finally {
            sqlUpdate("drop table if exists qatable");
        }
    }

    @Test
    public void testGetTableLabel() throws Exception {
        RowResult rows = null;
        Table table = null;
        try {
            sqlUpdate("drop table if exists qatable");
            sqlUpdate("create table qatable (_id varchar(32), a varchar(20), b date, c int)");
            sqlUpdate("insert into qatable values ('X', 'Abcd', '2016-03-07',10)");

            table = this.schema.getTable("qatable");
            rows = table.select("_id, a, b, c").execute();
            List<Column> metadata = rows.getColumns();
            assertEquals(4, metadata.size());

            Column idCol = metadata.get(0);
            assertEquals("qatable", idCol.getTableLabel());

            Column aCol = metadata.get(1);
            assertEquals("qatable", aCol.getTableLabel());

            Column bCol = metadata.get(2);
            assertEquals("qatable", bCol.getTableLabel());

            Column cCol = metadata.get(3);
            assertEquals("qatable", cCol.getTableLabel());
            assertEquals(table.getName(), cCol.getTableLabel());

        } finally {
            sqlUpdate("drop table if exists qatable");
        }
    }

    @Test
    public void testGetColumnName() throws Exception {
        RowResult rows = null;
        Table table = null;
        try {
            sqlUpdate("drop table if exists qatable");
            sqlUpdate("create table qatable (_id varchar(32), a varchar(20), b date, c int)");
            sqlUpdate("insert into qatable values ('X', 'Abcd', '2016-03-07',10)");

            table = this.schema.getTable("qatable");
            rows = table.select("_id, a, b, c").execute();
            List<Column> metadata = rows.getColumns();
            assertEquals(4, metadata.size());

            Column idCol = metadata.get(0);
            assertEquals("_id", idCol.getColumnName());

            Column aCol = metadata.get(1);
            assertEquals("a", aCol.getColumnName());

            Column bCol = metadata.get(2);
            assertEquals("b", bCol.getColumnName());

            Column cCol = metadata.get(3);
            assertEquals("c", cCol.getColumnName());

        } finally {
            sqlUpdate("drop table if exists qatable");
        }
    }

    @Test
    public void testGetColumnLabel() throws Exception {
        RowResult rows = null;
        Table table = null;
        try {
            sqlUpdate("drop table if exists qatable");
            sqlUpdate("create table qatable (_id varchar(32), a varchar(20), b date, c int)");
            sqlUpdate("insert into qatable values ('X', 'Abcd', '2016-03-07',10)");

            table = this.schema.getTable("qatable");
            rows = table.select("_id as col1, a as `a+1`, b as `a 1 1`, c as `a``q`").execute();
            List<Column> metadata = rows.getColumns();
            assertEquals(4, metadata.size());

            Column idCol = metadata.get(0);
            assertEquals("_id", idCol.getColumnName());
            assertEquals("col1", idCol.getColumnLabel());

            Column aCol = metadata.get(1);
            assertEquals("a", aCol.getColumnName());
            assertEquals("a+1", aCol.getColumnLabel());

            Column bCol = metadata.get(2);
            assertEquals("b", bCol.getColumnName());
            assertEquals("a 1 1", bCol.getColumnLabel());

            Column cCol = metadata.get(3);
            assertEquals("c", cCol.getColumnName());
            assertEquals("a`q", cCol.getColumnLabel());

        } finally {
            sqlUpdate("drop table if exists qatable");
        }
    }

    @Test
    public void testGetType() throws Exception {
        RowResult rows = null;
        Table table = null;
        try {
            sqlUpdate("drop table if exists qatable");
            sqlUpdate("create table qatable (_id varchar(32), a char(20), b date, c int,d double,e datetime,f time,"
                    + "g linestring,h tinyint,i mediumint,j bigint,k float, l set('1','2'), m enum('1','2'),n decimal(20,10),o bit)");

            table = this.schema.getTable("qatable");
            table.insert("j").values(10).execute();

            rows = table.select("*").execute();
            List<Column> metadata = rows.getColumns();
            assertEquals(16, metadata.size());

            Column idCol = metadata.get(0);
            assertEquals(Type.STRING, idCol.getType());

            Column aCol = metadata.get(1);
            assertEquals(Type.STRING, aCol.getType());

            Column bCol = metadata.get(2);
            assertEquals(Type.DATE, bCol.getType());

            Column cCol = metadata.get(3);
            assertEquals(Type.INT, cCol.getType());

            Column dCol = metadata.get(4);
            assertEquals(Type.DOUBLE, dCol.getType());

            Column eCol = metadata.get(5);
            assertEquals(Type.DATETIME, eCol.getType());

            Column fCol = metadata.get(6);
            assertEquals(Type.TIME, fCol.getType());

            Column gCol = metadata.get(7);
            assertEquals(Type.GEOMETRY, gCol.getType());

            Column hCol = metadata.get(8);
            assertEquals(Type.TINYINT, hCol.getType());

            Column iCol = metadata.get(9);
            assertEquals(Type.MEDIUMINT, iCol.getType());

            Column jCol = metadata.get(10);
            assertEquals(Type.BIGINT, jCol.getType());

            Column kCol = metadata.get(11);
            assertEquals(Type.FLOAT, kCol.getType());

            Column lCol = metadata.get(12);
            assertEquals(Type.SET, lCol.getType());

            Column mCol = metadata.get(13);
            assertEquals(Type.ENUM, mCol.getType());

            Column nCol = metadata.get(14);
            assertEquals(Type.DECIMAL, nCol.getType());

            Column oCol = metadata.get(15);
            assertEquals(Type.BIT, oCol.getType());

        } finally {
            sqlUpdate("drop table if exists qatable");
        }
    }

    @Test
    public void testGetFractionalDigits() throws Exception {
        RowResult rows = null;
        Table table = null;
        try {
            sqlUpdate("drop table if exists qatable");
            sqlUpdate("create table qatable (_id varchar(32), a char(20), b date, c int,d double,e datetime,f time,"
                    + "g linestring,h tinyint,i mediumint,j bigint,k float, l set('1','2'), m enum('1','2'),n decimal(20,10),o bit)");

            table = this.schema.getTable("qatable");
            table.insert("h").values(10).execute();
            rows = table.select("*").execute();
            List<Column> metadata = rows.getColumns();
            assertEquals(16, metadata.size());

            Column idCol = metadata.get(0);
            assertEquals(0, idCol.getFractionalDigits());

            Column aCol = metadata.get(1);
            assertEquals(0, aCol.getFractionalDigits());

            Column bCol = metadata.get(2);
            assertEquals(0, bCol.getFractionalDigits());

            Column cCol = metadata.get(3);
            assertEquals(0, cCol.getFractionalDigits());

            Column dCol = metadata.get(4);
            assertEquals(0, dCol.getFractionalDigits());

            Column eCol = metadata.get(5);
            assertEquals(0, eCol.getFractionalDigits());

            Column fCol = metadata.get(6);
            assertEquals(0, fCol.getFractionalDigits());

            Column gCol = metadata.get(7);
            assertEquals(0, gCol.getFractionalDigits());

            Column hCol = metadata.get(8);
            assertEquals(0, hCol.getFractionalDigits());

            Column iCol = metadata.get(9);
            assertEquals(0, iCol.getFractionalDigits());

            Column jCol = metadata.get(10);
            assertEquals(0, jCol.getFractionalDigits());

            Column kCol = metadata.get(11);
            assertEquals(0, kCol.getFractionalDigits());

            Column lCol = metadata.get(12);
            assertEquals(0, lCol.getFractionalDigits());

            Column mCol = metadata.get(13);
            assertEquals(0, mCol.getFractionalDigits());

            Column nCol = metadata.get(14);
            assertEquals(10, nCol.getFractionalDigits());

            Column oCol = metadata.get(15);
            assertEquals(0, oCol.getFractionalDigits());
        } finally {
            sqlUpdate("drop table if exists qatable");
        }
    }

    @Test
    public void testIsNumberSigned() throws Exception {
        RowResult rows = null;
        Table table = null;
        try {
            sqlUpdate("drop table if exists qatable");
            sqlUpdate("create table qatable (_id varchar(32), a char(20), b date, c int,d double signed,e datetime,f time,"
                    + "g linestring,h tinyint unsigned,i mediumint,j bigint unsigned,k float, l set('1','2'), m enum('1','2'),n decimal(20,10),o bit)");

            table = this.schema.getTable("qatable");
            table.insert("i").values(10).execute();
            rows = table.select("*").execute();
            List<Column> metadata = rows.getColumns();
            assertEquals(16, metadata.size());

            Column idCol = metadata.get(0);
            assertEquals(false, idCol.isNumberSigned());

            Column aCol = metadata.get(1);
            assertEquals(false, aCol.isNumberSigned());

            Column bCol = metadata.get(2);
            assertEquals(false, bCol.isNumberSigned());

            Column cCol = metadata.get(3);
            assertEquals(true, cCol.isNumberSigned());

            Column dCol = metadata.get(4);
            assertEquals(true, dCol.isNumberSigned());

            Column eCol = metadata.get(5);
            assertEquals(false, eCol.isNumberSigned());

            Column fCol = metadata.get(6);
            assertEquals(false, fCol.isNumberSigned());

            Column gCol = metadata.get(7);
            assertEquals(false, gCol.isNumberSigned());

            Column hCol = metadata.get(8);
            assertEquals(false, hCol.isNumberSigned());

            Column iCol = metadata.get(9);
            assertEquals(true, iCol.isNumberSigned());

            Column jCol = metadata.get(10);
            assertEquals(false, jCol.isNumberSigned());

            Column kCol = metadata.get(11);
            assertEquals(true, kCol.isNumberSigned());

            Column lCol = metadata.get(12);
            assertEquals(false, lCol.isNumberSigned());

            Column mCol = metadata.get(13);
            assertEquals(false, mCol.isNumberSigned());

            Column nCol = metadata.get(14);
            assertEquals(true, nCol.isNumberSigned());

            Column oCol = metadata.get(15);
            assertEquals(false, oCol.isNumberSigned());
        } finally {
            sqlUpdate("drop table if exists qatable");
        }
    }

    @Test
    public void testGetLength() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        RowResult rows = null;
        Table table = null;
        try {
            sqlUpdate("drop database if exists lengthTest");
            sqlUpdate("create database lengthTest DEFAULT CHARACTER SET latin1");
            sqlUpdate("drop table if exists lengthTest.qatable");
            sqlUpdate("create table lengthTest.qatable (_id varchar(32), a char(20), b date, c int,d double,e datetime,f time,"
                    + "g linestring,h tinyint,i mediumint,j bigint,k float, l set('1','2'), m enum('1','2'),n decimal(20,10),o bit(3))");

            table = this.session.getSchema("lengthTest").getTable("qatable");
            table.insert("k").values(10).execute();
            rows = table.select("*").execute();
            List<Column> metadata = rows.getColumns();
            assertEquals(16, metadata.size());
            Column myCol = null;
            long[] fLen = { 32, 20, 10, 11, 22, 19, 10, 0, 4, 9, 20, 12, 0, 0, 22, 3 };
            for (int i = 0; i < 16; i++) {
                myCol = metadata.get(i);
                assertEquals(fLen[i], myCol.getLength());
            }
        } finally {
            sqlUpdate("drop database if exists lengthTest");
        }
    }

    /*
     * Create table with 3 fields with primary key on 2 fields.Insert a record.Select the record.Get and Validate primary key property using isPrimaryKey()
     * Create table with 2 fields.Mention 1st field as unique.Insert a record.Select the record.Get and Validate unique property using isUniqueKey()
     * Create table with 2 fields.Mention key on field.Insert a record.Select the record.Get and Validate whether column is part of key or not isPartKey()
     */
    @Test
    public void testIsPrimaryKeyAndisUniqueKeyAndisPartKey() throws Exception {
        RowResult rows = null;
        Table table = null;
        try {
            sqlUpdate("drop table if exists qatable");
            sqlUpdate("create table qatable (a int Not Null,b bigint unique,c char(30) Not Null unique,d tinyint,e json ,"
                    + "f INT GENERATED ALWAYS AS (JSON_EXTRACT(e, '$.id')),g BIGINT GENERATED ALWAYS AS (JSON_EXTRACT(e, '$.id2')) STORED NOT NULL ,"
                    + "key(b,g,f),primary key(a,c,d), INDEX i (f))");

            table = this.schema.getTable("qatable");
            table.insert("a", "c", "d", "e").values(1, "S", 10, "{\"id2\": \"12345677890123\", \"name\": \"Fred\"}").execute();
            rows = table.select("*").execute();
            List<Column> metadata = rows.getColumns();
            assertEquals(7, metadata.size());

            Column aCol = metadata.get(0);
            assertEquals(true, aCol.isPrimaryKey());
            assertEquals(false, aCol.isNullable());
            assertEquals(false, aCol.isUniqueKey());
            assertEquals(false, aCol.isPartKey());

            Column bCol = metadata.get(1);
            assertEquals(false, bCol.isPrimaryKey());
            assertEquals(true, bCol.isNullable());
            assertEquals(true, bCol.isUniqueKey());
            assertEquals(true, bCol.isPartKey());

            Column cCol = metadata.get(2);
            assertEquals(true, cCol.isPrimaryKey());
            assertEquals(false, cCol.isNullable());
            assertEquals(true, cCol.isUniqueKey());
            assertEquals(false, cCol.isPartKey());

            Column dCol = metadata.get(3);
            assertEquals(true, dCol.isPrimaryKey());
            assertEquals(false, dCol.isNullable());
            assertEquals(false, dCol.isUniqueKey());
            assertEquals(false, dCol.isPartKey());

            Column eCol = metadata.get(4);
            assertEquals(false, eCol.isPrimaryKey());
            assertEquals(true, eCol.isNullable());
            assertEquals(false, eCol.isUniqueKey());
            assertEquals(false, eCol.isPartKey());

            Column fCol = metadata.get(5);
            assertEquals(false, fCol.isPrimaryKey());
            assertEquals(true, fCol.isNullable());
            assertEquals(false, fCol.isUniqueKey());
            assertEquals(true, fCol.isPartKey());

            Column gCol = metadata.get(6);
            assertEquals(false, gCol.isPrimaryKey());
            assertEquals(false, gCol.isNullable());
            assertEquals(false, gCol.isUniqueKey());
            assertEquals(false, gCol.isPartKey());
        } finally {
            sqlUpdate("drop table if exists qatable");
        }
    }

    /*
     * Create a table with int datatypes.Insert a record.Do some operation on column and select the column using alias(eg: select x+y AS sum from tab).Get and
     * Validate the column info
     * Create a table with valid datatypes.Insert a record.Do some operation on column and select the column without using alias name(eg: select x+y from
     * tab).Get and Validate the column info
     */
    @Test
    public void testGetColumnNameAndgetColumnLabel() throws Exception {
        RowResult rows = null;
        Table table = null;
        try {
            sqlUpdate("drop table if exists qatable");
            sqlUpdate("create table qatable (a varchar(32), b bigint, c double, d int,`b+c+d`  DOUBLE AS (c+b+d))");
            sqlUpdate("set sql_mode='STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'");

            table = this.schema.getTable("qatable");
            table.insert("a", "b", "c", "d").values("AB", 12345677890123L, -12345677890123.9, 1).execute();
            rows = table.select("a as col1, b as c, c as b,d as col1,d+1 as `sum()`, `b+c+d`,sum(c),count(*)/-1").execute();
            List<Column> metadata = rows.getColumns();
            assertEquals(8, metadata.size());

            Column aCol = metadata.get(0);
            assertEquals("a", aCol.getColumnName());
            assertEquals("col1", aCol.getColumnLabel());

            Column bCol = metadata.get(1);
            assertEquals("b", bCol.getColumnName());
            assertEquals("c", bCol.getColumnLabel());

            Column cCol = metadata.get(2);
            assertEquals("c", cCol.getColumnName());
            assertEquals("b", cCol.getColumnLabel());

            Column dCol = metadata.get(3);
            assertEquals("d", dCol.getColumnName());
            assertEquals("col1", dCol.getColumnLabel());

            Column eCol = metadata.get(4);
            assertEquals("", eCol.getColumnName());
            assertEquals("sum()", eCol.getColumnLabel());

            Column fCol = metadata.get(5);
            assertEquals("b+c+d", fCol.getColumnName());
            assertEquals("b+c+d", fCol.getColumnLabel());

            Column gCol = metadata.get(6);
            assertEquals("sum(`c`)", gCol.getColumnLabel());
            assertEquals("", gCol.getColumnName());

            Column hCol = metadata.get(7);
            assertEquals("(count(*) / -1)", hCol.getColumnLabel());
        } finally {
            sqlUpdate("drop table if exists qatable");
        }
    }

    @Test
    public void testMultiSelects() throws Exception {
        RowResult rows = null;
        Table table1 = null;
        Table table2 = null;
        List<Column> metadata1 = null;
        List<Column> metadata2 = null;
        Column col1 = null;
        try {
            sqlUpdate("drop table if exists t1");
            sqlUpdate("create table t1 (a varchar(32), b bigint)");
            sqlUpdate("drop table if exists t2");
            sqlUpdate("create table t2 (a1 int, b1 double)");

            table1 = this.schema.getTable("t1");
            table2 = this.schema.getTable("t2");

            table1.insert("a", "b").values("AB", 12345677890123L).values("CD", 123456778901234L).values("EF", 1234567789012345L).execute();
            table2.insert("a1", "b1").values(1234, 4321.123).values(12345, 54321.123).values(123456, 654321.123).execute();

            //Using different table Object
            rows = table1.select("a,b as bigintcol").execute();
            metadata1 = rows.getColumns();
            assertEquals(2, metadata1.size());
            col1 = metadata1.get(0);
            assertEquals("a", col1.getColumnName());
            assertEquals("a", col1.getColumnLabel());

            rows = table2.select("a1 as intcol ,b1, a1+b1 as sum").execute();
            metadata2 = rows.getColumns();
            assertEquals(3, metadata2.size());
            col1 = metadata2.get(0);
            assertEquals("a1", col1.getColumnName());
            assertEquals("intcol", col1.getColumnLabel());

            col1 = metadata2.get(1);
            assertEquals("b1", col1.getColumnName());
            assertEquals("b1", col1.getColumnLabel());

            col1 = metadata1.get(1);
            assertEquals("b", col1.getColumnName());
            assertEquals("bigintcol", col1.getColumnLabel());

            col1 = metadata2.get(2);
            assertEquals("sum", col1.getColumnLabel());
            assertEquals("", col1.getColumnName());

            //Using Same table Object
            rows = table1.select("a,b as bigintcol").execute();
            metadata1 = rows.getColumns();
            assertEquals(2, metadata1.size());
            col1 = metadata1.get(0);
            assertEquals("a", col1.getColumnName());
            assertEquals("a", col1.getColumnLabel());

            rows = table1.select("a as bigintcol2 , a+b as sum2,b").execute();
            metadata2 = rows.getColumns();
            assertEquals(3, metadata2.size());
            col1 = metadata2.get(0);
            assertEquals("a", col1.getColumnName());
            assertEquals("bigintcol2", col1.getColumnLabel());

            col1 = metadata2.get(1);
            assertEquals("", col1.getColumnName());
            assertEquals("sum2", col1.getColumnLabel());

            col1 = metadata1.get(1);
            assertEquals("b", col1.getColumnName());
            assertEquals("bigintcol", col1.getColumnLabel());

            col1 = metadata2.get(2);
            assertEquals("b", col1.getColumnLabel());
            assertEquals("b", col1.getColumnName());
        } finally {
            sqlUpdate("drop table if exists t1");
            sqlUpdate("drop table if exists t2");
        }
    }

    @Test
    public void testMultiSelectsAsync() throws Exception {
        RowResult rows = null;
        Table table1 = null;
        Table table2 = null;
        List<Column> metadata1 = null;
        List<Column> metadata2 = null;
        Column col1 = null;
        CompletableFuture<RowResult> asyncRowRes = null;

        try {
            sqlUpdate("drop table if exists t1");
            sqlUpdate("create table t1 (a varchar(32), b bigint)");
            sqlUpdate("drop table if exists t2");
            sqlUpdate("create table t2 (a1 int, b1 double)");

            table1 = this.schema.getTable("t1");
            table2 = this.schema.getTable("t2");

            table1.insert("a", "b").values("AB", 12345677890123L).values("CD", 123456778901234L).values("EF", 1234567789012345L).execute();
            table2.insert("a1", "b1").values(1234, 4321.123).values(12345, 54321.123).values(123456, 654321.123).execute();

            //Using different table Object
            asyncRowRes = table1.select("a,b as bigintcol").executeAsync();
            rows = asyncRowRes.get();
            metadata1 = rows.getColumns();
            assertEquals(2, metadata1.size());
            col1 = metadata1.get(0);
            assertEquals("a", col1.getColumnName());
            assertEquals("a", col1.getColumnLabel());

            asyncRowRes = table2.select("a1 as intcol ,b1, a1+b1 as sum").executeAsync();
            rows = asyncRowRes.get();
            metadata2 = rows.getColumns();
            assertEquals(3, metadata2.size());
            col1 = metadata2.get(0);
            assertEquals("a1", col1.getColumnName());
            assertEquals("intcol", col1.getColumnLabel());

            col1 = metadata2.get(1);
            assertEquals("b1", col1.getColumnName());
            assertEquals("b1", col1.getColumnLabel());

            col1 = metadata1.get(1);
            assertEquals("b", col1.getColumnName());
            assertEquals("bigintcol", col1.getColumnLabel());

            col1 = metadata2.get(2);
            assertEquals("sum", col1.getColumnLabel());
            assertEquals("", col1.getColumnName());

            //Using Same table Object
            asyncRowRes = table1.select("a,b as bigintcol").executeAsync();
            rows = asyncRowRes.get();
            metadata1 = rows.getColumns();
            assertEquals(2, metadata1.size());
            col1 = metadata1.get(0);
            assertEquals("a", col1.getColumnName());
            assertEquals("a", col1.getColumnLabel());

            asyncRowRes = table1.select("a as bigintcol2 , a+b as sum2,b").executeAsync();
            rows = asyncRowRes.get();
            metadata2 = rows.getColumns();
            assertEquals(3, metadata2.size());
            col1 = metadata2.get(0);
            assertEquals("a", col1.getColumnName());
            assertEquals("bigintcol2", col1.getColumnLabel());

            col1 = metadata2.get(1);
            assertEquals("", col1.getColumnName());
            assertEquals("sum2", col1.getColumnLabel());

            col1 = metadata1.get(1);
            assertEquals("b", col1.getColumnName());
            assertEquals("bigintcol", col1.getColumnLabel());

            col1 = metadata2.get(2);
            assertEquals("b", col1.getColumnLabel());
            assertEquals("b", col1.getColumnName());

        } finally {
            sqlUpdate("drop table if exists t1");
            sqlUpdate("drop table if exists t2");
        }
    }

    /*
     * Create table with char and int field.Insert a record.Select the record.Get and Validate the column collation name using getCollationName()
     * Create table with char and int field.Insert a record.Select the record.Get and Validate the column charset name using getCharacterSetName()
     * Create table with a auto-increment and a normal column.Insert a record.Select the record.Get and Validate auto increment property using isAutoIncrement()
     */
    @Test
    public void testIsPaddedAndisNullableAndisAutoIncrement() throws Exception {
        RowResult rows = null;
        Table table = null;
        try {
            sqlUpdate("drop table if exists qatable");
            sqlUpdate("create table qatable (a int Not Null auto_increment,b char(10),c char(30) Not Null,d bigint  zerofill not null,primary key(a))");

            table = this.schema.getTable("qatable");
            table.insert("b", "c", "d").values("a", "s", 12345677890123L).execute();
            rows = table.select("*").execute();
            List<Column> metadata = rows.getColumns();
            assertEquals(4, metadata.size());

            Column aCol = metadata.get(0);
            assertEquals(false, aCol.isPadded());
            assertEquals(false, aCol.isNullable());
            assertEquals(true, aCol.isAutoIncrement());

            Column bCol = metadata.get(1);
            assertEquals(true, bCol.isPadded());
            assertEquals(true, bCol.isNullable());
            assertEquals(false, bCol.isAutoIncrement());

            Column cCol = metadata.get(2);
            assertEquals(true, cCol.isPadded());
            assertEquals(false, cCol.isNullable());
            assertEquals(false, cCol.isAutoIncrement());

            Column dCol = metadata.get(3);
            assertEquals(true, dCol.isPadded());
            assertEquals(false, dCol.isNullable());
            assertEquals(false, dCol.isAutoIncrement());
        } finally {
            sqlUpdate("drop table if exists qatable");
        }
    }

    @Test
    public void testWithUnsignedData() throws Exception {
        RowResult rows = null;
        Table table = null;
        Column myCol = null;
        try {
            char[] array = new char[1024 * 1024];
            Arrays.fill(array, 'X');
            char[] array2 = new char[256];
            Arrays.fill(array2, 'X');
            sqlUpdate("drop table if exists qatable");
            sqlUpdate(
                    "create table qatable (a int unsigned ,b bigint unsigned,c tinyint unsigned,d smallint unsigned, e float unsigned,f double unsigned, g TEXT,h MEDIUMINT unsigned)");

            table = this.schema.getTable("qatable");
            table.insert("a", "b", "c", "d").values(1, 10, 1, 321).execute();
            rows = table.select("*").execute();
            List<Column> metadata = rows.getColumns();
            assertEquals(8, metadata.size());

            myCol = metadata.get(0);
            assertEquals("a", myCol.getColumnName());
            assertEquals("a", myCol.getColumnLabel());
            assertEquals(table.getName(), myCol.getTableLabel());
            assertEquals(table.getName(), myCol.getTableName());
            assertEquals(Type.INT, myCol.getType());
            assertEquals(10, myCol.getLength());
            assertEquals(0, myCol.getFractionalDigits());
            assertEquals(false, myCol.isNumberSigned());
            assertEquals(false, myCol.isPadded());
            assertEquals(true, myCol.isNullable());

            myCol = metadata.get(1);
            assertEquals("b", myCol.getColumnName());
            assertEquals("b", myCol.getColumnLabel());
            assertEquals(table.getName(), myCol.getTableLabel());
            assertEquals(table.getName(), myCol.getTableName());
            assertEquals(Type.BIGINT, myCol.getType());
            assertEquals(20, myCol.getLength());
            assertEquals(0, myCol.getFractionalDigits());
            assertEquals(false, myCol.isNumberSigned());
            assertEquals(false, myCol.isPadded());
            assertEquals(true, myCol.isNullable());

            myCol = metadata.get(2);
            assertEquals("c", myCol.getColumnName());
            assertEquals("c", myCol.getColumnLabel());
            assertEquals(table.getName(), myCol.getTableLabel());
            assertEquals(table.getName(), myCol.getTableName());
            assertEquals(Type.TINYINT, myCol.getType());
            assertEquals(3, myCol.getLength());
            assertEquals(0, myCol.getFractionalDigits());
            assertEquals(false, myCol.isNumberSigned());
            assertEquals(false, myCol.isPadded());
            assertEquals(true, myCol.isNullable());

            myCol = metadata.get(3);
            assertEquals("d", myCol.getColumnName());
            assertEquals("d", myCol.getColumnLabel());
            assertEquals(table.getName(), myCol.getTableLabel());
            assertEquals(table.getName(), myCol.getTableName());
            assertEquals(Type.SMALLINT, myCol.getType());
            assertEquals(5, myCol.getLength());
            assertEquals(0, myCol.getFractionalDigits());
            assertEquals(false, myCol.isNumberSigned());
            assertEquals(false, myCol.isPadded());
            assertEquals(true, myCol.isNullable());

            myCol = metadata.get(4);
            assertEquals("e", myCol.getColumnName());
            assertEquals("e", myCol.getColumnLabel());
            assertEquals(table.getName(), myCol.getTableLabel());
            assertEquals(table.getName(), myCol.getTableName());
            assertEquals(Type.FLOAT, myCol.getType());
            assertEquals(12, myCol.getLength());
            assertEquals(0, myCol.getFractionalDigits());
            assertEquals(false, myCol.isNumberSigned());
            assertEquals(false, myCol.isPadded());
            assertEquals(true, myCol.isNullable());

            myCol = metadata.get(5);
            assertEquals("f", myCol.getColumnName());
            assertEquals("f", myCol.getColumnLabel());
            assertEquals(table.getName(), myCol.getTableLabel());
            assertEquals(table.getName(), myCol.getTableName());
            assertEquals(Type.DOUBLE, myCol.getType());
            assertEquals(22, myCol.getLength());
            assertEquals(0, myCol.getFractionalDigits());
            assertEquals(false, myCol.isNumberSigned());
            assertEquals(false, myCol.isPadded());
            assertEquals(true, myCol.isNullable());

            myCol = metadata.get(6);
            assertEquals("g", myCol.getColumnName());
            assertEquals("g", myCol.getColumnLabel());
            assertEquals(table.getName(), myCol.getTableLabel());
            assertEquals(table.getName(), myCol.getTableName());
            assertEquals(Type.STRING, myCol.getType());
            assertEquals(65535, myCol.getLength());
            assertEquals(0, myCol.getFractionalDigits());
            assertEquals(false, myCol.isNumberSigned());
            assertEquals(false, myCol.isPadded());
            assertEquals(true, myCol.isNullable());

            myCol = metadata.get(7);
            assertEquals("h", myCol.getColumnName());
            assertEquals("h", myCol.getColumnLabel());
            assertEquals(table.getName(), myCol.getTableLabel());
            assertEquals(table.getName(), myCol.getTableName());
            assertEquals(Type.MEDIUMINT, myCol.getType());
            assertEquals(8, myCol.getLength());
            assertEquals(0, myCol.getFractionalDigits());
            assertEquals(false, myCol.isNumberSigned());
            assertEquals(false, myCol.isPadded());
            assertEquals(true, myCol.isNullable());
        } finally {
            sqlUpdate("drop table if exists qatable");
        }
    }
}
