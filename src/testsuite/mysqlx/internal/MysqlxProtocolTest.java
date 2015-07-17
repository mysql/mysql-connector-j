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

package testsuite.mysqlx.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.result.Row;
import com.mysql.cj.api.result.RowInputStream;
import com.mysql.cj.api.x.Warning;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.core.io.StringValueFactory;
import com.mysql.cj.jdbc.Field;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqlx.FilterParams;
import com.mysql.cj.mysqlx.MysqlxError;
import com.mysql.cj.mysqlx.io.MysqlxProtocol;

/**
 * Tests for protocol-level APIs against a running MySQL-X server.
 */
public class MysqlxProtocolTest extends BaseInternalMysqlxTest {
    private MysqlxProtocol protocol;

    @Before
    public void setupTestProtocol() {
        this.protocol = createAuthenticatedTestProtocol();
    }

    @After
    public void destroyTestProtocol() throws IOException {
        this.protocol.close();
    }

    /**
     * Create a temporary collection for testing.
     *
     * @return the temporary collection name
     */
    private String createTempTestCollection() {
        String collName = "protocol_test_collection";

        try {
            this.protocol.sendDropCollection(getTestDatabase(), collName);
            this.protocol.readStatementExecuteOk();
        } catch (MysqlxError err) {
            // ignore
        }
        this.protocol.sendCreateCollection(getTestDatabase(), collName);
        this.protocol.readStatementExecuteOk();

        return collName;
    }

    /**
     * Test the create/drop collection admin commands.
     */
    @Test
    public void testCreateAndDropCollection() {
        try {
            this.protocol.sendCreateCollection(getTestDatabase(), "testCreateAndDropCollection");
            this.protocol.readStatementExecuteOk();
        } catch (MysqlxError err) {
            // leftovers, clean them up now
            if (err.getErrorCode() == MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR) {
                this.protocol.sendDropCollection(getTestDatabase(), "testCreateAndDropCollection");
                this.protocol.readStatementExecuteOk();
                // try again
                this.protocol.sendCreateCollection(getTestDatabase(), "testCreateAndDropCollection");
                this.protocol.readStatementExecuteOk();
            } else {
                throw err;
            }
        }
        // we don't verify the existence. That's the job of the server/xplugin
        this.protocol.sendDropCollection(getTestDatabase(), "testCreateAndDropCollection");
        this.protocol.readStatementExecuteOk();
    }

    @Test
    public void testTrivialSqlQuery() {
        this.protocol.sendSqlStatement("select 'x' as y");
        assertTrue(this.protocol.hasResults());
        ArrayList<Field> metadata = this.protocol.readMetadata(DEFAULT_METADATA_CHARSET);
        assertEquals(1, metadata.size());
        Field f = metadata.get(0);
        // not an exhaustive metadata test
        assertEquals("y", f.getColumnLabel());
        assertEquals(MysqlaConstants.FIELD_TYPE_VARCHAR, f.getMysqlType());
        RowInputStream rowInputStream = this.protocol.getRowInputStream(metadata);
        Row r = rowInputStream.readRow();
        String value = r.getValue(0, new StringValueFactory());
        assertEquals("x", value);
        this.protocol.readStatementExecuteOk();
    }

    @Test
    public void testAnotherBasicSqlQuery() {
        this.protocol.sendSqlStatement("select 'x' as a_string, 42 as a_long, 7.6 as a_decimal union select 'y' as a_string, 11 as a_long, .1111 as a_decimal");
        assertTrue(this.protocol.hasResults());
        ArrayList<Field> metadata = this.protocol.readMetadata(DEFAULT_METADATA_CHARSET);
        assertEquals(3, metadata.size());
        assertEquals("a_string", metadata.get(0).getColumnLabel());
        assertEquals("a_long", metadata.get(1).getColumnLabel());
        assertEquals("a_decimal", metadata.get(2).getColumnLabel());
        assertEquals(MysqlaConstants.FIELD_TYPE_VARCHAR, metadata.get(0).getMysqlType());
        assertEquals(MysqlaConstants.FIELD_TYPE_LONGLONG, metadata.get(1).getMysqlType());
        assertEquals(MysqlaConstants.FIELD_TYPE_NEW_DECIMAL, metadata.get(2).getMysqlType());
        RowInputStream rowInputStream = this.protocol.getRowInputStream(metadata);

        // first row
        Row r = rowInputStream.readRow();
        String value = r.getValue(0, new StringValueFactory());
        assertEquals("x", value);
        value = r.getValue(1, new StringValueFactory());
        assertEquals("42", value);
        value = r.getValue(2, new StringValueFactory());
        assertEquals("7.6000", value); // TODO: zeroes ok here??? scale is adjusted to 4 due to ".1111" value in RS? not happening in decimal test down below

        // second row
        r = rowInputStream.readRow();
        value = r.getValue(0, new StringValueFactory());
        assertEquals("y", value);
        value = r.getValue(1, new StringValueFactory());
        assertEquals("11", value);
        value = r.getValue(2, new StringValueFactory());
        assertEquals("0.1111", value);

        assertNull(rowInputStream.readRow());
        this.protocol.readStatementExecuteOk();
    }

    /**
     * This tests that all types are decoded correctly. We retrieve them all as strings which happens after the decoding step. This is an exhaustive types of
     * type decoding and metadata from the server.
     */
    @Test
    public void testDecodingAllTypes() {
        // some types depend on this table
        this.protocol.sendSqlStatement("drop table if exists xprotocol_types_test");
        this.protocol.readStatementExecuteOk();
        String testTable = "create table xprotocol_types_test (";
        testTable += " a_float float";
        testTable += ",a_set SET('abc', 'def', 'xyz')";
        testTable += ",an_enum ENUM('enum value a', 'enum value b')";
        testTable += ",an_unsigned_int bigint unsigned";
        this.protocol.sendSqlStatement(testTable + ")");
        this.protocol.readStatementExecuteOk();
        this.protocol.sendSqlStatement("insert into xprotocol_types_test values ('2.42', 'xyz,def', 'enum value a', 9223372036854775808)");
        this.protocol.readStatementExecuteOk();

        Map<String, BiConsumer<ArrayList<Field>, Row>> tests = new HashMap<>();
        tests.put("'some string' as a_string", (metadata, row) -> {
                    assertEquals("a_string", metadata.get(0).getColumnLabel());
                    assertEquals(MysqlaConstants.FIELD_TYPE_VARCHAR, metadata.get(0).getMysqlType());
                    assertEquals("some string", row.getValue(0, new StringValueFactory()));
                });
        tests.put("date('2015-03-22') as a_date", (metadata, row) -> {
                    assertEquals("a_date", metadata.get(0).getColumnLabel());
                    assertEquals(MysqlaConstants.FIELD_TYPE_DATETIME, metadata.get(0).getMysqlType());
                    assertEquals("2015-03-22", row.getValue(0, new StringValueFactory()));
                });
        tests.put("curtime() as curtime, cast(curtime() as char(8)) as curtime_string", (metadata, row) -> {
                    assertEquals("curtime", metadata.get(0).getColumnLabel());
                    assertEquals("curtime_string", metadata.get(1).getColumnLabel());
                    assertEquals(MysqlaConstants.FIELD_TYPE_TIME, metadata.get(0).getMysqlType());
                    String curtimeString = row.getValue(1, new StringValueFactory());
                    assertEquals(curtimeString, row.getValue(0, new StringValueFactory()));
                });
        tests.put("timestamp('2015-05-01 12:01:32') as a_datetime", (metadata, row) -> {
                    assertEquals("a_datetime", metadata.get(0).getColumnLabel());
                    assertEquals(MysqlaConstants.FIELD_TYPE_DATETIME, metadata.get(0).getMysqlType());
                    assertEquals("2015-05-01 12:01:32", row.getValue(0, new StringValueFactory()));
                });
        tests.put("cos(1) as a_double", (metadata, row) -> {
                    assertEquals("a_double", metadata.get(0).getColumnLabel());
                    assertEquals(MysqlaConstants.FIELD_TYPE_DOUBLE, metadata.get(0).getMysqlType());
                    assertEquals("0.5403023058681398", row.getValue(0, new StringValueFactory()));
                });
        tests.put("2142 as an_int", (metadata, row) -> {
                    assertEquals("an_int", metadata.get(0).getColumnLabel());
                    assertEquals(MysqlaConstants.FIELD_TYPE_LONGLONG, metadata.get(0).getMysqlType());
                    assertEquals("2142", row.getValue(0, new StringValueFactory()));
                });
        tests.put("21.424 as decimal1, -1.0 as decimal2, -0.1 as decimal3, 1000.0 as decimal4", (metadata, row) -> {
                    assertEquals("decimal1", metadata.get(0).getColumnLabel());
                    assertEquals(MysqlaConstants.FIELD_TYPE_NEW_DECIMAL, metadata.get(0).getMysqlType());
                    assertEquals("21.424", row.getValue(0, new StringValueFactory()));

                    assertEquals("decimal2", metadata.get(1).getColumnLabel());
                    assertEquals(MysqlaConstants.FIELD_TYPE_NEW_DECIMAL, metadata.get(1).getMysqlType());
                    assertEquals("-1.0", row.getValue(1, new StringValueFactory()));

                    assertEquals("decimal3", metadata.get(2).getColumnLabel());
                    assertEquals(MysqlaConstants.FIELD_TYPE_NEW_DECIMAL, metadata.get(2).getMysqlType());
                    assertEquals("-0.1", row.getValue(2, new StringValueFactory()));

                    assertEquals("decimal4", metadata.get(3).getColumnLabel());
                    assertEquals(MysqlaConstants.FIELD_TYPE_NEW_DECIMAL, metadata.get(3).getMysqlType());
                    assertEquals("1000.0", row.getValue(3, new StringValueFactory()));
                });
        tests.put("9223372036854775807 as a_large_integer", (metadata, row) -> {
                    // max signed 64bit integer
                    assertEquals("a_large_integer", metadata.get(0).getColumnLabel());
                    assertEquals(MysqlaConstants.FIELD_TYPE_LONGLONG, metadata.get(0).getMysqlType());
                    assertEquals("9223372036854775807", row.getValue(0, new StringValueFactory()));
                });
        tests.put("a_float, a_set, an_enum from xprotocol_types_test", (metadata, row) -> {
                    assertEquals("a_float", metadata.get(0).getColumnLabel());
                    assertEquals("xprotocol_types_test", metadata.get(0).getTableName());
                    assertEquals("2.4200000762939453", row.getValue(0, new StringValueFactory()));

                    assertEquals("a_set", metadata.get(1).getColumnLabel());
                    assertEquals("xprotocol_types_test", metadata.get(1).getTableName());
                    assertEquals("def\0xyz", row.getValue(1, new StringValueFactory()));

                    assertEquals("an_enum", metadata.get(2).getColumnLabel());
                    assertEquals("xprotocol_types_test", metadata.get(2).getTableName());
                    assertEquals("enum value a", row.getValue(2, new StringValueFactory()));
                });
        tests.put("an_unsigned_int from xprotocol_types_test", (metadata, row) -> {
                    assertEquals("an_unsigned_int", metadata.get(0).getColumnLabel());
                    assertEquals("9223372036854775808", row.getValue(0, new StringValueFactory()));
                });

        // runner for above tests
        for (Map.Entry<String, BiConsumer<ArrayList<Field>, Row>> t : tests.entrySet()) {
            this.protocol.sendSqlStatement("select " + t.getKey());
            assertTrue(this.protocol.hasResults());
            ArrayList<Field> metadata = this.protocol.readMetadata(DEFAULT_METADATA_CHARSET);
            RowInputStream rowInputStream = this.protocol.getRowInputStream(metadata);
            t.getValue().accept(metadata, rowInputStream.readRow());
            this.protocol.readStatementExecuteOk();
        }
    }

    /**
     * Test DML that is executed with <i>StmtExecute</i> and does not return a result set.
     */
    @Test
    public void testSqlDml() {
        this.protocol.sendSqlStatement("drop table if exists mysqlx_sqlDmlTest");
        assertFalse(this.protocol.hasResults());
        StatementExecuteOk response = this.protocol.readStatementExecuteOk();
        // TODO: re-enable these when rowsaffected/etc are implement
        //assertEquals(new Long(0), response.getRowsAffected());

        this.protocol.sendSqlStatement("create table mysqlx_sqlDmlTest (w int primary key auto_increment, x int) auto_increment = 7");
        assertFalse(this.protocol.hasResults());
        response = this.protocol.readStatementExecuteOk();
        //assertEquals(new Long(0), response.getRowsAffected());

        this.protocol.sendSqlStatement("insert into mysqlx_sqlDmlTest (x) values (44),(29)");
        assertFalse(this.protocol.hasResults());
        response = this.protocol.readStatementExecuteOk();
        //assertEquals(new Long(2), response.getRowsAffected());
        //assertEquals(new Long(7), response.getLastInsertId());

        this.protocol.sendSqlStatement("drop table mysqlx_sqlDmlTest");
        assertFalse(this.protocol.hasResults());
        this.protocol.readStatementExecuteOk();
    }

    @Test
    public void testBasicCrudInsertFind() {
        String collName = createTempTestCollection();

        String json = "{'_id': '85983efc2a9a11e5b345feff819cdc9f', 'testVal': '1', 'insertedBy': 'Jess'}".replaceAll("'", "\"");
        this.protocol.sendDocInsert(getTestDatabase(), collName, json);
        this.protocol.readStatementExecuteOk();

        FilterParams filterParams = new FilterParams("@.testVal = 2-1");
        this.protocol.sendDocFind(getTestDatabase(), collName, filterParams);

        ArrayList<Field> metadata = this.protocol.readMetadata(DEFAULT_METADATA_CHARSET);
        RowInputStream ris = this.protocol.getRowInputStream(metadata);
        Row r = ris.readRow();
        assertEquals(json, r.getValue(0, new StringValueFactory()));
        this.protocol.readStatementExecuteOk();
    }

    @Test
    public void testMultiInsert() {
        String collName = createTempTestCollection();

        List<String> stringDocs = new ArrayList<>();
        stringDocs.add("{'a': 'A', 'a1': 'A1', '_id': 'a'}");
        stringDocs.add("{'b': 'B', 'b2': 'B2', '_id': 'b'}");
        stringDocs.add("{'c': 'C', 'c3': 'C3', '_id': 'c'}");
        stringDocs = stringDocs.stream().map(s -> s.replaceAll("'", "\"")).collect(Collectors.toList());
        this.protocol.sendDocInsert(getTestDatabase(), collName, stringDocs);
        this.protocol.readStatementExecuteOk();

        FilterParams filterParams = new FilterParams();
        filterParams.setOrder("_id");
        this.protocol.sendDocFind(getTestDatabase(), collName, filterParams);

        ArrayList<Field> metadata = this.protocol.readMetadata(DEFAULT_METADATA_CHARSET);
        RowInputStream ris = this.protocol.getRowInputStream(metadata);
        Row r = ris.readRow();
        assertEquals(stringDocs.get(0), r.getValue(0, new StringValueFactory()));
        r = ris.readRow();
        assertEquals(stringDocs.get(1), r.getValue(0, new StringValueFactory()));
        r = ris.readRow();
        assertEquals(stringDocs.get(2), r.getValue(0, new StringValueFactory()));
        this.protocol.readStatementExecuteOk();
    }

    @Test
    public void testWarnings() {
        this.protocol.sendSqlStatement("explain select 1");
        this.protocol.readMetadata(DEFAULT_METADATA_CHARSET);
        this.protocol.drainRows();
        StatementExecuteOk ok = this.protocol.readStatementExecuteOk();
        List<Warning> warnings = ok.getWarnings();
        assertEquals(1, warnings.size());
        Warning w = warnings.get(0);
        assertEquals(1, w.getLevel());
        assertEquals(1003, w.getCode());
        // this message format might change over time and have to be loosened up
        assertEquals("/* select#1 */ select 1 AS `1`", w.getMessage());
    }

    /**
     * This is a development method that will print a detailed result set for any command sent.
     */
    @Test
    public void testResultSet() {
        // begin "send" stage, change this as necessary
        //this.protocol.sendListObjects(getTestDatabase());
        this.protocol.sendListNotices();

        // this will read the metadata and result and print all data
        ArrayList<Field> metadata = this.protocol.readMetadata(DEFAULT_METADATA_CHARSET);
        metadata.forEach(f -> {
                    System.err.println("***************** field ****************");
                    System.err.println("Field: " + f.getColumnLabel());
                    System.err.println("Type: " + f.getMysqlType());
                    System.err.println("Encoding: " + f.getEncoding());
                });
        RowInputStream ris = this.protocol.getRowInputStream(metadata);

        ris.forEach(r -> {
                    System.err.println("***************** row ****************");
                    for (int i = 0; i < metadata.size(); ++i) {
                        System.err.println(metadata.get(i).getColumnLabel() + ": " + r.getValue(i, new StringValueFactory()));
                    }
                });
        this.protocol.readStatementExecuteOk();
    }
}
