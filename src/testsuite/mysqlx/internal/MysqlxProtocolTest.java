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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.result.Row;
import com.mysql.cj.api.result.RowInputStream;
import com.mysql.cj.core.conf.DefaultPropertySet;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.core.io.StringValueFactory;
import com.mysql.cj.jdbc.Field;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqlx.MysqlxError;
import com.mysql.cj.mysqlx.io.MysqlxProtocol;

/**
 * Tests for protocol-level APIs against a running MySQL-X server.
 */
public class MysqlxProtocolTest extends BaseInternalMysqlxTest {
    /**
     * The default character set used to interpret metadata. Use <i>latin1</i> - MySQL's default. This value is provided by higher layers above the protocol so
     * we avoid issues by using only ASCII characters for metadata in these tests.
     */
    private static final String DEFAULT_METADATA_CHARSET = "latin1";

    public MysqlxProtocolTest() throws Exception {
    }

    @Test
    public void testBasicSaslPlainAuth() throws Exception {
        MysqlxProtocol protocol = getTestProtocol();
        protocol.sendSaslAuthStart(getTestUser(), getTestPassword(), getTestDatabase());
        protocol.readAuthenticateOk();
        protocol.close();
    }

    @Test
    public void testBasicSaslMysql41Auth() throws Exception {
        MysqlxProtocol protocol = getTestProtocol();
        protocol.sendSaslMysql41AuthStart();
        byte[] salt = protocol.readAuthenticateContinue();
        protocol.sendSaslMysql41AuthContinue(getTestUser(), getTestPassword(), salt, getTestDatabase());
        protocol.readAuthenticateOk();
        protocol.close();
    }

    @Test
    public void testBasicSaslPlainAuthFailure() throws Exception {
        MysqlxProtocol protocol = getTestProtocol();
        try {
            protocol.sendSaslAuthStart(getTestUser(), "com.mysql.cj.theWrongPassword", getTestDatabase());
            protocol.readAuthenticateOk();
            fail("Auth using wrong password should fail");
        } catch (Exception ex) {
            // TODO: need better exception type here with auth fail details?
            assertEquals("Unexpected message class. Expected 'AuthenticateOk' but actually received 'AuthenticateFail'", ex.getMessage());
        }
        protocol.close();
    }

    /**
     * Test the create/drop collection admin commands.
     */
    @Test
    public void testCreateAndDropCollection() throws Exception {
        MysqlxProtocol protocol = getAuthenticatedTestProtocol();
        try {
            protocol.sendCreateCollection(getTestDatabase(), "testCreateAndDropCollection");
            protocol.readStatementExecuteOk();
        } catch (MysqlxError err) {
            // leftovers, clean them up now
            if (err.getErrorCode() == MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR) {
                protocol.sendDropCollection(getTestDatabase(), "testCreateAndDropCollection");
                protocol.readStatementExecuteOk();
                // try again
                protocol.sendCreateCollection(getTestDatabase(), "testCreateAndDropCollection");
                protocol.readStatementExecuteOk();
            } else {
                throw err;
            }
        }
        // we don't verify the existence. That's the job of the server/xplugin
        protocol.sendDropCollection(getTestDatabase(), "testCreateAndDropCollection");
        protocol.readStatementExecuteOk();
        protocol.close();
    }

    @Test
    public void testTrivialSqlQuery() throws Exception {
        MysqlxProtocol protocol = getAuthenticatedTestProtocol();
        protocol.sendSqlStatement("select 'x' as y");
        assertTrue(protocol.hasResults());
        // TODO: ??? should this be INSIDE protocol?
        PropertySet propertySet = new DefaultPropertySet();
        // latin1 is MySQL default
        ArrayList<Field> metadata = protocol.readMetadata(propertySet, DEFAULT_METADATA_CHARSET);
        assertEquals(1, metadata.size());
        Field f = metadata.get(0);
        // not an exhaustive metadata test
        assertEquals("y", f.getColumnLabel());
        assertEquals(MysqlaConstants.FIELD_TYPE_VARCHAR, f.getMysqlType());
        RowInputStream rowInputStream = protocol.getRowInputStream(metadata);
        Row r = rowInputStream.readRow();
        String value = r.getValue(0, new StringValueFactory());
        assertEquals("x", value);
        protocol.readStatementExecuteOk();
        protocol.close();
    }

    @Test
    public void testAnotherBasicSqlQuery() throws Exception {
        MysqlxProtocol protocol = getAuthenticatedTestProtocol();
        protocol.sendSqlStatement("select 'x' as a_string, 42 as a_long, 7.6 as a_decimal union select 'y' as a_string, 11 as a_long, .1111 as a_decimal");
        assertTrue(protocol.hasResults());
        // TODO: ??? should this be INSIDE protocol?
        PropertySet propertySet = new DefaultPropertySet();
        // latin1 is MySQL default
        ArrayList<Field> metadata = protocol.readMetadata(propertySet, DEFAULT_METADATA_CHARSET);
        assertEquals(3, metadata.size());
        assertEquals("a_string", metadata.get(0).getColumnLabel());
        assertEquals("a_long", metadata.get(1).getColumnLabel());
        assertEquals("a_decimal", metadata.get(2).getColumnLabel());
        assertEquals(MysqlaConstants.FIELD_TYPE_VARCHAR, metadata.get(0).getMysqlType());
        assertEquals(MysqlaConstants.FIELD_TYPE_LONGLONG, metadata.get(1).getMysqlType());
        assertEquals(MysqlaConstants.FIELD_TYPE_NEW_DECIMAL, metadata.get(2).getMysqlType());
        RowInputStream rowInputStream = protocol.getRowInputStream(metadata);

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
        protocol.readStatementExecuteOk();
        protocol.close();
    }

    /**
     * This tests that all types are decoded correctly. We retrieve them all as strings which happens after the decoding step. This is an exhaustive types of
     * type decoding and metadata from the server.
     */
    @Test
    public void testDecodingAllTypes() throws Exception {
        MysqlxProtocol protocol = getAuthenticatedTestProtocol();

        // some types depend on this table
        protocol.sendSqlStatement("drop table if exists xprotocol_types_test");
        protocol.readStatementExecuteOk();
        String testTable = "create table xprotocol_types_test (";
        testTable += " a_float float";
        testTable += ",a_set SET('abc', 'def', 'xyz')";
        testTable += ",an_enum ENUM('enum value a', 'enum value b')";
        testTable += ",an_unsigned_int bigint unsigned";
        protocol.sendSqlStatement(testTable + ")");
        protocol.readStatementExecuteOk();
        protocol.sendSqlStatement("insert into xprotocol_types_test values ('2.42', 'xyz,def', 'enum value a', 9223372036854775808)");
        protocol.readStatementExecuteOk();

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
            protocol.sendSqlStatement("select " + t.getKey());
            //assertTrue(protocol.hasResults());
            // TODO: ??? should this be INSIDE protocol?
            PropertySet propertySet = new DefaultPropertySet();
            // latin1 is MySQL default
            ArrayList<Field> metadata = protocol.readMetadata(propertySet, DEFAULT_METADATA_CHARSET);
            RowInputStream rowInputStream = protocol.getRowInputStream(metadata);
            t.getValue().accept(metadata, rowInputStream.readRow());
            protocol.readStatementExecuteOk();
        }

        protocol.close();
    }

    /**
     * Test DML that is executed with <i>StmtExecute</i> and does not return a result set.
     */
    @Test
    public void testSqlDml() throws Exception {
        MysqlxProtocol protocol = getAuthenticatedTestProtocol();

        protocol.sendSqlStatement("drop table if exists mysqlx_sqlDmlTest");
        assertFalse(protocol.hasResults());
        StatementExecuteOk response = protocol.readStatementExecuteOk();
        // TODO: re-enable these when rowsaffected/etc are implement
        //assertEquals(new Long(0), response.getRowsAffected());

        protocol.sendSqlStatement("create table mysqlx_sqlDmlTest (w int primary key auto_increment, x int) auto_increment = 7");
        assertFalse(protocol.hasResults());
        response = protocol.readStatementExecuteOk();
        //assertEquals(new Long(0), response.getRowsAffected());

        protocol.sendSqlStatement("insert into mysqlx_sqlDmlTest (x) values (44),(29)");
        assertFalse(protocol.hasResults());
        response = protocol.readStatementExecuteOk();
        //assertEquals(new Long(2), response.getRowsAffected());
        //assertEquals(new Long(7), response.getLastInsertId());

        protocol.sendSqlStatement("drop table mysqlx_sqlDmlTest");
        assertFalse(protocol.hasResults());
        protocol.readStatementExecuteOk();

        protocol.close();
    }

    @Test
    public void testBasicCrudInsertFind() throws Exception {
        MysqlxProtocol protocol = getAuthenticatedTestProtocol();
        String collName = "testBasicCrudInsertFind";

        try {
            protocol.sendDropCollection(getTestDatabase(), collName);
            protocol.readStatementExecuteOk();
        } catch (MysqlxError err) {
            // ignore
        }
        protocol.sendCreateCollection(getTestDatabase(), collName);
        protocol.readStatementExecuteOk();

        protocol.sendDocumentInsert(getTestDatabase(), collName,
                "{'_id':'85983efc2a9a11e5b345feff819cdc9f', 'testVal':'1', 'insertedBy':'Jess'}".replaceAll("'", "\""));
        protocol.readStatementExecuteOk();

        // protocol.sendDropCollection(getTestDatabase(), collName);
        // protocol.readStatementExecuteOk();

        protocol.close();
    }
}
