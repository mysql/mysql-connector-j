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

package testsuite.mysqlx.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.result.Row;
import com.mysql.cj.api.x.Warning;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.core.io.StringValueFactory;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqlx.DocFindParams;
import com.mysql.cj.mysqlx.FilterParams;
import com.mysql.cj.mysqlx.FindParams;
import com.mysql.cj.mysqlx.InsertParams;
import com.mysql.cj.mysqlx.MysqlxError;
import com.mysql.cj.mysqlx.TableFindParams;
import com.mysql.cj.mysqlx.UpdateSpec;
import com.mysql.cj.mysqlx.UpdateSpec.UpdateType;
import com.mysql.cj.mysqlx.io.MysqlxProtocol;

/**
 * Tests for protocol-level APIs against X Plugin via X Protocol.
 */
public class MysqlxProtocolTest extends InternalMysqlxBaseTestCase {
    private MysqlxProtocol protocol;

    @Before
    public void setupTestProtocol() {
        if (this.isSetForMySQLxTests) {
            this.protocol = createAuthenticatedTestProtocol();
        }
    }

    @After
    public void destroyTestProtocol() throws IOException {
        if (this.isSetForMySQLxTests) {
            try {
                this.protocol.sendSessionClose();
                this.protocol.readOk();
            } catch (Exception ex) {
                System.err.println("Exception during destroy");
                ex.printStackTrace();
            } finally {
                this.protocol.close();
            }
        }
    }

    /**
     * Test the create/drop collection admin commands.
     */
    @Test
    public void testCreateAndDropCollection() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
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
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.protocol.sendSqlStatement("select 'x' as y");
        assertTrue(this.protocol.hasResults());
        ArrayList<Field> metadata = this.protocol.readMetadata(DEFAULT_METADATA_CHARSET);
        assertEquals(1, metadata.size());
        Field f = metadata.get(0);
        // not an exhaustive metadata test
        assertEquals("y", f.getColumnLabel());
        assertEquals(MysqlaConstants.FIELD_TYPE_VARCHAR, f.getMysqlTypeId());
        Iterator<Row> rowInputStream = this.protocol.getRowInputStream(metadata);
        Row r = rowInputStream.next();
        String value = r.getValue(0, new StringValueFactory());
        assertEquals("x", value);
        this.protocol.readStatementExecuteOk();
    }

    @Test
    public void testAnotherBasicSqlQuery() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.protocol.sendSqlStatement("select 'x' as a_string, 42 as a_long, 7.6 as a_decimal union select 'y' as a_string, 11 as a_long, .1111 as a_decimal");
        assertTrue(this.protocol.hasResults());
        ArrayList<Field> metadata = this.protocol.readMetadata(DEFAULT_METADATA_CHARSET);
        assertEquals(3, metadata.size());
        assertEquals("a_string", metadata.get(0).getColumnLabel());
        assertEquals("a_long", metadata.get(1).getColumnLabel());
        assertEquals("a_decimal", metadata.get(2).getColumnLabel());
        assertEquals(MysqlaConstants.FIELD_TYPE_VARCHAR, metadata.get(0).getMysqlTypeId());
        assertEquals(MysqlaConstants.FIELD_TYPE_LONGLONG, metadata.get(1).getMysqlTypeId());
        assertEquals(MysqlaConstants.FIELD_TYPE_NEWDECIMAL, metadata.get(2).getMysqlTypeId());
        Iterator<Row> rowInputStream = this.protocol.getRowInputStream(metadata);

        // first row
        Row r = rowInputStream.next();
        String value = r.getValue(0, new StringValueFactory());
        assertEquals("x", value);
        value = r.getValue(1, new StringValueFactory());
        assertEquals("42", value);
        value = r.getValue(2, new StringValueFactory());
        assertEquals("7.6000", value); // TODO: zeroes ok here??? scale is adjusted to 4 due to ".1111" value in RS? not happening in decimal test down below

        // second row
        assertTrue(rowInputStream.hasNext());
        r = rowInputStream.next();
        value = r.getValue(0, new StringValueFactory());
        assertEquals("y", value);
        value = r.getValue(1, new StringValueFactory());
        assertEquals("11", value);
        value = r.getValue(2, new StringValueFactory());
        assertEquals("0.1111", value);

        assertFalse(rowInputStream.hasNext());
        this.protocol.readStatementExecuteOk();
    }

    /**
     * This tests that all types are decoded correctly. We retrieve them all as strings which happens after the decoding step. This is an exhaustive types of
     * type decoding and metadata from the server.
     */
    @Test
    public void testDecodingAllTypes() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
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
            assertEquals(MysqlaConstants.FIELD_TYPE_VARCHAR, metadata.get(0).getMysqlTypeId());
            assertEquals("some string", row.getValue(0, new StringValueFactory()));
        });
        tests.put("date('2015-03-22') as a_date", (metadata, row) -> {
            assertEquals("a_date", metadata.get(0).getColumnLabel());
            assertEquals(MysqlaConstants.FIELD_TYPE_DATETIME, metadata.get(0).getMysqlTypeId());
            assertEquals("2015-03-22", row.getValue(0, new StringValueFactory()));
        });
        tests.put("curtime() as curtime, cast(curtime() as char(8)) as curtime_string", (metadata, row) -> {
            assertEquals("curtime", metadata.get(0).getColumnLabel());
            assertEquals("curtime_string", metadata.get(1).getColumnLabel());
            assertEquals(MysqlaConstants.FIELD_TYPE_TIME, metadata.get(0).getMysqlTypeId());
            String curtimeString = row.getValue(1, new StringValueFactory());
            assertEquals(curtimeString, row.getValue(0, new StringValueFactory()));
        });
        tests.put("timestamp('2015-05-01 12:01:32') as a_datetime", (metadata, row) -> {
            assertEquals("a_datetime", metadata.get(0).getColumnLabel());
            assertEquals(MysqlaConstants.FIELD_TYPE_DATETIME, metadata.get(0).getMysqlTypeId());
            assertEquals("2015-05-01 12:01:32", row.getValue(0, new StringValueFactory()));
        });
        tests.put("cos(1) as a_double", (metadata, row) -> {
            assertEquals("a_double", metadata.get(0).getColumnLabel());
            assertEquals(MysqlaConstants.FIELD_TYPE_DOUBLE, metadata.get(0).getMysqlTypeId());
            // value is 0.5403023058681398. Test most of it
            assertTrue(row.getValue(0, new StringValueFactory()).startsWith("0.540302305868139"));
        });
        tests.put("2142 as an_int", (metadata, row) -> {
            assertEquals("an_int", metadata.get(0).getColumnLabel());
            assertEquals(MysqlaConstants.FIELD_TYPE_LONGLONG, metadata.get(0).getMysqlTypeId());
            assertEquals("2142", row.getValue(0, new StringValueFactory()));
        });
        tests.put("21.424 as decimal1, -1.0 as decimal2, -0.1 as decimal3, 1000.0 as decimal4", (metadata, row) -> {
            assertEquals("decimal1", metadata.get(0).getColumnLabel());
            assertEquals(MysqlaConstants.FIELD_TYPE_NEWDECIMAL, metadata.get(0).getMysqlTypeId());
            assertEquals("21.424", row.getValue(0, new StringValueFactory()));

            assertEquals("decimal2", metadata.get(1).getColumnLabel());
            assertEquals(MysqlaConstants.FIELD_TYPE_NEWDECIMAL, metadata.get(1).getMysqlTypeId());
            assertEquals("-1.0", row.getValue(1, new StringValueFactory()));

            assertEquals("decimal3", metadata.get(2).getColumnLabel());
            assertEquals(MysqlaConstants.FIELD_TYPE_NEWDECIMAL, metadata.get(2).getMysqlTypeId());
            assertEquals("-0.1", row.getValue(2, new StringValueFactory()));

            assertEquals("decimal4", metadata.get(3).getColumnLabel());
            assertEquals(MysqlaConstants.FIELD_TYPE_NEWDECIMAL, metadata.get(3).getMysqlTypeId());
            assertEquals("1000.0", row.getValue(3, new StringValueFactory()));
        });
        tests.put("9223372036854775807 as a_large_integer", (metadata, row) -> {
            // max signed 64bit integer
            assertEquals("a_large_integer", metadata.get(0).getColumnLabel());
            assertEquals(MysqlaConstants.FIELD_TYPE_LONGLONG, metadata.get(0).getMysqlTypeId());
            assertEquals("9223372036854775807", row.getValue(0, new StringValueFactory()));
        });
        tests.put("a_float, a_set, an_enum from xprotocol_types_test", (metadata, row) -> {
            assertEquals("a_float", metadata.get(0).getColumnLabel());
            assertEquals("xprotocol_types_test", metadata.get(0).getTableName());
            assertEquals("2.4200000762939453", row.getValue(0, new StringValueFactory()));

            assertEquals("a_set", metadata.get(1).getColumnLabel());
            assertEquals("xprotocol_types_test", metadata.get(1).getTableName());
            assertEquals("def,xyz", row.getValue(1, new StringValueFactory()));

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
            Iterator<Row> rowInputStream = this.protocol.getRowInputStream(metadata);
            t.getValue().accept(metadata, rowInputStream.next());
            this.protocol.readStatementExecuteOk();
        }
    }

    /**
     * Test DML that is executed with <i>StmtExecute</i> and does not return a result set.
     */
    @Test
    public void testSqlDml() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.protocol.sendSqlStatement("drop table if exists mysqlx_sqlDmlTest");
        assertFalse(this.protocol.hasResults());
        StatementExecuteOk response = this.protocol.readStatementExecuteOk();
        assertEquals(0, response.getRowsAffected());

        this.protocol.sendSqlStatement("create table mysqlx_sqlDmlTest (w int primary key auto_increment, x int) auto_increment = 7");
        assertFalse(this.protocol.hasResults());
        response = this.protocol.readStatementExecuteOk();
        assertEquals(0, response.getRowsAffected());

        this.protocol.sendSqlStatement("insert into mysqlx_sqlDmlTest (x) values (44),(29)");
        assertFalse(this.protocol.hasResults());
        response = this.protocol.readStatementExecuteOk();
        assertEquals(2, response.getRowsAffected());
        assertEquals(new Long(7), response.getLastInsertId());

        this.protocol.sendSqlStatement("drop table mysqlx_sqlDmlTest");
        assertFalse(this.protocol.hasResults());
        this.protocol.readStatementExecuteOk();
    }

    @Test
    public void testBasicCrudInsertFind() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        String collName = createTempTestCollection(this.protocol);

        String json = "{'_id': '85983efc2a9a11e5b345feff819cdc9f', 'testVal': 1, 'insertedBy': 'Jess'}".replaceAll("'", "\"");
        this.protocol.sendDocInsert(getTestDatabase(), collName, Arrays.asList(new String[] { json }));
        this.protocol.readStatementExecuteOk();

        FindParams findParams = new DocFindParams(getTestDatabase(), collName, "$.testVal = 2-1");
        this.protocol.sendFind(findParams);

        ArrayList<Field> metadata = this.protocol.readMetadata(DEFAULT_METADATA_CHARSET);
        Iterator<Row> ris = this.protocol.getRowInputStream(metadata);
        Row r = ris.next();
        assertEquals(json, r.getValue(0, new StringValueFactory()));
        this.protocol.readStatementExecuteOk();
    }

    @Test
    public void testMultiInsert() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        String collName = createTempTestCollection(this.protocol);

        List<String> stringDocs = new ArrayList<>();
        stringDocs.add("{'a': 'A', 'a1': 'A1', '_id': 'a'}");
        stringDocs.add("{'b': 'B', 'b2': 'B2', '_id': 'b'}");
        stringDocs.add("{'c': 'C', 'c3': 'C3', '_id': 'c'}");
        stringDocs = stringDocs.stream().map(s -> s.replaceAll("'", "\"")).collect(Collectors.toList());
        this.protocol.sendDocInsert(getTestDatabase(), collName, stringDocs);
        this.protocol.readStatementExecuteOk();

        FindParams findParams = new DocFindParams(getTestDatabase(), collName);
        findParams.setOrder("_id");
        this.protocol.sendFind(findParams);

        ArrayList<Field> metadata = this.protocol.readMetadata(DEFAULT_METADATA_CHARSET);
        Iterator<Row> ris = this.protocol.getRowInputStream(metadata);
        Row r = ris.next();
        assertEquals(stringDocs.get(0), r.getValue(0, new StringValueFactory()));
        r = ris.next();
        assertEquals(stringDocs.get(1), r.getValue(0, new StringValueFactory()));
        r = ris.next();
        assertEquals(stringDocs.get(2), r.getValue(0, new StringValueFactory()));
        this.protocol.readStatementExecuteOk();
    }

    @Test
    public void testDocUpdate() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        String collName = createTempTestCollection(this.protocol);

        String json = "{'_id': '85983efc2a9a11e5b345feff819cdc9f', 'testVal': '1', 'insertedBy': 'Jess'}".replaceAll("'", "\"");
        this.protocol.sendDocInsert(getTestDatabase(), collName, Arrays.asList(new String[] { json }));
        this.protocol.readStatementExecuteOk();

        List<UpdateSpec> updates = new ArrayList<>();
        updates.add(new UpdateSpec(UpdateType.ITEM_SET, "$.a").setValue("lemon"));
        updates.add(new UpdateSpec(UpdateType.ITEM_REMOVE, "$.insertedBy"));
        this.protocol.sendDocUpdates(new FilterParams(getTestDatabase(), collName, false), updates);
        this.protocol.readStatementExecuteOk();

        // verify
        this.protocol.sendFind(new DocFindParams(getTestDatabase(), collName));
        ArrayList<Field> metadata = this.protocol.readMetadata(DEFAULT_METADATA_CHARSET);
        Iterator<Row> ris = this.protocol.getRowInputStream(metadata);
        Row r = ris.next();
        assertEquals("{\"a\": \"lemon\", \"_id\": \"85983efc2a9a11e5b345feff819cdc9f\", \"testVal\": \"1\"}", r.getValue(0, new StringValueFactory()));
        this.protocol.readStatementExecuteOk();
    }

    @Test
    public void tableInsert() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.protocol.sendSqlStatement("drop table if exists tableInsert");
        this.protocol.readStatementExecuteOk();
        this.protocol.sendSqlStatement("create table tableInsert (x int, y varchar(20), z decimal(10, 2))");
        this.protocol.readStatementExecuteOk();
        InsertParams insertParams = new InsertParams();
        insertParams.setProjection(new String[] { "z", "x", "y" });
        insertParams.addRow(Arrays.asList("10.2", 40, "some string value"));
        insertParams.addRow(Arrays.asList("10.3", 50, "another string value"));
        this.protocol.sendRowInsert(getTestDatabase(), "tableInsert", insertParams);
        StatementExecuteOk ok = this.protocol.readStatementExecuteOk();
        assertEquals(2, ok.getRowsAffected());

        FindParams findParams = new TableFindParams(getTestDatabase(), "tableInsert");
        findParams.setOrder("x DESC");
        findParams.setFields("z, y, x");
        this.protocol.sendFind(findParams);

        ArrayList<Field> metadata = this.protocol.readMetadata(DEFAULT_METADATA_CHARSET);
        Iterator<Row> ris = this.protocol.getRowInputStream(metadata);
        Row r = ris.next();
        assertEquals("10.30", r.getValue(0, new StringValueFactory()));
        assertEquals("another string value", r.getValue(1, new StringValueFactory()));
        assertEquals("50", r.getValue(2, new StringValueFactory()));
        r = ris.next();
        assertEquals("10.20", r.getValue(0, new StringValueFactory()));
        assertEquals("some string value", r.getValue(1, new StringValueFactory()));
        assertEquals("40", r.getValue(2, new StringValueFactory()));
        this.protocol.readStatementExecuteOk();

        this.protocol.sendSqlStatement("drop table tableInsert");
        this.protocol.readStatementExecuteOk();
    }

    @Test
    public void testWarnings() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
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

    @Test
    public void testEnableDisableNotices() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.protocol.sendDisableNotices("warnings"); // TODO currently only "warnings" are allowed to be disabled
        this.protocol.readStatementExecuteOk();

        this.protocol.sendSqlStatement("select CAST('abc' as CHAR(1))");
        this.protocol.getRowInputStream(this.protocol.readMetadata(DEFAULT_METADATA_CHARSET)).next();
        StatementExecuteOk ok = this.protocol.readStatementExecuteOk();
        assertEquals(0, ok.getWarnings().size());

        // "produced_message" are already enabled, they're used here to check that multiple parameters are sent correctly
        this.protocol.sendEnableNotices("produced_message", "warnings");
        this.protocol.readStatementExecuteOk();

        this.protocol.sendSqlStatement("select CAST('abc' as CHAR(1))");
        this.protocol.getRowInputStream(this.protocol.readMetadata(DEFAULT_METADATA_CHARSET)).next();
        ok = this.protocol.readStatementExecuteOk();
        assertEquals(1, ok.getWarnings().size());
    }

    /**
     * This is a development method that will print a detailed result set for any command sent.
     */
    @Test
    public void testResultSet() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        // begin "send" stage, change this as necessary
        //this.protocol.sendListObjects(getTestDatabase());
        this.protocol.sendListNotices();

        // this will read the metadata and result and print all data
        ArrayList<Field> metadata = this.protocol.readMetadata(DEFAULT_METADATA_CHARSET);
        metadata.forEach(f -> {
            System.err.println("***************** field ****************");
            System.err.println("Field: " + f.getColumnLabel());
            System.err.println("Type: " + f.getMysqlTypeId());
            System.err.println("Encoding: " + f.getEncoding());
        });

        Iterator<Row> ris = this.protocol.getRowInputStream(metadata);
        ris.forEachRemaining(r -> {
            System.err.println("***************** row ****************");
            for (int i = 0; i < metadata.size(); ++i) {
                System.err.println(metadata.get(i).getColumnLabel() + ": " + r.getValue(i, new StringValueFactory()));
            }
        });

        this.protocol.readStatementExecuteOk();
    }

    @Test
    public void testCapabilities() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.protocol.getPluginVersion();
    }
}
