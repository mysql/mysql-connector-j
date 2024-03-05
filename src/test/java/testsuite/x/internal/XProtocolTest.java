/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package testsuite.x.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.x.StatementExecuteOkBuilder;
import com.mysql.cj.protocol.x.XMessageBuilder;
import com.mysql.cj.protocol.x.XProtocol;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.protocol.x.XProtocolRowInputStream;
import com.mysql.cj.protocol.x.XServerCapabilities;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.Row;
import com.mysql.cj.result.StringValueFactory;
import com.mysql.cj.xdevapi.DocFilterParams;
import com.mysql.cj.xdevapi.FilterParams;
import com.mysql.cj.xdevapi.InsertParams;
import com.mysql.cj.xdevapi.SqlResult;
import com.mysql.cj.xdevapi.SqlResultBuilder;
import com.mysql.cj.xdevapi.TableFilterParams;
import com.mysql.cj.xdevapi.UpdateSpec;
import com.mysql.cj.xdevapi.UpdateType;
import com.mysql.cj.xdevapi.Warning;
import com.mysql.cj.xdevapi.WarningImpl;

/**
 * Tests for protocol-level APIs against X Plugin via X Protocol.
 */
public class XProtocolTest extends InternalXBaseTestCase {

    private XProtocol protocol;
    private XMessageBuilder messageBuilder;

    @BeforeEach
    public void setupTestProtocol() {
        if (this.isSetForXTests) {
            this.protocol = createAuthenticatedTestProtocol(createTestProtocol(), this.testProperties);
            this.messageBuilder = (XMessageBuilder) this.protocol.getMessageBuilder();
        }
    }

    @AfterEach
    public void destroyTestProtocol() throws IOException {
        if (this.isSetForXTests && this.protocol != null) {
            try {
                this.protocol.close();
            } catch (Exception ex) {
                System.err.println("Exception during destroy");
                ex.printStackTrace();
            }
        }
    }

    /**
     * Test the create/drop collection admin commands.
     */
    @Test
    public void testCreateAndDropCollection() {
        assumeTrue(this.isSetForXTests, PropertyDefinitions.SYSP_testsuite_url_mysqlx + " must be set to run this test.");

        try {
            this.protocol.send(this.messageBuilder.buildCreateCollection(getTestDatabase(), "testCreateAndDropCollection"), 0);
            this.protocol.readQueryResult(new StatementExecuteOkBuilder());
        } catch (XProtocolError err) {
            // leftovers, clean them up now
            if (err.getErrorCode() == MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR) {
                this.protocol.send(this.messageBuilder.buildDropCollection(getTestDatabase(), "testCreateAndDropCollection"), 0);
                this.protocol.readQueryResult(new StatementExecuteOkBuilder());
                // try again
                this.protocol.send(this.messageBuilder.buildCreateCollection(getTestDatabase(), "testCreateAndDropCollection"), 0);
                this.protocol.readQueryResult(new StatementExecuteOkBuilder());
            } else {
                throw err;
            }
        }
        // we don't verify the existence. That's the job of the server/xplugin
        this.protocol.send(this.messageBuilder.buildDropCollection(getTestDatabase(), "testCreateAndDropCollection"), 0);
        this.protocol.readQueryResult(new StatementExecuteOkBuilder());
    }

    @Test
    public void testTrivialSqlQuery() {
        assumeTrue(this.isSetForXTests, PropertyDefinitions.SYSP_testsuite_url_mysqlx + " must be set to run this test.");

        this.protocol.send(this.messageBuilder.buildSqlStatement("select 'x' as y"), 0);
        assertTrue(this.protocol.hasResults());
        ColumnDefinition metadata = this.protocol.readMetadata();
        assertEquals(1, metadata.getFields().length);
        Field f = metadata.getFields()[0];
        // not an exhaustive metadata test
        assertEquals("y", f.getColumnLabel());
        assertEquals(MysqlType.FIELD_TYPE_VARCHAR, f.getMysqlTypeId());
        Iterator<Row> rowInputStream = new XProtocolRowInputStream(metadata, this.protocol, null);
        Row r = rowInputStream.next();
        String value = r.getValue(0, new StringValueFactory(this.protocol.getPropertySet()));
        assertEquals("x", value);
        this.protocol.readQueryResult(new StatementExecuteOkBuilder());
    }

    @Test
    public void testAnotherBasicSqlQuery() {
        assumeTrue(this.isSetForXTests, PropertyDefinitions.SYSP_testsuite_url_mysqlx + " must be set to run this test.");

        this.protocol.send(this.messageBuilder
                .buildSqlStatement("select 'x' as a_string, 42 as a_long, 7.6 as a_decimal union select 'y' as a_string, 11 as a_long, .1111 as a_decimal"), 0);
        assertTrue(this.protocol.hasResults());
        ColumnDefinition metadata = this.protocol.readMetadata();
        assertEquals(3, metadata.getFields().length);
        assertEquals("a_string", metadata.getFields()[0].getColumnLabel());
        assertEquals("a_long", metadata.getFields()[1].getColumnLabel());
        assertEquals("a_decimal", metadata.getFields()[2].getColumnLabel());
        assertEquals(MysqlType.FIELD_TYPE_VARCHAR, metadata.getFields()[0].getMysqlTypeId());
        assertEquals(MysqlType.FIELD_TYPE_LONGLONG, metadata.getFields()[1].getMysqlTypeId());
        assertEquals(MysqlType.FIELD_TYPE_NEWDECIMAL, metadata.getFields()[2].getMysqlTypeId());
        Iterator<Row> rowInputStream = new XProtocolRowInputStream(metadata, this.protocol, null);

        // first row
        Row r = rowInputStream.next();
        String value = r.getValue(0, new StringValueFactory(this.protocol.getPropertySet()));
        assertEquals("x", value);
        value = r.getValue(1, new StringValueFactory(this.protocol.getPropertySet()));
        assertEquals("42", value);
        value = r.getValue(2, new StringValueFactory(this.protocol.getPropertySet()));
        assertEquals("7.6000", value); // TODO: zeroes ok here??? scale is adjusted to 4 due to ".1111" value in RS? not happening in decimal test down below

        // second row
        assertTrue(rowInputStream.hasNext());
        r = rowInputStream.next();
        value = r.getValue(0, new StringValueFactory(this.protocol.getPropertySet()));
        assertEquals("y", value);
        value = r.getValue(1, new StringValueFactory(this.protocol.getPropertySet()));
        assertEquals("11", value);
        value = r.getValue(2, new StringValueFactory(this.protocol.getPropertySet()));
        assertEquals("0.1111", value);

        assertFalse(rowInputStream.hasNext());
        this.protocol.readQueryResult(new StatementExecuteOkBuilder());
    }

    /**
     * This tests that all types are decoded correctly. We retrieve them all as strings which happens after the decoding step. This is an exhaustive types of
     * type decoding and metadata from the server.
     */
    @Test
    public void testDecodingAllTypes() {
        assumeTrue(this.isSetForXTests, PropertyDefinitions.SYSP_testsuite_url_mysqlx + " must be set to run this test.");

        try {
            // some types depend on this table
            this.protocol.send(this.messageBuilder.buildSqlStatement("drop table if exists xprotocol_types_test"), 0);
            this.protocol.readQueryResult(new StatementExecuteOkBuilder());
            String testTable = "create table xprotocol_types_test (";
            testTable += " a_float float";
            testTable += ",a_set SET('abc', 'def', 'xyz')";
            testTable += ",an_enum ENUM('enum value a', 'enum value b')";
            testTable += ",an_unsigned_int bigint unsigned";
            this.protocol.send(this.messageBuilder.buildSqlStatement(testTable + ")"), 0);
            this.protocol.readQueryResult(new StatementExecuteOkBuilder());
            this.protocol.send(
                    this.messageBuilder.buildSqlStatement("insert into xprotocol_types_test values ('2.42', 'xyz,def', 'enum value a', 9223372036854775808)"),
                    0);
            this.protocol.readQueryResult(new StatementExecuteOkBuilder());

            Map<String, BiConsumer<ColumnDefinition, Row>> tests = new HashMap<>();
            tests.put("'some string' as a_string", (metadata, row) -> {
                assertEquals("a_string", metadata.getFields()[0].getColumnLabel());
                assertEquals(MysqlType.FIELD_TYPE_VARCHAR, metadata.getFields()[0].getMysqlTypeId());
                assertEquals("some string", row.getValue(0, new StringValueFactory(this.protocol.getPropertySet())));
            });
            tests.put("date('2015-03-22') as a_date", (metadata, row) -> {
                assertEquals("a_date", metadata.getFields()[0].getColumnLabel());
                assertEquals(MysqlType.FIELD_TYPE_DATETIME, metadata.getFields()[0].getMysqlTypeId());
                assertEquals("2015-03-22", row.getValue(0, new StringValueFactory(this.protocol.getPropertySet())));
            });
            tests.put("curtime() as curtime, cast(curtime() as char(8)) as curtime_string", (metadata, row) -> {
                assertEquals("curtime", metadata.getFields()[0].getColumnLabel());
                assertEquals("curtime_string", metadata.getFields()[1].getColumnLabel());
                assertEquals(MysqlType.FIELD_TYPE_TIME, metadata.getFields()[0].getMysqlTypeId());
                String curtimeString = row.getValue(1, new StringValueFactory(this.protocol.getPropertySet()));
                assertEquals(curtimeString, row.getValue(0, new StringValueFactory(this.protocol.getPropertySet())));
            });
            tests.put("timestamp('2015-05-01 12:01:32') as a_datetime", (metadata, row) -> {
                assertEquals("a_datetime", metadata.getFields()[0].getColumnLabel());
                assertEquals(MysqlType.FIELD_TYPE_DATETIME, metadata.getFields()[0].getMysqlTypeId());
                assertEquals("2015-05-01 12:01:32", row.getValue(0, new StringValueFactory(this.protocol.getPropertySet())));
            });
            tests.put("cos(1) as a_double", (metadata, row) -> {
                assertEquals("a_double", metadata.getFields()[0].getColumnLabel());
                assertEquals(MysqlType.FIELD_TYPE_DOUBLE, metadata.getFields()[0].getMysqlTypeId());
                // value is 0.5403023058681398. Test most of it
                assertTrue(row.getValue(0, new StringValueFactory(this.protocol.getPropertySet())).startsWith("0.540302305868139"));
            });
            tests.put("2142 as an_int", (metadata, row) -> {
                assertEquals("an_int", metadata.getFields()[0].getColumnLabel());
                assertEquals(MysqlType.FIELD_TYPE_LONGLONG, metadata.getFields()[0].getMysqlTypeId());
                assertEquals("2142", row.getValue(0, new StringValueFactory(this.protocol.getPropertySet())));
            });
            tests.put("21.424 as decimal1, -1.0 as decimal2, -0.1 as decimal3, 1000.0 as decimal4", (metadata, row) -> {
                assertEquals("decimal1", metadata.getFields()[0].getColumnLabel());
                assertEquals(MysqlType.FIELD_TYPE_NEWDECIMAL, metadata.getFields()[0].getMysqlTypeId());
                assertEquals("21.424", row.getValue(0, new StringValueFactory(this.protocol.getPropertySet())));

                assertEquals("decimal2", metadata.getFields()[1].getColumnLabel());
                assertEquals(MysqlType.FIELD_TYPE_NEWDECIMAL, metadata.getFields()[1].getMysqlTypeId());
                assertEquals("-1.0", row.getValue(1, new StringValueFactory(this.protocol.getPropertySet())));

                assertEquals("decimal3", metadata.getFields()[2].getColumnLabel());
                assertEquals(MysqlType.FIELD_TYPE_NEWDECIMAL, metadata.getFields()[2].getMysqlTypeId());
                assertEquals("-0.1", row.getValue(2, new StringValueFactory(this.protocol.getPropertySet())));

                assertEquals("decimal4", metadata.getFields()[3].getColumnLabel());
                assertEquals(MysqlType.FIELD_TYPE_NEWDECIMAL, metadata.getFields()[3].getMysqlTypeId());
                assertEquals("1000.0", row.getValue(3, new StringValueFactory(this.protocol.getPropertySet())));
            });
            tests.put("9223372036854775807 as a_large_integer", (metadata, row) -> {
                // max signed 64bit integer
                assertEquals("a_large_integer", metadata.getFields()[0].getColumnLabel());
                assertEquals(MysqlType.FIELD_TYPE_LONGLONG, metadata.getFields()[0].getMysqlTypeId());
                assertEquals("9223372036854775807", row.getValue(0, new StringValueFactory(this.protocol.getPropertySet())));
            });
            tests.put("a_float, a_set, an_enum from xprotocol_types_test", (metadata, row) -> {
                assertEquals("a_float", metadata.getFields()[0].getColumnLabel());
                assertEquals("xprotocol_types_test", metadata.getFields()[0].getTableName());
                assertEquals("2.4200000762939453", row.getValue(0, new StringValueFactory(this.protocol.getPropertySet())));

                assertEquals("a_set", metadata.getFields()[1].getColumnLabel());
                assertEquals("xprotocol_types_test", metadata.getFields()[1].getTableName());
                assertEquals("def,xyz", row.getValue(1, new StringValueFactory(this.protocol.getPropertySet())));

                assertEquals("an_enum", metadata.getFields()[2].getColumnLabel());
                assertEquals("xprotocol_types_test", metadata.getFields()[2].getTableName());
                assertEquals("enum value a", row.getValue(2, new StringValueFactory(this.protocol.getPropertySet())));
            });
            tests.put("an_unsigned_int from xprotocol_types_test", (metadata, row) -> {
                assertEquals("an_unsigned_int", metadata.getFields()[0].getColumnLabel());
                assertEquals("9223372036854775808", row.getValue(0, new StringValueFactory(this.protocol.getPropertySet())));
            });

            // runner for above tests
            for (Map.Entry<String, BiConsumer<ColumnDefinition, Row>> t : tests.entrySet()) {
                this.protocol.send(this.messageBuilder.buildSqlStatement("select " + t.getKey()), 0);
                assertTrue(this.protocol.hasResults());
                ColumnDefinition metadata = this.protocol.readMetadata();
                Iterator<Row> rowInputStream = new XProtocolRowInputStream(metadata, this.protocol, null);
                t.getValue().accept(metadata, rowInputStream.next());
                this.protocol.readQueryResult(new StatementExecuteOkBuilder());
            }
        } finally {
            this.protocol.send(this.messageBuilder.buildSqlStatement("drop table if exists xprotocol_types_test"), 0);
            this.protocol.readQueryResult(new StatementExecuteOkBuilder());
        }
    }

    /**
     * Test DML that is executed with <i>StmtExecute</i> and does not return a result set.
     */
    @Test
    public void testSqlDml() {
        assumeTrue(this.isSetForXTests, PropertyDefinitions.SYSP_testsuite_url_mysqlx + " must be set to run this test.");

        this.protocol.send(this.messageBuilder.buildSqlStatement("drop table if exists mysqlx_sqlDmlTest"), 0);
        assertFalse(this.protocol.hasResults());
        SqlResult res = this.protocol
                .readQueryResult(new SqlResultBuilder(this.protocol.getServerSession().getDefaultTimeZone(), this.protocol.getPropertySet()));
        assertEquals(0, res.getAffectedItemsCount());

        this.protocol.send(this.messageBuilder.buildSqlStatement("create table mysqlx_sqlDmlTest (w int primary key auto_increment, x int) auto_increment = 7"),
                0);
        assertFalse(this.protocol.hasResults());
        res = this.protocol.readQueryResult(new SqlResultBuilder(this.protocol.getServerSession().getDefaultTimeZone(), this.protocol.getPropertySet()));
        assertEquals(0, res.getAffectedItemsCount());

        this.protocol.send(this.messageBuilder.buildSqlStatement("insert into mysqlx_sqlDmlTest (x) values (44),(29)"), 0);
        assertFalse(this.protocol.hasResults());
        res = this.protocol.readQueryResult(new SqlResultBuilder(this.protocol.getServerSession().getDefaultTimeZone(), this.protocol.getPropertySet()));
        assertEquals(2, res.getAffectedItemsCount());
        assertEquals(new Long(7), res.getAutoIncrementValue());

        this.protocol.send(this.messageBuilder.buildSqlStatement("drop table mysqlx_sqlDmlTest"), 0);
        assertFalse(this.protocol.hasResults());
        this.protocol.readQueryResult(new StatementExecuteOkBuilder());
    }

    @Test
    public void testBasicCrudInsertFind() {
        assumeTrue(this.isSetForXTests, PropertyDefinitions.SYSP_testsuite_url_mysqlx + " must be set to run this test.");

        try {
            String collName = createTempTestCollection(this.protocol);

            String json = "{'_id': '85983efc2a9a11e5b345feff819cdc9f', 'testVal': 1, 'insertedBy': 'Jess'}".replaceAll("'", "\"");
            this.protocol.send(this.messageBuilder.buildDocInsert(getTestDatabase(), collName, Arrays.asList(new String[] { json }), false), 0);
            this.protocol.readQueryResult(new StatementExecuteOkBuilder());

            FilterParams filterParams = new DocFilterParams(getTestDatabase(), collName);
            filterParams.setCriteria("$.testVal = 2-1");
            this.protocol.send(this.messageBuilder.buildFind(filterParams), 0);

            ColumnDefinition metadata = this.protocol.readMetadata();
            Iterator<Row> ris = new XProtocolRowInputStream(metadata, this.protocol, null);
            Row r = ris.next();
            assertEquals(json, r.getValue(0, new StringValueFactory(this.protocol.getPropertySet())));
            this.protocol.readQueryResult(new StatementExecuteOkBuilder());
        } finally {
            dropTempTestCollection(this.protocol);
        }
    }

    @Test
    public void testMultiInsert() {
        assumeTrue(this.isSetForXTests, PropertyDefinitions.SYSP_testsuite_url_mysqlx + " must be set to run this test.");

        try {
            String collName = createTempTestCollection(this.protocol);

            List<String> stringDocs = new ArrayList<>();
            stringDocs.add("{'a': 'A', 'a1': 'A1', '_id': 'a'}");
            stringDocs.add("{'b': 'B', 'b2': 'B2', '_id': 'b'}");
            stringDocs.add("{'c': 'C', 'c3': 'C3', '_id': 'c'}");
            stringDocs = stringDocs.stream().map(s -> s.replaceAll("'", "\"")).collect(Collectors.toList());
            this.protocol.send(this.messageBuilder.buildDocInsert(getTestDatabase(), collName, stringDocs, false), 0);
            this.protocol.readQueryResult(new StatementExecuteOkBuilder());

            FilterParams filterParams = new DocFilterParams(getTestDatabase(), collName);
            filterParams.setOrder("_id");
            this.protocol.send(this.messageBuilder.buildFind(filterParams), 0);

            ColumnDefinition metadata = this.protocol.readMetadata();
            Iterator<Row> ris = new XProtocolRowInputStream(metadata, this.protocol, null);
            Row r = ris.next();
            assertEquals(stringDocs.get(0), r.getValue(0, new StringValueFactory(this.protocol.getPropertySet())));
            r = ris.next();
            assertEquals(stringDocs.get(1), r.getValue(0, new StringValueFactory(this.protocol.getPropertySet())));
            r = ris.next();
            assertEquals(stringDocs.get(2), r.getValue(0, new StringValueFactory(this.protocol.getPropertySet())));
            this.protocol.readQueryResult(new StatementExecuteOkBuilder());
        } finally {
            dropTempTestCollection(this.protocol);
        }
    }

    @Test
    public void testDocUpdate() {
        assumeTrue(this.isSetForXTests, PropertyDefinitions.SYSP_testsuite_url_mysqlx + " must be set to run this test.");

        try {
            String collName = createTempTestCollection(this.protocol);

            String json = "{'_id': '85983efc2a9a11e5b345feff819cdc9f', 'testVal': '1', 'insertedBy': 'Jess'}".replaceAll("'", "\"");
            this.protocol.send(this.messageBuilder.buildDocInsert(getTestDatabase(), collName, Arrays.asList(new String[] { json }), false), 0);
            this.protocol.readQueryResult(new StatementExecuteOkBuilder());

            List<UpdateSpec> updates = new ArrayList<>();
            updates.add(new UpdateSpec(UpdateType.ITEM_SET, "$.a").setValue("lemon"));
            updates.add(new UpdateSpec(UpdateType.ITEM_REMOVE, "$.insertedBy"));
            this.protocol.send(this.messageBuilder.buildDocUpdate(new DocFilterParams(getTestDatabase(), collName), updates), 0);
            this.protocol.readQueryResult(new StatementExecuteOkBuilder());

            // verify
            this.protocol.send(this.messageBuilder.buildFind(new DocFilterParams(getTestDatabase(), collName)), 0);
            ColumnDefinition metadata = this.protocol.readMetadata();
            Iterator<Row> ris = new XProtocolRowInputStream(metadata, this.protocol, null);
            Row r = ris.next();
            assertEquals("{\"a\": \"lemon\", \"_id\": \"85983efc2a9a11e5b345feff819cdc9f\", \"testVal\": \"1\"}",
                    r.getValue(0, new StringValueFactory(this.protocol.getPropertySet())));
            this.protocol.readQueryResult(new StatementExecuteOkBuilder());
        } finally {
            dropTempTestCollection(this.protocol);
        }
    }

    @Test
    public void tableInsert() {
        assumeTrue(this.isSetForXTests, PropertyDefinitions.SYSP_testsuite_url_mysqlx + " must be set to run this test.");

        this.protocol.send(this.messageBuilder.buildSqlStatement("drop table if exists tableInsert"), 0);
        this.protocol.readQueryResult(new StatementExecuteOkBuilder());
        this.protocol.send(this.messageBuilder.buildSqlStatement("create table tableInsert (x int, y varchar(20), z decimal(10, 2))"), 0);
        this.protocol.readQueryResult(new StatementExecuteOkBuilder());
        InsertParams insertParams = new InsertParams();
        insertParams.setProjection(new String[] { "z", "x", "y" });
        insertParams.addRow(Arrays.asList("10.2", 40, "some string value"));
        insertParams.addRow(Arrays.asList("10.3", 50, "another string value"));
        this.protocol.send(this.messageBuilder.buildRowInsert(getTestDatabase(), "tableInsert", insertParams), 0);
        SqlResult res = this.protocol
                .readQueryResult(new SqlResultBuilder(this.protocol.getServerSession().getDefaultTimeZone(), this.protocol.getPropertySet()));
        assertEquals(2, res.getAffectedItemsCount());

        FilterParams filterParams = new TableFilterParams(getTestDatabase(), "tableInsert");
        filterParams.setOrder("x DESC");
        filterParams.setFields("z, y, x");
        this.protocol.send(this.messageBuilder.buildFind(filterParams), 0);

        ColumnDefinition metadata = this.protocol.readMetadata();
        Iterator<Row> ris = new XProtocolRowInputStream(metadata, this.protocol, null);
        Row r = ris.next();
        assertEquals("10.30", r.getValue(0, new StringValueFactory(this.protocol.getPropertySet())));
        assertEquals("another string value", r.getValue(1, new StringValueFactory(this.protocol.getPropertySet())));
        assertEquals("50", r.getValue(2, new StringValueFactory(this.protocol.getPropertySet())));
        r = ris.next();
        assertEquals("10.20", r.getValue(0, new StringValueFactory(this.protocol.getPropertySet())));
        assertEquals("some string value", r.getValue(1, new StringValueFactory(this.protocol.getPropertySet())));
        assertEquals("40", r.getValue(2, new StringValueFactory(this.protocol.getPropertySet())));
        this.protocol.readQueryResult(new StatementExecuteOkBuilder());

        this.protocol.send(this.messageBuilder.buildSqlStatement("drop table tableInsert"), 0);
        this.protocol.readQueryResult(new StatementExecuteOkBuilder());
    }

    @Test
    public void testWarnings() {
        assumeTrue(this.isSetForXTests, PropertyDefinitions.SYSP_testsuite_url_mysqlx + " must be set to run this test.");

        this.protocol.send(this.messageBuilder.buildSqlStatement("explain select 1"), 0);
        this.protocol.readMetadata();
        this.protocol.drainRows();
        SqlResult res = this.protocol
                .readQueryResult(new SqlResultBuilder(this.protocol.getServerSession().getDefaultTimeZone(), this.protocol.getPropertySet()));
        Iterable<Warning> iterable = () -> res.getWarnings();
        List<Warning> warnings = StreamSupport.stream(iterable.spliterator(), false).map(WarningImpl::new).collect(Collectors.toList());

        assertEquals(1, warnings.size());
        Warning w = warnings.get(0);
        assertEquals(1, w.getLevel());
        assertEquals(1003, w.getCode());
        // this message format might change over time and have to be loosened up
        assertEquals("/* select#1 */ select 1 AS `1`", w.getMessage());
    }

    @Test
    public void testEnableDisableNotices() {
        assumeTrue(this.isSetForXTests, PropertyDefinitions.SYSP_testsuite_url_mysqlx + " must be set to run this test.");

        this.protocol.send(this.messageBuilder.buildDisableNotices("warnings"), 0); // TODO currently only "warnings" are allowed to be disabled
        this.protocol.readQueryResult(new StatementExecuteOkBuilder());

        this.protocol.send(this.messageBuilder.buildSqlStatement("select CAST('abc' as CHAR(1))"), 0);
        new XProtocolRowInputStream(this.protocol.readMetadata(), this.protocol, null).next();
        SqlResult res = this.protocol
                .readQueryResult(new SqlResultBuilder(this.protocol.getServerSession().getDefaultTimeZone(), this.protocol.getPropertySet()));
        assertEquals(0, res.getWarningsCount());

        // "produced_message" are already enabled, they're used here to check that multiple parameters are sent correctly
        this.protocol.send(this.messageBuilder.buildEnableNotices("produced_message", "warnings"), 0);

        this.protocol.readQueryResult(new StatementExecuteOkBuilder());

        this.protocol.send(this.messageBuilder.buildSqlStatement("select CAST('abc' as CHAR(1))"), 0);
        new XProtocolRowInputStream(this.protocol.readMetadata(), this.protocol, null).next();
        res = this.protocol.readQueryResult(new SqlResultBuilder(this.protocol.getServerSession().getDefaultTimeZone(), this.protocol.getPropertySet()));
        assertEquals(1, res.getWarningsCount());
    }

    /**
     * This is a development method that will print a detailed result set for any command sent.
     */
    @Test
    public void testResultSet() {
        assumeTrue(this.isSetForXTests, PropertyDefinitions.SYSP_testsuite_url_mysqlx + " must be set to run this test.");

        // begin "send" stage, change this as necessary
        this.protocol.send(this.messageBuilder.buildListNotices(), 0);

        // this will read the metadata and result and print all data
        ColumnDefinition metadata = this.protocol.readMetadata();
        Arrays.stream(metadata.getFields()).forEach(f -> {
            System.err.println("***************** field ****************");
            System.err.println("Field: " + f.getColumnLabel());
            System.err.println("Type: " + f.getMysqlTypeId());
            System.err.println("Encoding: " + f.getEncoding());
        });

        Iterator<Row> ris = new XProtocolRowInputStream(metadata, this.protocol, null);
        ris.forEachRemaining(r -> {
            System.err.println("***************** row ****************");
            for (int i = 0; i < metadata.getFields().length; ++i) {
                System.err.println(metadata.getFields()[i].getColumnLabel() + ": " + r.getValue(i, new StringValueFactory(this.protocol.getPropertySet())));
            }
        });

        this.protocol.readQueryResult(new StatementExecuteOkBuilder());
    }

    @Test
    public void testCapabilities() {
        assumeTrue(this.isSetForXTests, PropertyDefinitions.SYSP_testsuite_url_mysqlx + " must be set to run this test.");

        XServerCapabilities capabilities = (XServerCapabilities) this.protocol.getServerSession().getCapabilities();

        assertEquals("mysql", capabilities.getNodeType());
        assertTrue(capabilities.getTls());
        assertFalse(capabilities.getClientPwdExpireOk());
        assertTrue(capabilities.getAuthenticationMechanisms().contains("MYSQL41"));
        assertEquals("text", capabilities.getDocFormats());
    }

}
