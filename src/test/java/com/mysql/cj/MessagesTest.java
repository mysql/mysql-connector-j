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

package com.mysql.cj;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class MessagesTest {

    @Test
    public void testLocalizedErrorMessages() throws Exception {
        Exception ex = new Exception();

        assertEquals("The database URL cannot be null.", Messages.getString("ConnectionString.0"));
        assertEquals("Malformed database URL, failed to parse the main URL sections.", Messages.getString("ConnectionString.1"));
        assertEquals("Malformed database URL, failed to parse the URL authority segment 'Test'.",
                Messages.getString("ConnectionString.2", new Object[] { "Test" }));
        assertEquals("Failed to parse the host:port pair 'host:123'.", Messages.getString("ConnectionString.3", new Object[] { "host:123" }));
        assertEquals("Malformed database URL, failed to parse the connection string near 'Test'.",
                Messages.getString("ConnectionString.4", new Object[] { "Test" }));
        assertEquals("Connector/J cannot handle a database URL of type 'Test'.", Messages.getString("ConnectionString.5", new Object[] { "Test" }));
        assertEquals("Connector/J cannot handle a database URL of type 'Test' that takes 100 hosts.",
                Messages.getString("ConnectionString.6", new Object[] { "Test", 100 }));
        assertEquals("Malformed database URL, failed to parse the port '123' as a number.", Messages.getString("ConnectionString.7", new Object[] { 123 }));
        assertEquals("Illegal transformation to the 'Test' property. The value 'Ten' is not a valid number.",
                Messages.getString("ConnectionString.8", new Object[] { "Test", "Ten" }));
        assertEquals("Unable to create an instance of the specified  properties transform class.", Messages.getString("ConnectionString.9"));
        assertEquals("Can't find configuration template named 'Test'", Messages.getString("ConnectionString.10", new Object[] { "Test" }));
        assertEquals("Unable to load configuration template 'Test' due to underlying IOException",
                Messages.getString("ConnectionString.11", new Object[] { "Test" }));
        assertEquals("Illegal database URL, host 'Test1' is duplicated but 'Test2' connections can only handle one instance of each host:port pair.",
                Messages.getString("ConnectionString.12", new Object[] { "Test1", "Test2" }));
        assertEquals(
                "Illegal database URL, Host 'Test1' is duplicated in the combined hosts list (sources & replicas) but 'Test2' connections can only handle one instance of each host:port pair.",
                Messages.getString("ConnectionString.13", new Object[] { "Test1", "Test2" }));
        assertEquals("Illegal database URL, in a 'Test' multi-host connection it is required the same credentials in all hosts.",
                Messages.getString("ConnectionString.14", new Object[] { "Test" }));
        assertEquals("Illegal database URL, in a 'Test' multi-host connection it is required that all or none of the hosts set a \"priority\" value.",
                Messages.getString("ConnectionString.15", new Object[] { "Test" }));
        assertEquals("Illegal database URL, in a 'Test' multi-host connection the \"priority\" setting must be a value between 0 and 100.",
                Messages.getString("ConnectionString.16", new Object[] { "Test" }));

        assertEquals("Cannot load connection class because of underlying exception: " + ex.toString(),
                Messages.getString("NonRegisteringDriver.17", new Object[] { ex.toString() }));

        assertEquals("Unsupported character encoding 'Test'", Messages.getString("Field.12", new Object[] { "Test" }));
        assertEquals("Unsupported character encoding 'Test'", Messages.getString("StringUtils.0", new Object[] { "Test" }));

        assertEquals("indexToWriteAt must be >= 1", Messages.getString("Blob.0"));
        assertEquals("IO Error while writing bytes to blob", Messages.getString("Blob.1"));
        assertEquals("\"pos\" argument can not be < 1.", Messages.getString("Blob.2"));
        assertEquals("\"pos\" argument can not be larger than the BLOB's length.", Messages.getString("Blob.3"));
        assertEquals("\"pos\" + \"length\" arguments can not be larger than the BLOB's length.", Messages.getString("Blob.4"));
        assertEquals("\"len\" argument can not be < 1.", Messages.getString("Blob.5"));
        assertEquals("\"len\" argument can not be larger than the BLOB's length.", Messages.getString("Blob.6"));
        assertEquals("Invalid operation on closed BLOB", Messages.getString("Blob.7"));
        assertEquals("Requested stream length of Test2 is out of range, given blob length of Test0 and starting position of Test1.",
                Messages.getString("Blob.invalidStreamLength", new Object[] { "Test0", "Test1", "Test2" }));
        assertEquals("Position 'pos' can not be < 1 or > blob length.", Messages.getString("Blob.invalidStreamPos"));

        assertEquals("Emulated BLOB locators must come from a ResultSet with only one table selected, and all primary keys selected",
                Messages.getString("Blob.8"));
        assertEquals("BLOB data not found! Did primary keys change?", Messages.getString("Blob.9"));

        assertEquals("Unknown type '0' in column '1' of '2' in binary-encoded result set.", Messages.getString("MysqlIO.97", new Object[] { 0, 1, 2 }));

        assertEquals("No parameter named 'Test'", Messages.getString("CallableStatement.3", new Object[] { "Test" }));
        assertEquals("Parameter named 'Test' is not an OUT parameter", Messages.getString("CallableStatement.5", new Object[] { "Test" }));
        assertEquals("Can't find local placeholder mapping for parameter named 'Test'.", Messages.getString("CallableStatement.6", new Object[] { "Test" }));
        assertEquals("Parameter number 0 is not an OUT parameter", Messages.getString("CallableStatement.9", new Object[] { 0 }));
        assertEquals("Parameter index of 10 is out of range (1, 5)", Messages.getString("CallableStatement.11", new Object[] { 10, 5 }));
        assertEquals("Parameter 0 is not registered as an output parameter", Messages.getString("CallableStatement.21", new Object[] { 0 }));
        assertEquals("Can't set out parameters", Messages.getString("CallableStatement.24"));
        assertEquals("Can't call executeBatch() on CallableStatement with OUTPUT parameters", Messages.getString("CallableStatement.25"));

        assertEquals("Illegal starting position for search, '10'", Messages.getString("Clob.8", new Object[] { 10 }));

        assertEquals("Unknown Java encoding for the character set with index '1234'. Use the 'customCharsetMapping' property to force it.",
                Messages.getString("Connection.5", new Object[] { "1234" }));
        assertEquals(
                "Unknown character set index 'Test' received from server. The appropriate client character set can be forced via the 'characterEncoding' property.",
                Messages.getString("Connection.6", new Object[] { "Test" }));
        assertEquals("Can't map Test given for characterSetResults to a supported MySQL encoding.",
                Messages.getString("Connection.7", new Object[] { "Test" }));
        assertEquals(
                "Connection setting too low for 'maxAllowedPacket'. When 'useServerPrepStmts=true', 'maxAllowedPacket' must be higher than 10. Check also 'max_allowed_packet' in MySQL configuration files.",
                Messages.getString("Connection.15", new Object[] { 10 }));
        assertEquals("Savepoint 'Test' does not exist", Messages.getString("Connection.22", new Object[] { "Test" }));
        assertEquals("Unsupported transaction isolation level 'Test'", Messages.getString("Connection.25", new Object[] { "Test" }));

        assertEquals(
                "User does not have access to metadata required to determine stored procedure parameter types."
                        + " If rights can not be granted, configure connection with \"noAccessToProcedureBodies=true\" "
                        + "to have driver generate parameters that represent INOUT strings irregardless of actual parameter types.",
                Messages.getString("DatabaseMetaData.4"));

        assertEquals("Syntax error while processing {fn convert (... , ...)} token, missing opening parenthesis in token 'Test'.",
                Messages.getString("EscapeProcessor.4", new Object[] { "Test" }));
        assertEquals("Unsupported conversion type 'Test' found while processing escape token.",
                Messages.getString("EscapeProcessor.7", new Object[] { "Test" }));

        assertEquals("Can't perform requested operation after getResult() has been called to write XML data", Messages.getString("MysqlSQLXML.1"));

        assertEquals("Can't set IN parameter for return value of stored function call.", Messages.getString("PreparedStatement.63"));
        assertEquals("'Test' is not a valid numeric or approximate numeric value", Messages.getString("PreparedStatement.64", new Object[] { "Test" }));
        assertEquals("Can't set scale of 'Test1' for DECIMAL argument 'Test2'", Messages.getString("PreparedStatement.65", new Object[] { "Test1", "Test2" }));
        assertEquals("No conversion from Test to Types.BOOLEAN possible.", Messages.getString("PreparedStatement.66", new Object[] { "Test" }));

        assertEquals("Packet for query is too large (100 > 10). You can change this value on the server by setting the 'max_allowed_packet' variable.",
                Messages.getString("PacketTooBigException.0", new Object[] { 100, 10 }));

        assertEquals("Can't use configured regex due to underlying exception.", Messages.getString("ResultSetScannerInterceptor.1"));

        assertEquals("Can't set autocommit to 'true' on an XAConnection", Messages.getString("ConnectionWrapper.0"));
        assertEquals("Can't call commit() on an XAConnection associated with a global transaction", Messages.getString("ConnectionWrapper.1"));
        assertEquals("Can't call rollback() on an XAConnection associated with a global transaction", Messages.getString("ConnectionWrapper.2"));

        assertEquals("Illegal hour value '99' for java.sql.Time type in value 'Test'.", Messages.getString("TimeUtil.0", new Object[] { 99, "Test" }));
        assertEquals("Illegal minute value '99' for java.sql.Time type in value 'Test'.", Messages.getString("TimeUtil.1", new Object[] { 99, "Test" }));
        assertEquals("Illegal second value '99' for java.sql.Time type in value 'Test'.", Messages.getString("TimeUtil.2", new Object[] { 99, "Test" }));

        assertEquals("Can not call setNCharacterStream() when connection character set isn't UTF-8", Messages.getString("ServerPreparedStatement.28"));
        assertEquals("Can not call setNClob() when connection character set isn't UTF-8", Messages.getString("ServerPreparedStatement.29"));
        assertEquals("Can not call setNString() when connection character set isn't UTF-8", Messages.getString("ServerPreparedStatement.30"));

        assertEquals("Can not call getNCharacterStream() when field's charset isn't UTF-8", Messages.getString("ResultSet.11"));
        assertEquals("Can not call getNClob() when field's charset isn't UTF-8", Messages.getString("ResultSet.12"));
        assertEquals("Can not call getNString() when field's charset isn't UTF-8", Messages.getString("ResultSet.14"));
        assertEquals("Internal error - conversion method doesn't support this type", Messages.getString("ResultSet.15"));
        assertEquals("Can not call updateNCharacterStream() when field's character set isn't UTF-8", Messages.getString("ResultSet.16"));
        assertEquals("Can not call updateNClob() when field's character set isn't UTF-8", Messages.getString("ResultSet.17"));
        assertEquals("Can not call updateNString() when field's character set isn't UTF-8", Messages.getString("ResultSet.18"));

        // TODO: Extend for all escaped messages.
    }

}
