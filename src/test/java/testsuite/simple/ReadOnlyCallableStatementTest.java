/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
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

package testsuite.simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.mysql.cj.conf.PropertyDefinitions.DatabaseTerm;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.JdbcConnection;

import testsuite.BaseTestCase;

public class ReadOnlyCallableStatementTest extends BaseTestCase {
    @Test
    public void testReadOnlyWithProcBodyAccess() throws Exception {
        Connection replConn = null;
        Properties props = getHostFreePropertiesFromTestsuiteUrl();
        props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");

        try {
            createProcedure("testProc1", "()\nREADS SQL DATA\nbegin\nSELECT NOW();\nend\n");

            createProcedure("`testProc.1`", "()\nREADS SQL DATA\nbegin\nSELECT NOW();\nend\n");

            replConn = getMasterSlaveReplicationConnection();
            replConn.setReadOnly(true);

            CallableStatement cstmt = replConn.prepareCall("CALL testProc1()");
            cstmt.execute();
            cstmt.execute();

            String db = ((JdbcConnection) replConn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA
                    ? replConn.getSchema()
                    : replConn.getCatalog();

            cstmt = replConn.prepareCall("CALL `" + db + "`.testProc1()");
            cstmt.execute();

            cstmt = replConn.prepareCall("CALL `" + db + "`.`testProc.1`()");
            cstmt.execute();

        } finally {

            if (replConn != null) {
                replConn.close();
            }
        }
    }

    @Test
    public void testNotReadOnlyWithProcBodyAccess() throws Exception {
        Connection replConn = null;
        Properties props = getHostFreePropertiesFromTestsuiteUrl();
        props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");

        try {
            createProcedure("testProc2", "()\nMODIFIES SQL DATA\nbegin\nSELECT NOW();\nend\n");

            createProcedure("`testProc.2`", "()\nMODIFIES SQL DATA\nbegin\nSELECT NOW();\nend\n");

            replConn = getMasterSlaveReplicationConnection();
            replConn.setReadOnly(true);

            CallableStatement cstmt = replConn.prepareCall("CALL testProc2()");

            try {
                cstmt.execute();
                fail("Should not execute because procedure modifies data.");
            } catch (SQLException e) {
                assertEquals(e.getSQLState(), "S1009", "Should error for read-only connection.");
            }

            String db = ((JdbcConnection) replConn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA
                    ? replConn.getSchema()
                    : replConn.getCatalog();

            cstmt = replConn.prepareCall("CALL `" + db + "`.testProc2()");

            try {
                cstmt.execute();
                fail("Should not execute because procedure modifies data.");
            } catch (SQLException e) {
                assertEquals(e.getSQLState(), "S1009", "Should error for read-only connection.");
            }

            cstmt = replConn.prepareCall("CALL `" + db + "`.`testProc.2`()");

            try {
                cstmt.execute();
                fail("Should not execute because procedure modifies data.");
            } catch (SQLException e) {
                assertEquals(e.getSQLState(), "S1009", "Should error for read-only connection.");
            }

        } finally {

            if (replConn != null) {
                replConn.close();
            }
        }
    }

}
