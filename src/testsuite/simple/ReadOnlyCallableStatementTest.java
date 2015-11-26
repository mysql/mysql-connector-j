/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.simple;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import testsuite.BaseTestCase;

public class ReadOnlyCallableStatementTest extends BaseTestCase {
    public ReadOnlyCallableStatementTest(String name) {
        super(name);
    }

    public void testReadOnlyWithProcBodyAccess() throws Exception {
        if (versionMeetsMinimum(5, 0)) {
            Connection replConn = null;
            Properties props = getHostFreePropertiesFromTestsuiteUrl();
            props.setProperty("autoReconnect", "true");

            try {
                createProcedure("testProc1", "()\nREADS SQL DATA\nbegin\nSELECT NOW();\nend\n");

                createProcedure("`testProc.1`", "()\nREADS SQL DATA\nbegin\nSELECT NOW();\nend\n");

                replConn = getMasterSlaveReplicationConnection();
                replConn.setReadOnly(true);

                CallableStatement cstmt = replConn.prepareCall("CALL testProc1()");
                cstmt.execute();
                cstmt.execute();

                cstmt = replConn.prepareCall("CALL `" + replConn.getCatalog() + "`.testProc1()");
                cstmt.execute();

                cstmt = replConn.prepareCall("CALL `" + replConn.getCatalog() + "`.`testProc.1`()");
                cstmt.execute();

            } finally {

                if (replConn != null) {
                    replConn.close();
                }
            }
        }
    }

    public void testNotReadOnlyWithProcBodyAccess() throws Exception {
        if (versionMeetsMinimum(5, 0)) {

            Connection replConn = null;
            Properties props = getHostFreePropertiesFromTestsuiteUrl();
            props.setProperty("autoReconnect", "true");

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
                    assertEquals("Should error for read-only connection.", e.getSQLState(), "S1009");
                }

                cstmt = replConn.prepareCall("CALL `" + replConn.getCatalog() + "`.testProc2()");

                try {
                    cstmt.execute();
                    fail("Should not execute because procedure modifies data.");
                } catch (SQLException e) {
                    assertEquals("Should error for read-only connection.", e.getSQLState(), "S1009");
                }

                cstmt = replConn.prepareCall("CALL `" + replConn.getCatalog() + "`.`testProc.2`()");

                try {
                    cstmt.execute();
                    fail("Should not execute because procedure modifies data.");
                } catch (SQLException e) {
                    assertEquals("Should error for read-only connection.", e.getSQLState(), "S1009");
                }

            } finally {

                if (replConn != null) {
                    replConn.close();
                }
            }
        }
    }

}
