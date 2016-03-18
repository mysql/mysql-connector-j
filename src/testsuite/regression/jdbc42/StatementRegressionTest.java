/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.regression.jdbc42;

import java.sql.Connection;

import com.mysql.jdbc.JDBC42PreparedStatement;
import com.mysql.jdbc.JDBC42ServerPreparedStatement;

import testsuite.BaseTestCase;

public class StatementRegressionTest extends BaseTestCase {
    
    public StatementRegressionTest(String name) {
        super(name);
    }

    /**
     * Tests fix for Bug#79598 - Client side Prepared Statement caching bypasses JDBC42 Java 8 Time conversion.
     * 
     * Although in the bug report subject is mentioned a Java 8 Time conversion issue, the actual problem occurs because of wrong types being returned after
     * preparing statements when prepared statements cache is enabled. The Java 8 Time data has no relation to this bug.
     */
    public void testBug79598() throws Exception {
        Connection testConn = getConnectionWithProps("cachePrepStmts=true");
        this.pstmt = testConn.prepareStatement("SELECT 'testBug79598'");
        assertTrue(JDBC42PreparedStatement.class.isAssignableFrom(this.pstmt.getClass()));
        this.pstmt.close();
        this.pstmt = testConn.prepareStatement("SELECT 'testBug79598'");
        assertTrue(JDBC42PreparedStatement.class.isAssignableFrom(this.pstmt.getClass()));
        this.pstmt.close();
        testConn.close();

        testConn = getConnectionWithProps("cachePrepStmts=true,useServerPrepStmts=true");
        this.pstmt = testConn.prepareStatement("SELECT 'testBug79598'");
        assertTrue(JDBC42ServerPreparedStatement.class.isAssignableFrom(this.pstmt.getClass()));
        this.pstmt.close();
        this.pstmt = testConn.prepareStatement("SELECT 'testBug79598'");
        assertTrue(JDBC42ServerPreparedStatement.class.isAssignableFrom(this.pstmt.getClass()));
        this.pstmt.close();
        testConn.close();
    }
}
