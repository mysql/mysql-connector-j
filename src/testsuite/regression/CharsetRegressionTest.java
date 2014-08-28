/*
  Copyright (c) 2014, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.regression;

import java.sql.SQLException;
import java.util.Properties;

import testsuite.BaseTestCase;

import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.ResultSetInternalMethods;
import com.mysql.jdbc.StatementInterceptorV2;

public class CharsetRegressionTest extends BaseTestCase {

    public CharsetRegressionTest(String name) {
        super(name);
    }

    /**
     * Tests fix for Bug#73663 (19479242), utf8mb4 does not work for connector/j >=5.1.13
     * 
     * This test is only run when character_set_server=utf8mb4 and collation-server set to one of utf8mb4 collations (it's better to test two configurations:
     * with default utf8mb4_general_ci and one of non-default, say utf8mb4_bin)
     * 
     * @throws Exception
     */
    public void testBug73663() throws Exception {

        this.rs = this.stmt.executeQuery("show variables like 'collation_server'");
        this.rs.next();
        String collation = this.rs.getString(2);

        if (collation != null && collation.startsWith("utf8mb4") && "utf8mb4".equals(((MySQLConnection) this.conn).getServerVariable("character_set_server"))) {
            Properties p = new Properties();
            p.setProperty("characterEncoding", "UTF-8");
            p.setProperty("statementInterceptors", "testsuite.regression.CharsetRegressionTest$Bug73663StatementInterceptor");

            getConnectionWithProps(p);
            // exception will be thrown from the statement interceptor if any "SET NAMES utf8" statement is issued instead of "SET NAMES utf8mb4"
        } else {
            System.out
                    .println("testBug73663 was skipped: This test is only run when character_set_server=utf8mb4 and collation-server set to one of utf8mb4 collations.");
        }
    }

    /**
     * Statement interceptor used to implement preceding test.
     */
    public static class Bug73663StatementInterceptor implements StatementInterceptorV2 {
        public void init(com.mysql.jdbc.Connection conn, Properties props) throws SQLException {
        }

        public ResultSetInternalMethods preProcess(String sql, com.mysql.jdbc.Statement interceptedStatement, com.mysql.jdbc.Connection connection)
                throws SQLException {
            if (sql.contains("SET NAMES utf8") && !sql.contains("utf8mb4")) {
                throw new SQLException("Character set statement issued: " + sql);
            }
            return null;
        }

        public boolean executeTopLevelOnly() {
            return true;
        }

        public void destroy() {
        }

        public ResultSetInternalMethods postProcess(String sql, com.mysql.jdbc.Statement interceptedStatement, ResultSetInternalMethods originalResultSet,
                com.mysql.jdbc.Connection connection, int warningCount, boolean noIndexUsed, boolean noGoodIndexUsed, SQLException statementException)
                throws SQLException {
            return null;
        }
    }
}
