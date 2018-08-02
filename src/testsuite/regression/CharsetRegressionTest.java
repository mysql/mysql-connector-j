/*
  Copyright (c) 2014, 2018, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.regression;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.mysql.jdbc.CharsetMapping;
import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.ResultSetInternalMethods;

import testsuite.BaseStatementInterceptor;
import testsuite.BaseTestCase;

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
            p.setProperty("statementInterceptors", Bug73663StatementInterceptor.class.getName());

            getConnectionWithProps(p);
            // exception will be thrown from the statement interceptor if any "SET NAMES utf8" statement is issued instead of "SET NAMES utf8mb4"
        } else {
            System.out.println(
                    "testBug73663 was skipped: This test is only run when character_set_server=utf8mb4 and collation-server set to one of utf8mb4 collations.");
        }
    }

    /**
     * Statement interceptor used to implement preceding test.
     */
    public static class Bug73663StatementInterceptor extends BaseStatementInterceptor {
        @Override
        public ResultSetInternalMethods preProcess(String sql, com.mysql.jdbc.Statement interceptedStatement, com.mysql.jdbc.Connection connection)
                throws SQLException {
            if (sql.contains("SET NAMES utf8") && !sql.contains("utf8mb4")) {
                throw new SQLException("Character set statement issued: " + sql);
            }
            return null;
        }
    }

    /**
     * Tests fix for Bug#72630 (18758686), NullPointerException during handshake in some situations
     * 
     * @throws Exception
     */
    public void testBug72630() throws Exception {
        // bug is related to authentication plugins, available only in 5.5.7+ 
        if (versionMeetsMinimum(5, 5, 7)) {
            try {
                createUser("'Bug72630User'@'%'", "IDENTIFIED WITH mysql_native_password AS 'pwd'");
                this.stmt.execute("GRANT ALL ON *.* TO 'Bug72630User'@'%'");

                final Properties props = new Properties();
                props.setProperty("user", "Bug72630User");
                props.setProperty("password", "pwd");
                props.setProperty("characterEncoding", "NonexistentEncoding");

                assertThrows(SQLException.class, "Unsupported character encoding 'NonexistentEncoding'.", new Callable<Void>() {
                    public Void call() throws Exception {
                        getConnectionWithProps(props);
                        return null;
                    }
                });

                props.remove("characterEncoding");
                props.setProperty("passwordCharacterEncoding", "NonexistentEncoding");
                assertThrows(SQLException.class, "Unsupported character encoding 'NonexistentEncoding' for 'passwordCharacterEncoding' or 'characterEncoding'.",
                        new Callable<Void>() {
                            public Void call() throws Exception {
                                getConnectionWithProps(props);
                                return null;
                            }
                        });
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Tests fix for Bug#25504578, CONNECT FAILS WHEN CONNECTIONCOLLATION=ISO-8859-13
     * 
     * @throws Exception
     */
    public void testBug25504578() throws Exception {

        Properties p = new Properties();
        String cjCharset = CharsetMapping.getJavaEncodingForMysqlCharset("latin7");
        p.setProperty("characterEncoding", cjCharset);

        getConnectionWithProps(p);
    }

    /**
     * Tests fix for Bug#81196 (23227334), CONNECTOR/J NOT FOLLOWING DATABASE CHARACTER SET.
     * 
     * @throws Exception
     */
    public void testBug81196() throws Exception {
        // utf8mb4 was added in MySQL 5.5.2
        if (versionMeetsMinimum(5, 5, 2)) {
            final String fourBytesValue = "\ud841\udf0e";

            createTable("testBug81196", //"TestDb.TestTable",
                    "(`id` int AUTO_INCREMENT NOT NULL, `name` varchar(50)  NULL," + "CONSTRAINT `PK_LastViewedMatch_id` PRIMARY KEY  (`id`))"
                            + " ENGINE=InnoDB AUTO_INCREMENT=1 CHARSET=utf8mb4 DEFAULT COLLATE utf8mb4_unicode_ci");

            Properties p = new Properties();

            /* With a single-byte encoding */
            p.setProperty("characterEncoding", CharsetMapping.getJavaEncodingForMysqlCharset("latin1"));
            Connection conn1 = getConnectionWithProps(p);
            Statement st1 = conn1.createStatement();
            st1.executeUpdate("INSERT INTO testBug81196(name) VALUES ('" + fourBytesValue + "')");
            ResultSet rs1 = st1.executeQuery("SELECT * from testBug81196");
            assertTrue(rs1.next());
            assertFalse(fourBytesValue.equals(rs1.getString(2)));

            /* With a UTF-8 encoding */
            st1.executeUpdate("TRUNCATE TABLE testBug81196");
            p.setProperty("characterEncoding", "UTF-8");
            Connection conn2 = getConnectionWithProps(p);
            Statement st2 = conn2.createStatement();
            st2.executeUpdate("INSERT INTO testBug81196(name) VALUES ('" + fourBytesValue + "')");
            ResultSet rs2 = st2.executeQuery("SELECT * from testBug81196");
            assertTrue(rs2.next());
            assertEquals(fourBytesValue, rs2.getString(2));

            /* With a UTF-8 encoding and connectionCollation=utf8mb4_unicode_ci */
            st1.executeUpdate("TRUNCATE TABLE testBug81196");
            p.setProperty("characterEncoding", "UTF-8");
            p.setProperty("connectionCollation", "utf8mb4_unicode_ci");
            Connection conn2_1 = getConnectionWithProps(p);
            Statement st2_1 = conn2_1.createStatement();
            st2_1.executeUpdate("INSERT INTO testBug81196(name) VALUES ('" + fourBytesValue + "')");
            ResultSet rs2_1 = st2_1.executeQuery("SELECT * from testBug81196");
            assertTrue(rs2_1.next());
            assertEquals(fourBytesValue, rs2_1.getString(2));

            /* With connectionCollation=utf8_bin, SET NAMES utf8 is expected */
            st1.executeUpdate("TRUNCATE TABLE testBug81196");
            p.setProperty("characterEncoding", "UTF-8");
            p.setProperty("connectionCollation", "utf8_bin");
            Connection conn3 = getConnectionWithProps(p);
            final Statement st3 = conn3.createStatement();

            assertThrows(SQLException.class, "Incorrect string value: '\\\\xF0\\\\xA0\\\\x9C\\\\x8E' for column 'name' at row 1", new Callable<Void>() {
                public Void call() throws Exception {
                    st3.executeUpdate("INSERT INTO testBug81196(name) VALUES ('" + fourBytesValue + "')");
                    return null;
                }
            });
        }
    }
}
