/*
 * Copyright (c) 2014, 2018, Oracle and/or its affiliates. All rights reserved.
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

package testsuite.regression;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import com.mysql.cj.CharsetMapping;
import com.mysql.cj.MysqlConnection;
import com.mysql.cj.Query;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.protocol.Resultset;

import testsuite.BaseQueryInterceptor;
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

        if (collation != null && collation.startsWith("utf8mb4")
                && "utf8mb4".equals(((MysqlConnection) this.conn).getSession().getServerSession().getServerVariable("character_set_server"))) {
            Properties p = new Properties();
            p.setProperty(PropertyDefinitions.PNAME_characterEncoding, "UTF-8");
            p.setProperty(PropertyDefinitions.PNAME_queryInterceptors, Bug73663QueryInterceptor.class.getName());

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
    public static class Bug73663QueryInterceptor extends BaseQueryInterceptor {
        @Override
        public <T extends Resultset> T preProcess(Supplier<String> str, Query interceptedQuery) {
            String sql = str.get();
            if (sql.contains("SET NAMES utf8") && !sql.contains("utf8mb4")) {
                throw ExceptionFactory.createException("Character set statement issued: " + sql);
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
                createUser("'Bug72630User'@'%'", "IDENTIFIED WITH mysql_native_password");
                this.stmt.execute("GRANT ALL ON *.* TO 'Bug72630User'@'%'");
                this.stmt.executeUpdate(((MysqlConnection) this.conn).getSession().versionMeetsMinimum(5, 7, 6)
                        ? "ALTER USER 'Bug72630User'@'%' IDENTIFIED BY 'pwd'" : "set password for 'Bug72630User'@'%' = PASSWORD('pwd')");

                final Properties props = new Properties();
                props.setProperty(PropertyDefinitions.PNAME_user, "Bug72630User");
                props.setProperty(PropertyDefinitions.PNAME_password, "pwd");
                props.setProperty(PropertyDefinitions.PNAME_characterEncoding, "NonexistentEncoding");

                assertThrows(SQLException.class, "Unsupported character encoding 'NonexistentEncoding'", new Callable<Void>() {
                    public Void call() throws Exception {
                        try {
                            getConnectionWithProps(props);
                            return null;
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            throw ex;
                        }
                    }
                });

                props.remove(PropertyDefinitions.PNAME_characterEncoding);
                props.setProperty(PropertyDefinitions.PNAME_passwordCharacterEncoding, "NonexistentEncoding");
                assertThrows(SQLException.class, "Unsupported character encoding 'NonexistentEncoding'", new Callable<Void>() {
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
        p.setProperty(PropertyDefinitions.PNAME_characterEncoding, cjCharset);

        getConnectionWithProps(p);
    }
}
