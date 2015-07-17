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

import java.io.InputStream;
import java.util.Properties;

import com.mysql.cj.mysqlx.MysqlxSession;
import com.mysql.cj.mysqlx.io.MysqlxProtocol;
import com.mysql.cj.mysqlx.io.MysqlxProtocolFactory;

/**
 * Base class for tests of MySQL-X internal components.
 */
public class BaseInternalMysqlxTest {
    /**
     * The default character set used to interpret metadata. Use <i>latin1</i> - MySQL's default. This value is provided by higher layers above the protocol so
     * we avoid issues by using only ASCII characters for metadata in these tests.
     */
    protected static final String DEFAULT_METADATA_CHARSET = "latin1";

    public Properties testProperties = new Properties();

    public BaseInternalMysqlxTest() {
        try {
            InputStream propsFileStream = ClassLoader.getSystemResourceAsStream("test.mysqlx.properties");
            if (propsFileStream == null) {
                throw new Exception("Cannot load test.mysqlx.properties");
            }
            this.testProperties.load(propsFileStream);
        } catch (Exception ex) {
            throw new RuntimeException("Initialization via properties file failed", ex);
        }
    }

    public String getTestHost() {
        return this.testProperties.getProperty("com.mysql.mysqlx.testsuite.host");
    }

    public int getTestPort() {
        return Integer.valueOf(this.testProperties.getProperty("com.mysql.mysqlx.testsuite.port"));
    }

    public String getTestUser() {
        return this.testProperties.getProperty("com.mysql.mysqlx.testsuite.user");
    }

    public String getTestPassword() {
        return this.testProperties.getProperty("com.mysql.mysqlx.testsuite.password");
    }

    public String getTestDatabase() {
        return this.testProperties.getProperty("com.mysql.mysqlx.testsuite.database");
    }

    /**
     * Create a new {@link MysqlxProtocol} instance for testing.
     */
    public MysqlxProtocol createTestProtocol() {
        MysqlxProtocol protocol;
        if (true) { // TODO: make this configurable for tests to test BOTH
            protocol = MysqlxProtocolFactory.getAsyncInstance(getTestHost(), getTestPort());
        } else {
            protocol = MysqlxProtocolFactory.getSyncInstance(getTestHost(), getTestPort());
        }
        return protocol;
    }

    /**
     * Create a new {@link MysqlxProtocol} that is part of an authenicated session.
     */
    public MysqlxProtocol createAuthenticatedTestProtocol() {
        MysqlxProtocol protocol = createTestProtocol();

        protocol.sendSaslMysql41AuthStart();
        byte[] salt = protocol.readAuthenticateContinue();
        protocol.sendSaslMysql41AuthContinue(getTestUser(), getTestPassword(), salt, getTestDatabase());
        protocol.readAuthenticateOk();

        return protocol;
    }

    public MysqlxSession createTestSession() {
        MysqlxSession session = new MysqlxSession(createTestProtocol());
        session.changeUser(getTestUser(), getTestPassword(), getTestDatabase());
        return session;
    }
}
