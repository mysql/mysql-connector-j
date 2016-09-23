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

import static org.junit.Assert.fail;

import java.util.Properties;
import java.util.concurrent.Callable;

import com.mysql.cj.api.x.XSessionFactory;
import com.mysql.cj.core.conf.DefaultPropertySet;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.conf.url.ConnectionUrl;
import com.mysql.cj.mysqlx.MysqlxError;
import com.mysql.cj.mysqlx.MysqlxSession;
import com.mysql.cj.mysqlx.io.MysqlxProtocol;
import com.mysql.cj.mysqlx.io.MysqlxProtocolFactory;
import com.mysql.cj.x.MysqlxSessionFactory;

/**
 * Base class for tests of X DevAPI and X Protocol client internal components.
 */
public class InternalMysqlxBaseTestCase {
    /**
     * The default character set used to interpret metadata. Use <i>latin1</i> - MySQL's default. This value is provided by higher layers above the protocol so
     * we avoid issues by using only ASCII characters for metadata in these tests.
     */
    protected static final String DEFAULT_METADATA_CHARSET = "latin1";

    protected String baseUrl = System.getProperty(PropertyDefinitions.SYSP_testsuite_url_mysqlx);
    protected boolean isSetForMySQLxTests = this.baseUrl != null && this.baseUrl.length() > 0;
    protected XSessionFactory fact = new MysqlxSessionFactory();

    public Properties testProperties = new Properties();

    public InternalMysqlxBaseTestCase() {
        if (this.isSetForMySQLxTests) {
            ConnectionUrl conUrl = ConnectionUrl.getConnectionUrlInstance(this.baseUrl, null);
            if (conUrl.getType() == null) {
                throw new RuntimeException("Initialization via URL failed for \"" + this.baseUrl + "\"");
            }
            this.testProperties = conUrl.getMainHost().exposeAsProperties();
        }
    }

    public String getTestHost() {
        return this.testProperties.getProperty(PropertyDefinitions.HOST_PROPERTY_KEY);
    }

    public int getTestPort() {
        return Integer.valueOf(this.testProperties.getProperty(PropertyDefinitions.PORT_PROPERTY_KEY));
    }

    public String getTestUser() {
        return this.testProperties.getProperty(PropertyDefinitions.PNAME_user);
    }

    public String getTestPassword() {
        return this.testProperties.getProperty(PropertyDefinitions.PNAME_password);
    }

    public String getTestDatabase() {
        return this.testProperties.getProperty(PropertyDefinitions.DBNAME_PROPERTY_KEY);
    }

    /**
     * Create a new {@link MysqlxProtocol} instance for testing.
     */
    public MysqlxProtocol createTestProtocol() {
        // TODO pass prop. set
        MysqlxProtocol protocol = MysqlxProtocolFactory.getInstance(getTestHost(), getTestPort(), new DefaultPropertySet());
        return protocol;
    }

    /**
     * Create a new {@link MysqlxProtocol} that is part of an authenticated session.
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
        MysqlxSession session = new MysqlxSession(this.testProperties);
        session.changeUser(getTestUser(), getTestPassword(), getTestDatabase());
        return session;
    }

    /**
     * Create a temporary collection for testing.
     *
     * @return the temporary collection name
     */
    public String createTempTestCollection(MysqlxProtocol protocol) {
        String collName = "protocol_test_collection";

        try {
            protocol.sendDropCollection(getTestDatabase(), collName);
            protocol.readStatementExecuteOk();
        } catch (MysqlxError err) {
            // ignore
        }
        protocol.sendCreateCollection(getTestDatabase(), collName);
        protocol.readStatementExecuteOk();

        return collName;
    }

    protected static <EX extends Throwable> EX assertThrows(Class<EX> throwable, String msgMatchesRegex, Callable<?> testRoutine) {
        try {
            testRoutine.call();
        } catch (Throwable t) {
            if (!throwable.isAssignableFrom(t.getClass())) {
                fail("Expected exception of type '" + throwable.getName() + "' but instead a exception of type '" + t.getClass().getName() + "' was thrown.");
            }

            if (!t.getMessage().matches(msgMatchesRegex)) {
                fail("The error message «" + t.getMessage() + "» was expected to match «" + msgMatchesRegex + "».");
            }

            return throwable.cast(t);
        }
        fail("Expected exception of type '" + throwable.getName() + "'.");

        // never reaches here
        return null;
    }
}
