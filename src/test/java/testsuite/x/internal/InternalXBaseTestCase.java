/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

package testsuite.x.internal;

import static org.junit.Assert.fail;

import java.util.Properties;
import java.util.concurrent.Callable;

import org.junit.Assert;

import com.mysql.cj.MysqlxSession;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyDefinitions.AuthMech;
import com.mysql.cj.conf.PropertyDefinitions.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.x.XMessageBuilder;
import com.mysql.cj.protocol.x.XProtocol;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.protocol.x.XServerCapabilities;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;
import com.mysql.cj.xdevapi.SessionImpl;
import com.mysql.cj.xdevapi.SqlResult;
import com.mysql.cj.xdevapi.XDevAPIError;

import testsuite.TestUtils;

/**
 * Base class for tests of X DevAPI and X Protocol client internal components.
 */
public class InternalXBaseTestCase {
    /**
     * The default character set used to interpret metadata. Use <i>latin1</i> - MySQL's default. This value is provided by higher layers above the protocol so
     * we avoid issues by using only ASCII characters for metadata in these tests.
     */
    protected static final String DEFAULT_METADATA_CHARSET = "latin1";

    protected String baseUrl = System.getProperty(PropertyDefinitions.SYSP_testsuite_url_mysqlx);
    protected String baseOpensslUrl = System.getProperty(PropertyDefinitions.SYSP_testsuite_url_mysqlx_openssl);
    protected boolean isSetForXTests = this.baseUrl != null && this.baseUrl.length() > 0;
    protected boolean isSetForOpensslXTests = this.baseOpensslUrl != null && this.baseOpensslUrl.length() > 0;
    protected SessionFactory fact = new SessionFactory();

    public HostInfo testHostInfo;
    public Properties testProperties = new Properties();
    public Properties testPropertiesOpenSSL = new Properties();

    private ServerVersion mysqlVersion;

    public InternalXBaseTestCase() {
        if (this.isSetForXTests) {
            ConnectionUrl conUrl = ConnectionUrl.getConnectionUrlInstance(this.baseUrl, null);
            this.testHostInfo = conUrl.getMainHost();
            this.testProperties = conUrl.getMainHost().exposeAsProperties();
        }
        if (this.isSetForOpensslXTests) {
            ConnectionUrl conUrl = ConnectionUrl.getConnectionUrlInstance(this.baseOpensslUrl, null);
            this.testPropertiesOpenSSL = conUrl.getMainHost().exposeAsProperties();
        }
    }

    public String getTestHost() {
        return this.testProperties.getProperty(PropertyKey.HOST.getKeyName());
    }

    public int getTestPort() {
        return Integer.valueOf(this.testProperties.getProperty(PropertyKey.PORT.getKeyName()));
    }

    public String getTestUser() {
        return this.testProperties.getProperty(PropertyKey.USER.getKeyName());
    }

    public String getTestPassword() {
        return this.testProperties.getProperty(PropertyKey.PASSWORD.getKeyName());
    }

    public String getTestDatabase() {
        return this.testProperties.getProperty(PropertyKey.DBNAME.getKeyName());
    }

    public String getEncodedTestHost() {
        return TestUtils.encodePercent(getTestHost());
    }

    /**
     * Create a new {@link XProtocol} instance for testing.
     */
    public XProtocol createTestProtocol() {
        // TODO pass prop. set
        XProtocol protocol = XProtocol.getInstance(getTestHost(), getTestPort(), new DefaultPropertySet());
        protocol.beforeHandshake();
        return protocol;
    }

    /**
     * Create a new {@link XProtocol} that is part of an authenticated session.
     */
    public XProtocol createAuthenticatedTestProtocol() {
        XProtocol protocol = createTestProtocol();
        XMessageBuilder messageBuilder = (XMessageBuilder) protocol.getMessageBuilder();

        AuthMech authMech = protocol.getPropertySet().<AuthMech> getEnumProperty(PropertyDefinitions.PNAME_auth).getValue();
        boolean overTLS = ((XServerCapabilities) protocol.getServerSession().getCapabilities()).getTls();

        // Choose the best default auth mechanism.
        if (!overTLS) {
            authMech = AuthMech.MYSQL41;
        } else if (authMech != AuthMech.PLAIN) {
            authMech = AuthMech.PLAIN;
        }

        while (true) {
            switch (authMech) {
                case SHA256_MEMORY:
                    protocol.send(messageBuilder.buildSha256MemoryAuthStart(), 0);
                    byte[] nonce = protocol.readAuthenticateContinue();
                    protocol.send(messageBuilder.buildSha256MemoryAuthContinue(getTestUser(), getTestPassword(), nonce, getTestDatabase()), 0);
                    break;
                case MYSQL41:
                    protocol.send(messageBuilder.buildMysql41AuthStart(), 0);
                    byte[] salt = protocol.readAuthenticateContinue();
                    protocol.send(messageBuilder.buildMysql41AuthContinue(getTestUser(), getTestPassword(), salt, getTestDatabase()), 0);
                    break;
                case PLAIN:
                    if (overTLS) {
                        protocol.send(messageBuilder.buildPlainAuthStart(getTestUser(), getTestPassword(), getTestDatabase()), 0);
                    } else {
                        throw new XDevAPIError("PLAIN authentication is not allowed via unencrypted connection.");
                    }
                    break;
                case EXTERNAL:
                    protocol.send(messageBuilder.buildExternalAuthStart(getTestDatabase()), 0);
                    break;
                default:
                    throw new WrongArgumentException("Unknown authentication mechanism '" + authMech + "'.");
            }

            try {
                protocol.readAuthenticateOk();
                return protocol;
            } catch (XProtocolError e) {
                if (authMech == AuthMech.MYSQL41) {
                    authMech = AuthMech.SHA256_MEMORY; // try again using SHA256_MEMORY
                } else {
                    throw e;
                }
            }
        }
    }

    public MysqlxSession createTestSession() {
        PropertySet pset = new DefaultPropertySet();
        pset.initializeProperties(this.testProperties);
        MysqlxSession session = new MysqlxSession(this.testHostInfo, pset);
        return session;
    }

    /**
     * Create a temporary collection for testing.
     *
     * @return the temporary collection name
     */
    public String createTempTestCollection(XProtocol protocol) {
        String collName = "protocol_test_collection";
        XMessageBuilder messageBuilder = (XMessageBuilder) protocol.getMessageBuilder();

        try {
            protocol.send(messageBuilder.buildDropCollection(getTestDatabase(), collName), 0);
            protocol.readQueryResult();
        } catch (XProtocolError err) {
            // ignore
        }
        protocol.send(messageBuilder.buildCreateCollection(getTestDatabase(), collName), 0);
        protocol.readQueryResult();

        return collName;
    }

    protected static <EX extends Throwable> EX assertThrows(Class<EX> throwable, Callable<?> testRoutine) {
        return assertThrows("", throwable, null, testRoutine);
    }

    protected static <EX extends Throwable> EX assertThrows(String message, Class<EX> throwable, Callable<?> testRoutine) {
        return assertThrows(message, throwable, null, testRoutine);
    }

    protected static <EX extends Throwable> EX assertThrows(Class<EX> throwable, String msgMatchesRegex, Callable<?> testRoutine) {
        return assertThrows("", throwable, msgMatchesRegex, testRoutine);
    }

    protected static <EX extends Throwable> EX assertThrows(String message, Class<EX> throwable, String msgMatchesRegex, Callable<?> testRoutine) {
        if (message.length() > 0) {
            message += " ";
        }
        try {
            testRoutine.call();
        } catch (Throwable t) {
            if (!throwable.isAssignableFrom(t.getClass())) {
                fail(message + "expected exception of type '" + throwable.getName() + "' but instead a exception of type '" + t.getClass().getName()
                        + "' was thrown.");
            }

            if (msgMatchesRegex != null && !t.getMessage().matches(msgMatchesRegex)) {
                fail(message + "the error message «" + t.getMessage() + "» was expected to match «" + msgMatchesRegex + "».");
            }

            return throwable.cast(t);
        }
        fail(message + "expected exception of type '" + throwable.getName() + "'.");

        // never reaches here
        return null;
    }

    /**
     * Checks if the MySQL version we are connected to meets the minimum {@link ServerVersion} provided.
     * 
     * @param version
     *            the minimum {@link ServerVersion} accepted
     * @return true or false according to versions comparison
     */
    protected boolean mysqlVersionMeetsMinimum(ServerVersion version) {
        if (this.isSetForXTests) {
            if (this.mysqlVersion == null) {
                Session session = new SessionImpl(this.testHostInfo);
                this.mysqlVersion = ServerVersion.parseVersion(session.sql("SELECT version()").execute().fetchOne().getString(0));
                session.close();
            }
            return this.mysqlVersion.meetsMinimum(version);
        }
        return false;
    }

    protected void assertSessionStatusEquals(Session sess, String statusVariable, String expected) {
        SqlResult rs = sess.sql("SHOW SESSION STATUS LIKE '" + statusVariable + "'").execute();
        String actual = rs.fetchOne().getString(1);
        Assert.assertEquals(expected, actual);
    }

    protected void assertSessionStatusNotEquals(Session sess, String statusVariable, String unexpected) {
        SqlResult rs = sess.sql("SHOW SESSION STATUS LIKE '" + statusVariable + "'").execute();
        String actual = rs.fetchOne().getString(1);
        Assert.assertNotEquals(unexpected, actual);
    }
}
