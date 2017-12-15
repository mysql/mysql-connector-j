/*
 * Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.mysql.cj.api.xdevapi.Session;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.x.core.XDevAPIError;
import com.mysql.cj.x.io.XProtocol;

/**
 * Tests for protocol-level auth APIs against X Plugin via X Protocol.
 */
public class XProtocolAuthTest extends InternalXBaseTestCase {
    private static XProtocol protocol;

    @Before
    public void setupTestProtocol() throws Exception {
        if (this.isSetForXTests) {
            protocol = createTestProtocol();
        }
    }

    @After
    public void destroyTestProtocol() throws Exception {
        if (this.isSetForXTests) {
            protocol.close();
        }
    }

    /**
     * Test that we are disconnected with an error if we send a bad authentication message. The server responds by immediately closing the socket. The async
     * implementation may block indefinitely here and we need to prevent any regression.
     */
    @Test
    public void testBadAuthMessage() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        try {
            protocol.sendCreateCollection(getTestDatabase(), "wont_be_Created");
            protocol.readStatementExecuteOk();
            fail("Should fail after first message is sent");
        } catch (XDevAPIError err) {
            // expected
            //ex.printStackTrace();
        }
    }

    @Test
    @Ignore("PLAIN only supported over SSL")
    public void testBasicSaslPlainAuth() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        protocol.sendSaslPlainAuthStart(getTestUser(), getTestPassword(), getTestDatabase());
        protocol.readAuthenticateOk();
    }

    @Test
    public void testBasicSaslMysql41Auth() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        try {
            Session testSession = this.fact.getSession(this.baseUrl);
            testSession.sql("CREATE USER IF NOT EXISTS 'testPlainAuth'@'%' IDENTIFIED WITH mysql_native_password BY 'pwd'").execute();
            testSession.close();

            protocol.sendSaslMysql41AuthStart();
            byte[] salt = protocol.readAuthenticateContinue();
            protocol.sendSaslMysql41AuthContinue("testPlainAuth", "pwd", salt, getTestDatabase());
            protocol.readAuthenticateOk();
        } catch (Throwable t) {
            throw t;
        } finally {
            Session testSession = this.fact.getSession(this.baseUrl);
            testSession.sql("DROP USER if exists testPlainAuth").execute();
            testSession.close();
        }
    }

    @Test
    @Ignore("PLAIN only supported over SSL")
    public void testBasicSaslPlainAuthFailure() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        try {
            protocol.sendSaslPlainAuthStart(getTestUser(), "com.mysql.cj.theWrongPassword", getTestDatabase());
            protocol.readAuthenticateOk();
            fail("Auth using wrong password should fail");
        } catch (XDevAPIError ex) {
            assertEquals(MysqlErrorNumbers.ER_ACCESS_DENIED_ERROR, ex.getErrorCode());
            assertEquals("ERROR 1045 (HY000) Invalid user or password", ex.getMessage());
        }
    }

    /**
     * Bug#21680263 - NullPointerException When Try to connect without DB Name.
     */
    @Test
    public void testEmptyDatabaseMYSQL41() {
        if (!this.isSetForXTests) {
            return;
        }

        try {
            Session testSession = this.fact.getSession(this.baseUrl);
            testSession.sql("CREATE USER IF NOT EXISTS 'testPlainAuth'@'%' IDENTIFIED WITH mysql_native_password BY 'pwd'").execute();
            testSession.close();

            protocol.sendSaslMysql41AuthStart();
            byte[] salt = protocol.readAuthenticateContinue();
            protocol.sendSaslMysql41AuthContinue("testPlainAuth", "pwd", salt, null);
            protocol.readAuthenticateOk();
        } catch (Throwable t) {
            throw t;
        } finally {
            Session testSession = this.fact.getSession(this.baseUrl);
            testSession.sql("DROP USER if exists testPlainAuth").execute();
            testSession.close();
        }
    }

    /**
     * Bug#21680263 - NullPointerException When Try to connect without DB Name.
     */
    @Test
    @Ignore("PLAIN only supported over SSL")
    public void testEmptyDatabasePLAIN() {
        if (!this.isSetForXTests) {
            return;
        }
        protocol.sendSaslPlainAuthStart(getTestUser(), getTestPassword(), null);
        protocol.readAuthenticateOk();
    }
}
