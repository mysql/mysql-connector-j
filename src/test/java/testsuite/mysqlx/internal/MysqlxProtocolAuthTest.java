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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.mysqlx.MysqlxError;
import com.mysql.cj.mysqlx.io.MysqlxProtocol;

/**
 * Tests for protocol-level auth APIs against X Plugin via X Protocol.
 */
public class MysqlxProtocolAuthTest extends InternalMysqlxBaseTestCase {
    private static MysqlxProtocol protocol;

    @Before
    public void setupTestProtocol() throws Exception {
        if (this.isSetForMySQLxTests) {
            protocol = createTestProtocol();
        }
    }

    @After
    public void destroyTestProtocol() throws Exception {
        if (this.isSetForMySQLxTests) {
            protocol.close();
        }
    }

    /**
     * Test that we are disconnected with an error if we send a bad authentication message. The server responds by immediately closing the socket. The async
     * implementation may block indefinitely here and we need to prevent any regression.
     */
    @Test
    public void testBadAuthMessage() throws Exception {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        try {
            protocol.sendCreateCollection(getTestDatabase(), "wont_be_Created");
            protocol.readStatementExecuteOk();
            fail("Should fail after first message is sent");
        } catch (MysqlxError err) {
            // expected
            //ex.printStackTrace();
        }
    }

    @Test
    @Ignore("PLAIN only supported over SSL")
    public void testBasicSaslPlainAuth() throws Exception {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        protocol.sendSaslAuthStart(getTestUser(), getTestPassword(), getTestDatabase());
        protocol.readAuthenticateOk();
    }

    @Test
    public void testBasicSaslMysql41Auth() throws Exception {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        protocol.sendSaslMysql41AuthStart();
        byte[] salt = protocol.readAuthenticateContinue();
        protocol.sendSaslMysql41AuthContinue(getTestUser(), getTestPassword(), salt, getTestDatabase());
        protocol.readAuthenticateOk();
    }

    @Test
    @Ignore("PLAIN only supported over SSL")
    public void testBasicSaslPlainAuthFailure() throws Exception {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        try {
            protocol.sendSaslAuthStart(getTestUser(), "com.mysql.cj.theWrongPassword", getTestDatabase());
            protocol.readAuthenticateOk();
            fail("Auth using wrong password should fail");
        } catch (MysqlxError ex) {
            assertEquals(MysqlErrorNumbers.ER_ACCESS_DENIED_ERROR, ex.getErrorCode());
            assertEquals("ERROR 1045 (HY000) Invalid user or password", ex.getMessage());
        }
    }

    /**
     * Bug#21680263 - NullPointerException When Try to connect without DB Name.
     */
    @Test
    public void testEmptyDatabaseMYSQL41() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        protocol.sendSaslMysql41AuthStart();
        byte[] salt = protocol.readAuthenticateContinue();
        protocol.sendSaslMysql41AuthContinue(getTestUser(), getTestPassword(), salt, null);
        protocol.readAuthenticateOk();
    }

    /**
     * Bug#21680263 - NullPointerException When Try to connect without DB Name.
     */
    @Test
    @Ignore("PLAIN only supported over SSL")
    public void testEmptyDatabasePLAIN() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        protocol.sendSaslAuthStart(getTestUser(), getTestPassword(), null);
        protocol.readAuthenticateOk();
    }
}
