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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Ignore;
import org.junit.Test;

import com.mysql.cj.mysqlx.io.MysqlxProtocol;

/**
 * Tests for protocol-level APIs against a running MySQL-X server.
 */
public class MysqlxProtocolTest extends BaseInternalMysqlxTest {
    public MysqlxProtocolTest() throws Exception {
    }

    @Test
    public void testBasicSaslPlainAuth() throws Exception {
        MysqlxProtocol protocol = getTestProtocol();
        protocol.sendSaslAuthStart(getTestUser(), getTestPassword(), getTestDatabase());
        protocol.readAuthenticateOk();
        // TODO: protocol.close();
    }

    @Test
    public void testBasicSaslPlainAuthFailure() throws Exception {
        MysqlxProtocol protocol = getTestProtocol();
        try {
            protocol.sendSaslAuthStart(getTestUser(), "com.mysql.cj.theWrongPassword", getTestDatabase());
            protocol.readAuthenticateOk();
            fail("Auth using wrong password should fail");
        } catch (Exception ex) {
            // TODO: need better exception type here with auth fail details?
            assertEquals("Unexpected message class. Expected 'com.mysql.cj.mysqlx.protobuf.MysqlxSession$AuthenticateOk' but actually received 'com.mysql.cj.mysqlx.protobuf.MysqlxSession$AuthenticateFail'", ex.getMessage());
        }
        // TODO: protocol.close();
    }

    @Test
    @Ignore("Failed on server with 'ERROR 3102 (HY000): Expression of generated column '_id' contains a disallowed function.'. Need to check with Alfredo")
    public void testCreateCollection() throws Exception {
        MysqlxProtocol protocol = getAuthenticatedTestProtocol();
        protocol.sendCreateCollection("test", "com.mysql.cj.mysqlx.testCreateCollection");
        protocol.readOk();
        // TODO: protocol.close();
    }
}
