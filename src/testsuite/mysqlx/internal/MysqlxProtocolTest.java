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

import java.util.ArrayList;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Ignore;
import org.junit.Test;

import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.core.conf.DefaultPropertySet;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.jdbc.Field;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqlx.MysqlxError;
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
    public void testBasicSaslMysql41Auth() throws Exception {
        MysqlxProtocol protocol = getTestProtocol();
        protocol.sendSaslMysql41AuthStart();
        byte[] salt = protocol.readAuthenticateContinue();
        protocol.sendSaslMysql41AuthContinue(getTestUser(), getTestPassword(), salt, getTestDatabase());
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

    /**
     * Test the create/drop collection admin commands.
     */
    @Test
    public void testCreateAndDropCollection() throws Exception {
        MysqlxProtocol protocol = getAuthenticatedTestProtocol();
        try {
            protocol.sendCreateCollection(getTestDatabase(), "com.mysql.cj.mysqlx.testCreateCollection");
            protocol.readStatementExecuteOk();
        } catch (MysqlxError err) {
            // leftovers, clean them up now
            if (err.getErrorCode() == MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR) {
                protocol.sendDropCollection(getTestDatabase(), "com.mysql.cj.mysqlx.testCreateCollection");
                protocol.readStatementExecuteOk();
                // try again
                protocol.sendCreateCollection(getTestDatabase(), "com.mysql.cj.mysqlx.testCreateCollection");
                protocol.readStatementExecuteOk();
            } else {
                throw err;
            }
        }
        // we don't verify the existence. That's the job of the server/xplugin
        protocol.sendDropCollection(getTestDatabase(), "com.mysql.cj.mysqlx.testCreateCollection");
        protocol.readStatementExecuteOk();
        // TODO: protocol.close();
    }

    @Test
    public void testTrivialSqlQuery() throws Exception {
        MysqlxProtocol protocol = getAuthenticatedTestProtocol();
        protocol.sendSqlStatement("select 'x' as y");
        assertTrue(protocol.hasResults());
        // TODO: ??? should this be INSIDE protocol?
        PropertySet propertySet = new DefaultPropertySet();
        // latin1 is MySQL default
        ArrayList<Field> metadata = protocol.readMetadata(propertySet, "latin1");
        assertEquals(1, metadata.size());
        Field f = metadata.get(0);
        // not an exhaustive metadata test
        assertEquals("y", f.getColumnLabel());
        assertEquals(MysqlaConstants.FIELD_TYPE_VARCHAR, f.getMysqlType());
        // TODO: protocol.close();
    }

    /**
     * Test DML that is executed with <i>StmtExecute</i> and does not return a result set.
     */
    @Test
    public void testSqlDml() {
        MysqlxProtocol protocol = getAuthenticatedTestProtocol();

        protocol.sendSqlStatement("drop table if exists mysqlx_sqlDmlTest");
        assertFalse(protocol.hasResults());
        StatementExecuteOk response = protocol.readStatementExecuteOk();
        assertEquals(0, response.getRowsAffected());

        protocol.sendSqlStatement("create table mysqlx_sqlDmlTest (w int primary key auto_increment, x int) auto_increment = 7");
        assertFalse(protocol.hasResults());
        response = protocol.readStatementExecuteOk();
        assertEquals(0, response.getRowsAffected());

        protocol.sendSqlStatement("insert into mysqlx_sqlDmlTest (x) values (44),(29)");
        assertFalse(protocol.hasResults());
        response = protocol.readStatementExecuteOk();
        assertEquals(2, response.getRowsAffected());
        assertEquals(7, response.getLastInsertId());

        protocol.sendSqlStatement("drop table mysqlx_sqlDmlTest");
        assertFalse(protocol.hasResults());
        protocol.readStatementExecuteOk();
    }

    @Test
    @Ignore("not implemented until Row format is implemented in xplugin")
    public void testBasicCrudInsertFind() {
        MysqlxProtocol protocol = getAuthenticatedTestProtocol();
        String collName = "testBasicCrudInsertFind";

        try {
            protocol.sendDropCollection(getTestDatabase(), collName);
            protocol.readStatementExecuteOk();
        } catch (MysqlxError err) {
            // ignore
        }
        protocol.sendCreateCollection(getTestDatabase(), collName);
        protocol.readStatementExecuteOk();

        // TODO: insert a document, then find a document
        //protocol.

        protocol.sendDropCollection(getTestDatabase(), collName);
        protocol.readStatementExecuteOk();
    }
}
