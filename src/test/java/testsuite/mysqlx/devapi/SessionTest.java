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

package testsuite.mysqlx.devapi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.x.Schema;
import com.mysql.cj.core.exceptions.CJPacketTooBigException;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.mysqlx.MysqlxError;
import com.mysql.cj.x.MysqlxSessionFactory;

public class SessionTest extends DevApiBaseTestCase {
    @Before
    public void setupCollectionTest() {
        setupTestSession();
    }

    @After
    public void teardownCollectionTest() {
        if (this.isSetForMySQLxTests) {
            this.createdTestSchemas.forEach(schemaName -> {
                try {
                    this.session.dropSchema(schemaName);
                } catch (MysqlxError x) {
                    // ignored
                }
            });
            destroyTestSession();
        }
    }

    private List<String> createdTestSchemas = new ArrayList<>();

    /**
     * Create a random schema name. The schema will be dropped upon test cleanup.
     */
    private String getRandomTestSchemaName() {
        String n = "cj_test_schema_no_" + new Random().nextInt(1000);
        this.createdTestSchemas.add(n);
        return n;
    }

    @Test
    public void createDropSchema() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        String testSchemaName = getRandomTestSchemaName();
        Schema newSchema = this.session.createSchema(testSchemaName);
        assertTrue(this.session.getSchemas().contains(newSchema));
        this.session.dropSchema(testSchemaName);
        assertFalse(this.session.getSchemas().contains(newSchema));
    }

    @Test
    public void createAndReuseExistingSchema() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        String testSchemaName = getRandomTestSchemaName();
        Schema newSchema = this.session.createSchema(testSchemaName);
        assertTrue(this.session.getSchemas().contains(newSchema));
        Schema reusedSchema = this.session.createSchema(testSchemaName, true);
        assertTrue(this.session.getSchemas().contains(reusedSchema));
    }

    @Test
    public void listSchemas() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        List<Schema> schemas = this.session.getSchemas();
        // we should have visibility of at least these two
        Schema infoSchema = this.session.getSchema("information_schema");
        assertTrue(schemas.contains(infoSchema));
        Schema testSchema = this.session.getSchema(getTestDatabase());
        assertTrue(schemas.contains(testSchema));
    }

    @Test
    public void createExistingSchemaError() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        String testSchemaName = getRandomTestSchemaName();
        Schema newSchema = this.session.createSchema(testSchemaName);
        assertTrue(this.session.getSchemas().contains(newSchema));
        try {
            this.session.createSchema(testSchemaName);
            fail("Attempt to create a schema with the name of an existing schema should fail");
        } catch (MysqlxError err) {
            assertEquals(MysqlErrorNumbers.ER_DB_CREATE_EXISTS, err.getErrorCode());
        }
    }

    /**
     * Test the client-side enforcing of server `mysqlx_max_allowed_packet'. This assumes a server-side value of 1MiB.
     */
    @Test
    public void errorOnPacketTooBig() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        try {
            int size = 2 * 1024 * 1024;
            StringBuilder b = new StringBuilder();
            for (int i = 0; i < size; ++i) {
                b.append('.');
            }
            String s = b.append("\"}").toString();
            this.session.dropSchema(s);
            fail("Large packet should cause an exception");
        } catch (CJPacketTooBigException ex) {
            // expected
        }
    }

    /**
     * Tests fix for Bug#21690043, CONNECT FAILS WHEN PASSWORD IS BLANK.
     * 
     * @throws Exception
     *             if the test fails.
     */
    @Test
    public void testBug21690043() {
        if (!this.isSetForMySQLxTests) {
            return;
        }

        try {
            this.session.sql("CREATE USER 'bug21690043user1'@'%' IDENTIFIED WITH mysql_native_password").execute();

            Properties props = new Properties();
            props.putAll(this.testProperties);
            props.setProperty("user", "bug21690043user1");
            props.setProperty("password", "");
            new MysqlxSessionFactory().getSession(props);
        } catch (Throwable t) {
            throw t;
        } finally {
            this.session.sql("DROP USER 'bug21690043user1'@'%'").execute();
        }
    }
}
