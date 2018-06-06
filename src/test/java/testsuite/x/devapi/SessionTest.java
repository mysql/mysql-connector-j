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

package testsuite.x.devapi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.exceptions.CJPacketTooBigException;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.Schema;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;
import com.mysql.cj.xdevapi.SqlResult;
import com.mysql.cj.xdevapi.SqlStatement;
import com.mysql.cj.xdevapi.XDevAPIError;

public class SessionTest extends DevApiBaseTestCase {
    @Before
    public void setupCollectionTest() {
        setupTestSession();
    }

    @After
    public void teardownCollectionTest() {
        if (this.isSetForXTests) {
            this.createdTestSchemas.forEach(schemaName -> {
                try {
                    this.session.dropSchema(schemaName);
                } catch (XProtocolError x) {
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
        if (!this.isSetForXTests) {
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
        if (!this.isSetForXTests) {
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
        if (!this.isSetForXTests) {
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
        if (!this.isSetForXTests) {
            return;
        }
        String testSchemaName = getRandomTestSchemaName();
        Schema newSchema = this.session.createSchema(testSchemaName);
        assertTrue(this.session.getSchemas().contains(newSchema));
        try {
            this.session.createSchema(testSchemaName);
            fail("Attempt to create a schema with the name of an existing schema should fail");
        } catch (XProtocolError err) {
            assertEquals(MysqlErrorNumbers.ER_DB_CREATE_EXISTS, err.getErrorCode());
        }
    }

    /**
     * Test the client-side enforcing of server `mysqlx_max_allowed_packet'.
     */
    @Test
    public void errorOnPacketTooBig() {
        if (!this.isSetForXTests) {
            return;
        }
        try {
            SqlStatement stmt = this.session.sql("select @@mysqlx_max_allowed_packet");
            SqlResult res = stmt.execute();
            Row r = res.next();
            long mysqlxMaxAllowedPacket = r.getLong(0);

            long size = 100 + mysqlxMaxAllowedPacket;
            StringBuilder b = new StringBuilder();
            for (int i = 0; i < size; ++i) {
                b.append('a');
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
        if (!this.isSetForXTests) {
            return;
        }

        try {
            this.session.sql("CREATE USER 'bug21690043user1'@'%' IDENTIFIED WITH mysql_native_password").execute();
            this.session.sql("GRANT SELECT ON *.* TO 'bug21690043user1'@'%'").execute();

            Properties props = new Properties();
            props.putAll(this.testProperties);
            props.setProperty("user", "bug21690043user1");
            props.setProperty("password", "");
            new SessionFactory().getSession(props);
        } catch (Throwable t) {
            throw t;
        } finally {
            this.session.sql("DROP USER 'bug21690043user1'@'%'").execute();
        }
    }

    @Test
    public void basicSql() {
        if (!this.isSetForXTests) {
            return;
        }
        SqlStatement stmt = this.session.sql("select 1,2,3 from dual");
        SqlResult res = stmt.execute();
        assertTrue(res.hasData());
        Row r = res.next();
        assertEquals("1", r.getString(0));
        assertEquals("2", r.getString(1));
        assertEquals("3", r.getString(2));
        assertEquals("1", r.getString("1"));
        assertEquals("2", r.getString("2"));
        assertEquals("3", r.getString("3"));
        assertFalse(res.hasNext());

        assertThrows(XDevAPIError.class, "Method getAutoIncrementValue\\(\\) is allowed only for insert statements.", new Callable<Void>() {
            public Void call() throws Exception {
                assertEquals(null, res.getAutoIncrementValue());
                return null;
            }
        });
    }

    @Test
    public void sqlUpdate() {
        if (!this.isSetForXTests) {
            return;
        }
        SqlStatement stmt = this.session.sql("set @cjTestVar = 1");
        SqlResult res = stmt.execute();
        assertFalse(res.hasData());
        assertEquals(0, res.getAffectedItemsCount());
        assertEquals(null, res.getAutoIncrementValue());
        assertEquals(0, res.getWarningsCount());
        assertFalse(res.getWarnings().hasNext());

        // TODO SqlUpdateResult throws FeatureNotAvailableException("Not a multi-result");
        //res.nextResult();

        assertThrows(FeatureNotAvailableException.class, "No data", new Callable<Void>() {
            public Void call() throws Exception {
                res.fetchAll();
                return null;
            }
        });
        assertThrows(FeatureNotAvailableException.class, "No data", new Callable<Void>() {
            public Void call() throws Exception {
                res.next();
                return null;
            }
        });
        assertThrows(FeatureNotAvailableException.class, "No data", new Callable<Void>() {
            public Void call() throws Exception {
                res.hasNext();
                return null;
            }
        });
        assertThrows(FeatureNotAvailableException.class, "No data", new Callable<Void>() {
            public Void call() throws Exception {
                res.getColumnCount();
                return null;
            }
        });
        assertThrows(FeatureNotAvailableException.class, "No data", new Callable<Void>() {
            public Void call() throws Exception {
                res.getColumns();
                return null;
            }
        });
        assertThrows(FeatureNotAvailableException.class, "No data", new Callable<Void>() {
            public Void call() throws Exception {
                res.getColumnNames();
                return null;
            }
        });
        assertThrows(FeatureNotAvailableException.class, "No data", new Callable<Void>() {
            public Void call() throws Exception {
                res.count();
                return null;
            }
        });

    }

    @Test
    public void sqlArguments() {
        if (!this.isSetForXTests) {
            return;
        }
        SqlStatement stmt = this.session.sql("select ? as a, 40 + ? as b, ? as c");
        SqlResult res = stmt.bind(1).bind(2).bind(3).execute();
        Row r = res.next();
        assertEquals("1", r.getString("a"));
        assertEquals("42", r.getString("b"));
        assertEquals("3", r.getString("c"));
    }

    @Test
    public void basicMultipleResults() {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop procedure if exists basicMultipleResults");
        sqlUpdate("create procedure basicMultipleResults() begin explain select 1; explain select 2; end");
        SqlStatement stmt = this.session.sql("call basicMultipleResults()");
        SqlResult res = stmt.execute();
        assertTrue(res.hasData());
        /* Row r = */ res.next();
        assertFalse(res.hasNext());
        assertTrue(res.nextResult());
        assertTrue(res.hasData());
        assertFalse(res.nextResult());
        assertFalse(res.nextResult());
        assertFalse(res.nextResult());
    }

    @Test
    public void smartBufferMultipleResults() {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop procedure if exists basicMultipleResults");
        sqlUpdate("create procedure basicMultipleResults() begin explain select 1; explain select 2; end");
        SqlStatement stmt = this.session.sql("call basicMultipleResults()");
        /* SqlResult res = */ stmt.execute();
        // execute another statement, should work fine
        this.session.sql("call basicMultipleResults()");
        this.session.sql("call basicMultipleResults()");
        this.session.sql("call basicMultipleResults()");
    }

    @Test
    public void sqlInsertAutoIncrementValue() {
        if (!this.isSetForXTests) {
            return;
        }

        sqlUpdate("drop table if exists lastInsertId");
        sqlUpdate("create table lastInsertId (id int not null primary key auto_increment, name varchar(20) not null)");

        SqlStatement stmt = this.session.sql("insert into lastInsertId values (null, 'a')");
        SqlResult res = stmt.execute();

        assertFalse(res.hasData());
        assertEquals(1, res.getAffectedItemsCount());
        assertEquals(0, res.getWarningsCount());
        assertFalse(res.getWarnings().hasNext());
        assertEquals(new Long(1), res.getAutoIncrementValue());
    }

    /**
     * Tests fix for Bug #27652379, NPE FROM GETSESSION(PROPERTIES) WHEN HOST PARAMETER IS GIVEN IN SMALL LETTER.
     */
    @Test
    public void testBug27652379() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        Properties props = new Properties();

        // Upper case keys.
        props.clear();
        props.setProperty("HOST", getTestHost());
        props.setProperty("PORT", String.valueOf(getTestPort()));
        props.setProperty("USER", getTestUser());
        props.setProperty("PASSWORD", getTestPassword());
        props.setProperty("DBNAME", getTestDatabase());

        Session testSession = this.fact.getSession(props);
        assertEquals(getTestDatabase(), testSession.getDefaultSchemaName());
        testSession.close();

        testSession = this.fact.getSession(new Properties(props));
        assertEquals(getTestDatabase(), testSession.getDefaultSchemaName());
        testSession.close();

        // Lower case keys.
        props.clear();
        props.setProperty("host", getTestHost());
        props.setProperty("port", String.valueOf(getTestPort()));
        props.setProperty("user", getTestUser());
        props.setProperty("password", getTestPassword());
        props.setProperty("dbname", getTestDatabase());

        testSession = this.fact.getSession(props);
        assertEquals(getTestDatabase(), testSession.getDefaultSchemaName());
        testSession.close();

        testSession = this.fact.getSession(new Properties(props));
        assertEquals(getTestDatabase(), testSession.getDefaultSchemaName());
        testSession.close();

        // Random case keys.
        props.clear();
        props.setProperty("HOst", getTestHost());
        props.setProperty("poRT", String.valueOf(getTestPort()));
        props.setProperty("uSEr", getTestUser());
        props.setProperty("PassworD", getTestPassword());
        props.setProperty("DbNaMe", getTestDatabase());

        testSession = this.fact.getSession(props);
        assertEquals(getTestDatabase(), testSession.getDefaultSchemaName());
        testSession.close();

        testSession = this.fact.getSession(new Properties(props));
        assertEquals(getTestDatabase(), testSession.getDefaultSchemaName());
        testSession.close();
    }
}
