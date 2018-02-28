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

import java.util.List;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.Collection;
import com.mysql.cj.xdevapi.DatabaseObject.DbObjectStatus;
import com.mysql.cj.xdevapi.Schema;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionImpl;
import com.mysql.cj.xdevapi.Table;

public class SchemaTest extends DevApiBaseTestCase {
    @Before
    public void setupCollectionTest() {
        setupTestSession();
    }

    @After
    public void teardownCollectionTest() {
        destroyTestSession();
    }

    @Test
    public void testEquals() {
        if (!this.isSetForXTests) {
            return;
        }
        Schema otherDefaultSchema = this.session.getDefaultSchema();
        assertFalse(otherDefaultSchema == this.schema);
        assertTrue(otherDefaultSchema.equals(this.schema));
        assertTrue(this.schema.equals(otherDefaultSchema));
        assertFalse(this.schema.equals(this.session));

        Session otherSession = new SessionImpl(this.testHostInfo);
        Schema diffSessionSchema = otherSession.getDefaultSchema();
        assertEquals(this.schema.getName(), diffSessionSchema.getName());
        assertFalse(this.schema.equals(diffSessionSchema));
        assertFalse(diffSessionSchema.equals(this.schema));
        otherSession.close();
    }

    @Test
    public void testToString() {
        if (!this.isSetForXTests) {
            return;
        }
        // this will pass as long as the test database doesn't require identifier quoting
        assertEquals("Schema(" + getTestDatabase() + ")", this.schema.toString());
        Schema needsQuoted = this.session.getSchema("terrible'schema`name");
        assertEquals("Schema(`terrible'schema``name`)", needsQuoted.toString());
    }

    @Test
    public void testListCollections() {
        if (!this.isSetForXTests) {
            return;
        }
        String collName1 = "test_list_collections1";
        String collName2 = "test_list_collections2";
        dropCollection(collName1);
        dropCollection(collName2);
        Collection coll1 = this.schema.createCollection(collName1);
        Collection coll2 = this.schema.createCollection(collName2);

        List<Collection> colls = this.schema.getCollections();
        assertTrue(colls.contains(coll1));
        assertTrue(colls.contains(coll2));

        colls = this.schema.getCollections("%ions2");
        assertFalse(colls.contains(coll1));
        assertTrue(colls.contains(coll2));
    }

    @Test
    public void testExists() {
        if (!this.isSetForXTests) {
            return;
        }
        assertEquals(DbObjectStatus.EXISTS, this.schema.existsInDatabase());
        Schema nonExistingSchema = this.session.getSchema(getTestDatabase() + "_SHOULD_NOT_EXIST_0xCAFEBABE");
        assertEquals(DbObjectStatus.NOT_EXISTS, nonExistingSchema.existsInDatabase());
    }

    @Test
    public void testCreateCollection() {
        if (!this.isSetForXTests) {
            return;
        }
        String collName = "testCreateCollection";
        dropCollection(collName);
        Collection coll = this.schema.createCollection(collName);
        try {
            this.schema.createCollection(collName);
            fail("Exception should be thrown trying to create a collection that already exists");
        } catch (XProtocolError ex) {
            // expected
            assertEquals(MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR, ex.getErrorCode());
        }
        Collection coll2 = this.schema.createCollection(collName, true);
        assertEquals(coll, coll2);
    }

    @Test
    public void testDropCollection() {
        if (!this.isSetForXTests) {
            return;
        }
        String collName = "testDropCollection";
        dropCollection(collName);
        Collection coll = this.schema.getCollection(collName);
        assertEquals(DbObjectStatus.NOT_EXISTS, coll.existsInDatabase());

        // dropping non-existing collection should not fail
        this.schema.dropCollection(collName);

        coll = this.schema.createCollection(collName);
        assertEquals(DbObjectStatus.EXISTS, coll.existsInDatabase());
        this.schema.dropCollection(collName);

        // ensure that collection is dropped
        coll = this.schema.getCollection(collName);
        assertEquals(DbObjectStatus.NOT_EXISTS, coll.existsInDatabase());

        assertThrows(XProtocolError.class, "Parameter 'collectionName' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                SchemaTest.this.schema.dropCollection(null);
                return null;
            }
        });
    }

    @Test
    public void testListTables() {
        if (!this.isSetForXTests) {
            return;
        }
        String collName = "test_list_tables_collection";
        String tableName = "test_list_tables_table";
        String viewName = "test_list_tables_view";
        try {
            dropCollection(collName);
            sqlUpdate("drop view if exists " + viewName);
            sqlUpdate("drop table if exists " + tableName);

            Collection coll = this.schema.createCollection(collName);

            sqlUpdate("create table " + tableName + "(name varchar(32), age int, role int)");
            sqlUpdate("create view " + viewName + " as select name, age from " + tableName);

            Table table = this.schema.getTable(tableName);
            Table view = this.schema.getTable(viewName);

            List<Table> tables = this.schema.getTables();
            assertFalse(tables.contains(coll));
            assertTrue(tables.contains(table));
            assertTrue(tables.contains(view));

            tables = this.schema.getTables("%tables_t%");
            assertFalse(tables.contains(coll));
            assertTrue(tables.contains(table));
            assertFalse(tables.contains(view));

        } finally {
            dropCollection(collName);
            sqlUpdate("drop view if exists " + viewName);
            sqlUpdate("drop table if exists " + tableName);
        }

    }

}
