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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.xdevapi.Collection;
import com.mysql.cj.xdevapi.XDevAPIError;

public class TransactionTest extends DevApiBaseTestCase {
    protected Collection collection;

    @Before
    public void setupCollectionTest() {
        if (setupTestSession()) {
            dropCollection("txTest");
            this.collection = this.schema.createCollection("txTest");
        }
    }

    @After
    public void teardownCollectionTest() {
        if (this.isSetForXTests) {
            dropCollection("txTest");
            destroyTestSession();
        }
    }

    @Test
    public void basicRollback() {
        if (!this.isSetForXTests) {
            return;
        }

        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\"}").add("{\"_id\": \"2\"}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{}").add("{}").execute();
        }
        this.session.startTransaction();
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"3\"}").add("{\"_id\": \"4\"}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{}").add("{}").execute();
        }
        assertEquals(4, this.collection.find().execute().count());
        this.session.rollback();

        assertEquals(2, this.collection.find().execute().count());
    }

    @Test
    public void basicCommit() {
        if (!this.isSetForXTests) {
            return;
        }

        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\"}").add("{\"_id\": \"2\"}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{}").add("{}").execute();
        }
        this.session.startTransaction();
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"3\"}").add("{\"_id\": \"4\"}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{}").add("{}").execute();
        }
        assertEquals(4, this.collection.find().execute().count());
        this.session.commit();

        assertEquals(4, this.collection.find().execute().count());
    }

    @Test
    public void basicSavepoint() {
        if (!this.isSetForXTests) {
            return;
        }

        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\"}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{}").execute();
        }

        // Test for rollbackTo

        this.session.startTransaction();
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"2\"}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{}").execute();
        }

        String sp1 = this.session.setSavepoint();
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"3\"}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{}").execute();
        }

        assertThrows(XDevAPIError.class, "Parameter 'name' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                TransactionTest.this.session.setSavepoint(null);
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Parameter 'name' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                TransactionTest.this.session.setSavepoint("");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Parameter 'name' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                TransactionTest.this.session.setSavepoint("");
                return null;
            }
        });

        String sp2 = this.session.setSavepoint("sp2");
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"4\"}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{}").execute();
        }

        assertEquals(4, this.collection.find().execute().count());

        assertThrows(XDevAPIError.class, "Parameter 'name' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                TransactionTest.this.session.rollbackTo(null);
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Parameter 'name' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                TransactionTest.this.session.rollbackTo("");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Parameter 'name' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                TransactionTest.this.session.rollbackTo("");
                return null;
            }
        });

        this.session.rollbackTo(sp1);

        assertEquals(2, this.collection.find().execute().count());

        try {
            this.session.rollbackTo(sp2);
            fail("Error is expected here because 'sp2' savepoint should not exist at this point.");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("SAVEPOINT sp2 does not exist"));
        }

        this.session.commit();

        assertEquals(2, this.collection.find().execute().count());

        // Test for releaseSavepoint

        this.session.startTransaction();
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"5\"}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{}").execute();
        }

        sp1 = this.session.setSavepoint();
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"6\"}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{}").execute();
        }

        sp2 = this.session.setSavepoint("sp2");
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"7\"}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{}").execute();
        }

        assertEquals(5, this.collection.find().execute().count());

        assertThrows(XDevAPIError.class, "Parameter 'name' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                TransactionTest.this.session.releaseSavepoint(null);
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Parameter 'name' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                TransactionTest.this.session.releaseSavepoint("");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Parameter 'name' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                TransactionTest.this.session.releaseSavepoint("");
                return null;
            }
        });

        this.session.releaseSavepoint(sp2);

        assertEquals(5, this.collection.find().execute().count());

        try {
            this.session.rollbackTo(sp2);
            fail("Error is expected here because 'sp2' savepoint should not exist at this point.");
        } catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("SAVEPOINT sp2 does not exist"));
        }

        this.session.rollbackTo(sp1);

        this.session.commit();

        assertEquals(3, this.collection.find().execute().count());
    }
}
