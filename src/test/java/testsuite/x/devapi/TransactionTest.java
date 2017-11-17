/*
  Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.x.devapi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.xdevapi.Collection;
import com.mysql.cj.x.core.XDevAPIError;

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
        this.collection.add("{}").add("{}").execute();

        this.session.startTransaction();
        this.collection.add("{}").add("{}").execute();
        assertEquals(4, this.collection.find().execute().count());
        this.session.rollback();

        assertEquals(2, this.collection.find().execute().count());
    }

    @Test
    public void basicCommit() {
        if (!this.isSetForXTests) {
            return;
        }
        this.collection.add("{}").add("{}").execute();

        this.session.startTransaction();
        this.collection.add("{}").add("{}").execute();
        assertEquals(4, this.collection.find().execute().count());
        this.session.commit();

        assertEquals(4, this.collection.find().execute().count());
    }

    @Test
    public void basicSavepoint() {
        if (!this.isSetForXTests) {
            return;
        }
        this.collection.add("{}").execute();

        // Test for rollbackTo

        this.session.startTransaction();
        this.collection.add("{}").execute();

        String sp1 = this.session.setSavepoint();
        this.collection.add("{}").execute();

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
        this.collection.add("{}").execute();

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
        this.collection.add("{}").execute();

        sp1 = this.session.setSavepoint();
        this.collection.add("{}").execute();

        sp2 = this.session.setSavepoint("sp2");
        this.collection.add("{}").execute();

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
