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

package testsuite.x.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.xdevapi.DatabaseObject.DbObjectType;
import com.mysql.cj.core.io.IntegerValueFactory;
import com.mysql.cj.x.core.MysqlxSession;
import com.mysql.cj.xdevapi.DocFindParams;
import com.mysql.cj.xdevapi.DocResultImpl;
import com.mysql.cj.xdevapi.FindParams;

/**
 * Tests for (internal) session-level APIs against X Plugin via X Protocol.
 */
public class MysqlxSessionTest extends InternalXBaseTestCase {
    private MysqlxSession session;

    @Before
    public void setupTestSession() {
        if (this.isSetForXTests) {
            this.session = createTestSession();
        }
    }

    @After
    public void destroyTestSession() {
        if (this.isSetForXTests) {
            this.session.close();
        }
    }

    @Test
    public void testCreateDropCollection() {
        if (!this.isSetForXTests) {
            return;
        }
        String collName = "toBeCreatedAndDropped";
        this.session.dropCollectionIfExists(getTestDatabase(), collName);
        assertFalse(this.session.tableExists(getTestDatabase(), collName));
        this.session.createCollection(getTestDatabase(), collName);
        assertTrue(this.session.tableExists(getTestDatabase(), collName));
        this.session.dropCollection(getTestDatabase(), collName);
        assertFalse(this.session.tableExists(getTestDatabase(), collName));
        this.session.createCollection(getTestDatabase(), collName);
        assertTrue(this.session.tableExists(getTestDatabase(), collName));
        this.session.dropCollection(getTestDatabase(), collName);
        assertFalse(this.session.tableExists(getTestDatabase(), collName));
        this.session.createCollection(getTestDatabase(), collName);
        assertTrue(this.session.tableExists(getTestDatabase(), collName));
        this.session.dropCollection(getTestDatabase(), collName);
        assertFalse(this.session.tableExists(getTestDatabase(), collName));
    }

    @Test
    public void testGetObjects() {
        if (!this.isSetForXTests) {
            return;
        }
        String collName = "test_get_objects";
        this.session.dropCollectionIfExists(getTestDatabase(), collName);
        this.session.createCollection(getTestDatabase(), collName);
        List<String> collNames = this.session.getObjectNamesOfType(getTestDatabase(), DbObjectType.COLLECTION);
        assertTrue(collNames.contains(collName));
        collNames = this.session.getObjectNamesOfType(getTestDatabase(), "none%", DbObjectType.COLLECTION);
        assertFalse(collNames.contains(collName));
        collNames = this.session.getObjectNamesOfType(getTestDatabase(), "%get_obj%", DbObjectType.COLLECTION);
        assertTrue(collNames.contains(collName));
        this.session.dropCollection(getTestDatabase(), collName);
    }

    @Test
    public void testInterleavedResults() {
        if (!this.isSetForXTests) {
            return;
        }
        String collName = "testInterleavedResults";
        this.session.dropCollectionIfExists(getTestDatabase(), collName);
        this.session.createCollection(getTestDatabase(), collName);

        List<String> stringDocs = new ArrayList<>();
        stringDocs.add("{'_id':'0'}");
        stringDocs.add("{'_id':'1'}");
        stringDocs.add("{'_id':'2'}");
        stringDocs.add("{'_id':'3'}");
        stringDocs.add("{'_id':'4'}");
        stringDocs = stringDocs.stream().map(s -> s.replaceAll("'", "\"")).collect(Collectors.toList());
        this.session.addDocs(getTestDatabase(), collName, stringDocs);

        FindParams findParams = new DocFindParams(getTestDatabase(), collName);
        findParams.setOrder("$._id");
        DocResultImpl docs1 = this.session.findDocs(findParams);
        DocResultImpl docs2 = this.session.findDocs(findParams);
        DocResultImpl docs3 = this.session.findDocs(findParams);
        DocResultImpl docs4 = this.session.findDocs(findParams);
        DocResultImpl docs5 = this.session.findDocs(findParams);
        assertTrue(docs5.hasNext());
        assertTrue(docs4.hasNext());
        assertTrue(docs3.hasNext());
        assertTrue(docs2.hasNext());
        assertTrue(docs1.hasNext());
        for (int i = 0; i < 5; ++i) {
            assertEquals("{\n\"_id\" : \"" + i + "\"\n}", docs1.next().toString());
            assertEquals("{\n\"_id\" : \"" + i + "\"\n}", docs2.next().toString());
            assertEquals("{\n\"_id\" : \"" + i + "\"\n}", docs3.next().toString());
            assertEquals("{\n\"_id\" : \"" + i + "\"\n}", docs4.next().toString());
            assertEquals("{\n\"_id\" : \"" + i + "\"\n}", docs5.next().toString());
        }
        assertFalse(docs5.hasNext());
        assertFalse(docs4.hasNext());
        assertFalse(docs3.hasNext());
        assertFalse(docs2.hasNext());
        assertFalse(docs1.hasNext());
        // let the session be closed with all of these "open"
    }

    @Test
    public void testGenericQuery() {
        if (!this.isSetForXTests) {
            return;
        }
        List<Integer> ints = this.session.query("select 2 union select 1", r -> r.getValue(0, new IntegerValueFactory()), Collectors.toList());
        assertEquals(2, ints.size());
        assertEquals(new Integer(2), ints.get(0));
        assertEquals(new Integer(1), ints.get(1));
    }
}
