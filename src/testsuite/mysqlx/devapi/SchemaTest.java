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

package testsuite.mysqlx.devapi;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.x.Collection;
import com.mysql.cj.api.x.Schema;

public class SchemaTest extends BaseDevApiTest {

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
        Schema otherDefaultSchema = this.session.getDefaultSchema();
        assertFalse(otherDefaultSchema == this.schema);
        assertTrue(otherDefaultSchema.equals(this.schema));
        assertTrue(this.schema.equals(otherDefaultSchema));
        assertFalse(this.schema.equals(this.session));
    }

    @Test
    public void testListCollections() {
        String collName = "testListCollections";
        dropCollection(collName);
        Collection coll = this.schema.createCollection(collName);
        List<Collection> colls = this.schema.getCollections();
        System.err.println("Found: " + colls);
        assertTrue(colls.contains(coll));
    }
}
