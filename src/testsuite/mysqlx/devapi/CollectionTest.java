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

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import com.mysql.cj.api.x.Collection;
import com.mysql.cj.api.x.DatabaseObject.DbObjectStatus;

public class CollectionTest extends BaseDevApiTest {
    /** Collection for testing. */
    protected Collection collection;

    public CollectionTest() {
        dropCollection("CollectionTest");
        this.collection = this.schema.createCollection("CollectionTest");
    }

    @Test
    public void testCount() {
        this.collection.add("{'a':'a'}".replaceAll("'", "\"")).execute();
        this.collection.add("{'b':'b'}".replaceAll("'", "\"")).execute();
        this.collection.add("{'c':'c'}".replaceAll("'", "\"")).execute();
        assertEquals(3, this.collection.count());
    }

    @Test
    public void testExists() {
        String collName = "textExists_collection";
        dropCollection(collName);
        Collection coll = this.schema.getCollection(collName);
        assertEquals(DbObjectStatus.NOT_EXISTS, coll.existsInDatabase());
        coll = this.schema.createCollection(collName);
        assertEquals(DbObjectStatus.EXISTS, coll.existsInDatabase());
        this.schema.getCollection(collName).drop();
    }
}
