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

import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.x.core.XDevAPIError;

/**
 * @todo
 */
public class CollectionRemoveTest extends CollectionTest {
    @Before
    @Override
    public void setupCollectionTest() {
        super.setupCollectionTest();
    }

    @After
    @Override
    public void teardownCollectionTest() {
        super.teardownCollectionTest();
    }

    @Test
    public void deleteAll() {
        if (!this.isSetForXTests) {
            return;
        }
        this.collection.add("{}").execute();
        this.collection.add("{}").execute();
        this.collection.add("{}").execute();

        assertEquals(3, this.collection.count());

        assertThrows(XDevAPIError.class, "Parameter 'criteria' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionRemoveTest.this.collection.remove(null).execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'criteria' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionRemoveTest.this.collection.remove(" ").execute();
                return null;
            }
        });

        this.collection.remove("false").execute();
        assertEquals(3, this.collection.count());

        this.collection.remove("0 == 1").execute();
        assertEquals(3, this.collection.count());

        this.collection.remove("true").execute();
        assertEquals(0, this.collection.count());
    }

    @Test
    public void deleteSome() {
        if (!this.isSetForXTests) {
            return;
        }
        this.collection.add("{}").execute();
        this.collection.add("{}").execute();
        this.collection.add("{\"x\":22}").execute();

        assertEquals(3, this.collection.count());
        this.collection.remove("$.x = 22").orderBy("x", "x").execute();
        assertEquals(2, this.collection.count());
    }
}
