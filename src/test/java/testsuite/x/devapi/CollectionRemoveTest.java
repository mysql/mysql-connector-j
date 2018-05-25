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

import java.util.concurrent.Callable;

import org.junit.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.xdevapi.Result;
import com.mysql.cj.xdevapi.XDevAPIError;

/**
 * @todo
 */
public class CollectionRemoveTest extends BaseCollectionTestCase {

    @Test
    public void deleteAll() {
        if (!this.isSetForXTests) {
            return;
        }

        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\"}").execute(); // Requires manual _id.
            this.collection.add("{\"_id\": \"2\"}").execute();
            this.collection.add("{\"_id\": \"3\"}").execute();
        } else {
            this.collection.add("{}").execute();
            this.collection.add("{}").execute();
            this.collection.add("{}").execute();
        }

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

        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\"}").execute(); // Requires manual _id.
            this.collection.add("{\"_id\": \"2\"}").execute();
            this.collection.add("{\"_id\": \"3\", \"x\":22}").execute();
        } else {
            this.collection.add("{}").execute();
            this.collection.add("{}").execute();
            this.collection.add("{\"x\":22}").execute();
        }

        assertEquals(3, this.collection.count());
        this.collection.remove("$.x = 22").orderBy("x", "x").execute();
        assertEquals(2, this.collection.count());
    }

    @Test
    public void removeOne() {
        if (!this.isSetForXTests) {
            return;
        }

        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\", \"x\":1}").execute(); // Requires manual _id.
            this.collection.add("{\"_id\": \"2\", \"x\":2}").execute();
        } else {
            this.collection.add("{\"x\":1}").execute();
            this.collection.add("{\"x\":2}").execute();
        }
        this.collection.add("{\"_id\":\"existingId\",\"x\":3}").execute();

        assertEquals(3, this.collection.count());
        assertTrue(this.collection.find("x = 3").execute().hasNext());

        Result res = this.collection.removeOne("existingId");
        assertEquals(1, res.getAffectedItemsCount());

        assertEquals(2, this.collection.count());
        assertFalse(this.collection.find("x = 3").execute().hasNext());

        res = this.collection.removeOne("notExistingId");
        assertEquals(0, res.getAffectedItemsCount());

        assertEquals(2, this.collection.count());
        assertFalse(this.collection.find("x = 3").execute().hasNext());

        res = this.collection.removeOne(null);
        assertEquals(0, res.getAffectedItemsCount());

        assertEquals(2, this.collection.count());
        assertFalse(this.collection.find("x = 3").execute().hasNext());
    }
}
