/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package testsuite.x.devapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.exceptions.WrongArgumentException;

public class BindTest extends BaseCollectionTestCase {

    @Test
    public void removeWithBind() {
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": 1, \"x\":1}").execute();
            this.collection.add("{\"_id\": 2, \"x\":2}").execute();
            this.collection.add("{\"_id\": 3, \"x\":3}").execute();
        } else {
            this.collection.add("{\"x\":1}").execute();
            this.collection.add("{\"x\":2}").execute();
            this.collection.add("{\"x\":3}").execute();
        }

        assertEquals(3, this.collection.count());

        assertTrue(this.collection.find("x = 3").execute().hasNext());
        this.collection.remove("x = ?").bind(new Object[] { 3 }).execute();
        assertEquals(2, this.collection.count());
        assertFalse(this.collection.find("x = 3").execute().hasNext());
    }

    @Test
    public void removeWithNamedBinds() {
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": 1, \"x\":1}").execute();
            this.collection.add("{\"_id\": 2, \"x\":2}").execute();
            this.collection.add("{\"_id\": 3, \"x\":3}").execute();
        } else {
            this.collection.add("{\"x\":1}").execute();
            this.collection.add("{\"x\":2}").execute();
            this.collection.add("{\"x\":3}").execute();
        }

        assertEquals(3, this.collection.count());

        assertTrue(this.collection.find("x = ?").bind(new Object[] { 3 }).execute().hasNext());
        Map<String, Object> params = new HashMap<>();
        params.put("thePlaceholder", 3);
        this.collection.remove("x = :thePlaceholder").bind(params).execute();
        assertEquals(2, this.collection.count());
        assertFalse(this.collection.find("x = 3").execute().hasNext());
    }

    @Test
    public void bug21798850() {
        Map<String, Object> params = new HashMap<>();
        params.put("thePlaceholder1", 1);
        params.put("thePlaceholder2", 2);
        params.put("thePlaceholder3", 3);
        String q = "$.F1 =:thePlaceholder1 or $.F1 =:thePlaceholder2 or $.F1 =:thePlaceholder3";
        this.collection.find(q).fields("$._id as _id, $.F1 as f1").bind(params).orderBy("$.F1 asc").execute();
    }

    @Test
    public void properExceptionUnboundParams() {
        assertThrows(WrongArgumentException.class, "Placeholder 'arg2' is not bound",
                () -> this.collection.find("a = :arg1 or b = :arg2").bind("arg1", 1).execute());
    }

    @Test
    public void bindArgsOrder() {
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{'_id': 1, 'x':1,'y':2}".replaceAll("'", "\"")).execute();
        } else {
            this.collection.add("{'x':1,'y':2}".replaceAll("'", "\"")).execute();
        }
        // same order as query
        assertEquals(1, this.collection.find("x = :x and y = :y").bind("x", 1).bind("y", 2).execute().count());
        // opposite order as query
        assertEquals(1, this.collection.find("x = :x and y = :y").bind("y", 2).bind("x", 1).execute().count());
    }

    // TODO: more tests with unnamed (x = ?) and different bind value types
    // TODO: more tests find & modify
}
