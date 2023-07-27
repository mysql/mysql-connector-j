/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.xdevapi;

import java.util.TreeMap;

/**
 * Represents a JSON <b>object</b>:
 *
 * <pre>
 *   {}
 *   or
 *   {key : value}
 *   or
 *   {key : value, key : value, ...}
 * </pre>
 *
 * <b>key</b> is a JSON <b>string</b>.
 * <b>value</b> is any of JSON <b>object</b>, <b>array</b>, <b>number</b>, <b>string</b>, true, false, null.
 *
 * Example of valid JSON object:
 *
 * <pre>
 * {
 * "field1" : "value 1",
 * "field2" : 1.234544E+26,
 * "field3" : true,
 * "field4" : false,
 * "field5" : null,
 * "field6" : {
 *            "inner field 1" : "inner value 1",
 *            "inner field 2" : 2,
 *            "inner field 3" : true,
 *            "inner field 4" : false,
 *            "inner field 5" : null,
 *            "inner field 6" : [],
 *            "inner field 7" : {}
 *            },
 * "field7" : ["arr1", 3, true, false, null, [], {}]
 * }
 * </pre>
 *
 * To create {@link DbDoc} from existing string representation you need to use {@link JsonParser#parseDoc(java.io.StringReader)} method:
 *
 * <pre>
 *
 * DbDoc doc = JsonParser.parseDoc(new StringReader(&quot;{\&quot;key1\&quot; : \&quot;value1\&quot;}&quot;));
 * </pre>
 *
 * You can construct JSON document by {@link DbDoc}, {@link JsonString}, {@link JsonNumber}, {@link JsonArray} and {@link JsonLiteral} methods and get JSON
 * string representation by using {@link #toString()} method. For example, to get the document shown above:
 *
 * <pre>
 * DbDoc doc = new DbDoc().add(&quot;field1&quot;, new JsonString().setValue(&quot;value 1&quot;)).add(&quot;field2&quot;, new JsonNumber().setValue(&quot;12345.44E22&quot;))
 *         .add(&quot;field3&quot;, JsonLiteral.TRUE).add(&quot;field4&quot;, JsonLiteral.FALSE).add(&quot;field5&quot;, JsonLiteral.NULL)
 *         .add(&quot;field6&quot;,
 *                 new DbDoc().add(&quot;inner field 1&quot;, new JsonString().setValue(&quot;inner value 1&quot;)).add(&quot;inner field 2&quot;, new JsonNumber().setValue(&quot;2&quot;))
 *                         .add(&quot;inner field 3&quot;, JsonLiteral.TRUE).add(&quot;inner field 4&quot;, JsonLiteral.FALSE).add(&quot;inner field 5&quot;, JsonLiteral.NULL)
 *                         .add(&quot;inner field 6&quot;, new JsonArray()).add(&quot;inner field 7&quot;, new DbDoc()))
 *         .add(&quot;field7&quot;, new JsonArray().addValue(new JsonString().setValue(&quot;arr1&quot;)).addValue(new JsonNumber().setValue(&quot;3&quot;)).addValue(JsonLiteral.TRUE)
 *                 .addValue(JsonLiteral.FALSE).addValue(JsonLiteral.NULL).addValue(new JsonArray()).addValue(new DbDoc()));
 *
 * doc.toString();
 * </pre>
 */
public class DbDocImpl extends TreeMap<String, JsonValue> implements DbDoc {

    private static final long serialVersionUID = 6557406141541247905L;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        for (String key : keySet()) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append("\"").append(key).append("\":").append(get(key).toString());
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String toFormattedString() {
        StringBuilder sb = new StringBuilder("{");
        for (String key : keySet()) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append("\n\"").append(key).append("\" : ").append(get(key).toFormattedString());
        }
        if (size() > 0) {
            sb.append("\n");
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public DbDoc add(String key, JsonValue val) {
        put(key, val);
        return this;
    }

}
