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

package com.mysql.cj.x.json;

import java.util.TreeMap;

import com.mysql.cj.api.x.JsonValue;

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
 * To create {@link JsonDoc} from existing string representation you need to use {@link JsonParser#parseDoc(java.io.StringReader)} method:
 * 
 * <pre>
 * JsonDoc doc = JsonParser.parseDoc(new StringReader(&quot;{\&quot;key1\&quot; : \&quot;value1\&quot;}&quot;));
 * </pre>
 * 
 * You can construct JSON document by {@link JsonDoc}, {@link JsonString}, {@link JsonNumber}, {@link JsonArray} and {@link JsonLiteral} methods and get JSON
 * string representation by using {@link JsonDoc#toString()} method. For example, to get the document shown above:
 * 
 * <pre>
 * JsonDoc doc = new JsonDoc()
 *         .add(&quot;field1&quot;, new JsonString().setValue(&quot;value 1&quot;))
 *         .add(&quot;field2&quot;, new JsonNumber().setValue(&quot;12345.44E22&quot;))
 *         .add(&quot;field3&quot;, JsonLiteral.TRUE)
 *         .add(&quot;field4&quot;, JsonLiteral.FALSE)
 *         .add(&quot;field5&quot;, JsonLiteral.NULL)
 *         .add(&quot;field6&quot;,
 *                 new JsonDoc().add(&quot;inner field 1&quot;, new JsonString().setValue(&quot;inner value 1&quot;)).add(&quot;inner field 2&quot;, new JsonNumber().setValue(&quot;2&quot;))
 *                         .add(&quot;inner field 3&quot;, JsonLiteral.TRUE).add(&quot;inner field 4&quot;, JsonLiteral.FALSE).add(&quot;inner field 5&quot;, JsonLiteral.NULL)
 *                         .add(&quot;inner field 6&quot;, new JsonArray()).add(&quot;inner field 7&quot;, new JsonDoc()))
 *         .add(&quot;field7&quot;,
 *                 new JsonArray().addValue(new JsonString().setValue(&quot;arr1&quot;)).addValue(new JsonNumber().setValue(&quot;3&quot;)).addValue(JsonLiteral.TRUE)
 *                         .addValue(JsonLiteral.FALSE).addValue(JsonLiteral.NULL).addValue(new JsonArray()).addValue(new JsonDoc()));
 * 
 * doc.toString();
 * </pre>
 */
public class JsonDoc extends TreeMap<String, JsonValue> implements JsonValue {

    private static final long serialVersionUID = 6557406141541247905L;

    /**
     * @return JSON string
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        boolean isFirst = true;
        for (String key : keySet()) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(",");
            }
            sb.append("\n\"").append(key).append("\" : ").append(get(key).toString());
        }
        if (size() > 0) {
            sb.append("\n");
        }
        sb.append("}");
        return sb.toString();
    }

    public JsonDoc add(String key, JsonValue val) {
        put(key, val);
        return this;
    }

}
