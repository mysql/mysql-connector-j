/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

import java.util.ArrayList;

/**
 * Represents a JSON <b>array</b>.
 */
public class JsonArray extends ArrayList<JsonValue> implements JsonValue {

    private static final long serialVersionUID = 6557406141541247905L;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (JsonValue val : this) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(val.toString());
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public String toFormattedString() {
        StringBuilder sb = new StringBuilder("[");
        for (JsonValue val : this) {
            if (sb.length() > 1) {
                sb.append(", ");
            }
            sb.append(val.toFormattedString());
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Add value to this array.
     * 
     * @param val
     *            value
     * @return this JsonArray
     */
    public JsonArray addValue(JsonValue val) {
        add(val);
        return this;
    }

}
