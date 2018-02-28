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

package com.mysql.cj.xdevapi;

import java.util.HashMap;

import com.mysql.cj.xdevapi.JsonParser.EscapeChar;

/**
 * Represents a JSON <b>string</b>.
 */
public class JsonString implements JsonValue {

    static HashMap<Character, String> escapeChars = new HashMap<>();

    static {
        for (EscapeChar ec : EscapeChar.values()) {
            escapeChars.put(ec.CHAR, ec.ESCAPED);
        }
    }

    private String val = "";

    /**
     * 
     * @return internal (unescaped) representation of JSON value
     */
    public String getString() {
        return this.val;
    }

    public JsonString setValue(String value) {
        this.val = value;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\"");

        for (int i = 0; i < this.val.length(); i++) {
            if (escapeChars.containsKey(this.val.charAt(i))) {
                sb.append(escapeChars.get(this.val.charAt(i)));
            } else {
                sb.append(this.val.charAt(i));
            }
        }

        sb.append("\"");
        return sb.toString();
    }

}
