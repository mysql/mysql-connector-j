/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.x.json;

import java.util.HashMap;

import com.mysql.cj.api.x.JsonValue;
import com.mysql.cj.x.json.JsonParser.EscapeChar;

/**
 * Represents a JSON <b>string</b>.
 */
public class JsonString implements JsonValue {

    static HashMap<Character, String> escapeChars = new HashMap<Character, String>();

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
