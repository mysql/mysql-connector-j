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

import java.util.ArrayList;

import com.mysql.cj.api.x.JsonValue;

/**
 * Represents a JSON <b>array</b>.
 */
public class JsonArray extends ArrayList<JsonValue> implements JsonValue {

    private static final long serialVersionUID = 6557406141541247905L;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        boolean isFirst = true;
        for (JsonValue val : this) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(", ");
            }

            sb.append(val.toString());
        }

        sb.append("]");
        return sb.toString();
    }

    public String toPackedString() {
        StringBuilder sb = new StringBuilder("[");
        boolean isFirst = true;
        for (JsonValue val : this) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(",");
            }

            sb.append(val.toString());
        }

        sb.append("]");
        return sb.toString();
    }

    public JsonArray addValue(JsonValue val) {
        add(val);
        return this;
    }

}
