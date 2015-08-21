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

import com.mysql.cj.api.x.JsonValue;

/**
 * Represents JSON <b>true</b>, <b>false</b> and <b>null</b> literals.
 */
public enum JsonLiteral implements JsonValue {

    TRUE("\u0074\u0072\u0075\u0065"), FALSE("\u0066\u0061\u006c\u0073\u0065"), NULL("\u006E\u0075\u006C\u006C");

    public final String value;

    private JsonLiteral(String val) {
        this.value = val;
    }

    @Override
    public String toString() {
        return this.value;
    }

}
