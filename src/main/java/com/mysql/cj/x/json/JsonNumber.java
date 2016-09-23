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

import java.math.BigDecimal;

import com.mysql.cj.api.x.JsonValue;

/**
 * Represents a JSON <b>number</b>.
 */
public class JsonNumber implements JsonValue {

    private String val = "null";

    /**
     * 
     * @return value as Integer
     */
    public Integer getInteger() {
        return Integer.valueOf(this.val);
    }

    /**
     * 
     * @return value as BigDecimal
     */
    public BigDecimal getBigDecimal() {
        return new BigDecimal(this.val);
    }

    public JsonNumber setValue(String value) {
        // validate with BigDecimal
        this.val = new BigDecimal(value).toString();
        return this;
    }

    @Override
    public String toString() {
        return this.val;
    }

}
