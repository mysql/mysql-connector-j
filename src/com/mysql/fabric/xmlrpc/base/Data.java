/*
  Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.fabric.xmlrpc.base;

import java.util.ArrayList;
import java.util.List;

public class Data {

    protected List<Value> value;

    public List<Value> getValue() {
        if (this.value == null) {
            this.value = new ArrayList<Value>();
        }
        return this.value;
    }

    public void addValue(Value v) {
        getValue().add(v);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        if (this.value != null) {
            sb.append("<data>");
            for (int i = 0; i < this.value.size(); i++) {
                sb.append(this.value.get(i).toString());
            }
            sb.append("</data>");
        }
        return sb.toString();
    }

}
