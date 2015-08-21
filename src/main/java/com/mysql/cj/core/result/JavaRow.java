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

package com.mysql.cj.core.result;

import java.util.ArrayList;

import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.result.Row;
import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.core.conf.DefaultPropertySet;
import com.mysql.cj.core.util.LazyString;
import com.mysql.cj.jdbc.Field;

public class JavaRow implements Row {
    private ArrayList<Field> metadata = new ArrayList<>();

    public JavaRow() {
    }

    public <T> T getValue(int columnIndex, ValueFactory<T> vf) {
        throw new NullPointerException("TODO: ");
    }

    public boolean getNull(int columnIndex) {
        throw new NullPointerException("TODO: ");
    }

    public boolean wasNull() {
        throw new NullPointerException("TODO: ");
    }

    public void addField(int mysqlType) {
        PropertySet propertySet = new DefaultPropertySet();
        int length = 0;
        short colFlag = 0;
        int colDecimals = 0;
        int collationIndex = 33;
        String encoding = "utf8";
        Field f = new Field(propertySet, new LazyString(null), new LazyString(null), new LazyString(null), new LazyString(null), new LazyString(null), 0,
                mysqlType, colFlag, colDecimals, collationIndex, encoding);
        this.metadata.add(f);
    }
}
