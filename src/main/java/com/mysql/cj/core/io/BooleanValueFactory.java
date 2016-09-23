/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.io;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.mysql.cj.mysqla.MysqlaUtils;

/**
 * A value factory for creating {@link java.lang.Boolean} values.
 */
public class BooleanValueFactory extends DefaultValueFactory<Boolean> {
    @Override
    public Boolean createFromLong(long l) {
        // Goes back to ODBC driver compatibility, and VB/Automation Languages/COM, where in Windows "-1" can mean true as well.
        return (l == -1 || l > 0);
    }

    @Override
    public Boolean createFromBigInteger(BigInteger i) {
        return i.compareTo(BigInteger.valueOf(0)) > 0 || i.compareTo(BigInteger.valueOf(-1)) == 0;
    }

    @Override
    // getBoolean() from DOUBLE, DECIMAL are required by JDBC spec....
    public Boolean createFromDouble(double d) {
        // this means that 0.1 or -1 will be TRUE
        return d > 0 || d == -1.0d;
    }

    @Override
    public Boolean createFromBigDecimal(BigDecimal d) {
        // this means that 0.1 or -1 will be TRUE
        return d.compareTo(BigDecimal.valueOf(0)) > 0 || d.compareTo(BigDecimal.valueOf(-1)) == 0;
    }

    @Override
    public Boolean createFromBit(byte[] bytes, int offset, int length) {
        return createFromLong(MysqlaUtils.bitToLong(bytes, offset, length));
    }

    @Override
    public Boolean createFromNull() {
        return false;
    }

    public String getTargetTypeName() {
        return Boolean.class.getName();
    }
}
