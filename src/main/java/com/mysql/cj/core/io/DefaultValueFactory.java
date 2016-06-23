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

import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.DataConversionException;

/**
 * The default value factory provides a base class that can be used for value factories that do not support creation from every type. The default value factory
 * will thrown an UnsupportedOperationException for every method and individual methods must be overridden by subclasses.
 */
public abstract class DefaultValueFactory<T> implements ValueFactory<T> {
    private T unsupported(String sourceType) {
        throw new DataConversionException(Messages.getString("ResultSet.UnsupportedConversion", new Object[] { sourceType, getTargetTypeName() }));
    }

    public T createFromDate(int year, int month, int day) {
        return unsupported("DATE");
    }

    public T createFromTime(int hours, int minutes, int seconds, int nanos) {
        return unsupported("TIME");
    }

    public T createFromTimestamp(int year, int month, int day, int hours, int minutes, int seconds, int nanos) {
        return unsupported("TIMESTAMP");
    }

    public T createFromLong(long l) {
        return unsupported("LONG");
    }

    public T createFromBigInteger(BigInteger i) {
        return unsupported("BIGINT");
    }

    public T createFromDouble(double d) {
        return unsupported("DOUBLE");
    }

    public T createFromBigDecimal(BigDecimal d) {
        return unsupported("DECIMAL");
    }

    public T createFromBytes(byte[] bytes, int offset, int length) {
        return unsupported("VARCHAR/TEXT/BLOB");
    }

    public T createFromBit(byte[] bytes, int offset, int length) {
        return unsupported("BIT");
    }

    public T createFromNull() {
        return null;
    }
}
