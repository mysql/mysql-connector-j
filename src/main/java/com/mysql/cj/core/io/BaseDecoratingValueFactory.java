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

/**
 * A base class for value factory decorators. The default behavior of all methods is to delegate to the underlying value factory. Subclasses are expected to
 * override one or more creation functions to override or augment the behavior of the underlying value factory.
 */
public abstract class BaseDecoratingValueFactory<T> implements ValueFactory<T> {
    /**
     * The target value factory that the decorator delegates to.
     */
    protected ValueFactory<T> targetVf;

    public BaseDecoratingValueFactory(ValueFactory<T> targetVf) {
        this.targetVf = targetVf;
    }

    public T createFromDate(int year, int month, int day) {
        return this.targetVf.createFromDate(year, month, day);
    }

    public T createFromTime(int hours, int minutes, int seconds, int nanos) {
        return this.targetVf.createFromTime(hours, minutes, seconds, nanos);
    }

    public T createFromTimestamp(int year, int month, int day, int hours, int minutes, int seconds, int nanos) {
        return this.targetVf.createFromTimestamp(year, month, day, hours, minutes, seconds, nanos);
    }

    public T createFromLong(long l) {
        return this.targetVf.createFromLong(l);
    }

    public T createFromBigInteger(BigInteger i) {
        return this.targetVf.createFromBigInteger(i);
    }

    public T createFromDouble(double d) {
        return this.targetVf.createFromDouble(d);
    }

    public T createFromBigDecimal(BigDecimal d) {
        return this.targetVf.createFromBigDecimal(d);
    }

    public T createFromBytes(byte[] bytes, int offset, int length) {
        return this.targetVf.createFromBytes(bytes, offset, length);
    }

    public T createFromBit(byte[] bytes, int offset, int length) {
        return this.targetVf.createFromBit(bytes, offset, length);
    }

    public T createFromNull() {
        return this.targetVf.createFromNull();
    }

    public String getTargetTypeName() {
        return this.targetVf.getTargetTypeName();
    }
}
