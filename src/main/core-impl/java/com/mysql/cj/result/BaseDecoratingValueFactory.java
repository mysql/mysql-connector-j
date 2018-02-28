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

package com.mysql.cj.result;

import java.math.BigDecimal;
import java.math.BigInteger;

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
