/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.result;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;

/**
 * A class implements the <code>ValueFactory&lt;T&gt;</code> interface to create value instances from intermediate forms.
 * <p>
 * A <code>ValueFactory</code> implementation is responsible for creating instance of a single type, supplying a value for the type parameter <code>T</code>. If
 * an instance cannot be created from the intermediate form, an {@link java.lang.UnsupportedOperationException} can be thrown.
 *
 * @param <T>
 *            value type
 */
public interface ValueFactory<T> {

    void setPropertySet(PropertySet pset);

    T createFromDate(InternalDate idate);

    T createFromTime(InternalTime it);

    T createFromTimestamp(InternalTimestamp its);

    T createFromDatetime(InternalTimestamp its);

    T createFromLong(long l);

    T createFromBigInteger(BigInteger i);

    T createFromDouble(double d);

    T createFromBigDecimal(BigDecimal d);

    T createFromBytes(byte[] bytes, int offset, int length, Field f);

    T createFromBit(byte[] bytes, int offset, int length);

    T createFromYear(long l);

    /**
     * Create result value from intermediate null value.
     *
     * @return T object
     */
    T createFromNull();

    /**
     * Get the actual class name of T parameter.
     *
     * @return class name
     */
    String getTargetTypeName();

}
