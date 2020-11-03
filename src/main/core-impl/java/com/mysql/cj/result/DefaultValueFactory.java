/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

import com.mysql.cj.Messages;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.DataConversionException;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;

/**
 * The default value factory provides a base class that can be used for value factories that do not support creation from every type. The default value factory
 * will thrown an UnsupportedOperationException for every method and individual methods must be overridden by subclasses.
 * 
 * @param <T>
 *            value type
 */
public abstract class DefaultValueFactory<T> implements ValueFactory<T> {

    protected boolean jdbcCompliantTruncationForReads = true;

    public DefaultValueFactory(PropertySet pset) {
        this.pset = pset;

        // TODO we always check initial value here, whatever the setupServerForTruncationChecks() does for writes.
        // It also means that runtime changes of this variable have no effect on reads.
        this.jdbcCompliantTruncationForReads = this.pset.getBooleanProperty(PropertyKey.jdbcCompliantTruncation).getInitialValue();
    }

    protected PropertySet pset = null;

    @Override
    public void setPropertySet(PropertySet pset) {
        this.pset = pset;
    }

    protected T unsupported(String sourceType) {
        throw new DataConversionException(Messages.getString("ResultSet.UnsupportedConversion", new Object[] { sourceType, getTargetTypeName() }));
    }

    public T createFromDate(InternalDate idate) {
        return unsupported("DATE");
    }

    public T createFromTime(InternalTime it) {
        return unsupported("TIME");
    }

    public T createFromTimestamp(InternalTimestamp its) {
        return unsupported("TIMESTAMP");
    }

    @Override
    public T createFromDatetime(InternalTimestamp its) {
        return unsupported("DATETIME");
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

    public T createFromBit(byte[] bytes, int offset, int length) {
        return unsupported("BIT");
    }

    @Override
    public T createFromYear(long l) {
        return unsupported("YEAR");
    }

    public T createFromNull() {
        return null;
    }
}
