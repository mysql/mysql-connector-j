/*
 * Copyright (c) 2017, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj;

import java.util.Calendar;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mysql.cj.protocol.Message;
import com.mysql.cj.result.Field;

public interface BindValue {

    BindValue clone();

    void reset();

    boolean isNull();

    void setNull(boolean isNull);

    boolean isStream();

    MysqlType getMysqlType();

    void setMysqlType(MysqlType type);

    byte[] getByteValue();

    boolean isSet();

    void setBinding(Object obj, MysqlType type, int numberOfExecutions, AtomicBoolean sendTypesToServer);

    Calendar getCalendar();

    void setCalendar(Calendar cal);

    boolean escapeBytesIfNeeded();

    void setEscapeBytesIfNeeded(boolean val);

    Object getValue();

    boolean isNational();

    void setIsNational(boolean isNational);

    int getFieldType();

    /**
     * Gets the length of this bind value in the text protocol representation.
     *
     * @return
     *         the expected length, in bytes, of this bind value after being encoded.
     */
    long getTextLength();

    /**
     * Gets the length of this bind value in the binary protocol representation.
     *
     * @return
     *         the expected length, in bytes, of this bind value after being encoded.
     */
    long getBinaryLength();

    long getBoundBeforeExecutionNum();

    /**
     * Get a String representation of the value.
     *
     * @return value as a String
     */
    String getString();

    Field getField();

    void setField(Field field);

    boolean keepOrigNanos();

    /**
     * Should the value keep original fractional seconds ignoring sendFractionalSeconds and sendFractionalSecondsForTime?
     * <p>
     * <i>If the value is a part of key for UpdatableResultSet updater, it should keep original milliseconds.</i>
     * </p>
     *
     * @param value
     *            the value to set
     */
    void setKeepOrigNanos(boolean value);

    void setScaleOrLength(long scaleOrLength);

    long getScaleOrLength();

    /**
     * Gets the name of this query attribute.
     *
     * @return
     *         the name of this query attribute.
     */
    String getName();

    void setName(String name);

    void writeAsText(Message intoMessage);

    void writeAsBinary(Message intoMessage);

    void writeAsQueryAttribute(Message intoMessage);

}
