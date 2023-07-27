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

import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.ValueEncoder;
import com.mysql.cj.result.Field;

public class NativeQueryBindValue implements BindValue {

    /** NULL indicator */
    protected boolean isNull;

    protected boolean isNational = false;

    protected MysqlType targetType = MysqlType.NULL;

    /** The value to store */
    public Object value;

    /** has this parameter been set? */
    protected boolean isSet = false;

    /* Calendar to be used for DATE and DATETIME values storing */
    public Calendar calendar;

    protected boolean escapeBytesIfNeeded = true;

    /** Is this query a LOAD DATA query? */
    protected boolean isLoadDataQuery = false;

    PropertySet pset;
    Protocol<?> protocol;
    ServerSession serverSession;
    ExceptionInterceptor exceptionInterceptor;

    private Field field = null;
    protected boolean keepOrigNanos = false;
    protected ValueEncoder valueEncoder = null;
    protected long scaleOrLength = -1;
    protected long boundBeforeExecutionNum = 0; // specific to ServerPreparedQuery

    /** The query attribute name */
    private String name;

    public NativeQueryBindValue(Session sess) {
        this.pset = sess.getPropertySet();
        this.protocol = ((NativeSession) sess).getProtocol();
        this.serverSession = sess.getServerSession();
        this.exceptionInterceptor = sess.getExceptionInterceptor();
    }

    @Override
    public NativeQueryBindValue clone() {
        return new NativeQueryBindValue(this);
    }

    protected NativeQueryBindValue(NativeQueryBindValue copyMe) {
        this.isNull = copyMe.isNull;
        this.targetType = copyMe.targetType;
        if (copyMe.value != null && copyMe.value instanceof byte[]) {
            this.value = new byte[((byte[]) copyMe.value).length];
            System.arraycopy(copyMe.value, 0, this.value, 0, ((byte[]) copyMe.value).length);
        } else {
            this.value = copyMe.value;
        }
        this.isSet = copyMe.isSet;
        this.pset = copyMe.pset;
        this.protocol = copyMe.protocol;
        this.serverSession = copyMe.serverSession;
        this.calendar = copyMe.calendar;
        this.escapeBytesIfNeeded = copyMe.escapeBytesIfNeeded;
        this.isLoadDataQuery = copyMe.isLoadDataQuery;
        this.isNational = copyMe.isNational;
        this.field = copyMe.field;
        this.keepOrigNanos = copyMe.keepOrigNanos;
        this.valueEncoder = copyMe.valueEncoder;
        this.scaleOrLength = copyMe.scaleOrLength;
        this.boundBeforeExecutionNum = copyMe.boundBeforeExecutionNum;
    }

    private boolean resetToType(MysqlType newTargetType) { // specific to ServerPreparedQuery
        // clear any possible old value
        reset();

        if (newTargetType == MysqlType.NULL) {
            // preserve the previous type to (possibly) avoid sending types at execution time
        } else if (this.targetType != newTargetType) {
            return true;
        }

        return false;
    }

    @Override
    public void setBinding(Object obj, MysqlType type, int numberOfExecutions, AtomicBoolean sendTypesToServer) {
        if (sendTypesToServer != null) {
            sendTypesToServer.compareAndSet(false, resetToType(type)); // specific to ServerPreparedQuery
        }

        this.value = obj;
        this.targetType = type;
        this.boundBeforeExecutionNum = numberOfExecutions;

        this.isNull = this.targetType == MysqlType.NULL;
        this.isSet = true;
        this.escapeBytesIfNeeded = true;

        Supplier<ValueEncoder> vc = this.protocol.getValueEncoderSupplier(this.isNull ? null : this.value);
        if (vc != null) {
            this.valueEncoder = vc.get();
            this.valueEncoder.init(this.pset, this.serverSession, this.exceptionInterceptor);
        } else {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    Messages.getString("PreparedStatement.67", new Object[] { obj.getClass().getName(), type.name() }), this.exceptionInterceptor);
        }
    }

    @Override
    public byte[] getByteValue() {
        if (this.valueEncoder != null) {
            return this.valueEncoder.getBytes(this);
        }
        return null;
    }

    @Override
    public void reset() {
        this.isNull = false;
        this.targetType = MysqlType.NULL;
        this.value = null;
        this.isSet = false;
        this.calendar = null; // TODO how is it set again?
        this.escapeBytesIfNeeded = true;
        this.isLoadDataQuery = false;
        this.isNational = false;
        this.field = null;
        this.keepOrigNanos = false;
        this.valueEncoder = null;
        this.scaleOrLength = -1;
    }

    @Override
    public boolean isNull() {
        return this.isNull;
    }

    @Override
    public void setNull(boolean isNull) {
        this.isNull = isNull;
        if (isNull) {
            this.targetType = MysqlType.NULL;
        }
        this.isSet = true;
    }

    @Override
    public boolean isStream() {
        return this.value instanceof InputStream || this.value instanceof Reader || this.value instanceof Clob || this.value instanceof Blob;
    }

    @Override
    public boolean isNational() {
        return this.isNational;
    }

    @Override
    public void setIsNational(boolean isNational) {
        this.isNational = isNational;
    }

    @Override
    public Object getValue() {
        return this.value;
    }

    @Override
    public Field getField() {
        return this.field;
    }

    @Override
    public void setField(Field field) {
        this.field = field;
    }

    @Override
    public boolean keepOrigNanos() {
        return this.keepOrigNanos;
    }

    @Override
    public void setKeepOrigNanos(boolean value) {
        this.keepOrigNanos = value;
    }

    @Override
    public MysqlType getMysqlType() {
        return this.targetType;
    }

    @Override
    public void setMysqlType(MysqlType type) {
        this.targetType = type;
    }

    @Override
    public boolean escapeBytesIfNeeded() {
        return this.escapeBytesIfNeeded;
    }

    @Override
    public void setEscapeBytesIfNeeded(boolean val) {
        this.escapeBytesIfNeeded = val;
    }

    @Override
    public boolean isSet() {
        return this.isSet;
    }

    @Override
    public Calendar getCalendar() {
        return this.calendar;
    }

    @Override
    public void setCalendar(Calendar cal) {
        this.calendar = cal;
    }

    @Override
    public int getFieldType() {
        switch (this.targetType) {
            case NULL:
                return MysqlType.FIELD_TYPE_NULL;
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                return MysqlType.FIELD_TYPE_NEWDECIMAL;
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                return MysqlType.FIELD_TYPE_DOUBLE;
            case BIGINT:
            case BIGINT_UNSIGNED:
                return MysqlType.FIELD_TYPE_LONGLONG;
            case BIT:
            case BOOLEAN:
            case TINYINT:
            case TINYINT_UNSIGNED:
                return MysqlType.FIELD_TYPE_TINY;
            case BINARY:
            case VARBINARY:
            case CHAR:
            case VARCHAR:
                return MysqlType.FIELD_TYPE_VAR_STRING;
            case FLOAT:
            case FLOAT_UNSIGNED:
                return MysqlType.FIELD_TYPE_FLOAT;
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
                return MysqlType.FIELD_TYPE_SHORT;
            case INT:
            case INT_UNSIGNED:
            case YEAR:
                return MysqlType.FIELD_TYPE_LONG;
            case DATE:
                return MysqlType.FIELD_TYPE_DATE;
            case TIME:
                return MysqlType.FIELD_TYPE_TIME;
            case TIMESTAMP:
                return MysqlType.FIELD_TYPE_TIMESTAMP;
            case DATETIME:
                return MysqlType.FIELD_TYPE_DATETIME;
            case BLOB:
            case TEXT:
                return MysqlType.FIELD_TYPE_BLOB;
            case TINYBLOB:
            case TINYTEXT:
                return MysqlType.FIELD_TYPE_TINY_BLOB;
            case MEDIUMBLOB:
            case MEDIUMTEXT:
                return MysqlType.FIELD_TYPE_MEDIUM_BLOB;
            case LONGBLOB:
            case LONGTEXT:
                return MysqlType.FIELD_TYPE_LONG_BLOB;
            //            case JSON:
            //            case ENUM:
            //            case SET:
            //            case GEOMETRY:
            default:
                return MysqlType.FIELD_TYPE_VAR_STRING;
        }
    }

    @Override
    public long getTextLength() {
        return this.valueEncoder == null ? -1 : this.valueEncoder.getTextLength(this);
    }

    @Override
    public long getBinaryLength() {
        return this.valueEncoder == null ? -1 : this.valueEncoder.getBinaryLength(this);
    }

    @Override
    public long getBoundBeforeExecutionNum() { // specific to ServerPreparedQuery
        return this.boundBeforeExecutionNum;
    }

    @Override
    public String getString() {
        if (this.valueEncoder == null) {
            return "** NOT SPECIFIED **";
        }
        return this.valueEncoder.getString(this);
    }

    @Override
    public long getScaleOrLength() {
        return this.scaleOrLength;
    }

    @Override
    public void setScaleOrLength(long scaleOrLength) {
        this.scaleOrLength = scaleOrLength;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void writeAsText(Message intoMessage) {
        this.valueEncoder.encodeAsText(intoMessage, this);
    }

    @Override
    public void writeAsBinary(Message intoMessage) {
        this.valueEncoder.encodeAsBinary(intoMessage, this);
    }

    @Override
    public void writeAsQueryAttribute(Message intoMessage) {
        this.valueEncoder.encodeAsQueryAttribute(intoMessage, this);
    }

}
