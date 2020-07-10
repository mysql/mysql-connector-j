/*
 * Copyright (c) 2017, 2020, Oracle and/or its affiliates.
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

public class ClientPreparedQueryBindValue implements BindValue {

    /** NULL indicator */
    protected boolean isNull;

    protected boolean isStream = false;

    protected MysqlType parameterType = MysqlType.NULL;

    /** The value to store */
    public Object value;

    public Object origValue;

    protected long streamLength;

    /** has this parameter been set? */
    protected boolean isSet = false;

    public ClientPreparedQueryBindValue() {
    }

    @Override
    public ClientPreparedQueryBindValue clone() {
        return new ClientPreparedQueryBindValue(this);
    }

    protected ClientPreparedQueryBindValue(ClientPreparedQueryBindValue copyMe) {
        this.isNull = copyMe.isNull;
        this.isStream = copyMe.isStream;
        this.parameterType = copyMe.parameterType;
        if (copyMe.value != null && copyMe.value instanceof byte[]) {
            this.value = new byte[((byte[]) copyMe.value).length];
            System.arraycopy(copyMe.value, 0, this.value, 0, ((byte[]) copyMe.value).length);
        } else {
            this.value = copyMe.value;
        }
        this.streamLength = copyMe.streamLength;
        this.isSet = copyMe.isSet;
    }

    public void reset() {
        this.isNull = false;
        this.isStream = false;
        this.parameterType = MysqlType.NULL;
        this.value = null;
        this.origValue = null;
        this.streamLength = 0;
        this.isSet = false;
    }

    @Override
    public boolean isNull() {
        return this.isNull;
    }

    public void setNull(boolean isNull) {
        this.isNull = isNull;
        if (isNull) {
            this.parameterType = MysqlType.NULL;
        }
        this.isSet = true;
    }

    public boolean isStream() {
        return this.isStream;
    }

    public void setIsStream(boolean isStream) {
        this.isStream = isStream;
    }

    @Override
    public MysqlType getMysqlType() {
        return this.parameterType;
    }

    @Override
    public void setMysqlType(MysqlType type) {
        this.parameterType = type;
    }

    public byte[] getByteValue() {
        if (this.value instanceof byte[]) {
            return (byte[]) this.value;
        }
        return null;
    }

    public void setByteValue(byte[] parameterValue) {
        this.isNull = false;
        this.isStream = false;
        this.value = parameterValue;
        this.streamLength = 0;
        this.isSet = true;
    }

    @Override
    public void setOrigByteValue(byte[] origParamValue) {
        this.origValue = origParamValue;
    }

    @Override
    public byte[] getOrigByteValue() {
        return (byte[]) this.origValue;
    }

    public InputStream getStreamValue() {
        if (this.value instanceof InputStream) {
            return (InputStream) this.value;
        }
        return null;
    }

    public void setStreamValue(InputStream parameterStream, long streamLength) {
        this.value = parameterStream;
        this.streamLength = streamLength;
        this.isSet = true;
    }

    public long getStreamLength() {
        return this.streamLength;
    }

    public boolean isSet() {
        return this.isSet;
    }
}
