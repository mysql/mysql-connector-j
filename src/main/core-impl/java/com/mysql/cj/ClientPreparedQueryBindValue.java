/*
 * Copyright (c) 2017, 2018, Oracle and/or its affiliates. All rights reserved.
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

//TODO should not be protocol-specific

public class ClientPreparedQueryBindValue implements BindValue {

    /** NULL indicator */
    protected boolean isNull;

    private boolean isStream = false;

    protected MysqlType parameterType = MysqlType.NULL;

    private byte[] parameterValue = null;

    private InputStream parameterStream = null;

    private int streamLength;

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
        if (copyMe.parameterValue != null) {
            this.parameterValue = new byte[copyMe.parameterValue.length];
            System.arraycopy(copyMe.parameterValue, 0, this.parameterValue, 0, copyMe.parameterValue.length);
        }
        this.parameterStream = copyMe.parameterStream;
        this.streamLength = copyMe.streamLength;
    }

    public void reset() {
        this.isNull = false;
        this.isStream = false;
        this.parameterType = MysqlType.NULL;
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
        return this.parameterValue;
    }

    public void setByteValue(byte[] parameterValue) {
        this.isNull = false;
        this.isStream = false;
        this.parameterValue = parameterValue;
        this.parameterStream = null;
        this.streamLength = 0;
    }

    public InputStream getStreamValue() {
        return this.parameterStream;
    }

    public void setStreamValue(InputStream parameterStream, int streamLength) {
        this.parameterStream = parameterStream;
        this.streamLength = streamLength;
    }

    public int getStreamLength() {
        return this.streamLength;
    }

    public boolean isSet() {
        return this.parameterValue != null || this.parameterStream != null;
    }
}
