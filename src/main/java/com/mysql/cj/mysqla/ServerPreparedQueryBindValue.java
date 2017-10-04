/*
  Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqla;

import java.util.TimeZone;

import com.mysql.cj.api.BindValue;

public class ServerPreparedQueryBindValue extends ClientPreparedQueryBindValue implements BindValue {

    public long boundBeforeExecutionNum = 0;

    /** Default length of data */
    public long bindLength;

    public int bufferType;

    public double doubleBinding;

    public float floatBinding;

    public boolean isLongData;

    /** has this parameter been set? */
    private boolean isSet = false;

    /** all integral values are stored here */
    public long longBinding;

    /** The value to store */
    public Object value;

    /** The TimeZone for date/time types */
    public TimeZone tz;

    public ServerPreparedQueryBindValue() {
    }

    @Override
    public ServerPreparedQueryBindValue clone() {
        return new ServerPreparedQueryBindValue(this);
    }

    private ServerPreparedQueryBindValue(ServerPreparedQueryBindValue copyMe) {
        super(copyMe);

        this.value = copyMe.value;
        this.isSet = copyMe.isSet;
        this.isLongData = copyMe.isLongData;
        this.bufferType = copyMe.bufferType;
        this.bindLength = copyMe.bindLength;
        this.longBinding = copyMe.longBinding;
        this.floatBinding = copyMe.floatBinding;
        this.doubleBinding = copyMe.doubleBinding;
        this.tz = copyMe.tz;
    }

    @Override
    public void reset() {
        super.reset();

        this.isSet = false;
        this.value = null;
        this.isLongData = false;

        this.longBinding = 0L;
        this.floatBinding = 0;
        this.doubleBinding = 0D;
        this.tz = null;
    }

    /**
     * Reset a bind value to be used for a new value of the given type.
     * 
     * @param bufType
     * @param numberOfExecutions
     * @return true if we need to send/resend types to the server
     */
    public boolean resetToType(int bufType, long numberOfExecutions) {
        boolean sendTypesToServer = false;

        // clear any possible old value
        reset();

        if (bufType == MysqlaConstants.FIELD_TYPE_NULL && this.bufferType != 0) {
            // preserve the previous type to (possibly) avoid sending types at execution time
        } else if (this.bufferType != bufType) {
            sendTypesToServer = true;
            this.bufferType = bufType;
        }

        // setup bind value for use
        this.isSet = true;
        this.boundBeforeExecutionNum = numberOfExecutions;
        return sendTypesToServer;
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public String toString(boolean quoteIfNeeded) {
        if (this.isLongData) {
            return "' STREAM DATA '";
        }

        if (this.isNull) {
            return "NULL";
        }

        switch (this.bufferType) {
            case MysqlaConstants.FIELD_TYPE_TINY:
            case MysqlaConstants.FIELD_TYPE_SHORT:
            case MysqlaConstants.FIELD_TYPE_LONG:
            case MysqlaConstants.FIELD_TYPE_LONGLONG:
                return String.valueOf(this.longBinding);
            case MysqlaConstants.FIELD_TYPE_FLOAT:
                return String.valueOf(this.floatBinding);
            case MysqlaConstants.FIELD_TYPE_DOUBLE:
                return String.valueOf(this.doubleBinding);
            case MysqlaConstants.FIELD_TYPE_TIME:
            case MysqlaConstants.FIELD_TYPE_DATE:
            case MysqlaConstants.FIELD_TYPE_DATETIME:
            case MysqlaConstants.FIELD_TYPE_TIMESTAMP:
            case MysqlaConstants.FIELD_TYPE_VAR_STRING:
            case MysqlaConstants.FIELD_TYPE_STRING:
            case MysqlaConstants.FIELD_TYPE_VARCHAR:
                if (quoteIfNeeded) {
                    return "'" + String.valueOf(this.value) + "'";
                }
                return String.valueOf(this.value);

            default:
                if (this.value instanceof byte[]) {
                    return "byte data";
                }
                if (quoteIfNeeded) {
                    return "'" + String.valueOf(this.value) + "'";
                }
                return String.valueOf(this.value);
        }
    }

    public long getBoundLength() {
        if (this.isNull) {
            return 0;
        }

        if (this.isLongData) {
            return this.bindLength;
        }

        switch (this.bufferType) {

            case MysqlaConstants.FIELD_TYPE_TINY:
                return 1;
            case MysqlaConstants.FIELD_TYPE_SHORT:
                return 2;
            case MysqlaConstants.FIELD_TYPE_LONG:
                return 4;
            case MysqlaConstants.FIELD_TYPE_LONGLONG:
                return 8;
            case MysqlaConstants.FIELD_TYPE_FLOAT:
                return 4;
            case MysqlaConstants.FIELD_TYPE_DOUBLE:
                return 8;
            case MysqlaConstants.FIELD_TYPE_TIME:
                return 9;
            case MysqlaConstants.FIELD_TYPE_DATE:
                return 7;
            case MysqlaConstants.FIELD_TYPE_DATETIME:
            case MysqlaConstants.FIELD_TYPE_TIMESTAMP:
                return 11;
            case MysqlaConstants.FIELD_TYPE_VAR_STRING:
            case MysqlaConstants.FIELD_TYPE_STRING:
            case MysqlaConstants.FIELD_TYPE_VARCHAR:
            case MysqlaConstants.FIELD_TYPE_DECIMAL:
            case MysqlaConstants.FIELD_TYPE_NEWDECIMAL:
                if (this.value instanceof byte[]) {
                    return ((byte[]) this.value).length;
                }
                return ((String) this.value).length();

            default:
                return 0;
        }
    }

    @Override
    public boolean isSet() {
        return this.isSet;
    }
}
