/*
 * Copyright (c) 2018, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.x;

import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.x.protobuf.Mysqlx.Error;

/**
 * An Error returned from X Plugin.
 */
public class XProtocolError extends CJException {

    private static final long serialVersionUID = 6991120628391138584L;

    /**
     * The error message returned from the server.
     */
    private Error msg;

    public XProtocolError(String message) {
        super(message);
    }

    public XProtocolError(Error msg) {
        super(getFullErrorDescription(msg));
        this.msg = msg;
    }

    public XProtocolError(XProtocolError fromOtherThread) {
        super(getFullErrorDescription(fromOtherThread.msg), fromOtherThread);
        this.msg = fromOtherThread.msg;
    }

    public XProtocolError(String message, Throwable t) {
        super(message, t);
    }

    /**
     * Format the error message's contents into a complete error description for the exception.
     *
     * @param msg
     *            {@link Error}
     * @return string error message
     */
    private static String getFullErrorDescription(Error msg) {
        StringBuilder stringMessage = new StringBuilder("ERROR ");
        stringMessage.append(msg.getCode());
        stringMessage.append(" (");
        stringMessage.append(msg.getSqlState());
        stringMessage.append(") ");
        stringMessage.append(msg.getMsg());
        return stringMessage.toString();
    }

    public int getErrorCode() {
        return this.msg == null ? super.getVendorCode() : this.msg.getCode();
    }

    @Override
    public String getSQLState() {
        return this.msg == null ? super.getSQLState() : this.msg.getSqlState();
    }

}
