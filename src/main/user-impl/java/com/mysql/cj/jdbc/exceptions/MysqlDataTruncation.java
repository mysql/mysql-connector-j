/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.jdbc.exceptions;

import java.sql.DataTruncation;

/**
 * MySQL wrapper for DataTruncation until the server can support sending all needed information.
 */
public class MysqlDataTruncation extends DataTruncation {

    static final long serialVersionUID = 3263928195256986226L;

    private String message;

    private int vendorErrorCode;

    /**
     * Creates a new MysqlDataTruncation exception/warning.
     * 
     * @param message
     *            the message from the server
     * @param index
     *            of column or parameter
     * @param parameter
     *            was a parameter?
     * @param read
     *            was truncated on read?
     * @param dataSize
     *            size requested
     * @param transferSize
     *            size actually used
     */
    public MysqlDataTruncation(String message, int index, boolean parameter, boolean read, int dataSize, int transferSize, int vendorErrorCode) {
        super(index, parameter, read, dataSize, transferSize);

        this.message = message;
        this.vendorErrorCode = vendorErrorCode;
    }

    @Override
    public int getErrorCode() {
        return this.vendorErrorCode;
    }

    @Override
    public String getMessage() {
        return super.getMessage() + ": " + this.message;
    }
}
