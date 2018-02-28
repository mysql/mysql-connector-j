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

package com.mysql.cj.exceptions;

/**
 * Equivalent to SQLSTATE ER_MUST_CHANGE_PASSWORD = 1820
 * "You must SET PASSWORD before executing this statement"
 * 
 * Server entered to sandbox morde when this failure happens.
 */
public class PasswordExpiredException extends CJException {

    private static final long serialVersionUID = -3807215681364413250L;

    public PasswordExpiredException() {
        super();
        setVendorCode(MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD);
    }

    public PasswordExpiredException(String message) {
        super(message);
        setVendorCode(MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD);
    }

    public PasswordExpiredException(String message, Throwable cause) {
        super(message, cause);
        setVendorCode(MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD);
    }

    public PasswordExpiredException(Throwable cause) {
        super(cause);
        setVendorCode(MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD);
    }

    protected PasswordExpiredException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        setVendorCode(MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD);
    }

}
