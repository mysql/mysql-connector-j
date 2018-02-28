/*
 * Copyright (c) 2005, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.jdbc;

import javax.transaction.xa.XAException;

/**
 * The stock XAException class isn't too friendly (i.e. no error messages), so we extend it a bit.
 */
class MysqlXAException extends XAException {
    private static final long serialVersionUID = -9075817535836563004L;

    private String message;
    protected String xidAsString;

    public MysqlXAException(int errorCode, String message, String xidAsString) {
        super(errorCode);
        this.message = message;
        this.xidAsString = xidAsString;
    }

    public MysqlXAException(String message, String xidAsString) {
        super();

        this.message = message;
        this.xidAsString = xidAsString;
    }

    @Override
    public String getMessage() {
        String superMessage = super.getMessage();
        StringBuilder returnedMessage = new StringBuilder();

        if (superMessage != null) {
            returnedMessage.append(superMessage);
            returnedMessage.append(":");
        }

        if (this.message != null) {
            returnedMessage.append(this.message);
        }

        return returnedMessage.toString();
    }
}
