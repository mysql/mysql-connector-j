/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.exceptions;

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
