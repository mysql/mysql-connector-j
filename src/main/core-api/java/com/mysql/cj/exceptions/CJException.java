/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.exceptions;

/**
 * The base unchecked exception thrown internally in connector.
 */
public class CJException extends RuntimeException {

    private static final long serialVersionUID = -8618536991444733607L;

    /**
     * We can't override the {@link Throwable#detailMessage} directly because it has a private accessibility,
     * thus for that need we use this protected variable and override {@link #getMessage()}
     */
    protected String exceptionMessage;

    private String SQLState = "S1000"; // GENERAL_ERROR by default

    private int vendorCode = 0;

    private boolean isTransient = false;

    public CJException() {
        super();
    }

    public CJException(String message) {
        super(message);
    }

    public CJException(Throwable cause) {
        super(cause);
    }

    public CJException(String message, Throwable cause) {
        super(message, cause);
    }

    protected CJException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public String getSQLState() {
        return this.SQLState;
    }

    public void setSQLState(String sQLState) {
        this.SQLState = sQLState;
    }

    public int getVendorCode() {
        return this.vendorCode;
    }

    public void setVendorCode(int vendorCode) {
        this.vendorCode = vendorCode;
    }

    public boolean isTransient() {
        return this.isTransient;
    }

    public void setTransient(boolean isTransient) {
        this.isTransient = isTransient;
    }

    @Override
    public String getMessage() {
        return this.exceptionMessage != null ? this.exceptionMessage : super.getMessage();
    }

    public void appendMessage(String messageToAppend) {
        this.exceptionMessage = getMessage() + messageToAppend;
    }

}
