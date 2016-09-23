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
 * The base unchecked exception thrown internally in connector.
 */
public class CJException extends RuntimeException {

    private static final long serialVersionUID = -8618536991444733607L;

    /**
     * @serial
     */
    private String SQLState = "S1000"; // GENERAL_ERROR by default

    /**
     * @serial
     */
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
        return isTransient;
    }

    public void setTransient(boolean isTransient) {
        this.isTransient = isTransient;
    }
}
