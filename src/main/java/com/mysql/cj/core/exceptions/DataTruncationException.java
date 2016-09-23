/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

public class DataTruncationException extends CJException {

    private static final long serialVersionUID = -5209088385943506720L;

    /**
     * @serial
     */
    private int index;

    /**
     * @serial
     */
    private boolean parameter;

    /**
     * @serial
     */
    private boolean read;

    /**
     * @serial
     */
    private int dataSize;

    /**
     * @serial
     */
    private int transferSize;

    public DataTruncationException() {
        super();
    }

    public DataTruncationException(String message) {
        super(message);
    }

    public DataTruncationException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataTruncationException(Throwable cause) {
        super(cause);
    }

    protected DataTruncationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public DataTruncationException(String message, int index, boolean parameter, boolean read, int dataSize, int transferSize, int vendorErrorCode) {
        super(message);
        this.setIndex(index);
        this.setParameter(parameter);
        this.setRead(read);
        this.setDataSize(dataSize);
        this.setTransferSize(transferSize);
        setVendorCode(vendorErrorCode);
    }

    public int getIndex() {
        return this.index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public boolean isParameter() {
        return this.parameter;
    }

    public void setParameter(boolean parameter) {
        this.parameter = parameter;
    }

    public boolean isRead() {
        return this.read;
    }

    public void setRead(boolean read) {
        this.read = read;
    }

    public int getDataSize() {
        return this.dataSize;
    }

    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
    }

    public int getTransferSize() {
        return this.transferSize;
    }

    public void setTransferSize(int transferSize) {
        this.transferSize = transferSize;
    }

}
