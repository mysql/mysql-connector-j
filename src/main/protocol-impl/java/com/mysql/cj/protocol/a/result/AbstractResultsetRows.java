/*
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.a.result;

import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.ResultsetRow;
import com.mysql.cj.protocol.ResultsetRows;
import com.mysql.cj.protocol.ResultsetRowsOwner;
import com.mysql.cj.protocol.a.NativePacketPayload;

public abstract class AbstractResultsetRows implements ResultsetRows {

    protected final static int BEFORE_START_OF_ROWS = -1;

    /**
     * Field-level metadata from the server. We need this, because it is not
     * sent for each batch of rows, but we need the metadata to unpack the
     * results for each field.
     */
    protected ColumnDefinition metadata;

    /**
     * Position in cache of rows, used to determine if we need to fetch more
     * rows from the server to satisfy a request for the next row.
     */
    protected int currentPositionInFetchedRows = BEFORE_START_OF_ROWS;

    protected boolean wasEmpty = false; // we don't know until we attempt to traverse

    /**
     * The result set that we 'belong' to.
     */
    protected ResultsetRowsOwner owner;

    protected ProtocolEntityFactory<ResultsetRow, NativePacketPayload> rowFactory;

    @Override
    public void setOwner(ResultsetRowsOwner rs) {
        this.owner = rs;
    }

    @Override
    public ResultsetRowsOwner getOwner() {
        return this.owner;
    }

    @Override
    public void setMetadata(ColumnDefinition columnDefinition) {
        this.metadata = columnDefinition;
    }

    @Override
    public ColumnDefinition getMetadata() {
        return this.metadata;
    }

    @Override
    public boolean wasEmpty() {
        return this.wasEmpty;
    }

}
