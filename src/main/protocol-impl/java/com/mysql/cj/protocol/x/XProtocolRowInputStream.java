/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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

import java.util.NoSuchElementException;
import java.util.function.Consumer;

import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.result.Row;
import com.mysql.cj.result.RowList;

public class XProtocolRowInputStream implements RowList {

    private ColumnDefinition metadata;
    private XProtocol protocol;
    private boolean isDone = false;
    private int position = -1;
    /** XProtocolRow */
    private Row next; // TODO document
    private Consumer<Notice> noticeConsumer;

    public XProtocolRowInputStream(ColumnDefinition metadata, XProtocol protocol, Consumer<Notice> noticeConsumer) {
        this.metadata = metadata;
        this.protocol = protocol;
        this.noticeConsumer = noticeConsumer;
    }

    public XProtocolRowInputStream(ColumnDefinition metadata, Row row, XProtocol protocol, Consumer<Notice> noticeConsumer) {
        this.metadata = metadata;
        this.protocol = protocol;
        this.next = row;
        this.next.setMetadata(metadata);
        this.noticeConsumer = noticeConsumer;
    }

    public Row readRow() {
        if (!hasNext()) {
            this.isDone = true;
            return null;
        }
        this.position++;
        Row r = this.next;
        this.next = null;
        return r;
    }

    @Override
    public Row next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return readRow();
    }

    @Override
    public boolean hasNext() {
        if (this.isDone) {
            return false;
        } else if (this.next == null) {
            this.next = this.protocol.readRowOrNull(this.metadata, this.noticeConsumer);
        }
        return this.next != null;
    }

    @Override
    public int getPosition() {
        return this.position;
    }

}
