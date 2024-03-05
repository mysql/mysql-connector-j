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

package com.mysql.cj.xdevapi;

import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ProtocolEntity;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.RowList;

/**
 * {@link RowResult} implementation.
 */
public class RowResultImpl extends AbstractDataResult<Row> implements RowResult {

    private ColumnDefinition metadata;

    /**
     * Constructor.
     *
     * @param metadata
     *            {@link ColumnDefinition} object to use for new rows.
     * @param defaultTimeZone
     *            {@link TimeZone} object representing the default time zone
     * @param rows
     *            {@link RowList} provided by c/J core
     * @param completer
     *            supplier for completion task
     * @param pset
     *            {@link PropertySet}
     */
    public RowResultImpl(ColumnDefinition metadata, TimeZone defaultTimeZone, RowList rows, Supplier<ProtocolEntity> completer, PropertySet pset) {
        super(rows, completer, new RowFactory(metadata, defaultTimeZone, pset));
        this.metadata = metadata;
    }

    @Override
    public int getColumnCount() {
        return this.metadata.getFields().length;
    }

    @Override
    public List<Column> getColumns() {
        return Arrays.stream(this.metadata.getFields()).map(ColumnImpl::new).collect(Collectors.toList());
    }

    @Override
    public List<String> getColumnNames() {
        return Arrays.stream(this.metadata.getFields()).map(Field::getColumnLabel).collect(Collectors.toList());
    }

}
