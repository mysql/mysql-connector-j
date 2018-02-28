/*
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.xdevapi;

import java.util.Arrays;
import java.util.stream.Collectors;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.util.StringUtils;

public abstract class AbstractColumnDef<T extends ColumnDefinition<T>> implements ColumnDefinition<T> {

    protected String name;
    protected Type type;
    protected Number length;
    protected Boolean notNull;
    protected boolean uniqueIndex = false;
    protected boolean primaryKey = false;
    protected String comment = null;
    protected Boolean unsigned;
    protected Number decimals;
    protected String charset = null;
    protected String collation = null;
    protected Boolean binary;
    protected String[] values;

    abstract T self();

    @Override
    public T notNull() {
        this.notNull = true;
        return self();
    }

    @Override
    public T uniqueIndex() {
        this.uniqueIndex = true;
        return self();
    }

    @Override
    public T primaryKey() {
        this.primaryKey = true;
        return self();
    }

    @Override
    public T comment(String cmt) {
        this.comment = cmt;
        return self();
    }

    @Override
    public T unsigned() {
        this.unsigned = true;
        return self();
    }

    @Override
    public T decimals(int val) {
        this.decimals = val;
        return self();
    }

    @Override
    public T charset(String charsetName) {
        this.charset = charsetName;
        return self();
    }

    @Override
    public T collation(String collationName) {
        this.collation = collationName;
        return self();
    }

    @Override
    public T binary() {
        this.binary = true;
        return self();
    }

    @Override
    public T values(String... val) {
        this.values = val;
        return self();
    }

    protected String getMysqlType() {
        StringBuilder sb = new StringBuilder();
        String mysqlTypeName;
        switch (this.type) {
            case STRING:
                mysqlTypeName = "VARCHAR";
                break;
            case BYTES:
                mysqlTypeName = "VARBINARY";
                break;
            default:
                mysqlTypeName = this.type.name();
                break;
        }
        sb.append(mysqlTypeName);

        if (this.length != null) {
            switch (this.type) {
                case JSON:
                case GEOMETRY:
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("ColumnDefinition.0", new String[] { "Length parameter", mysqlTypeName, this.name }));
                default:
                    sb.append("(").append(this.length);
                    if (this.decimals != null) {
                        switch (this.type) {
                            case DECIMAL:
                            case DOUBLE:
                            case FLOAT:
                                sb.append(", ").append(this.decimals);
                                break;
                            default:
                                throw ExceptionFactory.createException(WrongArgumentException.class,
                                        Messages.getString("ColumnDefinition.0", new String[] { "Decimals parameter", mysqlTypeName, this.name }));
                        }
                    }
                    sb.append(")");
                    break;
            }

        } else {
            switch (this.type) {
                case ENUM:
                case SET:
                    sb.append(Arrays.stream(this.values).map(v -> StringUtils.quoteIdentifier(v, "'", true)).collect(Collectors.joining(",", "(", ")")));
                    break;
                default:
                    if (this.values != null) {
                        throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ColumnDefinition.0",
                                new String[] { Arrays.stream(this.values).collect(Collectors.joining(", ")), mysqlTypeName, this.name }));
                    }
                    if (this.decimals != null) {
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("ColumnDefinition.1", new String[] { this.name }));
                    }
                    break;
            }
        }

        if (this.unsigned != null && this.unsigned) {
            switch (this.type) {
                case TINYINT:
                case SMALLINT:
                case MEDIUMINT:
                case INT:
                case BIGINT:
                case DOUBLE:
                case FLOAT:
                case DECIMAL:
                    sb.append(" UNSIGNED");
                    break;
                default:
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("ColumnDefinition.0", new String[] { "UNSIGNED", mysqlTypeName, this.name }));
            }
        }
        if (this.binary != null && this.binary) {
            if (this.type == Type.STRING) {
                sb.append(" BINARY");
            } else {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("ColumnDefinition.0", new String[] { "BINARY", mysqlTypeName, this.name }));
            }
        }
        if (this.charset != null && !this.charset.isEmpty()) {
            switch (this.type) {
                case STRING:
                case ENUM:
                case SET:
                    sb.append(" CHARACTER SET ").append(this.charset);
                    break;
                default:
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("ColumnDefinition.0", new String[] { "CHARACTER SET", mysqlTypeName, this.name }));
            }
        }
        if (this.collation != null && !this.collation.isEmpty()) {
            switch (this.type) {
                case STRING:
                case ENUM:
                case SET:
                    sb.append(" COLLATE ").append(this.collation);
                    break;
                default:
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("ColumnDefinition.0", new String[] { "COLLATE", mysqlTypeName, this.name }));
            }
        }

        return sb.toString();
    }
}
