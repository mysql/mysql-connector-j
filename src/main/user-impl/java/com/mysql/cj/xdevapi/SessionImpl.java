/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.mysql.cj.Messages;
import com.mysql.cj.MysqlxSession;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.protocol.x.XMessage;
import com.mysql.cj.protocol.x.XMessageBuilder;
import com.mysql.cj.protocol.x.XProtocol;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.result.Row;
import com.mysql.cj.result.StringValueFactory;
import com.mysql.cj.util.StringUtils;

/**
 * {@link Session} implementation.
 */
public class SessionImpl implements Session {

    protected MysqlxSession session;
    protected String defaultSchemaName;
    private XMessageBuilder xbuilder;

    /**
     * Constructor.
     * 
     * @param hostInfo
     *            {@link HostInfo} instance
     */
    public SessionImpl(HostInfo hostInfo) {
        PropertySet pset = new DefaultPropertySet();
        pset.initializeProperties(hostInfo.exposeAsProperties());
        this.session = new MysqlxSession(hostInfo, pset);
        this.defaultSchemaName = hostInfo.getDatabase();
        this.xbuilder = (XMessageBuilder) this.session.<XMessage>getMessageBuilder();
    }

    public SessionImpl(XProtocol prot) {
        this.session = new MysqlxSession(prot);
        this.defaultSchemaName = prot.defaultSchemaName;
        this.xbuilder = (XMessageBuilder) this.session.<XMessage>getMessageBuilder();
    }

    protected SessionImpl() {
    }

    public List<Schema> getSchemas() {
        Function<Row, String> rowToName = r -> r.getValue(0, new StringValueFactory(this.session.getPropertySet()));
        Function<Row, Schema> rowToSchema = rowToName.andThen(n -> new SchemaImpl(this.session, this, n));
        return this.session.query(this.xbuilder.buildSqlStatement("select schema_name from information_schema.schemata"), null, rowToSchema,
                Collectors.toList());
    }

    public Schema getSchema(String schemaName) {
        return new SchemaImpl(this.session, this, schemaName);
    }

    public String getDefaultSchemaName() {
        return this.defaultSchemaName;
    }

    public Schema getDefaultSchema() {
        if (this.defaultSchemaName == null || this.defaultSchemaName.length() == 0) {
            return null;
        }
        return new SchemaImpl(this.session, this, this.defaultSchemaName);
    }

    public Schema createSchema(String schemaName) {
        StringBuilder stmtString = new StringBuilder("CREATE DATABASE ");
        stmtString.append(StringUtils.quoteIdentifier(schemaName, true));
        this.session.query(this.xbuilder.buildSqlStatement(stmtString.toString()), new UpdateResultBuilder<>());
        return getSchema(schemaName);
    }

    public Schema createSchema(String schemaName, boolean reuseExistingObject) {
        try {
            return createSchema(schemaName);
        } catch (XProtocolError ex) {
            if (ex.getErrorCode() == MysqlErrorNumbers.ER_DB_CREATE_EXISTS) {
                return getSchema(schemaName);
            }
            throw ex;
        }
    }

    public void dropSchema(String schemaName) {
        StringBuilder stmtString = new StringBuilder("DROP DATABASE ");
        stmtString.append(StringUtils.quoteIdentifier(schemaName, true));
        this.session.query(this.xbuilder.buildSqlStatement(stmtString.toString()), new UpdateResultBuilder<>());
    }

    public void startTransaction() {
        this.session.query(this.xbuilder.buildSqlStatement("START TRANSACTION"), new UpdateResultBuilder<>());
    }

    public void commit() {
        this.session.query(this.xbuilder.buildSqlStatement("COMMIT"), new UpdateResultBuilder<>());
    }

    public void rollback() {
        this.session.query(this.xbuilder.buildSqlStatement("ROLLBACK"), new UpdateResultBuilder<>());
    }

    @Override
    public String setSavepoint() {
        return setSavepoint(StringUtils.getUniqueSavepointId());
    }

    @Override
    public String setSavepoint(String name) {
        if (name == null || name.trim().length() == 0) {
            throw new XDevAPIError(Messages.getString("XSession.0", new String[] { "name" }));
        }

        this.session.query(this.xbuilder.buildSqlStatement("SAVEPOINT " + StringUtils.quoteIdentifier(name, true)), new UpdateResultBuilder<>());
        return name;
    }

    @Override
    public void rollbackTo(String name) {
        if (name == null || name.trim().length() == 0) {
            throw new XDevAPIError(Messages.getString("XSession.0", new String[] { "name" }));
        }

        this.session.query(this.xbuilder.buildSqlStatement("ROLLBACK TO " + StringUtils.quoteIdentifier(name, true)), new UpdateResultBuilder<>());
    }

    @Override
    public void releaseSavepoint(String name) {
        if (name == null || name.trim().length() == 0) {
            throw new XDevAPIError(Messages.getString("XSession.0", new String[] { "name" }));
        }

        this.session.query(this.xbuilder.buildSqlStatement("RELEASE SAVEPOINT " + StringUtils.quoteIdentifier(name, true)), new UpdateResultBuilder<>());
    }

    public String getUri() {
        PropertySet pset = this.session.getPropertySet();

        StringBuilder sb = new StringBuilder(ConnectionUrl.Type.XDEVAPI_SESSION.getScheme());
        sb.append("//").append(this.session.getProcessHost()).append(":").append(this.session.getPort()).append("/").append(this.defaultSchemaName).append("?");

        boolean isFirstParam = true;

        for (PropertyKey propKey : PropertyDefinitions.PROPERTY_KEY_TO_PROPERTY_DEFINITION.keySet()) {
            RuntimeProperty<?> propToGet = pset.getProperty(propKey);
            if (propToGet.isExplicitlySet()) {
                String propValue = propToGet.getStringValue();
                Object defaultValue = propToGet.getPropertyDefinition().getDefaultValue();
                if (defaultValue == null && !StringUtils.isNullOrEmpty(propValue) || defaultValue != null && propValue == null
                        || defaultValue != null && propValue != null && !propValue.equals(defaultValue.toString())) {
                    if (isFirstParam) {
                        isFirstParam = false;
                    } else {
                        sb.append("&");
                    }
                    sb.append(propKey.getKeyName());
                    sb.append("=");
                    sb.append(propValue);
                }

                // TODO custom properties?
            }
        }

        // TODO modify for multi-host connections

        return sb.toString();

    }

    public boolean isOpen() {
        return !this.session.isClosed();
    }

    public void close() {
        this.session.quit();
    }

    public SqlStatementImpl sql(String sql) {
        return new SqlStatementImpl(this.session, sql);
    }

    public MysqlxSession getSession() {
        return this.session;
    }
}
