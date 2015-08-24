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

package com.mysql.cj.mysqlx.devapi;

import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.result.Row;
import com.mysql.cj.api.x.Schema;
import com.mysql.cj.api.x.Session;
import com.mysql.cj.core.ConnectionString;
import com.mysql.cj.core.ConnectionString.ConnectionStringType;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.InvalidConnectionAttributeException;
import com.mysql.cj.core.io.StringValueFactory;
import com.mysql.cj.mysqlx.MysqlxSession;

public class SessionImpl extends AbstractSession implements Session {

    public SessionImpl(String url) {
        ConnectionString conStr = new ConnectionString(url, null);
        Properties properties = conStr.getProperties();

        if (properties == null) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, "Initialization via URL failed for \"" + url + "\"");
        }

        this.session = new MysqlxSession(properties);
        this.session.changeUser(properties.getProperty(PropertyDefinitions.PNAME_user), properties.getProperty(PropertyDefinitions.PNAME_password),
                properties.getProperty(PropertyDefinitions.DBNAME_PROPERTY_KEY));
        this.defaultSchemaName = properties.getProperty(PropertyDefinitions.DBNAME_PROPERTY_KEY);
    }

    // TODO extract to init method and reuse in both constructors?
    public SessionImpl(Properties properties) {
        this.session = new MysqlxSession(properties);
        this.session.changeUser(properties.getProperty(PropertyDefinitions.PNAME_user), properties.getProperty(PropertyDefinitions.PNAME_password),
                properties.getProperty(PropertyDefinitions.DBNAME_PROPERTY_KEY));
        this.defaultSchemaName = properties.getProperty(PropertyDefinitions.DBNAME_PROPERTY_KEY);
    }

    @Override
    public List<Schema> getSchemas() {
        // TODO: test
        Function<Row, String> rowToName = r -> r.getValue(0, new StringValueFactory());
        Function<Row, Schema> rowToSchema = rowToName.andThen(n -> new SchemaImpl(this, n));
        return this.session.query("select schema_name from information_schema.schemata", rowToSchema, Collectors.toList());
    }

    @Override
    public Schema getSchema(String name) {
        return new SchemaImpl(this, name);
    }

    @Override
    public Schema getDefaultSchema() {
        return new SchemaImpl(this, this.defaultSchemaName);
    }

    @Override
    public void startTransaction() {
        this.session.update("START TRANSACTION");
    }

    @Override
    public void commit() {
        this.session.update("COMMIT");
    }

    @Override
    public void rollback() {
        this.session.update("ROLLBACK");
    }

    @Override
    public String getUri() {
        PropertySet pset = this.session.getPropertySet();

        StringBuilder sb = new StringBuilder(ConnectionStringType.X_SESSION.urlPrefix);
        sb.append(this.session.getHost());
        sb.append(":");
        sb.append(this.session.getPort());
        sb.append("/");
        sb.append(this.defaultSchemaName);
        sb.append("?");

        for (String propName : PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.keySet()) {
            ReadableProperty<?> propToGet = pset.getReadableProperty(propName);

            String propValue = propToGet.getStringValue();

            if (propValue != null && !propValue.equals(propToGet.getPropertyDefinition().getDefaultValue().toString())) {
                sb.append(",");
                sb.append(propName);
                sb.append("=");
                sb.append(propValue);
            }
        }

        // TODO modify for multi-host connections

        return sb.toString();

    }
}
