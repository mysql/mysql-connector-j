/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.jdbc;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.ModifiableProperty;
import com.mysql.cj.conf.PropertyDefinition;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.ReadableProperty;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.util.StringUtils;

public class JdbcPropertySetImpl extends DefaultPropertySet implements JdbcPropertySet {

    private static final long serialVersionUID = -8223499903182568260L;

    @Override
    public <T> ModifiableProperty<T> getJdbcModifiableProperty(String name) throws SQLException {
        try {
            return getModifiableProperty(name);
        } catch (CJException ex) {
            throw SQLExceptionsMapping.translateException(ex);
        }
    }

    @Override
    public void postInitialization() {

        // Adjust max rows
        if (getIntegerReadableProperty(PropertyDefinitions.PNAME_maxRows).getValue() == 0) {
            // adjust so that it will become MysqlDefs.MAX_ROWS in execSQL()
            super.<Integer> getModifiableProperty(PropertyDefinitions.PNAME_maxRows).setValue(Integer.valueOf(-1), null);
        }

        //
        // Check character encoding
        //
        String testEncoding = getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue();

        if (testEncoding != null) {
            // Attempt to use the encoding, and bail out if it can't be used
            String testString = "abc";
            StringUtils.getBytes(testString, testEncoding);
        }

        if (getBooleanReadableProperty(PropertyDefinitions.PNAME_useCursorFetch).getValue()) {
            // assume server-side prepared statements are wanted because they're required for this functionality
            super.<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_useServerPrepStmts).setValue(true);
        }
    }

    public DriverPropertyInfo[] exposeAsDriverPropertyInfo(Properties info, int slotsToReserve) throws SQLException {
        initializeProperties(info);

        int numProperties = PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.size();

        int listSize = numProperties + slotsToReserve;

        DriverPropertyInfo[] driverProperties = new DriverPropertyInfo[listSize];

        int i = slotsToReserve;

        for (String propName : PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.keySet()) {
            driverProperties[i++] = getAsDriverPropertyInfo(getReadableProperty(propName));
        }

        return driverProperties;
    }

    private DriverPropertyInfo getAsDriverPropertyInfo(ReadableProperty<?> pr) {
        PropertyDefinition<?> pdef = pr.getPropertyDefinition();

        DriverPropertyInfo dpi = new DriverPropertyInfo(pdef.getName(), null);
        dpi.choices = pdef.getAllowableValues();
        dpi.value = (pr.getStringValue() != null) ? pr.getStringValue() : null;
        dpi.required = false;
        dpi.description = pdef.getDescription();

        return dpi;
    }
}
