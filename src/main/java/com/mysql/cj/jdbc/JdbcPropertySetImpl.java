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

package com.mysql.cj.jdbc;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import com.mysql.cj.api.conf.ModifiableProperty;
import com.mysql.cj.api.conf.PropertyDefinition;
import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.jdbc.JdbcPropertySet;
import com.mysql.cj.core.conf.DefaultPropertySet;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;

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
            // assume they want to use server-side prepared statements because they're required for this functionality
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
