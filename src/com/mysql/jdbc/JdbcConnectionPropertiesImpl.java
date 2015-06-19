/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package com.mysql.jdbc;

import java.io.Serializable;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.Reference;

import com.mysql.cj.api.conf.PropertyDefinition;
import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.exception.ExceptionInterceptor;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exception.CJException;
import com.mysql.cj.core.exception.ExceptionFactory;
import com.mysql.cj.core.exception.WrongArgumentException;
import com.mysql.cj.core.util.StringUtils;

/**
 * Represents configurable properties for Connections and DataSources. Can also expose properties as JDBC DriverPropertyInfo if required as well.
 */
public class JdbcConnectionPropertiesImpl implements Serializable, JdbcConnectionProperties {

    private static final long serialVersionUID = -1550312215415685578L;

    /**
     * Exposes all ConnectionPropertyInfo instances as DriverPropertyInfo
     * 
     * @param info
     *            the properties to load into these ConnectionPropertyInfo
     *            instances
     * @param slotsToReserve
     *            the number of DPI slots to reserve for 'standard' DPI
     *            properties (user, host, password, etc)
     * @return a list of all ConnectionPropertyInfo instances, as
     *         DriverPropertyInfo
     * @throws SQLException
     *             if an error occurs
     */
    protected static DriverPropertyInfo[] exposeAsDriverPropertyInfo(Properties info, int slotsToReserve) {
        return (new JdbcConnectionPropertiesImpl() {
            private static final long serialVersionUID = 4257801713007640581L;
        }).exposeAsDriverPropertyInfoInternal(info, slotsToReserve);
    }

    protected JdbcPropertySet propertySet = null;

    public JdbcConnectionPropertiesImpl() {
        this.propertySet = new JdbcPropertySetImpl();
    }

    @Override
    public JdbcPropertySet getPropertySet() {
        return this.propertySet;
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return null;
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

    protected DriverPropertyInfo[] exposeAsDriverPropertyInfoInternal(Properties info, int slotsToReserve) {
        initializeProperties(info);

        int numProperties = PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.keySet().size();

        int listSize = numProperties + slotsToReserve;

        DriverPropertyInfo[] driverProperties = new DriverPropertyInfo[listSize];

        int i = slotsToReserve;

        for (String propName : PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.keySet()) {
            ReadableProperty<?> propToExpose = getPropertySet().getReadableProperty(propName);

            if (info != null) {
                propToExpose.initializeFrom(info, getExceptionInterceptor());
            }

            driverProperties[i++] = getAsDriverPropertyInfo(propToExpose);
        }

        return driverProperties;
    }

    protected Properties exposeAsProperties(Properties info) {
        if (info == null) {
            info = new Properties();
        }

        for (String propName : PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.keySet()) {
            ReadableProperty<?> propToGet = getPropertySet().getReadableProperty(propName);

            String propValue = propToGet.getStringValue();

            if (propValue != null) {
                info.setProperty(propToGet.getPropertyDefinition().getName(), propValue);
            }
        }

        return info;
    }

    /**
     * Initializes driver properties that come from a JNDI reference (in the
     * case of a javax.sql.DataSource bound into some name service that doesn't
     * handle Java objects directly).
     * 
     * @param ref
     *            The JNDI Reference that holds RefAddrs for all properties
     */
    protected void initializeFromRef(Reference ref) {

        for (String propName : PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.keySet()) {
            ReadableProperty<?> propToSet = getPropertySet().getReadableProperty(propName);

            if (ref != null) {
                propToSet.initializeFrom(ref, getExceptionInterceptor());
            }
        }

        postInitialization();
    }

    /**
     * Initializes driver properties that come from URL or properties passed to
     * the driver manager.
     * 
     * @param info
     */
    protected void initializeProperties(Properties info) {
        if (info != null) {
            Properties infoCopy = (Properties) info.clone();

            infoCopy.remove(NonRegisteringDriver.HOST_PROPERTY_KEY);
            infoCopy.remove(PropertyDefinitions.PNAME_user);
            infoCopy.remove(PropertyDefinitions.PNAME_password);
            infoCopy.remove(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
            infoCopy.remove(NonRegisteringDriver.PORT_PROPERTY_KEY);

            for (String propName : PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.keySet()) {
                try {
                    ReadableProperty<?> propToSet = getPropertySet().getReadableProperty(propName);
                    propToSet.initializeFrom(infoCopy, getExceptionInterceptor());

                } catch (CJException e) {
                    throw ExceptionFactory.createException(WrongArgumentException.class, e.getMessage(), e, getExceptionInterceptor());
                }
            }

            postInitialization();
        }
    }

    protected void postInitialization() {

        // Adjust max rows
        if (getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_maxRows).getValue() == 0) {
            // adjust so that it will become MysqlDefs.MAX_ROWS in execSQL()
            getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_maxRows).setValue(Integer.valueOf(-1), getExceptionInterceptor());
        }

        //
        // Check character encoding
        //
        String testEncoding = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue();

        if (testEncoding != null) {
            // Attempt to use the encoding, and bail out if it can't be used
            String testString = "abc";
            StringUtils.getBytes(testString, testEncoding);
        }

        if (getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useCursorFetch).getValue()) {
            // assume they want to use server-side prepared statements because they're required for this functionality
            getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_useServerPrepStmts).setValue(true);
        }
    }

    /*
     * 
     * 
     * public boolean getAllowMasterDownConnections() {
     * return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_allowMasterDownConnections).getValue();
     * }
     * 
     * public void setAllowMasterDownConnections(boolean connectIfMasterDown) {
     * getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_allowMasterDownConnections).setValue(connectIfMasterDown);
     * }
     * 
     * public boolean getHaEnableJMX() {
     * return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_ha_enableJMX).getValue();
     * }
     * 
     * public void setHaEnableJMX(boolean replicationEnableJMX) {
     * getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_ha_enableJMX).setValue(replicationEnableJMX);
     * 
     * }
     * 
     * public void setGetProceduresReturnsFunctions(boolean getProcedureReturnsFunctions) {
     * getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_getProceduresReturnsFunctions).setValue(getProcedureReturnsFunctions);
     * }
     * 
     * public boolean getGetProceduresReturnsFunctions() {
     * return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_getProceduresReturnsFunctions).getValue();
     * }
     * 
     * public void setDetectCustomCollations(boolean detectCustomCollations) {
     * getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_detectCustomCollations).setValue(detectCustomCollations);
     * }
     * 
     * public boolean getDetectCustomCollations() {
     * return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_detectCustomCollations).getValue();
     * }
     * 
     * public void setDontCheckOnDuplicateKeyUpdateInSQL(boolean dontCheckOnDuplicateKeyUpdateInSQL) {
     * getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_dontCheckOnDuplicateKeyUpdateInSQL).setValue(
     * dontCheckOnDuplicateKeyUpdateInSQL);
     * }
     * 
     * public boolean getDontCheckOnDuplicateKeyUpdateInSQL() {
     * return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_dontCheckOnDuplicateKeyUpdateInSQL).getValue();
     * }
     * 
     * public void setSocksProxyHost(String socksProxyHost) {
     * getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_socksProxyHost).setValue(socksProxyHost);
     * }
     * 
     * public String getSocksProxyHost() {
     * return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_socksProxyHost).getStringValue();
     * }
     * 
     * public void setSocksProxyPort(int socksProxyPort) throws SQLException {
     * getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_socksProxyPort).setValue(socksProxyPort, null);
     * }
     * 
     * public int getSocksProxyPort() {
     * return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_socksProxyPort).getValue();
     * }
     * 
     * public boolean getReadOnlyPropagatesToServer() {
     * return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_readOnlyPropagatesToServer).getValue();
     * }
     * 
     * public void setReadOnlyPropagatesToServer(boolean flag) {
     * getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_readOnlyPropagatesToServer).setValue(flag);
     * }
     */
}
