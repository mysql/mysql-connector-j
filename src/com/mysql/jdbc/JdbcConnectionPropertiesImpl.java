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

    public boolean getRewriteBatchedStatements() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_rewriteBatchedStatements).getValue();
    }

    public void setRewriteBatchedStatements(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_rewriteBatchedStatements).setValue(flag);
    }

    public boolean getPinGlobalTxToPhysicalConnection() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_pinGlobalTxToPhysicalConnection).getValue();
    }

    public void setPinGlobalTxToPhysicalConnection(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_pinGlobalTxToPhysicalConnection).setValue(flag);
    }

    public boolean getNoAccessToProcedureBodies() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_noAccessToProcedureBodies).getValue();
    }

    public void setNoAccessToProcedureBodies(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_noAccessToProcedureBodies).setValue(flag);
    }

    public boolean getUseOldAliasMetadataBehavior() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useOldAliasMetadataBehavior).getValue();
    }

    public void setUseOldAliasMetadataBehavior(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_useOldAliasMetadataBehavior).setValue(flag);
    }

    public boolean getTreatUtilDateAsTimestamp() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_treatUtilDateAsTimestamp).getValue();
    }

    public void setTreatUtilDateAsTimestamp(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_treatUtilDateAsTimestamp).setValue(flag);
    }

    public String getLocalSocketAddress() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_localSocketAddress).getStringValue();
    }

    public void setLocalSocketAddress(String address) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_localSocketAddress).setValue(address);
    }

    public void setUseConfigs(String configs) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_useConfigs).setValue(configs);
    }

    public String getUseConfigs() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_useConfigs).getStringValue();
    }

    public boolean getGenerateSimpleParameterMetadata() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_generateSimpleParameterMetadata).getValue();
    }

    public void setGenerateSimpleParameterMetadata(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_generateSimpleParameterMetadata).setValue(flag);
    }

    public boolean getLogXaCommands() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_logXaCommands).getValue();
    }

    public void setLogXaCommands(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_logXaCommands).setValue(flag);
    }

    public int getResultSetSizeThreshold() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_resultSetSizeThreshold).getValue();
    }

    public void setResultSetSizeThreshold(int threshold) throws SQLException {
        getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_resultSetSizeThreshold).setValue(threshold, getExceptionInterceptor());
    }

    public int getNetTimeoutForStreamingResults() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_netTimeoutForStreamingResults).getValue();
    }

    public void setNetTimeoutForStreamingResults(int value) throws SQLException {
        getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_netTimeoutForStreamingResults).setValue(value, getExceptionInterceptor());
    }

    public boolean getEnableQueryTimeouts() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enableQueryTimeouts).getValue();
    }

    public void setEnableQueryTimeouts(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_enableQueryTimeouts).setValue(flag);
    }

    public boolean getPadCharsWithSpace() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_padCharsWithSpace).getValue();
    }

    public void setPadCharsWithSpace(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_padCharsWithSpace).setValue(flag);
    }

    public boolean getUseDynamicCharsetInfo() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useDynamicCharsetInfo).getValue();
    }

    public void setUseDynamicCharsetInfo(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_useDynamicCharsetInfo).setValue(flag);
    }

    public String getClientInfoProvider() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_clientInfoProvider).getStringValue();
    }

    public void setClientInfoProvider(String classname) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_clientInfoProvider).setValue(classname);
    }

    public boolean getPopulateInsertRowWithDefaultValues() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_populateInsertRowWithDefaultValues).getValue();
    }

    public void setPopulateInsertRowWithDefaultValues(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_populateInsertRowWithDefaultValues).setValue(flag);
    }

    public String getHaLoadBalanceStrategy() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceStrategy).getStringValue();
    }

    public void setHaLoadBalanceStrategy(String strategy) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_loadBalanceStrategy).setValue(strategy);
    }

    public boolean getTcpNoDelay() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_tcpNoDelay).getValue();
    }

    public void setTcpNoDelay(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_tcpNoDelay).setValue(flag);
    }

    public boolean getTcpKeepAlive() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_tcpKeepAlive).getValue();
    }

    public void setTcpKeepAlive(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_tcpKeepAlive).setValue(flag);
    }

    public int getTcpRcvBuf() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_tcpRcvBuf).getValue();
    }

    public void setTcpRcvBuf(int bufSize) throws SQLException {
        getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_tcpRcvBuf).setValue(bufSize, getExceptionInterceptor());
    }

    public int getTcpSndBuf() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_tcpSndBuf).getValue();
    }

    public void setTcpSndBuf(int bufSize) throws SQLException {
        getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_tcpSndBuf).setValue(bufSize, getExceptionInterceptor());
    }

    public int getTcpTrafficClass() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_tcpTrafficClass).getValue();
    }

    public void setTcpTrafficClass(int classFlags) throws SQLException {
        getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_tcpTrafficClass).setValue(classFlags, getExceptionInterceptor());
    }

    public boolean getUseNanosForElapsedTime() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useNanosForElapsedTime).getValue();
    }

    public void setUseNanosForElapsedTime(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_useNanosForElapsedTime).setValue(flag);
    }

    public long getSlowQueryThresholdNanos() {
        return getPropertySet().getLongReadableProperty(PropertyDefinitions.PNAME_slowQueryThresholdNanos).getValue();
    }

    public void setSlowQueryThresholdNanos(long nanos) throws SQLException {
        getPropertySet().<Long> getModifiableProperty(PropertyDefinitions.PNAME_slowQueryThresholdNanos).setValue(nanos, getExceptionInterceptor());
    }

    public String getStatementInterceptors() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_statementInterceptors).getStringValue();
    }

    public void setStatementInterceptors(String value) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_statementInterceptors).setValue(value);
    }

    public boolean getUseDirectRowUnpack() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useDirectRowUnpack).getValue();
    }

    public void setUseDirectRowUnpack(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_useDirectRowUnpack).setValue(flag);
    }

    public String getLargeRowSizeThreshold() {
        return getPropertySet().getMemorySizeReadableProperty(PropertyDefinitions.PNAME_largeRowSizeThreshold).getStringValue();
    }

    public void setLargeRowSizeThreshold(String value) throws SQLException {
        getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_largeRowSizeThreshold).setFromString(value, getExceptionInterceptor());
    }

    public boolean getUseBlobToStoreUTF8OutsideBMP() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useBlobToStoreUTF8OutsideBMP).getValue();
    }

    public void setUseBlobToStoreUTF8OutsideBMP(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_useBlobToStoreUTF8OutsideBMP).setValue(flag);
    }

    public String getUtf8OutsideBmpExcludedColumnNamePattern() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_utf8OutsideBmpExcludedColumnNamePattern).getStringValue();
    }

    public void setUtf8OutsideBmpExcludedColumnNamePattern(String regexPattern) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_utf8OutsideBmpExcludedColumnNamePattern).setValue(regexPattern);
    }

    public String getUtf8OutsideBmpIncludedColumnNamePattern() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_utf8OutsideBmpIncludedColumnNamePattern).getStringValue();
    }

    public void setUtf8OutsideBmpIncludedColumnNamePattern(String regexPattern) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_utf8OutsideBmpIncludedColumnNamePattern).setValue(regexPattern);
    }

    public boolean getIncludeInnodbStatusInDeadlockExceptions() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_includeInnodbStatusInDeadlockExceptions).getValue();
    }

    public void setIncludeInnodbStatusInDeadlockExceptions(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_includeInnodbStatusInDeadlockExceptions).setValue(flag);
    }

    public boolean getBlobsAreStrings() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_blobsAreStrings).getValue();
    }

    public void setBlobsAreStrings(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_blobsAreStrings).setValue(flag);
    }

    public boolean getFunctionsNeverReturnBlobs() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_functionsNeverReturnBlobs).getValue();
    }

    public void setFunctionsNeverReturnBlobs(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_functionsNeverReturnBlobs).setValue(flag);
    }

    public boolean getAutoSlowLog() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoSlowLog).getValue();
    }

    public void setAutoSlowLog(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_autoSlowLog).setValue(flag);
    }

    public String getConnectionLifecycleInterceptors() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_connectionLifecycleInterceptors).getStringValue();
    }

    public void setConnectionLifecycleInterceptors(String interceptors) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_connectionLifecycleInterceptors).setValue(interceptors);
    }

    public int getSelfDestructOnPingSecondsLifetime() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_selfDestructOnPingSecondsLifetime).getValue();
    }

    public void setSelfDestructOnPingSecondsLifetime(int seconds) throws SQLException {
        getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_selfDestructOnPingSecondsLifetime).setValue(seconds,
                getExceptionInterceptor());
    }

    public int getSelfDestructOnPingMaxOperations() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_selfDestructOnPingMaxOperations).getValue();
    }

    public void setSelfDestructOnPingMaxOperations(int maxOperations) throws SQLException {
        getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_selfDestructOnPingMaxOperations).setValue(maxOperations,
                getExceptionInterceptor());
    }

    public boolean getUseColumnNamesInFindColumn() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useColumnNamesInFindColumn).getValue();
    }

    public void setUseColumnNamesInFindColumn(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_useColumnNamesInFindColumn).setValue(flag);
    }

    public boolean getUseLocalTransactionState() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useLocalTransactionState).getValue();
    }

    public void setUseLocalTransactionState(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_useLocalTransactionState).setValue(flag);
    }

    public boolean getCompensateOnDuplicateKeyUpdateCounts() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_compensateOnDuplicateKeyUpdateCounts).getValue();
    }

    public void setCompensateOnDuplicateKeyUpdateCounts(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_compensateOnDuplicateKeyUpdateCounts).setValue(flag);
    }

    public int getLoadBalanceBlacklistTimeout() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_loadBalanceBlacklistTimeout).getValue();
    }

    public void setLoadBalanceBlacklistTimeout(int loadBalanceBlacklistTimeout) throws SQLException {
        getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_loadBalanceBlacklistTimeout).setValue(loadBalanceBlacklistTimeout,
                getExceptionInterceptor());
    }

    public int getLoadBalancePingTimeout() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_loadBalancePingTimeout).getValue();
    }

    public void setLoadBalancePingTimeout(int loadBalancePingTimeout) throws SQLException {
        getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_loadBalancePingTimeout).setValue(loadBalancePingTimeout,
                getExceptionInterceptor());
    }

    public void setRetriesAllDown(int retriesAllDown) throws SQLException {
        getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_retriesAllDown).setValue(retriesAllDown, getExceptionInterceptor());
    }

    public int getRetriesAllDown() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_retriesAllDown).getValue();
    }

    public void setUseAffectedRows(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_useAffectedRows).setValue(flag);
    }

    public boolean getUseAffectedRows() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useAffectedRows).getValue();
    }

    public void setExceptionInterceptors(String exceptionInterceptors) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_exceptionInterceptors).setValue(exceptionInterceptors);
    }

    public String getExceptionInterceptors() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_exceptionInterceptors).getStringValue();
    }

    public void setMaxAllowedPacket(int max) throws SQLException {
        getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_maxAllowedPacket).setValue(max, getExceptionInterceptor());
    }

    public int getMaxAllowedPacket() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_maxAllowedPacket).getValue();
    }

    public boolean getQueryTimeoutKillsConnection() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_queryTimeoutKillsConnection).getValue();
    }

    public void setQueryTimeoutKillsConnection(boolean queryTimeoutKillsConnection) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_queryTimeoutKillsConnection).setValue(queryTimeoutKillsConnection);
    }

    public boolean getLoadBalanceValidateConnectionOnSwapServer() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_loadBalanceValidateConnectionOnSwapServer).getValue();
    }

    public void setLoadBalanceValidateConnectionOnSwapServer(boolean loadBalanceValidateConnectionOnSwapServer) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_loadBalanceValidateConnectionOnSwapServer).setValue(
                loadBalanceValidateConnectionOnSwapServer);

    }

    public String getLoadBalanceConnectionGroup() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceConnectionGroup).getStringValue();
    }

    public void setLoadBalanceConnectionGroup(String loadBalanceConnectionGroup) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_loadBalanceConnectionGroup).setValue(loadBalanceConnectionGroup);
    }

    public String getLoadBalanceExceptionChecker() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceExceptionChecker).getStringValue();
    }

    public void setLoadBalanceExceptionChecker(String loadBalanceExceptionChecker) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_loadBalanceExceptionChecker).setValue(loadBalanceExceptionChecker);
    }

    public String getLoadBalanceSQLStateFailover() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceSQLStateFailover).getStringValue();
    }

    public void setLoadBalanceSQLStateFailover(String loadBalanceSQLStateFailover) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_loadBalanceSQLStateFailover).setValue(loadBalanceSQLStateFailover);
    }

    public String getLoadBalanceSQLExceptionSubclassFailover() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceSQLExceptionSubclassFailover).getStringValue();
    }

    public void setLoadBalanceSQLExceptionSubclassFailover(String loadBalanceSQLExceptionSubclassFailover) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_loadBalanceSQLExceptionSubclassFailover).setValue(
                loadBalanceSQLExceptionSubclassFailover);
    }

    public void setLoadBalanceAutoCommitStatementThreshold(int loadBalanceAutoCommitStatementThreshold) throws SQLException {
        getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementThreshold).setValue(
                loadBalanceAutoCommitStatementThreshold, getExceptionInterceptor());
    }

    public int getLoadBalanceAutoCommitStatementThreshold() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementThreshold).getValue();
    }

    public void setLoadBalanceAutoCommitStatementRegex(String loadBalanceAutoCommitStatementRegex) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementRegex).setValue(
                loadBalanceAutoCommitStatementRegex);
    }

    public String getLoadBalanceAutoCommitStatementRegex() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementRegex).getStringValue();
    }

    public void setIncludeThreadDumpInDeadlockExceptions(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_includeThreadDumpInDeadlockExceptions).setValue(flag);
    }

    public boolean getIncludeThreadDumpInDeadlockExceptions() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_includeThreadDumpInDeadlockExceptions).getValue();
    }

    public void setIncludeThreadNamesAsStatementComment(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_includeThreadNamesAsStatementComment).setValue(flag);
    }

    public boolean getIncludeThreadNamesAsStatementComment() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_includeThreadNamesAsStatementComment).getValue();
    }

    public void setAuthenticationPlugins(String authenticationPlugins) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_authenticationPlugins).setValue(authenticationPlugins);
    }

    public String getAuthenticationPlugins() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_authenticationPlugins).getStringValue();
    }

    public void setDisabledAuthenticationPlugins(String disabledAuthenticationPlugins) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_disabledAuthenticationPlugins).setValue(disabledAuthenticationPlugins);
    }

    public String getDisabledAuthenticationPlugins() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_disabledAuthenticationPlugins).getStringValue();
    }

    public void setDefaultAuthenticationPlugin(String defaultAuthenticationPlugin) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_defaultAuthenticationPlugin).setValue(defaultAuthenticationPlugin);

    }

    public String getDefaultAuthenticationPlugin() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_defaultAuthenticationPlugin).getStringValue();
    }

    public void setParseInfoCacheFactory(String factoryClassname) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_parseInfoCacheFactory).setValue(factoryClassname);
    }

    public String getParseInfoCacheFactory() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_parseInfoCacheFactory).getStringValue();
    }

    public void setServerConfigCacheFactory(String factoryClassname) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_serverConfigCacheFactory).setValue(factoryClassname);
    }

    public String getServerConfigCacheFactory() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_serverConfigCacheFactory).getStringValue();
    }

    public void setDisconnectOnExpiredPasswords(boolean disconnectOnExpiredPasswords) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_disconnectOnExpiredPasswords).setValue(disconnectOnExpiredPasswords);
    }

    public boolean getDisconnectOnExpiredPasswords() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_disconnectOnExpiredPasswords).getValue();
    }

    public boolean getAllowMasterDownConnections() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_allowMasterDownConnections).getValue();
    }

    public void setAllowMasterDownConnections(boolean connectIfMasterDown) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_allowMasterDownConnections).setValue(connectIfMasterDown);
    }

    public boolean getHaEnableJMX() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_ha_enableJMX).getValue();
    }

    public void setHaEnableJMX(boolean replicationEnableJMX) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_ha_enableJMX).setValue(replicationEnableJMX);

    }

    public void setGetProceduresReturnsFunctions(boolean getProcedureReturnsFunctions) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_getProceduresReturnsFunctions).setValue(getProcedureReturnsFunctions);
    }

    public boolean getGetProceduresReturnsFunctions() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_getProceduresReturnsFunctions).getValue();
    }

    public void setDetectCustomCollations(boolean detectCustomCollations) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_detectCustomCollations).setValue(detectCustomCollations);
    }

    public boolean getDetectCustomCollations() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_detectCustomCollations).getValue();
    }

    public void setDontCheckOnDuplicateKeyUpdateInSQL(boolean dontCheckOnDuplicateKeyUpdateInSQL) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_dontCheckOnDuplicateKeyUpdateInSQL).setValue(
                dontCheckOnDuplicateKeyUpdateInSQL);
    }

    public boolean getDontCheckOnDuplicateKeyUpdateInSQL() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_dontCheckOnDuplicateKeyUpdateInSQL).getValue();
    }

    public void setSocksProxyHost(String socksProxyHost) {
        getPropertySet().<String> getModifiableProperty(PropertyDefinitions.PNAME_socksProxyHost).setValue(socksProxyHost);
    }

    public String getSocksProxyHost() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_socksProxyHost).getStringValue();
    }

    public void setSocksProxyPort(int socksProxyPort) throws SQLException {
        getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_socksProxyPort).setValue(socksProxyPort, null);
    }

    public int getSocksProxyPort() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_socksProxyPort).getValue();
    }

    public boolean getReadOnlyPropagatesToServer() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_readOnlyPropagatesToServer).getValue();
    }

    public void setReadOnlyPropagatesToServer(boolean flag) {
        getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_readOnlyPropagatesToServer).setValue(flag);
    }
}
