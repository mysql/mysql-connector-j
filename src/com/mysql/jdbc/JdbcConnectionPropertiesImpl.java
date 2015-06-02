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
import com.mysql.cj.core.conf.CommonConnectionProperties;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exception.CJException;
import com.mysql.cj.core.exception.ExceptionFactory;
import com.mysql.cj.core.exception.WrongArgumentException;
import com.mysql.cj.core.util.StringUtils;

/**
 * Represents configurable properties for Connections and DataSources. Can also expose properties as JDBC DriverPropertyInfo if required as well.
 */
public class JdbcConnectionPropertiesImpl extends CommonConnectionProperties implements Serializable, JdbcConnectionProperties {

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

    private boolean autoGenerateTestcaseScriptAsBoolean = false;

    private boolean autoReconnectForPoolsAsBoolean = false;

    private boolean cacheResultSetMetaDataAsBoolean;

    protected boolean characterEncodingIsAliasForSjis = false;

    private boolean jdbcCompliantTruncationForReads = true;

    private boolean highAvailabilityAsBoolean = false;

    private boolean maintainTimeStatsAsBoolean = true;

    private int maxRowsAsInt = -1;

    private boolean useOldUTF8BehaviorAsBoolean = false;

    private boolean useUsageAdvisorAsBoolean = false;

    private boolean profileSQLAsBoolean = false;

    private boolean reconnectTxAtEndAsBoolean = false;

    public JdbcConnectionPropertiesImpl() {
        this.jdbcCompliantTruncationForReads = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_jdbcCompliantTruncation).getValue();
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

        this.reconnectTxAtEndAsBoolean = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_reconnectAtTxEnd).getValue();

        // Adjust max rows
        if (this.getMaxRows() == 0) {
            // adjust so that it will become MysqlDefs.MAX_ROWS in execSQL()
            getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_maxRows).setValue(Integer.valueOf(-1), getExceptionInterceptor());
        }

        //
        // Check character encoding
        //
        String testEncoding = this.getCharacterEncoding();

        if (testEncoding != null) {
            // Attempt to use the encoding, and bail out if it can't be used
            String testString = "abc";
            StringUtils.getBytes(testString, testEncoding);
        }

        this.cacheResultSetMetaDataAsBoolean = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheResultSetMetadata).getValue();
        this.highAvailabilityAsBoolean = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoReconnect).getValue();
        this.autoReconnectForPoolsAsBoolean = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoReconnectForPools).getValue();
        this.maxRowsAsInt = getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_maxRows).getValue();
        this.profileSQLAsBoolean = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_profileSQL).getValue();
        this.useUsageAdvisorAsBoolean = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useUsageAdvisor).getValue();
        this.useOldUTF8BehaviorAsBoolean = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useOldUTF8Behavior).getValue();
        this.autoGenerateTestcaseScriptAsBoolean = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoGenerateTestcaseScript).getValue();
        this.maintainTimeStatsAsBoolean = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_maintainTimeStats).getValue();
        this.jdbcCompliantTruncationForReads = getJdbcCompliantTruncation();

        if (getUseCursorFetch()) {
            // assume they want to use server-side prepared statements because they're required for this functionality
            setDetectServerPreparedStmts(true);
        }
    }

    public boolean getAllowLoadLocalInfile() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_allowLoadLocalInfile).getValue();
    }

    public boolean getAllowMultiQueries() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_allowMultiQueries).getValue();
    }

    public boolean getAllowNanAndInf() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_allowNanAndInf).getValue();
    }

    public boolean getAllowUrlInLocalInfile() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_allowUrlInLocalInfile).getValue();
    }

    public boolean getAlwaysSendSetIsolation() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_alwaysSendSetIsolation).getValue();
    }

    public boolean getAutoDeserialize() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoDeserialize).getValue();
    }

    public boolean getAutoGenerateTestcaseScript() {
        return this.autoGenerateTestcaseScriptAsBoolean;
    }

    public boolean getAutoReconnectForPools() {
        return this.autoReconnectForPoolsAsBoolean;
    }

    public int getBlobSendChunkSize() {
        return getPropertySet().getMemorySizeReadableProperty(PropertyDefinitions.PNAME_blobSendChunkSize).getValue();
    }

    public boolean getCacheCallableStmts() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheCallableStmts).getValue();
    }

    public boolean getCachePrepStmts() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_cachePrepStmts).getValue();
    }

    public boolean getCacheResultSetMetadata() {
        return this.cacheResultSetMetaDataAsBoolean;
    }

    public boolean getCacheServerConfiguration() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheServerConfiguration).getValue();
    }

    public int getCallableStmtCacheSize() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_callableStmtCacheSize).getValue();
    }

    public boolean getCapitalizeTypeNames() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_capitalizeTypeNames).getValue();
    }

    public String getCharacterSetResults() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_characterSetResults).getStringValue();
    }

    public String getConnectionAttributes() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_connectionAttributes).getStringValue();
    }

    public void setConnectionAttributes(String val) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_connectionAttributes).setValue(val);
    }

    public boolean getClobberStreamingResults() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_clobberStreamingResults).getValue();
    }

    public String getClobCharacterEncoding() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_clobCharacterEncoding).getStringValue();
    }

    public String getConnectionCollation() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_connectionCollation).getStringValue();
    }

    public int getConnectTimeout() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_connectTimeout).getValue();
    }

    public boolean getContinueBatchOnError() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_continueBatchOnError).getValue();
    }

    public boolean getCreateDatabaseIfNotExist() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_createDatabaseIfNotExist).getValue();
    }

    public int getDefaultFetchSize() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_defaultFetchSize).getValue();
    }

    public boolean getDontTrackOpenResources() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_dontTrackOpenResources).getValue();
    }

    public boolean getDumpQueriesOnException() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_dumpQueriesOnException).getValue();
    }

    public boolean getElideSetAutoCommits() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_elideSetAutoCommits).getValue();
    }

    public boolean getEmptyStringsConvertToZero() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_emptyStringsConvertToZero).getValue();
    }

    public boolean getEmulateLocators() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_emulateLocators).getValue();
    }

    public boolean getEmulateUnsupportedPstmts() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_emulateUnsupportedPstmts).getValue();
    }

    public boolean getEnablePacketDebug() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enablePacketDebug).getValue();
    }

    public boolean getExplainSlowQueries() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_explainSlowQueries).getValue();
    }

    public boolean getFailOverReadOnly() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_failOverReadOnly).getValue();
    }

    public boolean getGatherPerfMetrics() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_gatherPerfMetrics).getValue();
    }

    protected boolean getHighAvailability() {
        return this.highAvailabilityAsBoolean;
    }

    public boolean getHoldResultsOpenOverStatementClose() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_holdResultsOpenOverStatementClose).getValue();
    }

    public boolean getIgnoreNonTxTables() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_ignoreNonTxTables).getValue();
    }

    public int getInitialTimeout() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_initialTimeout).getValue();
    }

    public boolean getInteractiveClient() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_interactiveClient).getValue();
    }

    public boolean getJdbcCompliantTruncation() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_jdbcCompliantTruncation).getValue();
    }

    public int getLocatorFetchBufferSize() {
        return getPropertySet().getMemorySizeReadableProperty(PropertyDefinitions.PNAME_locatorFetchBufferSize).getValue();
    }

    public String getLogger() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_logger).getStringValue();
    }

    public String getLoggerClassName() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_logger).getStringValue();
    }

    public boolean getLogSlowQueries() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_logSlowQueries).getValue();
    }

    public boolean getMaintainTimeStats() {
        return this.maintainTimeStatsAsBoolean;
    }

    public int getMaxQuerySizeToLog() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_maxQuerySizeToLog).getValue();
    }

    public int getMaxReconnects() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_maxReconnects).getValue();
    }

    public int getMaxRows() {
        return this.maxRowsAsInt;
    }

    public int getMetadataCacheSize() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_metadataCacheSize).getValue();
    }

    public boolean getNoDatetimeStringSync() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_noDatetimeStringSync).getValue();
    }

    public boolean getNullCatalogMeansCurrent() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_nullCatalogMeansCurrent).getValue();
    }

    public boolean getNullNamePatternMatchesAll() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_nullNamePatternMatchesAll).getValue();
    }

    public int getPacketDebugBufferSize() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_packetDebugBufferSize).getValue();
    }

    public boolean getPedantic() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_pedantic).getValue();
    }

    public int getPrepStmtCacheSize() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_prepStmtCacheSize).getValue();
    }

    public int getPrepStmtCacheSqlLimit() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_prepStmtCacheSqlLimit).getValue();
    }

    public boolean getProfileSQL() {
        return this.profileSQLAsBoolean;
    }

    public String getPropertiesTransform() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_propertiesTransform).getStringValue();
    }

    public int getQueriesBeforeRetryMaster() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_queriesBeforeRetryMaster).getValue();
    }

    public boolean getReconnectAtTxEnd() {
        return this.reconnectTxAtEndAsBoolean;
    }

    public int getReportMetricsIntervalMillis() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_reportMetricsIntervalMillis).getValue();
    }

    public boolean getRequireSSL() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_requireSSL).getValue();
    }

    public boolean getRollbackOnPooledClose() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_rollbackOnPooledClose).getValue();
    }

    public boolean getRoundRobinLoadBalance() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_roundRobinLoadBalance).getValue();
    }

    public int getSecondsBeforeRetryMaster() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_secondsBeforeRetryMaster).getValue();
    }

    public String getServerTimezone() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_serverTimezone).getStringValue();
    }

    public String getSessionVariables() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_sessionVariables).getStringValue();
    }

    public int getSlowQueryThresholdMillis() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_slowQueryThresholdMillis).getValue();
    }

    public String getSocketFactory() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_socketFactory).getStringValue();
    }

    public int getSocketTimeout() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_socketTimeout).getValue();
    }

    public boolean getStrictUpdates() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_strictUpdates).getValue();
    }

    public boolean getTinyInt1isBit() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_tinyInt1isBit).getValue();
    }

    public boolean getTraceProtocol() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_traceProtocol).getValue();
    }

    public boolean getTransformedBitIsBoolean() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_transformedBitIsBoolean).getValue();
    }

    public boolean getUseCompression() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useCompression).getValue();
    }

    public boolean getUseHostsInPrivileges() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useHostsInPrivileges).getValue();
    }

    public boolean getUseInformationSchema() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useInformationSchema).getValue();
    }

    public boolean getUseLocalSessionState() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useLocalSessionState).getValue();
    }

    public boolean getUseOldUTF8Behavior() {
        return this.useOldUTF8BehaviorAsBoolean;
    }

    public boolean getUseOnlyServerErrorMessages() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useOnlyServerErrorMessages).getValue();
    }

    public boolean getUseReadAheadInput() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useReadAheadInput).getValue();
    }

    public boolean getUseServerPrepStmts() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useServerPrepStmts).getValue();
    }

    public boolean getUseSSL() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useSSL).getValue();
    }

    public boolean getUseStreamLengthsInPrepStmts() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useStreamLengthsInPrepStmts).getValue();
    }

    public boolean getUltraDevHack() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_ultraDevHack).getValue();
    }

    public boolean getUseUsageAdvisor() {
        return this.useUsageAdvisorAsBoolean;
    }

    public boolean getYearIsDateType() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_yearIsDateType).getValue();
    }

    public String getZeroDateTimeBehavior() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_zeroDateTimeBehavior).getStringValue();
    }

    public void setAllowLoadLocalInfile(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_allowLoadLocalInfile).setValue(property);
    }

    public void setAllowMultiQueries(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_allowMultiQueries).setValue(property);
    }

    public void setAllowNanAndInf(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_allowNanAndInf).setValue(flag);
    }

    public void setAllowUrlInLocalInfile(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_allowUrlInLocalInfile).setValue(flag);
    }

    public void setAlwaysSendSetIsolation(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_alwaysSendSetIsolation).setValue(flag);
    }

    public void setAutoDeserialize(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_autoDeserialize).setValue(flag);
    }

    public void setAutoGenerateTestcaseScript(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_autoGenerateTestcaseScript).setValue(flag);
        this.autoGenerateTestcaseScriptAsBoolean = flag;
    }

    public void setAutoReconnect(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_autoReconnect).setValue(flag);
    }

    public void setAutoReconnectForConnectionPools(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_autoReconnectForPools).setValue(property);
        this.autoReconnectForPoolsAsBoolean = property;
    }

    public void setAutoReconnectForPools(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_autoReconnectForPools).setValue(flag);
    }

    public void setBlobSendChunkSize(String value) throws SQLException {
        getPropertySet().getMemorySizeModifiableProperty(PropertyDefinitions.PNAME_blobSendChunkSize).setFromString(value, getExceptionInterceptor());
    }

    public void setCacheCallableStmts(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_cacheCallableStmts).setValue(flag);
    }

    public void setCachePrepStmts(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_cachePrepStmts).setValue(flag);
    }

    public void setCacheResultSetMetadata(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_cacheResultSetMetadata).setValue(property);
        this.cacheResultSetMetaDataAsBoolean = property;
    }

    public void setCacheServerConfiguration(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_cacheServerConfiguration).setValue(flag);
    }

    public void setCallableStmtCacheSize(int size) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_callableStmtCacheSize).setValue(size, getExceptionInterceptor());
    }

    public void setCapitalizeDBMDTypes(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_capitalizeTypeNames).setValue(property);
    }

    public void setCapitalizeTypeNames(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_capitalizeTypeNames).setValue(flag);
    }

    public void setCharacterSetResults(String characterSet) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_characterSetResults).setValue(characterSet);
    }

    public void setClobberStreamingResults(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_clobberStreamingResults).setValue(flag);
    }

    public void setClobCharacterEncoding(String encoding) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_clobCharacterEncoding).setValue(encoding);
    }

    public void setConnectionCollation(String collation) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_connectionCollation).setValue(collation);
    }

    public void setConnectTimeout(int timeoutMs) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_connectTimeout).setValue(timeoutMs, getExceptionInterceptor());
    }

    public void setContinueBatchOnError(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_continueBatchOnError).setValue(property);
    }

    public void setCreateDatabaseIfNotExist(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_createDatabaseIfNotExist).setValue(flag);
    }

    public void setDefaultFetchSize(int n) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_defaultFetchSize).setValue(n, getExceptionInterceptor());
    }

    public void setDetectServerPreparedStmts(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useServerPrepStmts).setValue(property);
    }

    public void setDontTrackOpenResources(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_dontTrackOpenResources).setValue(flag);
    }

    public void setDumpQueriesOnException(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_dumpQueriesOnException).setValue(flag);
    }

    public void setElideSetAutoCommits(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_elideSetAutoCommits).setValue(flag);
    }

    public void setEmptyStringsConvertToZero(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_emptyStringsConvertToZero).setValue(flag);
    }

    public void setEmulateLocators(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_emulateLocators).setValue(property);
    }

    public void setEmulateUnsupportedPstmts(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_emulateUnsupportedPstmts).setValue(flag);
    }

    public void setEnablePacketDebug(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_enablePacketDebug).setValue(flag);
    }

    public void setExplainSlowQueries(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_explainSlowQueries).setValue(flag);
    }

    public void setFailOverReadOnly(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_failOverReadOnly).setValue(flag);
    }

    public void setGatherPerfMetrics(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_gatherPerfMetrics).setValue(flag);
    }

    protected void setHighAvailability(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_autoReconnect).setValue(property);
        this.highAvailabilityAsBoolean = property;
    }

    public void setHoldResultsOpenOverStatementClose(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_holdResultsOpenOverStatementClose).setValue(flag);
    }

    public void setIgnoreNonTxTables(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_ignoreNonTxTables).setValue(property);
    }

    public void setInitialTimeout(int property) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_initialTimeout).setValue(property, getExceptionInterceptor());
    }

    public void setInteractiveClient(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_interactiveClient).setValue(property);
    }

    public void setJdbcCompliantTruncation(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_jdbcCompliantTruncation).setValue(flag);
    }

    public void setLocatorFetchBufferSize(String value) throws SQLException {
        getPropertySet().getMemorySizeModifiableProperty(PropertyDefinitions.PNAME_locatorFetchBufferSize).setFromString(value, getExceptionInterceptor());
    }

    public void setLogger(String property) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_logger).setValue(property);
    }

    public void setLoggerClassName(String className) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_logger).setValue(className);
    }

    public void setLogSlowQueries(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_logSlowQueries).setValue(flag);
    }

    public void setMaintainTimeStats(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_maintainTimeStats).setValue(flag);
        this.maintainTimeStatsAsBoolean = flag;
    }

    public void setMaxQuerySizeToLog(int sizeInBytes) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_maxQuerySizeToLog).setValue(sizeInBytes, getExceptionInterceptor());
    }

    public void setMaxReconnects(int property) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_maxReconnects).setValue(property, getExceptionInterceptor());
    }

    public void setMaxRows(int property) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_maxRows).setValue(property, getExceptionInterceptor());
        this.maxRowsAsInt = getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_maxRows).getValue();
    }

    public void setMetadataCacheSize(int value) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_metadataCacheSize).setValue(value, getExceptionInterceptor());
    }

    public void setNoDatetimeStringSync(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_noDatetimeStringSync).setValue(flag);
    }

    public void setNullCatalogMeansCurrent(boolean value) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_nullCatalogMeansCurrent).setValue(value);
    }

    public void setNullNamePatternMatchesAll(boolean value) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_nullNamePatternMatchesAll).setValue(value);
    }

    public void setPacketDebugBufferSize(int size) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_packetDebugBufferSize).setValue(size, getExceptionInterceptor());
    }

    public void setPedantic(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_pedantic).setValue(property);
    }

    public void setPrepStmtCacheSize(int cacheSize) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_prepStmtCacheSize).setValue(cacheSize, getExceptionInterceptor());
    }

    public void setPrepStmtCacheSqlLimit(int cacheSqlLimit) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_prepStmtCacheSqlLimit).setValue(cacheSqlLimit, getExceptionInterceptor());
    }

    public void setProfileSQL(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_profileSQL).setValue(flag);
        this.profileSQLAsBoolean = flag;
    }

    public void setPropertiesTransform(String value) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_propertiesTransform).setValue(value);
    }

    public void setQueriesBeforeRetryMaster(int property) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_queriesBeforeRetryMaster).setValue(property, getExceptionInterceptor());
    }

    public void setReconnectAtTxEnd(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_reconnectAtTxEnd).setValue(property);
        this.reconnectTxAtEndAsBoolean = property;
    }

    public void setReportMetricsIntervalMillis(int millis) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_reportMetricsIntervalMillis).setValue(millis, getExceptionInterceptor());
    }

    public void setRequireSSL(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_requireSSL).setValue(property);
    }

    public void setRollbackOnPooledClose(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_rollbackOnPooledClose).setValue(flag);
    }

    public void setRoundRobinLoadBalance(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_roundRobinLoadBalance).setValue(flag);
    }

    public void setSecondsBeforeRetryMaster(int property) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_secondsBeforeRetryMaster).setValue(property, getExceptionInterceptor());
    }

    public void setServerTimezone(String property) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_serverTimezone).setValue(property);
    }

    public void setSessionVariables(String variables) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_sessionVariables).setValue(variables);
    }

    public void setSlowQueryThresholdMillis(int millis) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_slowQueryThresholdMillis).setValue(millis, getExceptionInterceptor());
    }

    public void setSocketFactory(String property) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_socketFactory).setValue(property);
    }

    public void setSocketTimeout(int property) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_socketTimeout).setValue(property, getExceptionInterceptor());
    }

    public void setStrictUpdates(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_strictUpdates).setValue(property);
    }

    public void setTinyInt1isBit(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_tinyInt1isBit).setValue(flag);
    }

    public void setTraceProtocol(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_traceProtocol).setValue(flag);
    }

    public void setTransformedBitIsBoolean(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_transformedBitIsBoolean).setValue(flag);
    }

    public void setUseCompression(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useCompression).setValue(property);
    }

    public void setUseHostsInPrivileges(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useHostsInPrivileges).setValue(property);
    }

    public void setUseInformationSchema(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useInformationSchema).setValue(flag);
    }

    public void setUseLocalSessionState(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useLocalSessionState).setValue(flag);
    }

    public void setUseOldUTF8Behavior(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useOldUTF8Behavior).setValue(flag);
        this.useOldUTF8BehaviorAsBoolean = flag;
    }

    public void setUseOnlyServerErrorMessages(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useOnlyServerErrorMessages).setValue(flag);
    }

    public void setUseReadAheadInput(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useReadAheadInput).setValue(flag);
    }

    public void setUseServerPrepStmts(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useServerPrepStmts).setValue(flag);
    }

    public void setUseSSL(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useSSL).setValue(property);
    }

    public void setUseStreamLengthsInPrepStmts(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useStreamLengthsInPrepStmts).setValue(property);
    }

    public void setUltraDevHack(boolean property) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_ultraDevHack).setValue(property);
    }

    public void setUseUnbufferedInput(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useUnbufferedInput).setValue(flag);
    }

    public void setUseUsageAdvisor(boolean useUsageAdvisorFlag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useUsageAdvisor).setValue(useUsageAdvisorFlag);
        this.useUsageAdvisorAsBoolean = useUsageAdvisorFlag;
    }

    public void setYearIsDateType(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_yearIsDateType).setValue(flag);
    }

    public void setZeroDateTimeBehavior(String behavior) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_zeroDateTimeBehavior).setValue(behavior);
    }

    public boolean useUnbufferedInput() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useUnbufferedInput).getValue();
    }

    public boolean getUseCursorFetch() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useCursorFetch).getValue();
    }

    public void setUseCursorFetch(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useCursorFetch).setValue(flag);
    }

    public boolean getOverrideSupportsIntegrityEnhancementFacility() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_overrideSupportsIntegrityEnhancementFacility).getValue();
    }

    public void setOverrideSupportsIntegrityEnhancementFacility(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_overrideSupportsIntegrityEnhancementFacility).setValue(flag);
    }

    public boolean getAutoClosePStmtStreams() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoClosePStmtStreams).getValue();
    }

    public void setAutoClosePStmtStreams(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_autoClosePStmtStreams).setValue(flag);
    }

    public boolean getProcessEscapeCodesForPrepStmts() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_processEscapeCodesForPrepStmts).getValue();
    }

    public void setProcessEscapeCodesForPrepStmts(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_processEscapeCodesForPrepStmts).setValue(flag);
    }

    public String getResourceId() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_resourceId).getStringValue();
    }

    public void setResourceId(String resourceId) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_resourceId).setValue(resourceId);
    }

    public boolean getRewriteBatchedStatements() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_rewriteBatchedStatements).getValue();
    }

    public void setRewriteBatchedStatements(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_rewriteBatchedStatements).setValue(flag);
    }

    public boolean getJdbcCompliantTruncationForReads() {
        return this.jdbcCompliantTruncationForReads;
    }

    public void setJdbcCompliantTruncationForReads(boolean jdbcCompliantTruncationForReads) {
        this.jdbcCompliantTruncationForReads = jdbcCompliantTruncationForReads;
    }

    public boolean getPinGlobalTxToPhysicalConnection() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_pinGlobalTxToPhysicalConnection).getValue();
    }

    public void setPinGlobalTxToPhysicalConnection(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_pinGlobalTxToPhysicalConnection).setValue(flag);
    }

    public boolean getNoAccessToProcedureBodies() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_noAccessToProcedureBodies).getValue();
    }

    public void setNoAccessToProcedureBodies(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_noAccessToProcedureBodies).setValue(flag);
    }

    public boolean getUseOldAliasMetadataBehavior() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useOldAliasMetadataBehavior).getValue();
    }

    public void setUseOldAliasMetadataBehavior(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useOldAliasMetadataBehavior).setValue(flag);
    }

    public boolean getTreatUtilDateAsTimestamp() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_treatUtilDateAsTimestamp).getValue();
    }

    public void setTreatUtilDateAsTimestamp(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_treatUtilDateAsTimestamp).setValue(flag);
    }

    public String getLocalSocketAddress() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_localSocketAddress).getStringValue();
    }

    public void setLocalSocketAddress(String address) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_localSocketAddress).setValue(address);
    }

    public void setUseConfigs(String configs) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_useConfigs).setValue(configs);
    }

    public String getUseConfigs() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_useConfigs).getStringValue();
    }

    public boolean getGenerateSimpleParameterMetadata() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_generateSimpleParameterMetadata).getValue();
    }

    public void setGenerateSimpleParameterMetadata(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_generateSimpleParameterMetadata).setValue(flag);
    }

    public boolean getLogXaCommands() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_logXaCommands).getValue();
    }

    public void setLogXaCommands(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_logXaCommands).setValue(flag);
    }

    public int getResultSetSizeThreshold() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_resultSetSizeThreshold).getValue();
    }

    public void setResultSetSizeThreshold(int threshold) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_resultSetSizeThreshold).setValue(threshold, getExceptionInterceptor());
    }

    public int getNetTimeoutForStreamingResults() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_netTimeoutForStreamingResults).getValue();
    }

    public void setNetTimeoutForStreamingResults(int value) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_netTimeoutForStreamingResults).setValue(value, getExceptionInterceptor());
    }

    public boolean getEnableQueryTimeouts() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enableQueryTimeouts).getValue();
    }

    public void setEnableQueryTimeouts(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_enableQueryTimeouts).setValue(flag);
    }

    public boolean getPadCharsWithSpace() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_padCharsWithSpace).getValue();
    }

    public void setPadCharsWithSpace(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_padCharsWithSpace).setValue(flag);
    }

    public boolean getUseDynamicCharsetInfo() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useDynamicCharsetInfo).getValue();
    }

    public void setUseDynamicCharsetInfo(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useDynamicCharsetInfo).setValue(flag);
    }

    public String getClientInfoProvider() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_clientInfoProvider).getStringValue();
    }

    public void setClientInfoProvider(String classname) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_clientInfoProvider).setValue(classname);
    }

    public boolean getPopulateInsertRowWithDefaultValues() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_populateInsertRowWithDefaultValues).getValue();
    }

    public void setPopulateInsertRowWithDefaultValues(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_populateInsertRowWithDefaultValues).setValue(flag);
    }

    public String getHaLoadBalanceStrategy() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceStrategy).getStringValue();
    }

    public void setHaLoadBalanceStrategy(String strategy) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_loadBalanceStrategy).setValue(strategy);
    }

    public boolean getTcpNoDelay() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_tcpNoDelay).getValue();
    }

    public void setTcpNoDelay(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_tcpNoDelay).setValue(flag);
    }

    public boolean getTcpKeepAlive() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_tcpKeepAlive).getValue();
    }

    public void setTcpKeepAlive(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_tcpKeepAlive).setValue(flag);
    }

    public int getTcpRcvBuf() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_tcpRcvBuf).getValue();
    }

    public void setTcpRcvBuf(int bufSize) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_tcpRcvBuf).setValue(bufSize, getExceptionInterceptor());
    }

    public int getTcpSndBuf() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_tcpSndBuf).getValue();
    }

    public void setTcpSndBuf(int bufSize) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_tcpSndBuf).setValue(bufSize, getExceptionInterceptor());
    }

    public int getTcpTrafficClass() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_tcpTrafficClass).getValue();
    }

    public void setTcpTrafficClass(int classFlags) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_tcpTrafficClass).setValue(classFlags, getExceptionInterceptor());
    }

    public boolean getUseNanosForElapsedTime() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useNanosForElapsedTime).getValue();
    }

    public void setUseNanosForElapsedTime(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useNanosForElapsedTime).setValue(flag);
    }

    public long getSlowQueryThresholdNanos() {
        return getPropertySet().getLongReadableProperty(PropertyDefinitions.PNAME_slowQueryThresholdNanos).getValue();
    }

    public void setSlowQueryThresholdNanos(long nanos) throws SQLException {
        getPropertySet().getLongModifiableProperty(PropertyDefinitions.PNAME_slowQueryThresholdNanos).setValue(nanos, getExceptionInterceptor());
    }

    public String getStatementInterceptors() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_statementInterceptors).getStringValue();
    }

    public void setStatementInterceptors(String value) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_statementInterceptors).setValue(value);
    }

    public boolean getUseDirectRowUnpack() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useDirectRowUnpack).getValue();
    }

    public void setUseDirectRowUnpack(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useDirectRowUnpack).setValue(flag);
    }

    public String getLargeRowSizeThreshold() {
        return getPropertySet().getMemorySizeReadableProperty(PropertyDefinitions.PNAME_largeRowSizeThreshold).getStringValue();
    }

    public void setLargeRowSizeThreshold(String value) throws SQLException {
        getPropertySet().getMemorySizeModifiableProperty(PropertyDefinitions.PNAME_largeRowSizeThreshold).setFromString(value, getExceptionInterceptor());
    }

    public boolean getUseBlobToStoreUTF8OutsideBMP() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useBlobToStoreUTF8OutsideBMP).getValue();
    }

    public void setUseBlobToStoreUTF8OutsideBMP(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useBlobToStoreUTF8OutsideBMP).setValue(flag);
    }

    public String getUtf8OutsideBmpExcludedColumnNamePattern() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_utf8OutsideBmpExcludedColumnNamePattern).getStringValue();
    }

    public void setUtf8OutsideBmpExcludedColumnNamePattern(String regexPattern) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_utf8OutsideBmpExcludedColumnNamePattern).setValue(regexPattern);
    }

    public String getUtf8OutsideBmpIncludedColumnNamePattern() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_utf8OutsideBmpIncludedColumnNamePattern).getStringValue();
    }

    public void setUtf8OutsideBmpIncludedColumnNamePattern(String regexPattern) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_utf8OutsideBmpIncludedColumnNamePattern).setValue(regexPattern);
    }

    public boolean getIncludeInnodbStatusInDeadlockExceptions() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_includeInnodbStatusInDeadlockExceptions).getValue();
    }

    public void setIncludeInnodbStatusInDeadlockExceptions(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_includeInnodbStatusInDeadlockExceptions).setValue(flag);
    }

    public boolean getBlobsAreStrings() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_blobsAreStrings).getValue();
    }

    public void setBlobsAreStrings(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_blobsAreStrings).setValue(flag);
    }

    public boolean getFunctionsNeverReturnBlobs() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_functionsNeverReturnBlobs).getValue();
    }

    public void setFunctionsNeverReturnBlobs(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_functionsNeverReturnBlobs).setValue(flag);
    }

    public boolean getAutoSlowLog() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoSlowLog).getValue();
    }

    public void setAutoSlowLog(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_autoSlowLog).setValue(flag);
    }

    public String getConnectionLifecycleInterceptors() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_connectionLifecycleInterceptors).getStringValue();
    }

    public void setConnectionLifecycleInterceptors(String interceptors) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_connectionLifecycleInterceptors).setValue(interceptors);
    }

    public int getSelfDestructOnPingSecondsLifetime() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_selfDestructOnPingSecondsLifetime).getValue();
    }

    public void setSelfDestructOnPingSecondsLifetime(int seconds) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_selfDestructOnPingSecondsLifetime).setValue(seconds, getExceptionInterceptor());
    }

    public int getSelfDestructOnPingMaxOperations() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_selfDestructOnPingMaxOperations).getValue();
    }

    public void setSelfDestructOnPingMaxOperations(int maxOperations) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_selfDestructOnPingMaxOperations).setValue(maxOperations,
                getExceptionInterceptor());
    }

    public boolean getUseColumnNamesInFindColumn() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useColumnNamesInFindColumn).getValue();
    }

    public void setUseColumnNamesInFindColumn(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useColumnNamesInFindColumn).setValue(flag);
    }

    public boolean getUseLocalTransactionState() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useLocalTransactionState).getValue();
    }

    public void setUseLocalTransactionState(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useLocalTransactionState).setValue(flag);
    }

    public boolean getCompensateOnDuplicateKeyUpdateCounts() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_compensateOnDuplicateKeyUpdateCounts).getValue();
    }

    public void setCompensateOnDuplicateKeyUpdateCounts(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_compensateOnDuplicateKeyUpdateCounts).setValue(flag);
    }

    public int getLoadBalanceBlacklistTimeout() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_loadBalanceBlacklistTimeout).getValue();
    }

    public void setLoadBalanceBlacklistTimeout(int loadBalanceBlacklistTimeout) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_loadBalanceBlacklistTimeout).setValue(loadBalanceBlacklistTimeout,
                getExceptionInterceptor());
    }

    public int getLoadBalancePingTimeout() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_loadBalancePingTimeout).getValue();
    }

    public void setLoadBalancePingTimeout(int loadBalancePingTimeout) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_loadBalancePingTimeout).setValue(loadBalancePingTimeout,
                getExceptionInterceptor());
    }

    public void setRetriesAllDown(int retriesAllDown) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_retriesAllDown).setValue(retriesAllDown, getExceptionInterceptor());
    }

    public int getRetriesAllDown() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_retriesAllDown).getValue();
    }

    public void setUseAffectedRows(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_useAffectedRows).setValue(flag);
    }

    public boolean getUseAffectedRows() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useAffectedRows).getValue();
    }

    public void setExceptionInterceptors(String exceptionInterceptors) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_exceptionInterceptors).setValue(exceptionInterceptors);
    }

    public String getExceptionInterceptors() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_exceptionInterceptors).getStringValue();
    }

    public void setMaxAllowedPacket(int max) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_maxAllowedPacket).setValue(max, getExceptionInterceptor());
    }

    public int getMaxAllowedPacket() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_maxAllowedPacket).getValue();
    }

    public boolean getQueryTimeoutKillsConnection() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_queryTimeoutKillsConnection).getValue();
    }

    public void setQueryTimeoutKillsConnection(boolean queryTimeoutKillsConnection) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_queryTimeoutKillsConnection).setValue(queryTimeoutKillsConnection);
    }

    public boolean getLoadBalanceValidateConnectionOnSwapServer() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_loadBalanceValidateConnectionOnSwapServer).getValue();
    }

    public void setLoadBalanceValidateConnectionOnSwapServer(boolean loadBalanceValidateConnectionOnSwapServer) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_loadBalanceValidateConnectionOnSwapServer).setValue(
                loadBalanceValidateConnectionOnSwapServer);

    }

    public String getLoadBalanceConnectionGroup() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceConnectionGroup).getStringValue();
    }

    public void setLoadBalanceConnectionGroup(String loadBalanceConnectionGroup) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_loadBalanceConnectionGroup).setValue(loadBalanceConnectionGroup);
    }

    public String getLoadBalanceExceptionChecker() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceExceptionChecker).getStringValue();
    }

    public void setLoadBalanceExceptionChecker(String loadBalanceExceptionChecker) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_loadBalanceExceptionChecker).setValue(loadBalanceExceptionChecker);
    }

    public String getLoadBalanceSQLStateFailover() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceSQLStateFailover).getStringValue();
    }

    public void setLoadBalanceSQLStateFailover(String loadBalanceSQLStateFailover) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_loadBalanceSQLStateFailover).setValue(loadBalanceSQLStateFailover);
    }

    public String getLoadBalanceSQLExceptionSubclassFailover() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceSQLExceptionSubclassFailover).getStringValue();
    }

    public void setLoadBalanceSQLExceptionSubclassFailover(String loadBalanceSQLExceptionSubclassFailover) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_loadBalanceSQLExceptionSubclassFailover).setValue(
                loadBalanceSQLExceptionSubclassFailover);
    }

    public void setLoadBalanceAutoCommitStatementThreshold(int loadBalanceAutoCommitStatementThreshold) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementThreshold).setValue(
                loadBalanceAutoCommitStatementThreshold, getExceptionInterceptor());
    }

    public int getLoadBalanceAutoCommitStatementThreshold() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementThreshold).getValue();
    }

    public void setLoadBalanceAutoCommitStatementRegex(String loadBalanceAutoCommitStatementRegex) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementRegex).setValue(
                loadBalanceAutoCommitStatementRegex);
    }

    public String getLoadBalanceAutoCommitStatementRegex() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementRegex).getStringValue();
    }

    public void setIncludeThreadDumpInDeadlockExceptions(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_includeThreadDumpInDeadlockExceptions).setValue(flag);
    }

    public boolean getIncludeThreadDumpInDeadlockExceptions() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_includeThreadDumpInDeadlockExceptions).getValue();
    }

    public void setIncludeThreadNamesAsStatementComment(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_includeThreadNamesAsStatementComment).setValue(flag);
    }

    public boolean getIncludeThreadNamesAsStatementComment() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_includeThreadNamesAsStatementComment).getValue();
    }

    public void setAuthenticationPlugins(String authenticationPlugins) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_authenticationPlugins).setValue(authenticationPlugins);
    }

    public String getAuthenticationPlugins() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_authenticationPlugins).getStringValue();
    }

    public void setDisabledAuthenticationPlugins(String disabledAuthenticationPlugins) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_disabledAuthenticationPlugins).setValue(disabledAuthenticationPlugins);
    }

    public String getDisabledAuthenticationPlugins() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_disabledAuthenticationPlugins).getStringValue();
    }

    public void setDefaultAuthenticationPlugin(String defaultAuthenticationPlugin) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_defaultAuthenticationPlugin).setValue(defaultAuthenticationPlugin);

    }

    public String getDefaultAuthenticationPlugin() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_defaultAuthenticationPlugin).getStringValue();
    }

    public void setParseInfoCacheFactory(String factoryClassname) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_parseInfoCacheFactory).setValue(factoryClassname);
    }

    public String getParseInfoCacheFactory() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_parseInfoCacheFactory).getStringValue();
    }

    public void setServerConfigCacheFactory(String factoryClassname) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_serverConfigCacheFactory).setValue(factoryClassname);
    }

    public String getServerConfigCacheFactory() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_serverConfigCacheFactory).getStringValue();
    }

    public void setDisconnectOnExpiredPasswords(boolean disconnectOnExpiredPasswords) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_disconnectOnExpiredPasswords).setValue(disconnectOnExpiredPasswords);
    }

    public boolean getDisconnectOnExpiredPasswords() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_disconnectOnExpiredPasswords).getValue();
    }

    public boolean getAllowMasterDownConnections() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_allowMasterDownConnections).getValue();
    }

    public void setAllowMasterDownConnections(boolean connectIfMasterDown) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_allowMasterDownConnections).setValue(connectIfMasterDown);
    }

    public boolean getHaEnableJMX() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_ha_enableJMX).getValue();
    }

    public void setHaEnableJMX(boolean replicationEnableJMX) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_ha_enableJMX).setValue(replicationEnableJMX);

    }

    public void setGetProceduresReturnsFunctions(boolean getProcedureReturnsFunctions) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_getProceduresReturnsFunctions).setValue(getProcedureReturnsFunctions);
    }

    public boolean getGetProceduresReturnsFunctions() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_getProceduresReturnsFunctions).getValue();
    }

    public void setDetectCustomCollations(boolean detectCustomCollations) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_detectCustomCollations).setValue(detectCustomCollations);
    }

    public boolean getDetectCustomCollations() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_detectCustomCollations).getValue();
    }

    public void setServerRSAPublicKeyFile(String serverRSAPublicKeyFile) throws SQLException {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_serverRSAPublicKeyFile).setValue(serverRSAPublicKeyFile);
    }

    public void setAllowPublicKeyRetrieval(boolean allowPublicKeyRetrieval) throws SQLException {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_allowPublicKeyRetrieval).setValue(allowPublicKeyRetrieval);
    }

    public void setDontCheckOnDuplicateKeyUpdateInSQL(boolean dontCheckOnDuplicateKeyUpdateInSQL) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_dontCheckOnDuplicateKeyUpdateInSQL)
                .setValue(dontCheckOnDuplicateKeyUpdateInSQL);
    }

    public boolean getDontCheckOnDuplicateKeyUpdateInSQL() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_dontCheckOnDuplicateKeyUpdateInSQL).getValue();
    }

    public void setSocksProxyHost(String socksProxyHost) {
        getPropertySet().getStringModifiableProperty(PropertyDefinitions.PNAME_socksProxyHost).setValue(socksProxyHost);
    }

    public String getSocksProxyHost() {
        return getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_socksProxyHost).getStringValue();
    }

    public void setSocksProxyPort(int socksProxyPort) throws SQLException {
        getPropertySet().getIntegerModifiableProperty(PropertyDefinitions.PNAME_socksProxyPort).setValue(socksProxyPort, null);
    }

    public int getSocksProxyPort() {
        return getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_socksProxyPort).getValue();
    }

    public boolean getReadOnlyPropagatesToServer() {
        return getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_readOnlyPropagatesToServer).getValue();
    }

    public void setReadOnlyPropagatesToServer(boolean flag) {
        getPropertySet().getBooleanModifiableProperty(PropertyDefinitions.PNAME_readOnlyPropagatesToServer).setValue(flag);
    }
}
