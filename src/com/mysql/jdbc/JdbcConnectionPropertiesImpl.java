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
import java.io.UnsupportedEncodingException;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.Reference;

import com.mysql.cj.api.conf.PropertyDefinition;
import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.conf.CommonConnectionProperties;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exception.CJException;
import com.mysql.cj.core.exception.ExceptionFactory;
import com.mysql.cj.core.exception.WrongArgumentException;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.jdbc.exceptions.SQLError;

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
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_holdResultsOpenOverStatementClose));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_allowLoadLocalInfile));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_allowMultiQueries));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_allowNanAndInf));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_allowUrlInLocalInfile));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_alwaysSendSetIsolation));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_autoClosePStmtStreams));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_allowMasterDownConnections));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_autoDeserialize));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_autoGenerateTestcaseScript));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_autoReconnect));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_autoReconnectForPools));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_autoSlowLog));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_blobsAreStrings));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_functionsNeverReturnBlobs));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_cacheCallableStmts));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_cachePrepStmts));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_cacheResultSetMetadata));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_cacheServerConfiguration));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_capitalizeTypeNames));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_clobberStreamingResults));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_compensateOnDuplicateKeyUpdateCounts));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_continueBatchOnError));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_createDatabaseIfNotExist));

        // Think really long and hard about changing the default for this many, many applications have come to be acustomed to the latency profile of preparing
        // stuff client-side, rather than prepare (round-trip), execute (round-trip), close (round-trip).
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useServerPrepStmts));

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_dontTrackOpenResources));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_dumpQueriesOnException));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_dynamicCalendars));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_elideSetAutoCommits));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_emptyStringsConvertToZero));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_emulateLocators));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_emulateUnsupportedPstmts));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_enablePacketDebug));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_enableQueryTimeouts));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_explainSlowQueries));

        /** When failed-over, set connection to read-only? */
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_failOverReadOnly));

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_gatherPerfMetrics));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_generateSimpleParameterMetadata));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_includeInnodbStatusInDeadlockExceptions));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_includeThreadDumpInDeadlockExceptions));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_includeThreadNamesAsStatementComment));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_ignoreNonTxTables));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_interactiveClient));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_jdbcCompliantTruncation));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceValidateConnectionOnSwapServer));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceEnableJMX));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_logSlowQueries));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_logXaCommands));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_maintainTimeStats));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_noAccessToProcedureBodies));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_noDatetimeStringSync));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_noTimezoneConversionForTimeType));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_noTimezoneConversionForDateType));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_cacheDefaultTimezone));

        // TODO: rename this property according to WL#8120; default value is already changed as required by this WL
        // TODO: make this property consistent to nullNamePatternMatchesAll; nullCatalogMeansCurrent never cause an exception, but nullNamePatternMatchesAll does.
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_nullCatalogMeansCurrent));

        // TODO: rename this property according to WL#8120; default value is already changed as required by this WL
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_nullNamePatternMatchesAll));

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_padCharsWithSpace));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_pedantic));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_pinGlobalTxToPhysicalConnection));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_populateInsertRowWithDefaultValues));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_processEscapeCodesForPrepStmts));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_profileSQL));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_queryTimeoutKillsConnection));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_reconnectAtTxEnd));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_relaxAutoCommit));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_requireSSL));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_rewriteBatchedStatements));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_rollbackOnPooledClose));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_roundRobinLoadBalance));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_runningCTS13));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_replicationEnableJMX));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_strictFloatingPoint));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_strictUpdates));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_overrideSupportsIntegrityEnhancementFacility));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_tcpNoDelay));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_tcpKeepAlive));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_tinyInt1isBit));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_traceProtocol));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_treatUtilDateAsTimestamp));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_transformedBitIsBoolean));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useBlobToStoreUTF8OutsideBMP));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useCompression));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useColumnNamesInFindColumn));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useCursorFetch));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useDynamicCharsetInfo));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useDirectRowUnpack));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useFastIntParsing));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useFastDateParsing));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useHostsInPrivileges));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useInformationSchema));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useJDBCCompliantTimezoneShift));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useLocalSessionState));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useLocalTransactionState));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useLegacyDatetimeCode));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useNanosForElapsedTime));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useOldAliasMetadataBehavior));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useOldUTF8Behavior));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useOnlyServerErrorMessages));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useReadAheadInput));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useSSL));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useSSPSCompatibleTimezoneShift));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useStreamLengthsInPrepStmts));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useTimezone));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_ultraDevHack));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useUnbufferedInput));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useUnicode));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useUsageAdvisor));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_yearIsDateType));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useJvmCharsetConverters));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useGmtMillisForDatetimes));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_dumpMetadataOnColumnNotFound));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useAffectedRows));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_disconnectOnExpiredPasswords));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_getProceduresReturnsFunctions));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_detectCustomCollations));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_allowPublicKeyRetrieval));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_dontCheckOnDuplicateKeyUpdateInSQL));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_readOnlyPropagatesToServer));

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_callableStmtCacheSize));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_connectTimeout));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_defaultFetchSize));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_initialTimeout));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceBlacklistTimeout));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalancePingTimeout));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementThreshold));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_maxQuerySizeToLog));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_maxReconnects));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_retriesAllDown));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_maxRows));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_metadataCacheSize));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_netTimeoutForStreamingResults));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_packetDebugBufferSize));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_prepStmtCacheSize));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_prepStmtCacheSqlLimit));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_queriesBeforeRetryMaster));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_reportMetricsIntervalMillis));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_resultSetSizeThreshold));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_secondsBeforeRetryMaster));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_selfDestructOnPingSecondsLifetime));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_selfDestructOnPingMaxOperations));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_slowQueryThresholdMillis));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_socksProxyPort));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_socketTimeout));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_tcpRcvBuf));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_tcpSndBuf));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_tcpTrafficClass));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_maxAllowedPacket));

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_slowQueryThresholdNanos));

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_blobSendChunkSize));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_largeRowSizeThreshold));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_locatorFetchBufferSize));

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_serverConfigCacheFactory));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_characterEncoding));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_characterSetResults));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_connectionAttributes));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_clientInfoProvider));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_clobCharacterEncoding));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_connectionCollation));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_connectionLifecycleInterceptors));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_exceptionInterceptors));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceStrategy));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceConnectionGroup));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceExceptionChecker));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceSQLStateFailover));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceSQLExceptionSubclassFailover));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementRegex));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_localSocketAddress));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_logger));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_parseInfoCacheFactory));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_propertiesTransform));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_resourceId));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_serverTimezone));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_sessionVariables));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_socketFactory));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_socksProxyHost));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_statementInterceptors));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_utf8OutsideBmpExcludedColumnNamePattern));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_utf8OutsideBmpIncludedColumnNamePattern));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useConfigs));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_zeroDateTimeBehavior));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_authenticationPlugins));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_disabledAuthenticationPlugins));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_defaultAuthenticationPlugin));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_serverRSAPublicKeyFile));

        this.jdbcCompliantTruncationForReads = getBooleanReadableProperty(PropertyDefinitions.PNAME_jdbcCompliantTruncation).getValueAsBoolean();

    }

    private DriverPropertyInfo getAsDriverPropertyInfo(ReadableProperty pr) {
        PropertyDefinition pdef = pr.getPropertyDefinition();

        DriverPropertyInfo dpi = new DriverPropertyInfo(pdef.getName(), null);
        dpi.choices = pdef.getAllowableValues();
        dpi.value = (pr.getStringValue() != null) ? pr.getStringValue() : null;
        dpi.required = pdef.isRequired();
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
            ReadableProperty propToExpose = getReadableProperty(propName);

            if (info != null) {
                propToExpose.initializeFrom(info, getExceptionInterceptor());
            }

            driverProperties[i++] = getAsDriverPropertyInfo(propToExpose);
        }

        return driverProperties;
    }

    protected Properties exposeAsProperties(Properties info) throws SQLException {
        if (info == null) {
            info = new Properties();
        }

        for (String propName : PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.keySet()) {
            ReadableProperty propToGet = getReadableProperty(propName);

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
     * @throws SQLException
     */
    protected void initializeFromRef(Reference ref) throws SQLException {

        for (String propName : PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.keySet()) {
            try {
                ReadableProperty propToSet = getReadableProperty(propName);

                if (ref != null) {
                    propToSet.initializeFrom(ref, getExceptionInterceptor());
                }
            } catch (Exception e) {
                throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
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
            infoCopy.remove(NonRegisteringDriver.USER_PROPERTY_KEY);
            infoCopy.remove(NonRegisteringDriver.PASSWORD_PROPERTY_KEY);
            infoCopy.remove(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
            infoCopy.remove(NonRegisteringDriver.PORT_PROPERTY_KEY);

            for (String propName : PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.keySet()) {
                try {
                    ReadableProperty propToSet = getReadableProperty(propName);
                    propToSet.initializeFrom(infoCopy, getExceptionInterceptor());

                } catch (CJException e) {
                    throw ExceptionFactory.createException(WrongArgumentException.class, e.getMessage(), e, getExceptionInterceptor());
                }
            }

            postInitialization();
        }
    }

    protected void postInitialization() {

        this.reconnectTxAtEndAsBoolean = getBooleanReadableProperty(PropertyDefinitions.PNAME_reconnectAtTxEnd).getValueAsBoolean();

        // Adjust max rows
        if (this.getMaxRows() == 0) {
            // adjust so that it will become MysqlDefs.MAX_ROWS in execSQL()
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_maxRows).setValue(Integer.valueOf(-1), getExceptionInterceptor());
        }

        //
        // Check character encoding
        //
        String testEncoding = this.getEncoding();

        if (testEncoding != null) {
            // Attempt to use the encoding, and bail out if it can't be used
            try {
                String testString = "abc";
                StringUtils.getBytes(testString, testEncoding);
            } catch (UnsupportedEncodingException e) {
                throw ExceptionFactory.createException(WrongArgumentException.class, e.getMessage(), e, getExceptionInterceptor());
            }
        }

        this.cacheResultSetMetaDataAsBoolean = getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheResultSetMetadata).getValueAsBoolean();
        this.useUnicodeAsBoolean = getBooleanReadableProperty(PropertyDefinitions.PNAME_useUnicode).getValueAsBoolean();
        this.characterEncodingAsString = getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getStringValue();
        this.highAvailabilityAsBoolean = getBooleanReadableProperty(PropertyDefinitions.PNAME_autoReconnect).getValueAsBoolean();
        this.autoReconnectForPoolsAsBoolean = getBooleanReadableProperty(PropertyDefinitions.PNAME_autoReconnectForPools).getValueAsBoolean();
        this.maxRowsAsInt = getIntegerReadableProperty(PropertyDefinitions.PNAME_maxRows).getIntValue();
        this.profileSQLAsBoolean = getBooleanReadableProperty(PropertyDefinitions.PNAME_profileSQL).getValueAsBoolean();
        this.useUsageAdvisorAsBoolean = getBooleanReadableProperty(PropertyDefinitions.PNAME_useUsageAdvisor).getValueAsBoolean();
        this.useOldUTF8BehaviorAsBoolean = getBooleanReadableProperty(PropertyDefinitions.PNAME_useOldUTF8Behavior).getValueAsBoolean();
        this.autoGenerateTestcaseScriptAsBoolean = getBooleanReadableProperty(PropertyDefinitions.PNAME_autoGenerateTestcaseScript).getValueAsBoolean();
        this.maintainTimeStatsAsBoolean = getBooleanReadableProperty(PropertyDefinitions.PNAME_maintainTimeStats).getValueAsBoolean();
        this.jdbcCompliantTruncationForReads = getJdbcCompliantTruncation();

        if (getUseCursorFetch()) {
            // assume they want to use server-side prepared statements because they're required for this functionality
            setDetectServerPreparedStmts(true);
        }
    }

    public boolean getAllowLoadLocalInfile() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_allowLoadLocalInfile).getValueAsBoolean();
    }

    public boolean getAllowMultiQueries() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_allowMultiQueries).getValueAsBoolean();
    }

    public boolean getAllowNanAndInf() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_allowNanAndInf).getValueAsBoolean();
    }

    public boolean getAllowUrlInLocalInfile() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_allowUrlInLocalInfile).getValueAsBoolean();
    }

    public boolean getAlwaysSendSetIsolation() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_alwaysSendSetIsolation).getValueAsBoolean();
    }

    public boolean getAutoDeserialize() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_autoDeserialize).getValueAsBoolean();
    }

    public boolean getAutoGenerateTestcaseScript() {
        return this.autoGenerateTestcaseScriptAsBoolean;
    }

    public boolean getAutoReconnectForPools() {
        return this.autoReconnectForPoolsAsBoolean;
    }

    public int getBlobSendChunkSize() {
        return getMemorySizeReadableProperty(PropertyDefinitions.PNAME_blobSendChunkSize).getIntValue();
    }

    public boolean getCacheCallableStatements() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheCallableStmts).getValueAsBoolean();
    }

    public boolean getCachePreparedStatements() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_cachePrepStmts).getValueAsBoolean();
    }

    public boolean getCacheResultSetMetadata() {
        return this.cacheResultSetMetaDataAsBoolean;
    }

    public boolean getCacheServerConfiguration() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheServerConfiguration).getValueAsBoolean();
    }

    public int getCallableStatementCacheSize() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_callableStmtCacheSize).getIntValue();
    }

    public boolean getCapitalizeTypeNames() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_capitalizeTypeNames).getValueAsBoolean();
    }

    public String getCharacterSetResults() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_characterSetResults).getStringValue();
    }

    public String getConnectionAttributes() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_connectionAttributes).getStringValue();
    }

    public void setConnectionAttributes(String val) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_connectionAttributes).setValue(val);
    }

    public boolean getClobberStreamingResults() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_clobberStreamingResults).getValueAsBoolean();
    }

    public String getClobCharacterEncoding() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_clobCharacterEncoding).getStringValue();
    }

    public String getConnectionCollation() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_connectionCollation).getStringValue();
    }

    public int getConnectTimeout() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_connectTimeout).getIntValue();
    }

    public boolean getContinueBatchOnError() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_continueBatchOnError).getValueAsBoolean();
    }

    public boolean getCreateDatabaseIfNotExist() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_createDatabaseIfNotExist).getValueAsBoolean();
    }

    public int getDefaultFetchSize() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_defaultFetchSize).getIntValue();
    }

    public boolean getDontTrackOpenResources() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_dontTrackOpenResources).getValueAsBoolean();
    }

    public boolean getDumpQueriesOnException() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_dumpQueriesOnException).getValueAsBoolean();
    }

    public boolean getDynamicCalendars() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_dynamicCalendars).getValueAsBoolean();
    }

    public boolean getElideSetAutoCommits() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_elideSetAutoCommits).getValueAsBoolean();
    }

    public boolean getEmptyStringsConvertToZero() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_emptyStringsConvertToZero).getValueAsBoolean();
    }

    public boolean getEmulateLocators() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_emulateLocators).getValueAsBoolean();
    }

    public boolean getEmulateUnsupportedPstmts() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_emulateUnsupportedPstmts).getValueAsBoolean();
    }

    public boolean getEnablePacketDebug() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_enablePacketDebug).getValueAsBoolean();
    }

    public boolean getExplainSlowQueries() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_explainSlowQueries).getValueAsBoolean();
    }

    public boolean getFailOverReadOnly() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_failOverReadOnly).getValueAsBoolean();
    }

    public boolean getGatherPerformanceMetrics() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_gatherPerfMetrics).getValueAsBoolean();
    }

    protected boolean getHighAvailability() {
        return this.highAvailabilityAsBoolean;
    }

    public boolean getHoldResultsOpenOverStatementClose() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_holdResultsOpenOverStatementClose).getValueAsBoolean();
    }

    public boolean getIgnoreNonTxTables() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_ignoreNonTxTables).getValueAsBoolean();
    }

    public int getInitialTimeout() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_initialTimeout).getIntValue();
    }

    public boolean getInteractiveClient() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_interactiveClient).getValueAsBoolean();
    }

    public boolean getIsInteractiveClient() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_interactiveClient).getValueAsBoolean();
    }

    public boolean getJdbcCompliantTruncation() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_jdbcCompliantTruncation).getValueAsBoolean();
    }

    public int getLocatorFetchBufferSize() {
        return getMemorySizeReadableProperty(PropertyDefinitions.PNAME_locatorFetchBufferSize).getIntValue();
    }

    public String getLogger() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_logger).getStringValue();
    }

    public String getLoggerClassName() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_logger).getStringValue();
    }

    public boolean getLogSlowQueries() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_logSlowQueries).getValueAsBoolean();
    }

    public boolean getMaintainTimeStats() {
        return this.maintainTimeStatsAsBoolean;
    }

    public int getMaxQuerySizeToLog() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_maxQuerySizeToLog).getIntValue();
    }

    public int getMaxReconnects() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_maxReconnects).getIntValue();
    }

    public int getMaxRows() {
        return this.maxRowsAsInt;
    }

    public int getMetadataCacheSize() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_metadataCacheSize).getIntValue();
    }

    public boolean getNoDatetimeStringSync() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_noDatetimeStringSync).getValueAsBoolean();
    }

    public boolean getNullCatalogMeansCurrent() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_nullCatalogMeansCurrent).getValueAsBoolean();
    }

    public boolean getNullNamePatternMatchesAll() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_nullNamePatternMatchesAll).getValueAsBoolean();
    }

    public int getPacketDebugBufferSize() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_packetDebugBufferSize).getIntValue();
    }

    public boolean getPedantic() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_pedantic).getValueAsBoolean();
    }

    public int getPreparedStatementCacheSize() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_prepStmtCacheSize).getIntValue();
    }

    public int getPreparedStatementCacheSqlLimit() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_prepStmtCacheSqlLimit).getIntValue();
    }

    public boolean getProfileSQL() {
        return this.profileSQLAsBoolean;
    }

    public String getPropertiesTransform() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_propertiesTransform).getStringValue();
    }

    public int getQueriesBeforeRetryMaster() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_queriesBeforeRetryMaster).getIntValue();
    }

    public boolean getReconnectAtTxEnd() {
        return this.reconnectTxAtEndAsBoolean;
    }

    public boolean getRelaxAutoCommit() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_relaxAutoCommit).getValueAsBoolean();
    }

    public int getReportMetricsIntervalMillis() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_reportMetricsIntervalMillis).getIntValue();
    }

    public boolean getRequireSSL() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_requireSSL).getValueAsBoolean();
    }

    public boolean getRollbackOnPooledClose() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_rollbackOnPooledClose).getValueAsBoolean();
    }

    public boolean getRoundRobinLoadBalance() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_roundRobinLoadBalance).getValueAsBoolean();
    }

    public boolean getRunningCTS13() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_runningCTS13).getValueAsBoolean();
    }

    public int getSecondsBeforeRetryMaster() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_secondsBeforeRetryMaster).getIntValue();
    }

    public String getServerTimezone() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_serverTimezone).getStringValue();
    }

    public String getSessionVariables() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_sessionVariables).getStringValue();
    }

    public int getSlowQueryThresholdMillis() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_slowQueryThresholdMillis).getIntValue();
    }

    public String getSocketFactoryClassName() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_socketFactory).getStringValue();
    }

    public int getSocketTimeout() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_socketTimeout).getIntValue();
    }

    public boolean getStrictFloatingPoint() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_strictFloatingPoint).getValueAsBoolean();
    }

    public boolean getStrictUpdates() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_strictUpdates).getValueAsBoolean();
    }

    public boolean getTinyInt1isBit() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_tinyInt1isBit).getValueAsBoolean();
    }

    public boolean getTraceProtocol() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_traceProtocol).getValueAsBoolean();
    }

    public boolean getTransformedBitIsBoolean() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_transformedBitIsBoolean).getValueAsBoolean();
    }

    public boolean getUseCompression() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useCompression).getValueAsBoolean();
    }

    public boolean getUseFastIntParsing() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useFastIntParsing).getValueAsBoolean();
    }

    public boolean getUseHostsInPrivileges() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useHostsInPrivileges).getValueAsBoolean();
    }

    public boolean getUseInformationSchema() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useInformationSchema).getValueAsBoolean();
    }

    public boolean getUseLocalSessionState() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useLocalSessionState).getValueAsBoolean();
    }

    public boolean getUseOldUTF8Behavior() {
        return this.useOldUTF8BehaviorAsBoolean;
    }

    public boolean getUseOnlyServerErrorMessages() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useOnlyServerErrorMessages).getValueAsBoolean();
    }

    public boolean getUseReadAheadInput() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useReadAheadInput).getValueAsBoolean();
    }

    public boolean getUseServerPreparedStmts() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useServerPrepStmts).getValueAsBoolean();
    }

    public boolean getUseSSL() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useSSL).getValueAsBoolean();
    }

    public boolean getUseStreamLengthsInPrepStmts() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useStreamLengthsInPrepStmts).getValueAsBoolean();
    }

    public boolean getUseTimezone() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useTimezone).getValueAsBoolean();
    }

    public boolean getUseUltraDevWorkAround() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_ultraDevHack).getValueAsBoolean();
    }

    public boolean getUseUsageAdvisor() {
        return this.useUsageAdvisorAsBoolean;
    }

    public boolean getYearIsDateType() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_yearIsDateType).getValueAsBoolean();
    }

    public String getZeroDateTimeBehavior() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_zeroDateTimeBehavior).getStringValue();
    }

    public void setAllowLoadLocalInfile(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_allowLoadLocalInfile).setValue(property);
    }

    public void setAllowMultiQueries(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_allowMultiQueries).setValue(property);
    }

    public void setAllowNanAndInf(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_allowNanAndInf).setValue(flag);
    }

    public void setAllowUrlInLocalInfile(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_allowUrlInLocalInfile).setValue(flag);
    }

    public void setAlwaysSendSetIsolation(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_alwaysSendSetIsolation).setValue(flag);
    }

    public void setAutoDeserialize(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_autoDeserialize).setValue(flag);
    }

    public void setAutoGenerateTestcaseScript(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_autoGenerateTestcaseScript).setValue(flag);
        this.autoGenerateTestcaseScriptAsBoolean = flag;
    }

    public void setAutoReconnect(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_autoReconnect).setValue(flag);
    }

    public void setAutoReconnectForConnectionPools(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_autoReconnectForPools).setValue(property);
        this.autoReconnectForPoolsAsBoolean = property;
    }

    public void setAutoReconnectForPools(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_autoReconnectForPools).setValue(flag);
    }

    public void setBlobSendChunkSize(String value) throws SQLException {
        try {
            getMemorySizeModifiableProperty(PropertyDefinitions.PNAME_blobSendChunkSize).setFromString(value, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setCacheCallableStatements(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_cacheCallableStmts).setValue(flag);
    }

    public void setCachePreparedStatements(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_cachePrepStmts).setValue(flag);
    }

    public void setCacheResultSetMetadata(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_cacheResultSetMetadata).setValue(property);
        this.cacheResultSetMetaDataAsBoolean = property;
    }

    public void setCacheServerConfiguration(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_cacheServerConfiguration).setValue(flag);
    }

    public void setCallableStatementCacheSize(int size) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_callableStmtCacheSize).setValue(size, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setCapitalizeDBMDTypes(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_capitalizeTypeNames).setValue(property);
    }

    public void setCapitalizeTypeNames(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_capitalizeTypeNames).setValue(flag);
    }

    public void setCharacterEncoding(String encoding) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_characterEncoding).setValue(encoding);
    }

    public void setCharacterSetResults(String characterSet) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_characterSetResults).setValue(characterSet);
    }

    public void setClobberStreamingResults(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_clobberStreamingResults).setValue(flag);
    }

    public void setClobCharacterEncoding(String encoding) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_clobCharacterEncoding).setValue(encoding);
    }

    public void setConnectionCollation(String collation) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_connectionCollation).setValue(collation);
    }

    public void setConnectTimeout(int timeoutMs) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_connectTimeout).setValue(timeoutMs, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setContinueBatchOnError(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_continueBatchOnError).setValue(property);
    }

    public void setCreateDatabaseIfNotExist(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_createDatabaseIfNotExist).setValue(flag);
    }

    public void setDefaultFetchSize(int n) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_defaultFetchSize).setValue(n, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setDetectServerPreparedStmts(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useServerPrepStmts).setValue(property);
    }

    public void setDontTrackOpenResources(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_dontTrackOpenResources).setValue(flag);
    }

    public void setDumpQueriesOnException(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_dumpQueriesOnException).setValue(flag);
    }

    public void setDynamicCalendars(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_dynamicCalendars).setValue(flag);
    }

    public void setElideSetAutoCommits(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_elideSetAutoCommits).setValue(flag);
    }

    public void setEmptyStringsConvertToZero(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_emptyStringsConvertToZero).setValue(flag);
    }

    public void setEmulateLocators(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_emulateLocators).setValue(property);
    }

    public void setEmulateUnsupportedPstmts(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_emulateUnsupportedPstmts).setValue(flag);
    }

    public void setEnablePacketDebug(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_enablePacketDebug).setValue(flag);
    }

    public void setEncoding(String property) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_characterEncoding).setValue(property);
        this.characterEncodingAsString = property;
    }

    public void setExplainSlowQueries(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_explainSlowQueries).setValue(flag);
    }

    public void setFailOverReadOnly(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_failOverReadOnly).setValue(flag);
    }

    public void setGatherPerformanceMetrics(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_gatherPerfMetrics).setValue(flag);
    }

    protected void setHighAvailability(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_autoReconnect).setValue(property);
        this.highAvailabilityAsBoolean = property;
    }

    public void setHoldResultsOpenOverStatementClose(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_holdResultsOpenOverStatementClose).setValue(flag);
    }

    public void setIgnoreNonTxTables(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_ignoreNonTxTables).setValue(property);
    }

    public void setInitialTimeout(int property) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_initialTimeout).setValue(property, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setIsInteractiveClient(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_interactiveClient).setValue(property);
    }

    public void setJdbcCompliantTruncation(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_jdbcCompliantTruncation).setValue(flag);
    }

    public void setLocatorFetchBufferSize(String value) throws SQLException {
        try {
            getMemorySizeModifiableProperty(PropertyDefinitions.PNAME_locatorFetchBufferSize).setFromString(value, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setLogger(String property) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_logger).setValue(property);
    }

    public void setLoggerClassName(String className) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_logger).setValue(className);
    }

    public void setLogSlowQueries(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_logSlowQueries).setValue(flag);
    }

    public void setMaintainTimeStats(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_maintainTimeStats).setValue(flag);
        this.maintainTimeStatsAsBoolean = flag;
    }

    public void setMaxQuerySizeToLog(int sizeInBytes) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_maxQuerySizeToLog).setValue(sizeInBytes, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setMaxReconnects(int property) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_maxReconnects).setValue(property, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setMaxRows(int property) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_maxRows).setValue(property, getExceptionInterceptor());
            this.maxRowsAsInt = getIntegerModifiableProperty(PropertyDefinitions.PNAME_maxRows).getIntValue();
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setMetadataCacheSize(int value) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_metadataCacheSize).setValue(value, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setNoDatetimeStringSync(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_noDatetimeStringSync).setValue(flag);
    }

    public void setNullCatalogMeansCurrent(boolean value) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_nullCatalogMeansCurrent).setValue(value);
    }

    public void setNullNamePatternMatchesAll(boolean value) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_nullNamePatternMatchesAll).setValue(value);
    }

    public void setPacketDebugBufferSize(int size) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_packetDebugBufferSize).setValue(size, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setPedantic(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_pedantic).setValue(property);
    }

    public void setPreparedStatementCacheSize(int cacheSize) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_prepStmtCacheSize).setValue(cacheSize, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setPreparedStatementCacheSqlLimit(int cacheSqlLimit) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_prepStmtCacheSqlLimit).setValue(cacheSqlLimit, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setProfileSQL(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_profileSQL).setValue(flag);
        this.profileSQLAsBoolean = flag;
    }

    public void setPropertiesTransform(String value) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_propertiesTransform).setValue(value);
    }

    public void setQueriesBeforeRetryMaster(int property) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_queriesBeforeRetryMaster).setValue(property, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setReconnectAtTxEnd(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_reconnectAtTxEnd).setValue(property);
        this.reconnectTxAtEndAsBoolean = property;
    }

    public void setRelaxAutoCommit(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_relaxAutoCommit).setValue(property);
    }

    public void setReportMetricsIntervalMillis(int millis) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_reportMetricsIntervalMillis).setValue(millis, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setRequireSSL(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_requireSSL).setValue(property);
    }

    public void setRollbackOnPooledClose(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_rollbackOnPooledClose).setValue(flag);
    }

    public void setRoundRobinLoadBalance(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_roundRobinLoadBalance).setValue(flag);
    }

    public void setRunningCTS13(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_runningCTS13).setValue(flag);
    }

    public void setSecondsBeforeRetryMaster(int property) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_secondsBeforeRetryMaster).setValue(property, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setServerTimezone(String property) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_serverTimezone).setValue(property);
    }

    public void setSessionVariables(String variables) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_sessionVariables).setValue(variables);
    }

    public void setSlowQueryThresholdMillis(int millis) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_slowQueryThresholdMillis).setValue(millis, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setSocketFactoryClassName(String property) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_socketFactory).setValue(property);
    }

    public void setSocketTimeout(int property) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_socketTimeout).setValue(property, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setStrictFloatingPoint(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_strictFloatingPoint).setValue(property);
    }

    public void setStrictUpdates(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_strictUpdates).setValue(property);
    }

    public void setTinyInt1isBit(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_tinyInt1isBit).setValue(flag);
    }

    public void setTraceProtocol(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_traceProtocol).setValue(flag);
    }

    public void setTransformedBitIsBoolean(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_transformedBitIsBoolean).setValue(flag);
    }

    public void setUseCompression(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useCompression).setValue(property);
    }

    public void setUseFastIntParsing(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useFastIntParsing).setValue(flag);
    }

    public void setUseHostsInPrivileges(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useHostsInPrivileges).setValue(property);
    }

    public void setUseInformationSchema(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useInformationSchema).setValue(flag);
    }

    public void setUseLocalSessionState(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useLocalSessionState).setValue(flag);
    }

    public void setUseOldUTF8Behavior(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useOldUTF8Behavior).setValue(flag);
        this.useOldUTF8BehaviorAsBoolean = flag;
    }

    public void setUseOnlyServerErrorMessages(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useOnlyServerErrorMessages).setValue(flag);
    }

    public void setUseReadAheadInput(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useReadAheadInput).setValue(flag);
    }

    public void setUseServerPreparedStmts(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useServerPrepStmts).setValue(flag);
    }

    public void setUseSSL(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useSSL).setValue(property);
    }

    public void setUseStreamLengthsInPrepStmts(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useStreamLengthsInPrepStmts).setValue(property);
    }

    public void setUseTimezone(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useTimezone).setValue(property);
    }

    public void setUseUltraDevWorkAround(boolean property) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_ultraDevHack).setValue(property);
    }

    public void setUseUnbufferedInput(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useUnbufferedInput).setValue(flag);
    }

    public void setUseUnicode(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useUnicode).setValue(flag);
        this.useUnicodeAsBoolean = flag;
    }

    public void setUseUsageAdvisor(boolean useUsageAdvisorFlag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useUsageAdvisor).setValue(useUsageAdvisorFlag);
        this.useUsageAdvisorAsBoolean = useUsageAdvisorFlag;
    }

    public void setYearIsDateType(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_yearIsDateType).setValue(flag);
    }

    public void setZeroDateTimeBehavior(String behavior) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_zeroDateTimeBehavior).setValue(behavior);
    }

    public boolean useUnbufferedInput() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useUnbufferedInput).getValueAsBoolean();
    }

    public boolean getUseCursorFetch() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useCursorFetch).getValueAsBoolean();
    }

    public void setUseCursorFetch(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useCursorFetch).setValue(flag);
    }

    public boolean getOverrideSupportsIntegrityEnhancementFacility() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_overrideSupportsIntegrityEnhancementFacility).getValueAsBoolean();
    }

    public void setOverrideSupportsIntegrityEnhancementFacility(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_overrideSupportsIntegrityEnhancementFacility).setValue(flag);
    }

    public boolean getNoTimezoneConversionForTimeType() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_noTimezoneConversionForTimeType).getValueAsBoolean();
    }

    public void setNoTimezoneConversionForTimeType(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_noTimezoneConversionForTimeType).setValue(flag);
    }

    public boolean getNoTimezoneConversionForDateType() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_noTimezoneConversionForDateType).getValueAsBoolean();
    }

    public void setNoTimezoneConversionForDateType(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_noTimezoneConversionForDateType).setValue(flag);
    }

    public boolean getCacheDefaultTimezone() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheDefaultTimezone).getValueAsBoolean();
    }

    public void setCacheDefaultTimezone(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_cacheDefaultTimezone).setValue(flag);
    }

    public boolean getUseJDBCCompliantTimezoneShift() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useJDBCCompliantTimezoneShift).getValueAsBoolean();
    }

    public void setUseJDBCCompliantTimezoneShift(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useJDBCCompliantTimezoneShift).setValue(flag);
    }

    public boolean getAutoClosePStmtStreams() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_autoClosePStmtStreams).getValueAsBoolean();
    }

    public void setAutoClosePStmtStreams(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_autoClosePStmtStreams).setValue(flag);
    }

    public boolean getProcessEscapeCodesForPrepStmts() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_processEscapeCodesForPrepStmts).getValueAsBoolean();
    }

    public void setProcessEscapeCodesForPrepStmts(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_processEscapeCodesForPrepStmts).setValue(flag);
    }

    public boolean getUseGmtMillisForDatetimes() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useGmtMillisForDatetimes).getValueAsBoolean();
    }

    public void setUseGmtMillisForDatetimes(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useGmtMillisForDatetimes).setValue(flag);
    }

    public boolean getDumpMetadataOnColumnNotFound() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_dumpMetadataOnColumnNotFound).getValueAsBoolean();
    }

    public void setDumpMetadataOnColumnNotFound(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_dumpMetadataOnColumnNotFound).setValue(flag);
    }

    public String getResourceId() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_resourceId).getStringValue();
    }

    public void setResourceId(String resourceId) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_resourceId).setValue(resourceId);
    }

    public boolean getRewriteBatchedStatements() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_rewriteBatchedStatements).getValueAsBoolean();
    }

    public void setRewriteBatchedStatements(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_rewriteBatchedStatements).setValue(flag);
    }

    public boolean getJdbcCompliantTruncationForReads() {
        return this.jdbcCompliantTruncationForReads;
    }

    public void setJdbcCompliantTruncationForReads(boolean jdbcCompliantTruncationForReads) {
        this.jdbcCompliantTruncationForReads = jdbcCompliantTruncationForReads;
    }

    public boolean getUseJvmCharsetConverters() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useJvmCharsetConverters).getValueAsBoolean();
    }

    public void setUseJvmCharsetConverters(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useJvmCharsetConverters).setValue(flag);
    }

    public boolean getPinGlobalTxToPhysicalConnection() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_pinGlobalTxToPhysicalConnection).getValueAsBoolean();
    }

    public void setPinGlobalTxToPhysicalConnection(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_pinGlobalTxToPhysicalConnection).setValue(flag);
    }

    /*
     * "Aliases" which match the property names to make using
     * from datasources easier.
     */

    public void setGatherPerfMetrics(boolean flag) {
        setGatherPerformanceMetrics(flag);
    }

    public boolean getGatherPerfMetrics() {
        return getGatherPerformanceMetrics();
    }

    public void setUltraDevHack(boolean flag) {
        setUseUltraDevWorkAround(flag);
    }

    public boolean getUltraDevHack() {
        return getUseUltraDevWorkAround();
    }

    public void setInteractiveClient(boolean property) {
        setIsInteractiveClient(property);
    }

    public void setSocketFactory(String name) {
        setSocketFactoryClassName(name);
    }

    public String getSocketFactory() {
        return getSocketFactoryClassName();
    }

    public void setUseServerPrepStmts(boolean flag) {
        setUseServerPreparedStmts(flag);
    }

    public boolean getUseServerPrepStmts() {
        return getUseServerPreparedStmts();
    }

    public void setCacheCallableStmts(boolean flag) {
        setCacheCallableStatements(flag);
    }

    public boolean getCacheCallableStmts() {
        return getCacheCallableStatements();
    }

    public void setCachePrepStmts(boolean flag) {
        setCachePreparedStatements(flag);
    }

    public boolean getCachePrepStmts() {
        return getCachePreparedStatements();
    }

    public void setCallableStmtCacheSize(int cacheSize) throws SQLException {
        setCallableStatementCacheSize(cacheSize);
    }

    public int getCallableStmtCacheSize() {
        return getCallableStatementCacheSize();
    }

    public void setPrepStmtCacheSize(int cacheSize) throws SQLException {
        setPreparedStatementCacheSize(cacheSize);
    }

    public int getPrepStmtCacheSize() {
        return getPreparedStatementCacheSize();
    }

    public void setPrepStmtCacheSqlLimit(int sqlLimit) throws SQLException {
        setPreparedStatementCacheSqlLimit(sqlLimit);
    }

    public int getPrepStmtCacheSqlLimit() {
        return getPreparedStatementCacheSqlLimit();
    }

    public boolean getNoAccessToProcedureBodies() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_noAccessToProcedureBodies).getValueAsBoolean();
    }

    public void setNoAccessToProcedureBodies(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_noAccessToProcedureBodies).setValue(flag);
    }

    public boolean getUseOldAliasMetadataBehavior() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useOldAliasMetadataBehavior).getValueAsBoolean();
    }

    public void setUseOldAliasMetadataBehavior(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useOldAliasMetadataBehavior).setValue(flag);
    }

    public boolean getUseSSPSCompatibleTimezoneShift() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useSSPSCompatibleTimezoneShift).getValueAsBoolean();
    }

    public void setUseSSPSCompatibleTimezoneShift(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useSSPSCompatibleTimezoneShift).setValue(flag);
    }

    public boolean getTreatUtilDateAsTimestamp() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_treatUtilDateAsTimestamp).getValueAsBoolean();
    }

    public void setTreatUtilDateAsTimestamp(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_treatUtilDateAsTimestamp).setValue(flag);
    }

    public boolean getUseFastDateParsing() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useFastDateParsing).getValueAsBoolean();
    }

    public void setUseFastDateParsing(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useFastDateParsing).setValue(flag);
    }

    public String getLocalSocketAddress() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_localSocketAddress).getStringValue();
    }

    public void setLocalSocketAddress(String address) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_localSocketAddress).setValue(address);
    }

    public void setUseConfigs(String configs) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_useConfigs).setValue(configs);
    }

    public String getUseConfigs() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_useConfigs).getStringValue();
    }

    public boolean getGenerateSimpleParameterMetadata() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_generateSimpleParameterMetadata).getValueAsBoolean();
    }

    public void setGenerateSimpleParameterMetadata(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_generateSimpleParameterMetadata).setValue(flag);
    }

    public boolean getLogXaCommands() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_logXaCommands).getValueAsBoolean();
    }

    public void setLogXaCommands(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_logXaCommands).setValue(flag);
    }

    public int getResultSetSizeThreshold() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_resultSetSizeThreshold).getIntValue();
    }

    public void setResultSetSizeThreshold(int threshold) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_resultSetSizeThreshold).setValue(threshold, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public int getNetTimeoutForStreamingResults() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_netTimeoutForStreamingResults).getIntValue();
    }

    public void setNetTimeoutForStreamingResults(int value) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_netTimeoutForStreamingResults).setValue(value, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public boolean getEnableQueryTimeouts() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_enableQueryTimeouts).getValueAsBoolean();
    }

    public void setEnableQueryTimeouts(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_enableQueryTimeouts).setValue(flag);
    }

    public boolean getPadCharsWithSpace() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_padCharsWithSpace).getValueAsBoolean();
    }

    public void setPadCharsWithSpace(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_padCharsWithSpace).setValue(flag);
    }

    public boolean getUseDynamicCharsetInfo() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useDynamicCharsetInfo).getValueAsBoolean();
    }

    public void setUseDynamicCharsetInfo(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useDynamicCharsetInfo).setValue(flag);
    }

    public String getClientInfoProvider() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_clientInfoProvider).getStringValue();
    }

    public void setClientInfoProvider(String classname) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_clientInfoProvider).setValue(classname);
    }

    public boolean getPopulateInsertRowWithDefaultValues() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_populateInsertRowWithDefaultValues).getValueAsBoolean();
    }

    public void setPopulateInsertRowWithDefaultValues(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_populateInsertRowWithDefaultValues).setValue(flag);
    }

    public String getLoadBalanceStrategy() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceStrategy).getStringValue();
    }

    public void setLoadBalanceStrategy(String strategy) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_loadBalanceStrategy).setValue(strategy);
    }

    public boolean getTcpNoDelay() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_tcpNoDelay).getValueAsBoolean();
    }

    public void setTcpNoDelay(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_tcpNoDelay).setValue(flag);
    }

    public boolean getTcpKeepAlive() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_tcpKeepAlive).getValueAsBoolean();
    }

    public void setTcpKeepAlive(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_tcpKeepAlive).setValue(flag);
    }

    public int getTcpRcvBuf() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_tcpRcvBuf).getIntValue();
    }

    public void setTcpRcvBuf(int bufSize) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_tcpRcvBuf).setValue(bufSize, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public int getTcpSndBuf() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_tcpSndBuf).getIntValue();
    }

    public void setTcpSndBuf(int bufSize) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_tcpSndBuf).setValue(bufSize, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public int getTcpTrafficClass() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_tcpTrafficClass).getIntValue();
    }

    public void setTcpTrafficClass(int classFlags) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_tcpTrafficClass).setValue(classFlags, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public boolean getUseNanosForElapsedTime() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useNanosForElapsedTime).getValueAsBoolean();
    }

    public void setUseNanosForElapsedTime(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useNanosForElapsedTime).setValue(flag);
    }

    public long getSlowQueryThresholdNanos() {
        return getLongReadableProperty(PropertyDefinitions.PNAME_slowQueryThresholdNanos).getLongValue();
    }

    public void setSlowQueryThresholdNanos(long nanos) throws SQLException {
        try {
            getLongModifiableProperty(PropertyDefinitions.PNAME_slowQueryThresholdNanos).setValue(nanos, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public String getStatementInterceptors() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_statementInterceptors).getStringValue();
    }

    public void setStatementInterceptors(String value) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_statementInterceptors).setValue(value);
    }

    public boolean getUseDirectRowUnpack() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useDirectRowUnpack).getValueAsBoolean();
    }

    public void setUseDirectRowUnpack(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useDirectRowUnpack).setValue(flag);
    }

    public String getLargeRowSizeThreshold() {
        return getMemorySizeReadableProperty(PropertyDefinitions.PNAME_largeRowSizeThreshold).getStringValue();
    }

    public void setLargeRowSizeThreshold(String value) throws SQLException {
        try {
            getMemorySizeModifiableProperty(PropertyDefinitions.PNAME_largeRowSizeThreshold).setFromString(value, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public boolean getUseBlobToStoreUTF8OutsideBMP() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useBlobToStoreUTF8OutsideBMP).getValueAsBoolean();
    }

    public void setUseBlobToStoreUTF8OutsideBMP(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useBlobToStoreUTF8OutsideBMP).setValue(flag);
    }

    public String getUtf8OutsideBmpExcludedColumnNamePattern() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_utf8OutsideBmpExcludedColumnNamePattern).getStringValue();
    }

    public void setUtf8OutsideBmpExcludedColumnNamePattern(String regexPattern) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_utf8OutsideBmpExcludedColumnNamePattern).setValue(regexPattern);
    }

    public String getUtf8OutsideBmpIncludedColumnNamePattern() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_utf8OutsideBmpIncludedColumnNamePattern).getStringValue();
    }

    public void setUtf8OutsideBmpIncludedColumnNamePattern(String regexPattern) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_utf8OutsideBmpIncludedColumnNamePattern).setValue(regexPattern);
    }

    public boolean getIncludeInnodbStatusInDeadlockExceptions() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_includeInnodbStatusInDeadlockExceptions).getValueAsBoolean();
    }

    public void setIncludeInnodbStatusInDeadlockExceptions(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_includeInnodbStatusInDeadlockExceptions).setValue(flag);
    }

    public boolean getBlobsAreStrings() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_blobsAreStrings).getValueAsBoolean();
    }

    public void setBlobsAreStrings(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_blobsAreStrings).setValue(flag);
    }

    public boolean getFunctionsNeverReturnBlobs() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_functionsNeverReturnBlobs).getValueAsBoolean();
    }

    public void setFunctionsNeverReturnBlobs(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_functionsNeverReturnBlobs).setValue(flag);
    }

    public boolean getAutoSlowLog() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_autoSlowLog).getValueAsBoolean();
    }

    public void setAutoSlowLog(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_autoSlowLog).setValue(flag);
    }

    public String getConnectionLifecycleInterceptors() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_connectionLifecycleInterceptors).getStringValue();
    }

    public void setConnectionLifecycleInterceptors(String interceptors) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_connectionLifecycleInterceptors).setValue(interceptors);
    }

    public boolean getUseLegacyDatetimeCode() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useLegacyDatetimeCode).getValueAsBoolean();
    }

    public void setUseLegacyDatetimeCode(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useLegacyDatetimeCode).setValue(flag);
    }

    public int getSelfDestructOnPingSecondsLifetime() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_selfDestructOnPingSecondsLifetime).getIntValue();
    }

    public void setSelfDestructOnPingSecondsLifetime(int seconds) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_selfDestructOnPingSecondsLifetime).setValue(seconds, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public int getSelfDestructOnPingMaxOperations() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_selfDestructOnPingMaxOperations).getIntValue();
    }

    public void setSelfDestructOnPingMaxOperations(int maxOperations) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_selfDestructOnPingMaxOperations).setValue(maxOperations, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public boolean getUseColumnNamesInFindColumn() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useColumnNamesInFindColumn).getValueAsBoolean();
    }

    public void setUseColumnNamesInFindColumn(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useColumnNamesInFindColumn).setValue(flag);
    }

    public boolean getUseLocalTransactionState() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useLocalTransactionState).getValueAsBoolean();
    }

    public void setUseLocalTransactionState(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useLocalTransactionState).setValue(flag);
    }

    public boolean getCompensateOnDuplicateKeyUpdateCounts() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_compensateOnDuplicateKeyUpdateCounts).getValueAsBoolean();
    }

    public void setCompensateOnDuplicateKeyUpdateCounts(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_compensateOnDuplicateKeyUpdateCounts).setValue(flag);
    }

    public int getLoadBalanceBlacklistTimeout() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_loadBalanceBlacklistTimeout).getIntValue();
    }

    public void setLoadBalanceBlacklistTimeout(int loadBalanceBlacklistTimeout) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_loadBalanceBlacklistTimeout)
                    .setValue(loadBalanceBlacklistTimeout, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public int getLoadBalancePingTimeout() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_loadBalancePingTimeout).getIntValue();
    }

    public void setLoadBalancePingTimeout(int loadBalancePingTimeout) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_loadBalancePingTimeout).setValue(loadBalancePingTimeout, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public void setRetriesAllDown(int retriesAllDown) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_retriesAllDown).setValue(retriesAllDown, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public int getRetriesAllDown() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_retriesAllDown).getIntValue();
    }

    public void setUseAffectedRows(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_useAffectedRows).setValue(flag);
    }

    public boolean getUseAffectedRows() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_useAffectedRows).getValueAsBoolean();
    }

    public void setExceptionInterceptors(String exceptionInterceptors) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_exceptionInterceptors).setValue(exceptionInterceptors);
    }

    public String getExceptionInterceptors() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_exceptionInterceptors).getStringValue();
    }

    public void setMaxAllowedPacket(int max) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_maxAllowedPacket).setValue(max, getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public int getMaxAllowedPacket() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_maxAllowedPacket).getIntValue();
    }

    public boolean getQueryTimeoutKillsConnection() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_queryTimeoutKillsConnection).getValueAsBoolean();
    }

    public void setQueryTimeoutKillsConnection(boolean queryTimeoutKillsConnection) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_queryTimeoutKillsConnection).setValue(queryTimeoutKillsConnection);
    }

    public boolean getLoadBalanceValidateConnectionOnSwapServer() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_loadBalanceValidateConnectionOnSwapServer).getValueAsBoolean();
    }

    public void setLoadBalanceValidateConnectionOnSwapServer(boolean loadBalanceValidateConnectionOnSwapServer) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_loadBalanceValidateConnectionOnSwapServer).setValue(loadBalanceValidateConnectionOnSwapServer);

    }

    public String getLoadBalanceConnectionGroup() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceConnectionGroup).getStringValue();
    }

    public void setLoadBalanceConnectionGroup(String loadBalanceConnectionGroup) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_loadBalanceConnectionGroup).setValue(loadBalanceConnectionGroup);
    }

    public String getLoadBalanceExceptionChecker() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceExceptionChecker).getStringValue();
    }

    public void setLoadBalanceExceptionChecker(String loadBalanceExceptionChecker) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_loadBalanceExceptionChecker).setValue(loadBalanceExceptionChecker);
    }

    public String getLoadBalanceSQLStateFailover() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceSQLStateFailover).getStringValue();
    }

    public void setLoadBalanceSQLStateFailover(String loadBalanceSQLStateFailover) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_loadBalanceSQLStateFailover).setValue(loadBalanceSQLStateFailover);
    }

    public String getLoadBalanceSQLExceptionSubclassFailover() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceSQLExceptionSubclassFailover).getStringValue();
    }

    public void setLoadBalanceSQLExceptionSubclassFailover(String loadBalanceSQLExceptionSubclassFailover) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_loadBalanceSQLExceptionSubclassFailover).setValue(loadBalanceSQLExceptionSubclassFailover);
    }

    public boolean getLoadBalanceEnableJMX() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_loadBalanceEnableJMX).getValueAsBoolean();
    }

    public void setLoadBalanceEnableJMX(boolean loadBalanceEnableJMX) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_loadBalanceEnableJMX).setValue(loadBalanceEnableJMX);
    }

    public void setLoadBalanceAutoCommitStatementThreshold(int loadBalanceAutoCommitStatementThreshold) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementThreshold).setValue(loadBalanceAutoCommitStatementThreshold,
                    getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public int getLoadBalanceAutoCommitStatementThreshold() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementThreshold).getIntValue();
    }

    public void setLoadBalanceAutoCommitStatementRegex(String loadBalanceAutoCommitStatementRegex) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementRegex).setValue(loadBalanceAutoCommitStatementRegex);
    }

    public String getLoadBalanceAutoCommitStatementRegex() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementRegex).getStringValue();
    }

    public void setIncludeThreadDumpInDeadlockExceptions(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_includeThreadDumpInDeadlockExceptions).setValue(flag);
    }

    public boolean getIncludeThreadDumpInDeadlockExceptions() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_includeThreadDumpInDeadlockExceptions).getValueAsBoolean();
    }

    public void setIncludeThreadNamesAsStatementComment(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_includeThreadNamesAsStatementComment).setValue(flag);
    }

    public boolean getIncludeThreadNamesAsStatementComment() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_includeThreadNamesAsStatementComment).getValueAsBoolean();
    }

    public void setAuthenticationPlugins(String authenticationPlugins) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_authenticationPlugins).setValue(authenticationPlugins);
    }

    public String getAuthenticationPlugins() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_authenticationPlugins).getStringValue();
    }

    public void setDisabledAuthenticationPlugins(String disabledAuthenticationPlugins) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_disabledAuthenticationPlugins).setValue(disabledAuthenticationPlugins);
    }

    public String getDisabledAuthenticationPlugins() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_disabledAuthenticationPlugins).getStringValue();
    }

    public void setDefaultAuthenticationPlugin(String defaultAuthenticationPlugin) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_defaultAuthenticationPlugin).setValue(defaultAuthenticationPlugin);

    }

    public String getDefaultAuthenticationPlugin() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_defaultAuthenticationPlugin).getStringValue();
    }

    public void setParseInfoCacheFactory(String factoryClassname) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_parseInfoCacheFactory).setValue(factoryClassname);
    }

    public String getParseInfoCacheFactory() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_parseInfoCacheFactory).getStringValue();
    }

    public void setServerConfigCacheFactory(String factoryClassname) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_serverConfigCacheFactory).setValue(factoryClassname);
    }

    public String getServerConfigCacheFactory() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_serverConfigCacheFactory).getStringValue();
    }

    public void setDisconnectOnExpiredPasswords(boolean disconnectOnExpiredPasswords) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_disconnectOnExpiredPasswords).setValue(disconnectOnExpiredPasswords);
    }

    public boolean getDisconnectOnExpiredPasswords() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_disconnectOnExpiredPasswords).getValueAsBoolean();
    }

    public boolean getAllowMasterDownConnections() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_allowMasterDownConnections).getValueAsBoolean();
    }

    public void setAllowMasterDownConnections(boolean connectIfMasterDown) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_allowMasterDownConnections).setValue(connectIfMasterDown);
    }

    public boolean getReplicationEnableJMX() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_replicationEnableJMX).getValueAsBoolean();
    }

    public void setReplicationEnableJMX(boolean replicationEnableJMX) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_replicationEnableJMX).setValue(replicationEnableJMX);

    }

    public void setGetProceduresReturnsFunctions(boolean getProcedureReturnsFunctions) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_getProceduresReturnsFunctions).setValue(getProcedureReturnsFunctions);
    }

    public boolean getGetProceduresReturnsFunctions() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_getProceduresReturnsFunctions).getValueAsBoolean();
    }

    public void setDetectCustomCollations(boolean detectCustomCollations) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_detectCustomCollations).setValue(detectCustomCollations);
    }

    public boolean getDetectCustomCollations() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_detectCustomCollations).getValueAsBoolean();
    }

    public void setServerRSAPublicKeyFile(String serverRSAPublicKeyFile) throws SQLException {
        if (getStringModifiableProperty(PropertyDefinitions.PNAME_serverRSAPublicKeyFile).getUpdateCount() > 0) {
            throw SQLError.createSQLException(
                    Messages.getString("ConnectionProperties.dynamicChangeIsNotAllowed", new Object[] { "'serverRSAPublicKeyFile'" }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }
        getStringModifiableProperty(PropertyDefinitions.PNAME_serverRSAPublicKeyFile).setValue(serverRSAPublicKeyFile);
    }

    public void setAllowPublicKeyRetrieval(boolean allowPublicKeyRetrieval) throws SQLException {
        if ((getReadableProperty(PropertyDefinitions.PNAME_allowPublicKeyRetrieval)).getUpdateCount() > 0) {
            throw SQLError.createSQLException(
                    Messages.getString("ConnectionProperties.dynamicChangeIsNotAllowed", new Object[] { "'allowPublicKeyRetrieval'" }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_allowPublicKeyRetrieval).setValue(allowPublicKeyRetrieval);
    }

    public void setDontCheckOnDuplicateKeyUpdateInSQL(boolean dontCheckOnDuplicateKeyUpdateInSQL) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_dontCheckOnDuplicateKeyUpdateInSQL).setValue(dontCheckOnDuplicateKeyUpdateInSQL);
    }

    public boolean getDontCheckOnDuplicateKeyUpdateInSQL() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_dontCheckOnDuplicateKeyUpdateInSQL).getValueAsBoolean();
    }

    public void setSocksProxyHost(String socksProxyHost) {
        getStringModifiableProperty(PropertyDefinitions.PNAME_socksProxyHost).setValue(socksProxyHost);
    }

    public String getSocksProxyHost() {
        return getStringReadableProperty(PropertyDefinitions.PNAME_socksProxyHost).getStringValue();
    }

    public void setSocksProxyPort(int socksProxyPort) throws SQLException {
        try {
            getIntegerModifiableProperty(PropertyDefinitions.PNAME_socksProxyPort).setValue(socksProxyPort, null);
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e, getExceptionInterceptor());
        }
    }

    public int getSocksProxyPort() {
        return getIntegerReadableProperty(PropertyDefinitions.PNAME_socksProxyPort).getIntValue();
    }

    public boolean getReadOnlyPropagatesToServer() {
        return getBooleanReadableProperty(PropertyDefinitions.PNAME_readOnlyPropagatesToServer).getValueAsBoolean();
    }

    public void setReadOnlyPropagatesToServer(boolean flag) {
        getBooleanModifiableProperty(PropertyDefinitions.PNAME_readOnlyPropagatesToServer).setValue(flag);
    }
}
