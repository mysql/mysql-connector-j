/*
 * Copyright (c) 2018, 2019, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.conf;

import java.util.Map;
import java.util.TreeMap;

/**
 * PropertyKey handles connection property names, their camel-case aliases and case sensitivity.
 */
public enum PropertyKey {
    /*
     * Properties individually managed after parsing connection string. These property keys are case insensitive.
     */
    /** The database user name. */
    USER("user", false),
    /** The database user password. */
    PASSWORD("password", false),
    /** The hostname value from the properties instance passed to the driver. */
    HOST("host", false),
    /** The port number value from the properties instance passed to the driver. */
    PORT("port", false),
    /** The communications protocol. Possible values: "tcp" and "pipe". */
    PROTOCOL("protocol", false),
    /** The name pipes path to use when "protocol=pipe'. */
    PATH("path", "namedPipePath", false),
    /** The server type in a replication setup. Possible values: "master" and "slave". */
    TYPE("type", false),
    /** The address value ("host:port") from the properties instance passed to the driver. */
    ADDRESS("address", false),
    /** The host priority in a list of hosts. */
    PRIORITY("priority", false),
    /** The database value from the properties instance passed to the driver. */
    DBNAME("dbname", false), //

    allowLoadLocalInfile("allowLoadLocalInfile", true), //
    allowMasterDownConnections("allowMasterDownConnections", true), //
    allowMultiQueries("allowMultiQueries", true), //
    allowNanAndInf("allowNanAndInf", true), //
    allowPublicKeyRetrieval("allowPublicKeyRetrieval", true), //
    allowSlaveDownConnections("allowSlaveDownConnections", true), //
    allowUrlInLocalInfile("allowUrlInLocalInfile", true), //
    alwaysSendSetIsolation("alwaysSendSetIsolation", true), //
    authenticationPlugins("authenticationPlugins", true), //
    autoClosePStmtStreams("autoClosePStmtStreams", true), //
    autoDeserialize("autoDeserialize", true), //
    autoGenerateTestcaseScript("autoGenerateTestcaseScript", true), //
    autoReconnect("autoReconnect", true), //
    autoReconnectForPools("autoReconnectForPools", true), //
    autoSlowLog("autoSlowLog", true), //
    blobsAreStrings("blobsAreStrings", true), //
    blobSendChunkSize("blobSendChunkSize", true), //
    cacheCallableStmts("cacheCallableStmts", true), //
    cachePrepStmts("cachePrepStmts", true), //
    cacheResultSetMetadata("cacheResultSetMetadata", true), //
    cacheServerConfiguration("cacheServerConfiguration", true), //
    callableStmtCacheSize("callableStmtCacheSize", true), //
    characterEncoding("characterEncoding", true), //
    characterSetResults("characterSetResults", true), //
    clientCertificateKeyStorePassword("clientCertificateKeyStorePassword", true), //
    clientCertificateKeyStoreType("clientCertificateKeyStoreType", true), //
    clientCertificateKeyStoreUrl("clientCertificateKeyStoreUrl", true), //
    clientInfoProvider("clientInfoProvider", true), //
    clobberStreamingResults("clobberStreamingResults", true), //
    clobCharacterEncoding("clobCharacterEncoding", true), //
    compensateOnDuplicateKeyUpdateCounts("compensateOnDuplicateKeyUpdateCounts", true), //
    connectionAttributes("connectionAttributes", true), //
    connectionCollation("connectionCollation", true), //
    connectionLifecycleInterceptors("connectionLifecycleInterceptors", true), //
    connectTimeout("connectTimeout", true), //
    continueBatchOnError("continueBatchOnError", true), //
    createDatabaseIfNotExist("createDatabaseIfNotExist", true), //
    databaseTerm("databaseTerm", true), //
    defaultAuthenticationPlugin("defaultAuthenticationPlugin", true), //
    defaultFetchSize("defaultFetchSize", true), //
    detectCustomCollations("detectCustomCollations", true), //
    disabledAuthenticationPlugins("disabledAuthenticationPlugins", true), //
    disconnectOnExpiredPasswords("disconnectOnExpiredPasswords", true), //
    dontCheckOnDuplicateKeyUpdateInSQL("dontCheckOnDuplicateKeyUpdateInSQL", true), //
    dontTrackOpenResources("dontTrackOpenResources", true), //
    dumpQueriesOnException("dumpQueriesOnException", true), //
    elideSetAutoCommits("elideSetAutoCommits", true), //
    emptyStringsConvertToZero("emptyStringsConvertToZero", true), //
    emulateLocators("emulateLocators", true), //
    emulateUnsupportedPstmts("emulateUnsupportedPstmts", true), //
    enabledSSLCipherSuites("enabledSSLCipherSuites", true), //
    enabledTLSProtocols("enabledTLSProtocols", true), //
    enableEscapeProcessing("enableEscapeProcessing", true), //
    enablePacketDebug("enablePacketDebug", true), //
    enableQueryTimeouts("enableQueryTimeouts", true), //
    exceptionInterceptors("exceptionInterceptors", true), //
    explainSlowQueries("explainSlowQueries", true), //
    failOverReadOnly("failOverReadOnly", true), //
    functionsNeverReturnBlobs("functionsNeverReturnBlobs", true), //
    gatherPerfMetrics("gatherPerfMetrics", true), //
    generateSimpleParameterMetadata("generateSimpleParameterMetadata", true), //
    getProceduresReturnsFunctions("getProceduresReturnsFunctions", true), //
    holdResultsOpenOverStatementClose("holdResultsOpenOverStatementClose", true), //
    ha_enableJMX("ha.enableJMX", "haEnableJMX", true), //
    ha_loadBalanceStrategy("ha.loadBalanceStrategy", "haLoadBalanceStrategy", true), //
    ignoreNonTxTables("ignoreNonTxTables", true), //
    includeInnodbStatusInDeadlockExceptions("includeInnodbStatusInDeadlockExceptions", true), //
    includeThreadDumpInDeadlockExceptions("includeThreadDumpInDeadlockExceptions", true), //
    includeThreadNamesAsStatementComment("includeThreadNamesAsStatementComment", true), //
    initialTimeout("initialTimeout", true), //
    interactiveClient("interactiveClient", true), //
    jdbcCompliantTruncation("jdbcCompliantTruncation", true), //
    largeRowSizeThreshold("largeRowSizeThreshold", true), //
    loadBalanceAutoCommitStatementRegex("loadBalanceAutoCommitStatementRegex", true), //
    loadBalanceAutoCommitStatementThreshold("loadBalanceAutoCommitStatementThreshold", true), //
    loadBalanceBlacklistTimeout("loadBalanceBlacklistTimeout", true), //
    loadBalanceConnectionGroup("loadBalanceConnectionGroup", true), //
    loadBalanceExceptionChecker("loadBalanceExceptionChecker", true), //
    loadBalanceHostRemovalGracePeriod("loadBalanceHostRemovalGracePeriod", true), //
    loadBalancePingTimeout("loadBalancePingTimeout", true), //
    loadBalanceSQLStateFailover("loadBalanceSQLStateFailover", true), //
    loadBalanceSQLExceptionSubclassFailover("loadBalanceSQLExceptionSubclassFailover", true), //
    loadBalanceValidateConnectionOnSwapServer("loadBalanceValidateConnectionOnSwapServer", true), //
    localSocketAddress("localSocketAddress", true), //
    locatorFetchBufferSize("locatorFetchBufferSize", true), //
    logger("logger", true), //
    logSlowQueries("logSlowQueries", true), //
    logXaCommands("logXaCommands", true), //
    maintainTimeStats("maintainTimeStats", true), //
    maxAllowedPacket("maxAllowedPacket", true), //
    maxQuerySizeToLog("maxQuerySizeToLog", true), //
    maxReconnects("maxReconnects", true), //
    maxRows("maxRows", true), //
    metadataCacheSize("metadataCacheSize", true), //
    netTimeoutForStreamingResults("netTimeoutForStreamingResults", true), //
    noAccessToProcedureBodies("noAccessToProcedureBodies", true), //
    noDatetimeStringSync("noDatetimeStringSync", true), //
    nullDatabaseMeansCurrent("nullDatabaseMeansCurrent", "nullCatalogMeansCurrent", true), //
    overrideSupportsIntegrityEnhancementFacility("overrideSupportsIntegrityEnhancementFacility", true), //
    packetDebugBufferSize("packetDebugBufferSize", true), //
    padCharsWithSpace("padCharsWithSpace", true), //
    paranoid("paranoid", false), //
    parseInfoCacheFactory("parseInfoCacheFactory", true), //
    passwordCharacterEncoding("passwordCharacterEncoding", true), //
    pedantic("pedantic", true), //
    pinGlobalTxToPhysicalConnection("pinGlobalTxToPhysicalConnection", true), //
    populateInsertRowWithDefaultValues("populateInsertRowWithDefaultValues", true), //
    prepStmtCacheSize("prepStmtCacheSize", true), //
    prepStmtCacheSqlLimit("prepStmtCacheSqlLimit", true), //
    processEscapeCodesForPrepStmts("processEscapeCodesForPrepStmts", true), //
    profilerEventHandler("profilerEventHandler", true), //
    profileSQL("profileSQL", true), //
    propertiesTransform("propertiesTransform", true), //
    queriesBeforeRetryMaster("queriesBeforeRetryMaster", true), //
    queryInterceptors("queryInterceptors", true), //
    queryTimeoutKillsConnection("queryTimeoutKillsConnection", true), //
    readFromMasterWhenNoSlaves("readFromMasterWhenNoSlaves", true), //
    readOnlyPropagatesToServer("readOnlyPropagatesToServer", true), //
    reconnectAtTxEnd("reconnectAtTxEnd", true), //
    replicationConnectionGroup("replicationConnectionGroup", true), //
    reportMetricsIntervalMillis("reportMetricsIntervalMillis", true), //
    requireSSL("requireSSL", true), //
    resourceId("resourceId", true), //
    resultSetSizeThreshold("resultSetSizeThreshold", true), //
    retriesAllDown("retriesAllDown", true), //
    rewriteBatchedStatements("rewriteBatchedStatements", true), //
    rollbackOnPooledClose("rollbackOnPooledClose", true), //
    secondsBeforeRetryMaster("secondsBeforeRetryMaster", true), //
    selfDestructOnPingMaxOperations("selfDestructOnPingMaxOperations", true), //
    selfDestructOnPingSecondsLifetime("selfDestructOnPingSecondsLifetime", true), //
    sendFractionalSeconds("sendFractionalSeconds", true), //
    serverAffinityOrder("serverAffinityOrder", true), //
    serverConfigCacheFactory("serverConfigCacheFactory", true), //
    serverRSAPublicKeyFile("serverRSAPublicKeyFile", true), //
    serverTimezone("serverTimezone", true), //
    sessionVariables("sessionVariables", true), //
    slowQueryThresholdMillis("slowQueryThresholdMillis", true), //
    slowQueryThresholdNanos("slowQueryThresholdNanos", true), //
    socketFactory("socketFactory", true), //
    socketTimeout("socketTimeout", true), //
    socksProxyHost("socksProxyHost", true), //
    socksProxyPort("socksProxyPort", true), //
    sslMode("sslMode", true), //
    strictUpdates("strictUpdates", true), //
    tcpKeepAlive("tcpKeepAlive", true), //
    tcpNoDelay("tcpNoDelay", true), //
    tcpRcvBuf("tcpRcvBuf", true), //
    tcpSndBuf("tcpSndBuf", true), //
    tcpTrafficClass("tcpTrafficClass", true), //
    tinyInt1isBit("tinyInt1isBit", true), //
    traceProtocol("traceProtocol", true), //
    transformedBitIsBoolean("transformedBitIsBoolean", true), //
    treatUtilDateAsTimestamp("treatUtilDateAsTimestamp", true), //
    trustCertificateKeyStorePassword("trustCertificateKeyStorePassword", true), //
    trustCertificateKeyStoreType("trustCertificateKeyStoreType", true), //
    trustCertificateKeyStoreUrl("trustCertificateKeyStoreUrl", true), //
    ultraDevHack("ultraDevHack", true), //
    useAffectedRows("useAffectedRows", true), //
    useColumnNamesInFindColumn("useColumnNamesInFindColumn", true), //
    useCompression("useCompression", true), //
    useConfigs("useConfigs", true), //
    useCursorFetch("useCursorFetch", true), //
    useHostsInPrivileges("useHostsInPrivileges", true), //
    useInformationSchema("useInformationSchema", true), //
    useLocalSessionState("useLocalSessionState", true), //
    useLocalTransactionState("useLocalTransactionState", true), //
    useNanosForElapsedTime("useNanosForElapsedTime", true), //
    useOldAliasMetadataBehavior("useOldAliasMetadataBehavior", true), //
    useOnlyServerErrorMessages("useOnlyServerErrorMessages", true), //
    useReadAheadInput("useReadAheadInput", true), //
    useServerPrepStmts("useServerPrepStmts", true), //
    useSSL("useSSL", true), //
    useStreamLengthsInPrepStmts("useStreamLengthsInPrepStmts", true), //
    useUnbufferedInput("useUnbufferedInput", true), //
    useUsageAdvisor("useUsageAdvisor", true), //
    verifyServerCertificate("verifyServerCertificate", true), //

    xdevapiAsyncResponseTimeout("xdevapi.asyncResponseTimeout", "xdevapiAsyncResponseTimeout", true), //
    xdevapiAuth("xdevapi.auth", "xdevapiAuth", true), //
    xdevapiConnectTimeout("xdevapi.connect-timeout", "xdevapiConnectTimeout", true), //
    xdevapiConnectionAttributes("xdevapi.connection-attributes", "xdevapiConnectionAttributes", true), //
    xdevapiSSLMode("xdevapi.ssl-mode", "xdevapiSSLMode", true), //
    xdevapiSSLTrustStoreUrl("xdevapi.ssl-truststore", "xdevapiSSLTruststore", true), //
    xdevapiSSLTrustStoreType("xdevapi.ssl-truststore-type", "xdevapiSSLTruststoreType", true), //
    xdevapiSSLTrustStorePassword("xdevapi.ssl-truststore-password", "xdevapiSSLTruststorePassword", true), //
    xdevapiUseAsyncProtocol("xdevapi.useAsyncProtocol", "xdevapiUseAsyncProtocol", true), //

    yearIsDateType("yearIsDateType", true), //
    zeroDateTimeBehavior("zeroDateTimeBehavior", true) //
    ;

    private String keyName;
    private String ccAlias = null;
    private boolean isCaseSensitive = false;

    private static Map<String, PropertyKey> caseInsensitiveValues = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    static {
        for (PropertyKey pk : values()) {
            if (!pk.isCaseSensitive) {
                caseInsensitiveValues.put(pk.getKeyName(), pk);
                if (pk.getCcAlias() != null) {
                    caseInsensitiveValues.put(pk.getCcAlias(), pk);
                }
            }
        }
    }

    /**
     * Initializes each enum element with the proper key name to be used in the connection string or properties maps.
     * 
     * @param keyName
     *            the key name for the enum element.
     * @param isCaseSensitive
     *            is this name case sensitive
     */
    PropertyKey(String keyName, boolean isCaseSensitive) {
        this.keyName = keyName;
        this.isCaseSensitive = isCaseSensitive;
    }

    /**
     * Initializes each enum element with the proper key name to be used in the connection string or properties maps.
     * 
     * @param keyName
     *            the key name for the enum element.
     * @param alias
     *            camel-case alias key name
     * @param isCaseSensitive
     *            is this name case sensitive
     */
    PropertyKey(String keyName, String alias, boolean isCaseSensitive) {
        this(keyName, isCaseSensitive);
        this.ccAlias = alias;
    }

    @Override
    public String toString() {
        return this.keyName;
    }

    /**
     * Gets the key name of this enum element.
     * 
     * @return
     *         the key name associated with the enum element.
     */
    public String getKeyName() {
        return this.keyName;
    }

    /**
     * Gets the camel-case alias key name of this enum element.
     * 
     * @return
     *         the camel-case alias key name associated with the enum element or null.
     */
    public String getCcAlias() {
        return this.ccAlias;
    }

    /**
     * Looks for a {@link PropertyKey} that matches the given value as key name.
     * 
     * @param value
     *            the key name to look for.
     * @return
     *         the {@link PropertyKey} element that matches the given key name value or <code>null</code> if none is found.
     */
    public static PropertyKey fromValue(String value) {
        for (PropertyKey k : values()) {
            if (k.isCaseSensitive) {
                if (k.getKeyName().equals(value) || (k.getCcAlias() != null && k.getCcAlias().equals(value))) {
                    return k;
                }
            } else {
                if (k.getKeyName().equalsIgnoreCase(value) || (k.getCcAlias() != null && k.getCcAlias().equalsIgnoreCase(value))) {
                    return k;
                }
            }
        }
        return null;
    }

    /**
     * Helper method that normalizes the case of the given key, if it is one of {@link PropertyKey} elements.
     * 
     * @param keyName
     *            the key name to normalize.
     * @return
     *         the normalized key name if it belongs to this enum, otherwise returns the input unchanged.
     */
    public static String normalizeCase(String keyName) {
        PropertyKey pk = caseInsensitiveValues.get(keyName);
        return pk == null ? keyName : pk.getKeyName();
        //return keyName;
    }
}
