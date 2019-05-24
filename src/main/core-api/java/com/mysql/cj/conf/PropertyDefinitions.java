/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.mysql.cj.Messages;
import com.mysql.cj.PerConnectionLRUFactory;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.StandardLogger;
import com.mysql.cj.util.PerVmServerConfigCacheFactory;

public class PropertyDefinitions {
    /*
     * Built-in system properties
     */
    public static final String SYSP_line_separator = "line.separator";
    public static final String SYSP_java_vendor = "java.vendor";
    public static final String SYSP_java_version = "java.version";
    public static final String SYSP_java_vm_vendor = "java.vm.vendor";
    public static final String SYSP_os_name = "os.name";
    public static final String SYSP_os_arch = "os.arch";
    public static final String SYSP_os_version = "os.version";
    public static final String SYSP_file_encoding = "file.encoding";

    /*
     * Custom system properties
     */
    public static final String SYSP_testsuite_url /*                          */ = "com.mysql.cj.testsuite.url";
    public final static String SYSP_testsuite_url_admin /*                    */ = "com.mysql.cj.testsuite.url.admin";
    public static final String SYSP_testsuite_url_cluster /*                  */ = "com.mysql.cj.testsuite.url.cluster";
    /** Connection string to server compiled with OpenSSL */
    public static final String SYSP_testsuite_url_openssl /*                  */ = "com.mysql.cj.testsuite.url.openssl";

    public static final String SYSP_testsuite_url_mysqlx /*                   */ = "com.mysql.cj.testsuite.mysqlx.url";
    public static final String SYSP_testsuite_url_mysqlx_openssl /*           */ = "com.mysql.cj.testsuite.mysqlx.url.openssl";

    public static final String SYSP_testsuite_cantGrant /*                    */ = "com.mysql.cj.testsuite.cantGrant";
    public static final String SYSP_testsuite_disable_multihost_tests /*      */ = "com.mysql.cj.testsuite.disable.multihost.tests"; // TODO should be more specific for different types of multi-host configs

    public static final String SYSP_testsuite_unavailable_host /*             */ = "com.mysql.cj.testsuite.unavailable.host";

    /** For testsuite.regression.DataSourceRegressionTest */
    public static final String SYSP_testsuite_ds_host /*                      */ = "com.mysql.cj.testsuite.ds.host";
    /** For testsuite.regression.DataSourceRegressionTest */
    public static final String SYSP_testsuite_ds_port /*                      */ = "com.mysql.cj.testsuite.ds.port";
    /** For testsuite.regression.DataSourceRegressionTest */
    public static final String SYSP_testsuite_ds_db /*                        */ = "com.mysql.cj.testsuite.ds.db";
    /** For testsuite.regression.DataSourceRegressionTest */
    public static final String SYSP_testsuite_ds_user /*                      */ = "com.mysql.cj.testsuite.ds.user";
    /** For testsuite.regression.DataSourceRegressionTest */
    public static final String SYSP_testsuite_ds_password /*                  */ = "com.mysql.cj.testsuite.ds.password";

    /** For testsuite.perf.LoadStorePerfTest */
    public static final String SYSP_testsuite_loadstoreperf_tabletype /*      */ = "com.mysql.cj.testsuite.loadstoreperf.tabletype"; // TODO document allowed types
    /** For testsuite.perf.LoadStorePerfTest */
    public static final String SYSP_testsuite_loadstoreperf_useBigResults /*  */ = "com.mysql.cj.testsuite.loadstoreperf.useBigResults";

    /** The system property that must exist to run the shutdown test in testsuite.simple.MiniAdminTest */
    public static final String SYSP_testsuite_miniAdminTest_runShutdown /*    */ = "com.mysql.cj.testsuite.miniAdminTest.runShutdown";

    /** Suppress debug output when running testsuite */
    public static final String SYSP_testsuite_noDebugOutput /*                */ = "com.mysql.cj.testsuite.noDebugOutput";
    /** Don't remove database object created by tests */
    public static final String SYSP_testsuite_retainArtifacts /*              */ = "com.mysql.cj.testsuite.retainArtifacts";
    public static final String SYSP_testsuite_runLongTests /*                 */ = "com.mysql.cj.testsuite.runLongTests";
    public static final String SYSP_testsuite_serverController_basedir /*     */ = "com.mysql.cj.testsuite.serverController.basedir";

    public static final String SYSP_com_mysql_cj_build_verbose /*             */ = "com.mysql.cj.build.verbose";

    /*
     * Categories of connection properties
     */
    public static final String CATEGORY_AUTH = Messages.getString("ConnectionProperties.categoryAuthentication");
    public static final String CATEGORY_CONNECTION = Messages.getString("ConnectionProperties.categoryConnection");
    public static final String CATEGORY_SESSION = Messages.getString("ConnectionProperties.categorySession");
    public static final String CATEGORY_NETWORK = Messages.getString("ConnectionProperties.categoryNetworking");
    public static final String CATEGORY_SECURITY = Messages.getString("ConnectionProperties.categorySecurity");
    public static final String CATEGORY_STATEMENTS = Messages.getString("ConnectionProperties.categoryStatements");
    public static final String CATEGORY_PREPARED_STATEMENTS = Messages.getString("ConnectionProperties.categoryPreparedStatements");
    public static final String CATEGORY_RESULT_SETS = Messages.getString("ConnectionProperties.categoryResultSets");
    public static final String CATEGORY_METADATA = Messages.getString("ConnectionProperties.categoryMetadata");
    public static final String CATEGORY_BLOBS = Messages.getString("ConnectionProperties.categoryBlobs");
    public static final String CATEGORY_DATETIMES = Messages.getString("ConnectionProperties.categoryDatetimes");
    public static final String CATEGORY_HA = Messages.getString("ConnectionProperties.categoryHA");
    public static final String CATEGORY_PERFORMANCE = Messages.getString("ConnectionProperties.categoryPerformance");
    public static final String CATEGORY_DEBUGING_PROFILING = Messages.getString("ConnectionProperties.categoryDebuggingProfiling");
    public static final String CATEGORY_EXCEPTIONS = Messages.getString("ConnectionProperties.categoryExceptions");
    public static final String CATEGORY_INTEGRATION = Messages.getString("ConnectionProperties.categoryIntegration");
    public static final String CATEGORY_JDBC = Messages.getString("ConnectionProperties.categoryJDBC");
    public static final String CATEGORY_XDEVAPI = Messages.getString("ConnectionProperties.categoryXDevAPI");
    public static final String CATEGORY_USER_DEFINED = Messages.getString("ConnectionProperties.categoryUserDefined");

    public static final String[] PROPERTY_CATEGORIES = new String[] { CATEGORY_AUTH, CATEGORY_CONNECTION, CATEGORY_SESSION, CATEGORY_NETWORK, CATEGORY_SECURITY,
            CATEGORY_STATEMENTS, CATEGORY_PREPARED_STATEMENTS, CATEGORY_RESULT_SETS, CATEGORY_METADATA, CATEGORY_BLOBS, CATEGORY_DATETIMES, CATEGORY_HA,
            CATEGORY_PERFORMANCE, CATEGORY_DEBUGING_PROFILING, CATEGORY_EXCEPTIONS, CATEGORY_INTEGRATION, CATEGORY_JDBC, CATEGORY_XDEVAPI };

    /*
     * Property modifiers
     */
    public static final boolean DEFAULT_VALUE_TRUE = true;
    public static final boolean DEFAULT_VALUE_FALSE = false;
    public static final String DEFAULT_VALUE_NULL_STRING = null;
    public static final String NO_ALIAS = null;

    /** is modifiable in run-time */
    public static final boolean RUNTIME_MODIFIABLE = true;

    /** is not modifiable in run-time (will allow to set not-null value only once) */
    public static final boolean RUNTIME_NOT_MODIFIABLE = false;

    /*
     * Property enums
     */
    public enum ZeroDatetimeBehavior { // zeroDateTimeBehavior 
        CONVERT_TO_NULL, EXCEPTION, ROUND;
    }

    public enum SslMode {
        PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY, DISABLED;
    }

    public enum XdevapiSslMode {
        REQUIRED, VERIFY_CA, VERIFY_IDENTITY, DISABLED;
    }

    public enum AuthMech { // xdevapi.auth
        PLAIN, MYSQL41, SHA256_MEMORY, EXTERNAL;
    }

    /**
     * Static unmodifiable {@link PropertyKey} -&gt; {@link PropertyDefinition} map.
     */
    public static final Map<PropertyKey, PropertyDefinition<?>> PROPERTY_KEY_TO_PROPERTY_DEFINITION;

    static {
        String STANDARD_LOGGER_NAME = StandardLogger.class.getName();

        PropertyDefinition<?>[] pdefs = new PropertyDefinition<?>[] {
                new StringPropertyDefinition(PropertyKey.USER, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.Username"), Messages.getString("ConnectionProperties.allVersions"), CATEGORY_AUTH,
                        Integer.MIN_VALUE + 1),

                new StringPropertyDefinition(PropertyKey.PASSWORD, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.Password"), Messages.getString("ConnectionProperties.allVersions"), CATEGORY_AUTH,
                        Integer.MIN_VALUE + 2),

                new BooleanPropertyDefinition(PropertyKey.paranoid, DEFAULT_VALUE_FALSE, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.paranoid"), "3.0.1", CATEGORY_SECURITY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.passwordCharacterEncoding, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.passwordCharacterEncoding"), "5.1.7", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.serverRSAPublicKeyFile, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.serverRSAPublicKeyFile"), "5.1.31", CATEGORY_SECURITY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.allowPublicKeyRetrieval, DEFAULT_VALUE_FALSE, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowPublicKeyRetrieval"), "5.1.31", CATEGORY_SECURITY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.clientCertificateKeyStoreUrl, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clientCertificateKeyStoreUrl"), "5.1.0", CATEGORY_SECURITY, 5),

                new StringPropertyDefinition(PropertyKey.trustCertificateKeyStoreUrl, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.trustCertificateKeyStoreUrl"), "5.1.0", CATEGORY_SECURITY, 8),

                new StringPropertyDefinition(PropertyKey.clientCertificateKeyStoreType, "JKS", RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clientCertificateKeyStoreType"), "5.1.0", CATEGORY_SECURITY, 6),

                new StringPropertyDefinition(PropertyKey.clientCertificateKeyStorePassword, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clientCertificateKeyStorePassword"), "5.1.0", CATEGORY_SECURITY, 7),

                new StringPropertyDefinition(PropertyKey.trustCertificateKeyStoreType, "JKS", RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.trustCertificateKeyStoreType"), "5.1.0", CATEGORY_SECURITY, 9),

                new StringPropertyDefinition(PropertyKey.trustCertificateKeyStorePassword, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.trustCertificateKeyStorePassword"), "5.1.0", CATEGORY_SECURITY, 10),

                new StringPropertyDefinition(PropertyKey.enabledSSLCipherSuites, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.enabledSSLCipherSuites"), "5.1.35", CATEGORY_SECURITY, 11),

                new StringPropertyDefinition(PropertyKey.enabledTLSProtocols, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.enabledTLSProtocols"), "8.0.8", CATEGORY_SECURITY, 12),

                new BooleanPropertyDefinition(PropertyKey.useUnbufferedInput, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useUnbufferedInput"), "3.0.11", CATEGORY_NETWORK, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.profilerEventHandler, "com.mysql.cj.log.LoggingProfilerEventHandler", RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.profilerEventHandler"), "5.1.6", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.allowLoadLocalInfile, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadDataLocal"), "3.0.3", CATEGORY_SECURITY, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.allowMultiQueries, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowMultiQueries"), "3.1.1", CATEGORY_SECURITY, 1),

                new BooleanPropertyDefinition(PropertyKey.allowNanAndInf, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowNANandINF"), "3.1.5", CATEGORY_PREPARED_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.allowUrlInLocalInfile, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowUrlInLoadLocal"), "3.1.4", CATEGORY_SECURITY, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.alwaysSendSetIsolation, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.alwaysSendSetIsolation"), "3.1.7", CATEGORY_PERFORMANCE, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.autoClosePStmtStreams, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoClosePstmtStreams"), "3.1.12", CATEGORY_PREPARED_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.allowMasterDownConnections, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowMasterDownConnections"), "5.1.27", CATEGORY_HA, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.allowSlaveDownConnections, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowSlaveDownConnections"), "6.0.2", CATEGORY_HA, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.readFromMasterWhenNoSlaves, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.readFromMasterWhenNoSlaves"), "6.0.2", CATEGORY_HA, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.autoDeserialize, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoDeserialize"), "3.1.5", CATEGORY_BLOBS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.autoGenerateTestcaseScript, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoGenerateTestcaseScript"), "3.1.9", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.autoReconnect, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoReconnect"), "1.1", CATEGORY_HA, 0),

                new BooleanPropertyDefinition(PropertyKey.autoReconnectForPools, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoReconnectForPools"), "3.1.3", CATEGORY_HA, 1),

                new MemorySizePropertyDefinition(PropertyKey.blobSendChunkSize, 1024 * 1024, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.blobSendChunkSize"), "3.1.9", CATEGORY_BLOBS, Integer.MIN_VALUE, 0, 0),

                new BooleanPropertyDefinition(PropertyKey.autoSlowLog, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoSlowLog"), "5.1.4", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.blobsAreStrings, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.blobsAreStrings"), "5.0.8", CATEGORY_BLOBS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.functionsNeverReturnBlobs, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.functionsNeverReturnBlobs"), "5.0.8", CATEGORY_BLOBS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.cacheCallableStmts, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.cacheCallableStatements"), "3.1.2", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.cachePrepStmts, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.cachePrepStmts"), "3.0.10", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.cacheResultSetMetadata, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.cacheRSMetadata"), "3.1.1", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.serverConfigCacheFactory, PerVmServerConfigCacheFactory.class.getName(), RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.serverConfigCacheFactory"), "5.1.1", CATEGORY_PERFORMANCE, 12),

                new BooleanPropertyDefinition(PropertyKey.cacheServerConfiguration, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.cacheServerConfiguration"), "3.1.5", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PropertyKey.callableStmtCacheSize, 100, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.callableStmtCacheSize"), "3.1.2", CATEGORY_PERFORMANCE, 5, 0, Integer.MAX_VALUE),

                new StringPropertyDefinition(PropertyKey.characterEncoding, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.characterEncoding"), "1.1g", CATEGORY_SESSION, 5),

                new StringPropertyDefinition(PropertyKey.characterSetResults, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.characterSetResults"), "3.0.13", CATEGORY_SESSION, 6),

                new StringPropertyDefinition(PropertyKey.connectionAttributes, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.connectionAttributes"), "5.1.25", CATEGORY_CONNECTION, 7),

                new StringPropertyDefinition(PropertyKey.clientInfoProvider, "com.mysql.cj.jdbc.CommentClientInfoProvider", RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clientInfoProvider"), "5.1.0", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.clobberStreamingResults, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clobberStreamingResults"), "3.0.9", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.clobCharacterEncoding, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clobCharacterEncoding"), "5.0.0", CATEGORY_BLOBS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.compensateOnDuplicateKeyUpdateCounts, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.compensateOnDuplicateKeyUpdateCounts"), "5.1.7", CATEGORY_PREPARED_STATEMENTS,
                        Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.connectionCollation, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.connectionCollation"), "3.0.13", CATEGORY_SESSION, 7),

                new StringPropertyDefinition(PropertyKey.connectionLifecycleInterceptors, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.connectionLifecycleInterceptors"), "5.1.4", CATEGORY_CONNECTION, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.connectTimeout, 0, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.connectTimeout"),
                        "3.0.1", CATEGORY_NETWORK, 9, 0, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.continueBatchOnError, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.continueBatchOnError"), "3.0.3", CATEGORY_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.createDatabaseIfNotExist, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.createDatabaseIfNotExist"), "3.1.9", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PropertyKey.defaultFetchSize, 0, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.defaultFetchSize"),
                        "3.1.9", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.useServerPrepStmts, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useServerPrepStmts"), "3.1.0", CATEGORY_PREPARED_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.dontTrackOpenResources, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.dontTrackOpenResources"), "3.1.7", CATEGORY_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.dumpQueriesOnException, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.dumpQueriesOnException"), "3.1.3", CATEGORY_EXCEPTIONS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.elideSetAutoCommits, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.eliseSetAutoCommit"), "3.1.3", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.emptyStringsConvertToZero, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.emptyStringsConvertToZero"), "3.1.8", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.emulateLocators, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.emulateLocators"), "3.1.0", CATEGORY_BLOBS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.emulateUnsupportedPstmts, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.emulateUnsupportedPstmts"), "3.1.7", CATEGORY_PREPARED_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.enablePacketDebug, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.enablePacketDebug"), "3.1.3", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.enableQueryTimeouts, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.enableQueryTimeouts"), "5.0.6", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.explainSlowQueries, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.explainSlowQueries"), "3.1.2", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.exceptionInterceptors, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.exceptionInterceptors"), "5.1.8", CATEGORY_EXCEPTIONS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.failOverReadOnly, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.failoverReadOnly"), "3.0.12", CATEGORY_HA, 2),

                new BooleanPropertyDefinition(PropertyKey.gatherPerfMetrics, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.gatherPerfMetrics"), "3.1.2", CATEGORY_DEBUGING_PROFILING, 1),

                new BooleanPropertyDefinition(PropertyKey.generateSimpleParameterMetadata, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.generateSimpleParameterMetadata"), "5.0.5", CATEGORY_PREPARED_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.holdResultsOpenOverStatementClose, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.holdRSOpenOverStmtClose"), "3.1.7", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.includeInnodbStatusInDeadlockExceptions, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.includeInnodbStatusInDeadlockExceptions"), "5.0.7", CATEGORY_EXCEPTIONS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.includeThreadDumpInDeadlockExceptions, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.includeThreadDumpInDeadlockExceptions"), "5.1.15", CATEGORY_EXCEPTIONS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.includeThreadNamesAsStatementComment, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.includeThreadNamesAsStatementComment"), "5.1.15", CATEGORY_EXCEPTIONS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.ignoreNonTxTables, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.ignoreNonTxTables"), "3.0.9", CATEGORY_EXCEPTIONS, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PropertyKey.initialTimeout, 2, RUNTIME_NOT_MODIFIABLE, Messages.getString("ConnectionProperties.initialTimeout"),
                        "1.1", CATEGORY_HA, 5, 1, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.interactiveClient, DEFAULT_VALUE_FALSE, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.interactiveClient"), "3.1.0", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.jdbcCompliantTruncation, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.jdbcCompliantTruncation"), "3.1.2", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new MemorySizePropertyDefinition(PropertyKey.largeRowSizeThreshold, 2048, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.largeRowSizeThreshold"), "5.1.1", CATEGORY_PERFORMANCE, Integer.MIN_VALUE, 0,
                        Integer.MAX_VALUE),

                new StringPropertyDefinition(PropertyKey.ha_loadBalanceStrategy, "random", RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceStrategy"), "5.0.6", CATEGORY_HA, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PropertyKey.loadBalanceBlacklistTimeout, 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceBlacklistTimeout"), "5.1.0", CATEGORY_HA, Integer.MIN_VALUE, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.loadBalancePingTimeout, 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalancePingTimeout"), "5.1.13", CATEGORY_HA, Integer.MIN_VALUE, 0, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.loadBalanceValidateConnectionOnSwapServer, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceValidateConnectionOnSwapServer"), "5.1.13", CATEGORY_HA, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.loadBalanceConnectionGroup, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceConnectionGroup"), "5.1.13", CATEGORY_HA, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.loadBalanceExceptionChecker, "com.mysql.cj.jdbc.ha.StandardLoadBalanceExceptionChecker",
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.loadBalanceExceptionChecker"), "5.1.13", CATEGORY_HA, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.loadBalanceSQLStateFailover, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceSQLStateFailover"), "5.1.13", CATEGORY_HA, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.loadBalanceSQLExceptionSubclassFailover, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceSQLExceptionSubclassFailover"), "5.1.13", CATEGORY_HA, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.loadBalanceAutoCommitStatementRegex, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceAutoCommitStatementRegex"), "5.1.15", CATEGORY_HA, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PropertyKey.loadBalanceAutoCommitStatementThreshold, 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceAutoCommitStatementThreshold"), "5.1.15", CATEGORY_HA, Integer.MIN_VALUE, 0,
                        Integer.MAX_VALUE),

                new StringPropertyDefinition(PropertyKey.localSocketAddress, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.localSocketAddress"), "5.0.5", CATEGORY_NETWORK, Integer.MIN_VALUE),

                new MemorySizePropertyDefinition(PropertyKey.locatorFetchBufferSize, 1024 * 1024, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.locatorFetchBufferSize"), "3.2.1", CATEGORY_BLOBS, Integer.MIN_VALUE, 0, Integer.MAX_VALUE),

                new StringPropertyDefinition(PropertyKey.logger, STANDARD_LOGGER_NAME, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.logger", new Object[] { Log.class.getName(), STANDARD_LOGGER_NAME }), "3.1.1",
                        CATEGORY_DEBUGING_PROFILING, 0),

                new BooleanPropertyDefinition(PropertyKey.logSlowQueries, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.logSlowQueries"), "3.1.2", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.logXaCommands, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.logXaCommands"), "5.0.5", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.maintainTimeStats, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.maintainTimeStats"), "3.1.9", CATEGORY_PERFORMANCE, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.maxQuerySizeToLog, 2048, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.maxQuerySizeToLog"), "3.1.3", CATEGORY_DEBUGING_PROFILING, 4, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.maxReconnects, 3, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.maxReconnects"), "1.1",
                        CATEGORY_HA, 4, 1, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.retriesAllDown, 120, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.retriesAllDown"),
                        "5.1.6", CATEGORY_HA, 4, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.maxRows, -1, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.maxRows"),
                        Messages.getString("ConnectionProperties.allVersions"), CATEGORY_RESULT_SETS, Integer.MIN_VALUE, -1, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.metadataCacheSize, 50, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.metadataCacheSize"), "3.1.1", CATEGORY_PERFORMANCE, 5, 1, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.netTimeoutForStreamingResults, 600, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.netTimeoutForStreamingResults"), "5.1.0", CATEGORY_RESULT_SETS, Integer.MIN_VALUE, 0,
                        Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.noAccessToProcedureBodies, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.noAccessToProcedureBodies"), "5.0.3", CATEGORY_METADATA, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.noDatetimeStringSync, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.noDatetimeStringSync"), "3.1.7", CATEGORY_DATETIMES, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.nullCatalogMeansCurrent, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.nullCatalogMeansCurrent"), "3.1.8", CATEGORY_METADATA, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PropertyKey.packetDebugBufferSize, 20, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.packetDebugBufferSize"), "3.1.3", CATEGORY_DEBUGING_PROFILING, 7, 1, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.padCharsWithSpace, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.padCharsWithSpace"), "5.0.6", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.pedantic, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.pedantic"), "3.0.0", CATEGORY_JDBC, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.pinGlobalTxToPhysicalConnection, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.pinGlobalTxToPhysicalConnection"), "5.0.1", CATEGORY_HA, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.populateInsertRowWithDefaultValues, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.populateInsertRowWithDefaultValues"), "5.0.5", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PropertyKey.prepStmtCacheSize, 25, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.prepStmtCacheSize"), "3.0.10", CATEGORY_PERFORMANCE, 10, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.prepStmtCacheSqlLimit, 256, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.prepStmtCacheSqlLimit"), "3.0.10", CATEGORY_PERFORMANCE, 11, 1, Integer.MAX_VALUE),

                new StringPropertyDefinition(PropertyKey.parseInfoCacheFactory, PerConnectionLRUFactory.class.getName(), RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.parseInfoCacheFactory"), "5.1.1", CATEGORY_PERFORMANCE, 12),

                new BooleanPropertyDefinition(PropertyKey.processEscapeCodesForPrepStmts, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.processEscapeCodesForPrepStmts"), "3.1.12", CATEGORY_PREPARED_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.profileSQL, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.profileSQL"), "3.1.0", CATEGORY_DEBUGING_PROFILING, 1),

                new StringPropertyDefinition(PropertyKey.propertiesTransform, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.connectionPropertiesTransform"), "3.1.4", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PropertyKey.queriesBeforeRetryMaster, 50, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.queriesBeforeRetryMaster"), "3.0.2", CATEGORY_HA, 7, 0, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.queryTimeoutKillsConnection, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.queryTimeoutKillsConnection"), "5.1.9", CATEGORY_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.reconnectAtTxEnd, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.reconnectAtTxEnd"), "3.0.10", CATEGORY_HA, 4),

                new StringPropertyDefinition(PropertyKey.replicationConnectionGroup, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.replicationConnectionGroup"), "8.0.7", CATEGORY_HA, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PropertyKey.reportMetricsIntervalMillis, 30000, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.reportMetricsIntervalMillis"), "3.1.2", CATEGORY_DEBUGING_PROFILING, 3, 0, Integer.MAX_VALUE),

                new StringPropertyDefinition(PropertyKey.resourceId, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.resourceId"), "5.0.1", CATEGORY_HA, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PropertyKey.resultSetSizeThreshold, 100, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.resultSetSizeThreshold"), "5.0.5", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.rewriteBatchedStatements, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.rewriteBatchedStatements"), "3.1.13", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.rollbackOnPooledClose, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.rollbackOnPooledClose"), "3.0.15", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PropertyKey.secondsBeforeRetryMaster, 30, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.secondsBeforeRetryMaster"), "3.0.2", CATEGORY_HA, 8, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.selfDestructOnPingSecondsLifetime, 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.selfDestructOnPingSecondsLifetime"), "5.1.6", CATEGORY_HA, Integer.MAX_VALUE, 0,
                        Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.selfDestructOnPingMaxOperations, 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.selfDestructOnPingMaxOperations"), "5.1.6", CATEGORY_HA, Integer.MAX_VALUE, 0,
                        Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.ha_enableJMX, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.ha.enableJMX"), "5.1.27", CATEGORY_HA, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.loadBalanceHostRemovalGracePeriod, 15000, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceHostRemovalGracePeriod"), "6.0.3", CATEGORY_HA, Integer.MAX_VALUE, 0,
                        Integer.MAX_VALUE),

                new StringPropertyDefinition(PropertyKey.serverTimezone, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.serverTimezone"), "3.0.2", CATEGORY_DATETIMES, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.sessionVariables, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.sessionVariables"), "3.1.8", CATEGORY_SESSION, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.slowQueryThresholdMillis, 2000, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.slowQueryThresholdMillis"), "3.1.2", CATEGORY_DEBUGING_PROFILING, 9, 0, Integer.MAX_VALUE),

                new LongPropertyDefinition(PropertyKey.slowQueryThresholdNanos, 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.slowQueryThresholdNanos"), "5.0.7", CATEGORY_DEBUGING_PROFILING, 10),

                new StringPropertyDefinition(PropertyKey.socketFactory, "com.mysql.cj.protocol.StandardSocketFactory", RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.socketFactory"), "3.0.3", CATEGORY_NETWORK, 4),

                new StringPropertyDefinition(PropertyKey.socksProxyHost, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.socksProxyHost"), "5.1.34", CATEGORY_NETWORK, 1),

                new IntegerPropertyDefinition(PropertyKey.socksProxyPort, 1080, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.socksProxyPort"),
                        "5.1.34", CATEGORY_NETWORK, 2, 0, 65535),

                new IntegerPropertyDefinition(PropertyKey.socketTimeout, 0, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.socketTimeout"),
                        "3.0.1", CATEGORY_NETWORK, 10, 0, Integer.MAX_VALUE),

                new StringPropertyDefinition(PropertyKey.queryInterceptors, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.queryInterceptors"), "8.0.7", CATEGORY_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.strictUpdates, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.strictUpdates"), "3.0.4", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.overrideSupportsIntegrityEnhancementFacility, false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.overrideSupportsIEF"), "3.1.12", CATEGORY_INTEGRATION, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.tcpNoDelay, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.tcpNoDelay"), "5.0.7", CATEGORY_NETWORK, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.tcpKeepAlive, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.tcpKeepAlive"), "5.0.7", CATEGORY_NETWORK, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PropertyKey.tcpRcvBuf, 0, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.tcpSoRcvBuf"), "5.0.7",
                        CATEGORY_NETWORK, Integer.MIN_VALUE, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.tcpSndBuf, 0, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.tcpSoSndBuf"), "5.0.7",
                        CATEGORY_NETWORK, Integer.MIN_VALUE, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.tcpTrafficClass, 0, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.tcpTrafficClass"),
                        "5.0.7", CATEGORY_NETWORK, Integer.MIN_VALUE, 0, 255),

                new BooleanPropertyDefinition(PropertyKey.tinyInt1isBit, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.tinyInt1isBit"), "3.0.16", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.traceProtocol, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.traceProtocol"), "3.1.2", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.treatUtilDateAsTimestamp, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.treatUtilDateAsTimestamp"), "5.0.5", CATEGORY_DATETIMES, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.transformedBitIsBoolean, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.transformedBitIsBoolean"), "3.1.9", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.useCompression, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useCompression"), "3.0.17", CATEGORY_NETWORK, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.useColumnNamesInFindColumn, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useColumnNamesInFindColumn"), "5.1.7", CATEGORY_JDBC, Integer.MAX_VALUE),

                new StringPropertyDefinition(PropertyKey.useConfigs, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useConfigs"), "3.1.5", CATEGORY_CONNECTION, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.useCursorFetch, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useCursorFetch"), "5.0.0", CATEGORY_PERFORMANCE, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.useHostsInPrivileges, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useHostsInPrivileges"), "3.0.2", CATEGORY_METADATA, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.useInformationSchema, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useInformationSchema"), "5.0.0", CATEGORY_METADATA, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.useLocalSessionState, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useLocalSessionState"), "3.1.7", CATEGORY_PERFORMANCE, 5),

                new BooleanPropertyDefinition(PropertyKey.useLocalTransactionState, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useLocalTransactionState"), "5.1.7", CATEGORY_PERFORMANCE, 6),

                new BooleanPropertyDefinition(PropertyKey.sendFractionalSeconds, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.sendFractionalSeconds"), "5.1.37", CATEGORY_DATETIMES, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.useNanosForElapsedTime, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useNanosForElapsedTime"), "5.0.7", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.useOldAliasMetadataBehavior, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useOldAliasMetadataBehavior"), "5.0.4", CATEGORY_JDBC, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.useOnlyServerErrorMessages, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useOnlyServerErrorMessages"), "3.0.15", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.useReadAheadInput, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useReadAheadInput"), "3.1.5", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.useSSL, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.useSSL"),
                        "3.0.2", CATEGORY_SECURITY, 2),
                new BooleanPropertyDefinition(PropertyKey.requireSSL, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.requireSSL"), "3.1.0", CATEGORY_SECURITY, 3),
                new BooleanPropertyDefinition(PropertyKey.verifyServerCertificate, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.verifyServerCertificate"), "5.1.6", CATEGORY_SECURITY, 4),

                new EnumPropertyDefinition<>(PropertyKey.sslMode, SslMode.PREFERRED, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.sslMode"),
                        "8.0.13", CATEGORY_SECURITY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.useStreamLengthsInPrepStmts, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useStreamLengthsInPrepStmts"), "3.0.2", CATEGORY_PREPARED_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.ultraDevHack, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.ultraDevHack"), "2.0.3", CATEGORY_INTEGRATION, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.useUsageAdvisor, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useUsageAdvisor"), "3.1.1", CATEGORY_DEBUGING_PROFILING, 10),

                new BooleanPropertyDefinition(PropertyKey.yearIsDateType, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.yearIsDateType"), "3.1.9", CATEGORY_DATETIMES, Integer.MIN_VALUE),

                new EnumPropertyDefinition<>(PropertyKey.zeroDateTimeBehavior, ZeroDatetimeBehavior.EXCEPTION, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.zeroDateTimeBehavior",
                                new Object[] { ZeroDatetimeBehavior.EXCEPTION, ZeroDatetimeBehavior.ROUND, ZeroDatetimeBehavior.CONVERT_TO_NULL }),
                        "3.1.4", CATEGORY_DATETIMES, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.useAffectedRows, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useAffectedRows"), "5.1.7", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PropertyKey.maxAllowedPacket, 65535, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.maxAllowedPacket"), "5.1.8", CATEGORY_NETWORK, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.authenticationPlugins, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.authenticationPlugins"), "5.1.19", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.disabledAuthenticationPlugins, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.disabledAuthenticationPlugins"), "5.1.19", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.defaultAuthenticationPlugin, "com.mysql.cj.protocol.a.authentication.MysqlNativePasswordPlugin",
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.defaultAuthenticationPlugin"), "5.1.19", CATEGORY_CONNECTION,
                        Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.disconnectOnExpiredPasswords, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.disconnectOnExpiredPasswords"), "5.1.23", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.getProceduresReturnsFunctions, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.getProceduresReturnsFunctions"), "5.1.26", CATEGORY_METADATA, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.detectCustomCollations, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.detectCustomCollations"), "5.1.29", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.dontCheckOnDuplicateKeyUpdateInSQL, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.dontCheckOnDuplicateKeyUpdateInSQL"), "5.1.32", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.readOnlyPropagatesToServer, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.readOnlyPropagatesToServer"), "5.1.35", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.xdevapiUseAsyncProtocol, DEFAULT_VALUE_FALSE, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useAsyncProtocol"), "6.0.0", CATEGORY_XDEVAPI, Integer.MIN_VALUE),
                new EnumPropertyDefinition<>(PropertyKey.xdevapiSSLMode, XdevapiSslMode.REQUIRED, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.xdevapiSslMode"), "8.0.7", CATEGORY_XDEVAPI, Integer.MIN_VALUE),
                new StringPropertyDefinition(PropertyKey.xdevapiSSLTrustStoreUrl, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.sslTrustStoreUrl"), "6.0.6", CATEGORY_XDEVAPI, Integer.MIN_VALUE),
                new StringPropertyDefinition(PropertyKey.xdevapiSSLTrustStoreType, "JKS", RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.sslTrustStoreType"), "6.0.6", CATEGORY_XDEVAPI, Integer.MIN_VALUE),
                new StringPropertyDefinition(PropertyKey.xdevapiSSLTrustStorePassword, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.sslTrustStorePassword"), "6.0.6", CATEGORY_XDEVAPI, Integer.MIN_VALUE),
                new IntegerPropertyDefinition(PropertyKey.xdevapiAsyncResponseTimeout, 300, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.asyncResponseTimeout"), "8.0.7", CATEGORY_XDEVAPI, Integer.MIN_VALUE),
                new EnumPropertyDefinition<>(PropertyKey.xdevapiAuth, AuthMech.PLAIN, RUNTIME_NOT_MODIFIABLE, Messages.getString("ConnectionProperties.auth"),
                        "8.0.8", CATEGORY_XDEVAPI, Integer.MIN_VALUE),
                new IntegerPropertyDefinition(PropertyKey.xdevapiConnectTimeout, 10000, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.xdevapiConnectTimeout"), "8.0.13", CATEGORY_XDEVAPI, Integer.MIN_VALUE, 0, Integer.MAX_VALUE),
                new StringPropertyDefinition(PropertyKey.xdevapiConnectionAttributes, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.xdevapiConnectionAttributes"), "8.0.16", CATEGORY_XDEVAPI, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.enableEscapeProcessing, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.enableEscapeProcessing"), "6.0.1", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.serverAffinityOrder, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.serverAffinityOrder"), "8.0.8", CATEGORY_HA, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.clientAuthenticationRequired, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clientAuthenticationRequired"), "8.0.8", CATEGORY_SECURITY, Integer.MIN_VALUE) };

        HashMap<PropertyKey, PropertyDefinition<?>> propertyKeyToPropertyDefinitionMap = new HashMap<>();
        for (PropertyDefinition<?> pdef : pdefs) {
            propertyKeyToPropertyDefinitionMap.put(pdef.getPropertyKey(), pdef);
        }
        PROPERTY_KEY_TO_PROPERTY_DEFINITION = Collections.unmodifiableMap(propertyKeyToPropertyDefinitionMap);
    }

    public static PropertyDefinition<?> getPropertyDefinition(PropertyKey propertyKey) {
        return PROPERTY_KEY_TO_PROPERTY_DEFINITION.get(propertyKey);
    }
}
