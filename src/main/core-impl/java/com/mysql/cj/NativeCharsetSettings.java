/*
 * Copyright (c) 2021, Oracle and/or its affiliates.
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

package com.mysql.cj;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.Resultset.Type;
import com.mysql.cj.protocol.ServerCapabilities;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.a.NativeConstants;
import com.mysql.cj.protocol.a.NativeMessageBuilder;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.protocol.a.ResultsetFactory;
import com.mysql.cj.result.IntegerValueFactory;
import com.mysql.cj.result.Row;
import com.mysql.cj.result.StringValueFactory;
import com.mysql.cj.result.ValueFactory;
import com.mysql.cj.util.StringUtils;

public class NativeCharsetSettings extends CharsetMapping implements CharsetSettings {

    private NativeSession session;

    private ServerSession serverSession;

    public Map<Integer, String> collationIndexToCollationName = null;
    public Map<String, Integer> collationNameToCollationIndex = null;
    public Map<Integer, String> collationIndexToCharsetName = null;
    public Map<String, Integer> charsetNameToMblen = null;
    public Map<String, String> charsetNameToJavaEncoding = null;
    public Map<String, Integer> charsetNameToCollationIndex = null;
    public Map<String, String> javaEncodingUcToCharsetName = null;
    public Set<String> multibyteEncodings = null;

    private Integer sessionCollationIndex = null;

    /**
     * What character set is the metadata returned in?
     */
    private String metadataEncoding = null;
    private int metadataCollationIndex;

    /**
     * The (Java) encoding used to interpret error messages received from the server.
     * We use character_set_results (since MySQL 5.5) if it is not null or UTF-8 otherwise.
     */
    private String errorMessageEncoding = "Cp1252"; // to begin with, changes after we talk to the server

    protected RuntimeProperty<String> characterEncoding;
    protected RuntimeProperty<String> passwordCharacterEncoding;
    protected RuntimeProperty<String> characterSetResults;
    protected RuntimeProperty<String> connectionCollation;
    protected RuntimeProperty<Boolean> cacheServerConfiguration;

    /**
     * If a CharsetEncoder is required for escaping. Needed for SJIS and related problems with \u00A5.
     */
    private boolean requiresEscapingEncoder;

    private NativeMessageBuilder commandBuilder = null;

    private static final Map<String, Map<Integer, String>> customCollationIndexToCollationNameByUrl = new HashMap<>();
    private static final Map<String, Map<String, Integer>> customCollationNameToCollationIndexByUrl = new HashMap<>();

    /**
     * Actual collation index to mysql charset name map of user defined charsets for given server URLs.
     */
    private static final Map<String, Map<Integer, String>> customCollationIndexToCharsetNameByUrl = new HashMap<>();

    /**
     * Actual mysql charset name to mblen map of user defined charsets for given server URLs.
     */
    private static final Map<String, Map<String, Integer>> customCharsetNameToMblenByUrl = new HashMap<>();

    private static final Map<String, Map<String, String>> customCharsetNameToJavaEncodingByUrl = new HashMap<>();
    private static final Map<String, Map<String, Integer>> customCharsetNameToCollationIndexByUrl = new HashMap<>();
    private static final Map<String, Map<String, String>> customJavaEncodingUcToCharsetNameByUrl = new HashMap<>();
    private static final Map<String, Set<String>> customMultibyteEncodingsByUrl = new HashMap<>();

    /**
     * We store the platform 'encoding' here, only used to avoid munging filenames for LOAD DATA LOCAL INFILE...
     */
    private static Charset jvmPlatformCharset = null;

    /**
     * Does the character set of this connection match the character set of the platform
     */
    private boolean platformDbCharsetMatches = true; // changed once we've connected.

    static {
        OutputStreamWriter outWriter = null;
        // Use the I/O system to get the encoding (if possible), to avoid security restrictions on System.getProperty("file.encoding") in applets (why is that restricted?)
        try {
            outWriter = new OutputStreamWriter(new ByteArrayOutputStream());
            jvmPlatformCharset = Charset.forName(outWriter.getEncoding());
        } finally {
            try {
                if (outWriter != null) {
                    outWriter.close();
                }
            } catch (IOException ioEx) {
                // ignore
            }
        }
    }

    private NativeMessageBuilder getCommandBuilder() {
        if (this.commandBuilder == null) {
            this.commandBuilder = new NativeMessageBuilder(this.serverSession.supportsQueryAttributes());
        }
        return this.commandBuilder;
    }

    /**
     * Determines if the database charset is the same as the platform charset
     */
    private void checkForCharsetMismatch() {
        String characterEncodingValue = this.characterEncoding.getValue();
        if (characterEncodingValue != null) {
            Charset characterEncodingCs = Charset.forName(characterEncodingValue);
            Charset encodingToCheck = jvmPlatformCharset;

            if (encodingToCheck == null) {
                encodingToCheck = Charset.forName(Constants.PLATFORM_ENCODING);
            }

            this.platformDbCharsetMatches = encodingToCheck == null ? false : encodingToCheck.equals(characterEncodingCs);
        }
    }

    @Override
    public boolean doesPlatformDbCharsetMatches() {
        return this.platformDbCharsetMatches;
    }

    public NativeCharsetSettings(NativeSession sess) {
        this.session = sess;
        this.serverSession = this.session.getServerSession();

        this.characterEncoding = sess.getPropertySet().getStringProperty(PropertyKey.characterEncoding);
        this.characterSetResults = this.session.getPropertySet().getProperty(PropertyKey.characterSetResults);
        this.passwordCharacterEncoding = this.session.getPropertySet().getStringProperty(PropertyKey.passwordCharacterEncoding);
        this.connectionCollation = this.session.getPropertySet().getStringProperty(PropertyKey.connectionCollation);
        this.cacheServerConfiguration = sess.getPropertySet().getBooleanProperty(PropertyKey.cacheServerConfiguration);

        tryAndFixEncoding(this.characterEncoding, true);
        tryAndFixEncoding(this.passwordCharacterEncoding, true);
        if (!"null".equalsIgnoreCase(this.characterSetResults.getValue())) { // the "null" instead of an encoding name is allowed for characterSetResults
            tryAndFixEncoding(this.characterSetResults, false);
        }
    }

    /**
     * Attempt to use the encoding, and bail out if it can't be used.
     * 
     * @param encodingProperty
     *            connection property containing the Java encoding to try
     * @param replaceImpermissibleEncodings
     *            The character_set_client system variable cannot be set to ucs2, utf16, utf16le, utf32 charsets. If "true" the corresponding connection
     *            property value will be replaced with "UTF-8"
     */
    private void tryAndFixEncoding(RuntimeProperty<String> encodingProperty, boolean replaceImpermissibleEncodings) {
        String oldEncoding = encodingProperty.getValue();
        if (oldEncoding != null) {
            if (replaceImpermissibleEncodings && ("UnicodeBig".equalsIgnoreCase(oldEncoding) || "UTF-16".equalsIgnoreCase(oldEncoding)
                    || "UTF-16LE".equalsIgnoreCase(oldEncoding) || "UTF-32".equalsIgnoreCase(oldEncoding))) {
                encodingProperty.setValue("UTF-8");
            } else {
                try {
                    StringUtils.getBytes("abc", oldEncoding);
                } catch (WrongArgumentException waEx) {
                    // Try the MySQL character set name, then....
                    String newEncoding = getStaticJavaEncodingForMysqlCharset(oldEncoding);
                    if (newEncoding == null) {
                        throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("StringUtils.0", new Object[] { oldEncoding }),
                                this.session.getExceptionInterceptor());
                    }
                    StringUtils.getBytes("abc", newEncoding);
                    encodingProperty.setValue(newEncoding);
                }
            }
        }
    }

    @Override
    public int configurePreHandshake(boolean reset) {
        if (reset) {
            this.sessionCollationIndex = null;
        }

        // Avoid the second execution of this method
        if (this.sessionCollationIndex != null) {
            return this.sessionCollationIndex;
        }

        ServerCapabilities capabilities = this.serverSession.getCapabilities();
        String encoding = this.passwordCharacterEncoding.getStringValue();
        if (encoding == null) {
            String connectionColl = this.connectionCollation.getStringValue();
            if ((connectionColl == null || (this.sessionCollationIndex = getStaticCollationIndexForCollationName(connectionColl)) == null)
                    && (encoding = this.characterEncoding.getValue()) == null) {
                // If none of "passwordCharacterEncoding", "connectionCollation" or "characterEncoding" is specified then use UTF-8.
                // It would be better to use the server default collation here, to avoid unnecessary SET NAMES queries after the handshake if server
                // default charset if not utf8, but we can not do it until server Bug#32729185 is fixed. Server cuts collation index to lower byte and, for example,
                // if the server is started with character-set-server=utf8mb4 and collation-server=utf8mb4_is_0900_ai_ci (collation index 257) the Protocol::HandshakeV10
                // will contain character_set=1, "big5_chinese_ci". This is true not only for MySQL 8.0, where built-in collations with indexes > 255 were first introduced,
                // but also other server series would be affected when configured with custom collations, for which the reserved collation id range is >= 1024.
                this.sessionCollationIndex = MYSQL_COLLATION_INDEX_utf8mb4_0900_ai_ci;
            }
        }

        if (this.sessionCollationIndex == null) {
            if ((this.sessionCollationIndex = getStaticCollationIndexForJavaEncoding(encoding, capabilities.getServerVersion())) == 0) {
                throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("StringUtils.0", new Object[] { encoding }));
            }
        }

        if (this.sessionCollationIndex > Constants.UNSIGNED_BYTE_MAX_VALUE  //
                || isStaticImpermissibleCollation(this.sessionCollationIndex)) { // At this point, impermissible charset can be set only with "connectionCollation".
            this.sessionCollationIndex = MYSQL_COLLATION_INDEX_utf8mb4_0900_ai_ci;
        }

        if (this.sessionCollationIndex == MYSQL_COLLATION_INDEX_utf8mb4_0900_ai_ci
                && !capabilities.getServerVersion().meetsMinimum(new ServerVersion(8, 0, 1))) {
            this.sessionCollationIndex = MYSQL_COLLATION_INDEX_utf8mb4_general_ci; // use utf8mb4_general_ci instead of utf8mb4_0900_ai_ci for old servers
        }

        // error messages are returned according to character_set_results which, at this point, is set from the response packet
        this.errorMessageEncoding = getStaticJavaEncodingForCollationIndex(this.sessionCollationIndex);
        this.serverSession.getServerVariables().put(CHARACTER_SET_RESULTS, getStaticMysqlCharsetNameForCollationIndex(this.sessionCollationIndex));

        return this.sessionCollationIndex;
    }

    @Override
    public void configurePostHandshake(boolean dontCheckServerMatch) {

        buildCollationMapping();

        /*
         * Configuring characterEncoding.
         */

        String requiredCollation = this.connectionCollation.getStringValue();
        String requiredEncoding = this.characterEncoding.getValue();
        String passwordEncoding = this.passwordCharacterEncoding.getValue();
        Integer requiredCollationIndex;
        String sessionCharsetName = getServerDefaultCharset();
        String sessionCollationClause = "";

        try {

            // connectionCollation overrides the characterEncoding value
            if (requiredCollation != null && (requiredCollationIndex = getCollationIndexForCollationName(requiredCollation)) != null) {
                if (isImpermissibleCollation(requiredCollationIndex)) {
                    if (this.serverSession.getCapabilities().getServerVersion().meetsMinimum(new ServerVersion(8, 0, 1))) {
                        requiredCollationIndex = MYSQL_COLLATION_INDEX_utf8mb4_0900_ai_ci;
                        requiredCollation = "utf8mb4_0900_ai_ci";
                    } else {
                        requiredCollationIndex = MYSQL_COLLATION_INDEX_utf8mb4_general_ci;
                        requiredCollation = "utf8mb4_general_ci";
                    }
                }
                sessionCollationClause = " COLLATE " + requiredCollation;
                sessionCharsetName = getMysqlCharsetNameForCollationIndex(requiredCollationIndex);
                requiredEncoding = getJavaEncodingForCollationIndex(requiredCollationIndex, requiredEncoding);
                this.sessionCollationIndex = requiredCollationIndex;
            }

            if (requiredEncoding != null) { // If either connectionCollation or characterEncoding is defined.
                if (sessionCollationClause.length() == 0) {  // If no connectionCollation is defined.
                    sessionCharsetName = getMysqlCharsetForJavaEncoding(requiredEncoding.toUpperCase(Locale.ENGLISH), this.serverSession.getServerVersion());
                }

            } else { // Neither connectionCollation nor characterEncoding are defined.
                // Collations with index > 255 don't fit into server greeting packet.
                // Now we can set sessionCollationIndex according to "collation_server" value.
                if (!StringUtils.isNullOrEmpty(passwordEncoding)) {
                    if (this.serverSession.getCapabilities().getServerVersion().meetsMinimum(new ServerVersion(8, 0, 1))) {
                        this.sessionCollationIndex = MYSQL_COLLATION_INDEX_utf8mb4_0900_ai_ci; // We can't do more, just trying to use utf8mb4_0900_ai_ci because the most of collations in that range are utf8mb4.
                        requiredCollation = "utf8mb4_0900_ai_ci";
                    } else {
                        this.sessionCollationIndex = MYSQL_COLLATION_INDEX_utf8mb4_general_ci;
                        requiredCollation = "utf8mb4_general_ci";
                    }
                    sessionCollationClause = " COLLATE " + getCollationNameForCollationIndex(this.sessionCollationIndex);
                }

                if (((requiredEncoding = getJavaEncodingForCollationIndex(this.sessionCollationIndex, requiredEncoding)) == null)) {
                    // if there is no mapping for default collation index leave characterEncoding as specified by user
                    throw ExceptionFactory.createException(Messages.getString("Connection.5", new Object[] { this.sessionCollationIndex.toString() }),
                            this.session.getExceptionInterceptor());
                }

                sessionCharsetName = getMysqlCharsetNameForCollationIndex(this.sessionCollationIndex);
            }

        } catch (ArrayIndexOutOfBoundsException outOfBoundsEx) {
            throw ExceptionFactory.createException(Messages.getString("Connection.6", new Object[] { this.sessionCollationIndex }),
                    this.session.getExceptionInterceptor());
        }

        this.characterEncoding.setValue(requiredEncoding);

        if (sessionCharsetName != null) {
            boolean isCharsetDifferent = !characterSetNamesMatches(sessionCharsetName);
            boolean isCollationDifferent = sessionCollationClause.length() > 0
                    && !requiredCollation.equalsIgnoreCase(this.serverSession.getServerVariable(COLLATION_CONNECTION));
            if (dontCheckServerMatch || isCharsetDifferent || isCollationDifferent) {
                this.session.sendCommand(getCommandBuilder().buildComQuery(null, "SET NAMES " + sessionCharsetName + sessionCollationClause), false, 0);
                this.serverSession.getServerVariables().put(CHARACTER_SET_CLIENT, sessionCharsetName);
                this.serverSession.getServerVariables().put(CHARACTER_SET_CONNECTION, sessionCharsetName);
            }
        }

        /*
         * Configuring characterSetResults.
         * 
         * We know how to deal with any charset coming back from the database, so tell the server not to do conversion
         * if the user hasn't 'forced' a result-set character set.
         */

        String sessionResultsCharset = this.serverSession.getServerVariable(CHARACTER_SET_RESULTS);
        String characterSetResultsValue = this.characterSetResults.getValue();
        if (StringUtils.isNullOrEmpty(characterSetResultsValue) || "null".equalsIgnoreCase(characterSetResultsValue)) {
            if (!StringUtils.isNullOrEmpty(sessionResultsCharset) && !"NULL".equalsIgnoreCase(sessionResultsCharset)) {
                this.session.sendCommand(getCommandBuilder().buildComQuery(null, "SET character_set_results = NULL"), false, 0);
                this.serverSession.getServerVariables().put(CHARACTER_SET_RESULTS, null);
            }

            String defaultMetadataCharsetMysql = this.serverSession.getServerVariable("character_set_system");
            this.metadataEncoding = defaultMetadataCharsetMysql != null ? getJavaEncodingForMysqlCharset(defaultMetadataCharsetMysql) : "UTF-8";
            this.errorMessageEncoding = "UTF-8";

        } else {
            String resultsCharsetName = getMysqlCharsetForJavaEncoding(characterSetResultsValue.toUpperCase(Locale.ENGLISH),
                    this.serverSession.getServerVersion());

            if (resultsCharsetName == null) {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("Connection.7", new Object[] { characterSetResultsValue }), this.session.getExceptionInterceptor());
            }

            if (!resultsCharsetName.equalsIgnoreCase(sessionResultsCharset)) {
                this.session.sendCommand(getCommandBuilder().buildComQuery(null, "SET character_set_results = " + resultsCharsetName), false, 0);
                this.serverSession.getServerVariables().put(CHARACTER_SET_RESULTS, resultsCharsetName);
            }

            this.metadataEncoding = characterSetResultsValue;
            this.errorMessageEncoding = characterSetResultsValue;
        }

        this.metadataCollationIndex = getCollationIndexForJavaEncoding(this.metadataEncoding, this.serverSession.getServerVersion());

        checkForCharsetMismatch();

        /**
         * Check if we need a CharsetEncoder for escaping codepoints that are
         * transformed to backslash (0x5c) in the connection encoding.
         */
        try {
            CharsetEncoder enc = Charset.forName(this.characterEncoding.getValue()).newEncoder();
            CharBuffer cbuf = CharBuffer.allocate(1);
            ByteBuffer bbuf = ByteBuffer.allocate(1);

            cbuf.put("\u00a5");
            cbuf.position(0);
            enc.encode(cbuf, bbuf, true);
            if (bbuf.get(0) == '\\') {
                this.requiresEscapingEncoder = true;
            } else {
                cbuf.clear();
                bbuf.clear();

                cbuf.put("\u20a9");
                cbuf.position(0);
                enc.encode(cbuf, bbuf, true);
                if (bbuf.get(0) == '\\') {
                    this.requiresEscapingEncoder = true;
                }
            }
        } catch (java.nio.charset.UnsupportedCharsetException ucex) {
            // fallback to String API
            byte bbuf[] = StringUtils.getBytes("\u00a5", this.characterEncoding.getValue());
            if (bbuf[0] == '\\') {
                this.requiresEscapingEncoder = true;
            } else {
                bbuf = StringUtils.getBytes("\u20a9", this.characterEncoding.getValue());
                if (bbuf[0] == '\\') {
                    this.requiresEscapingEncoder = true;
                }
            }
        }
    }

    private boolean characterSetNamesMatches(String mysqlEncodingName) {
        // set names is equivalent to character_set_client ..._results and ..._connection, but we set _results later, so don't check it here.
        return (mysqlEncodingName != null && mysqlEncodingName.equalsIgnoreCase(this.serverSession.getServerVariable(CHARACTER_SET_CLIENT))
                && mysqlEncodingName.equalsIgnoreCase(this.serverSession.getServerVariable(CHARACTER_SET_CONNECTION)));
    }

    /**
     * Get the server's default character set name according to collation index from server greeting,
     * or value of 'character_set_server' variable if there is no mapping for that index
     * 
     * @return MySQL charset name
     */
    public String getServerDefaultCharset() {
        String charset = getStaticMysqlCharsetNameForCollationIndex(this.sessionCollationIndex);
        return charset != null ? charset : this.serverSession.getServerVariable("character_set_server");
    }

    @Override
    public String getErrorMessageEncoding() {
        return this.errorMessageEncoding;
    }

    @Override
    public String getMetadataEncoding() {
        return this.metadataEncoding;
    }

    @Override
    public int getMetadataCollationIndex() {
        return this.metadataCollationIndex;
    }

    @Override
    public boolean getRequiresEscapingEncoder() {
        return this.requiresEscapingEncoder;
    }

    @Override
    public String getPasswordCharacterEncoding() {
        return getStaticJavaEncodingForCollationIndex(this.sessionCollationIndex);
    }

    /**
     * Builds the map needed for 4.1.0 and newer servers that maps field-level
     * charset/collation info to a java character encoding name.
     */
    private void buildCollationMapping() {

        Map<Integer, String> customCollationIndexToCollationName = null;
        Map<String, Integer> customCollationNameToCollationIndex = null;
        Map<Integer, String> customCollationIndexToCharsetName = null;
        Map<String, Integer> customCharsetNameToMblen = null;
        Map<String, String> customCharsetNameToJavaEncoding = new HashMap<>();
        Map<String, String> customJavaEncodingUcToCharsetName = new HashMap<>();
        Map<String, Integer> customCharsetNameToCollationIndex = new HashMap<>();
        Set<String> customMultibyteEncodings = new HashSet<>();

        String databaseURL = this.session.getHostInfo().getDatabaseUrl();

        if (this.cacheServerConfiguration.getValue()) {
            synchronized (customCollationIndexToCharsetNameByUrl) {
                customCollationIndexToCollationName = customCollationIndexToCollationNameByUrl.get(databaseURL);
                customCollationNameToCollationIndex = customCollationNameToCollationIndexByUrl.get(databaseURL);
                customCollationIndexToCharsetName = customCollationIndexToCharsetNameByUrl.get(databaseURL);
                customCharsetNameToMblen = customCharsetNameToMblenByUrl.get(databaseURL);
                customCharsetNameToJavaEncoding = customCharsetNameToJavaEncodingByUrl.get(databaseURL);
                customJavaEncodingUcToCharsetName = customJavaEncodingUcToCharsetNameByUrl.get(databaseURL);
                customCharsetNameToCollationIndex = customCharsetNameToCollationIndexByUrl.get(databaseURL);
                customMultibyteEncodings = customMultibyteEncodingsByUrl.get(databaseURL);
            }
        }

        if (customCollationIndexToCharsetName == null && this.session.getPropertySet().getBooleanProperty(PropertyKey.detectCustomCollations).getValue()) {
            customCollationIndexToCollationName = new HashMap<>();
            customCollationNameToCollationIndex = new HashMap<>();
            customCollationIndexToCharsetName = new HashMap<>();
            customCharsetNameToMblen = new HashMap<>();

            String customCharsetMapping = this.session.getPropertySet().getStringProperty(PropertyKey.customCharsetMapping).getValue();
            if (customCharsetMapping != null) {
                String[] pairs = customCharsetMapping.split(",");
                for (String pair : pairs) {
                    int keyEnd = pair.indexOf(":");
                    if (keyEnd > 0 && (keyEnd + 1) < pair.length()) {
                        String charset = pair.substring(0, keyEnd);
                        String encoding = pair.substring(keyEnd + 1);
                        customCharsetNameToJavaEncoding.put(charset, encoding);
                        customJavaEncodingUcToCharsetName.put(encoding.toUpperCase(Locale.ENGLISH), charset);
                    }
                }
            }

            ValueFactory<Integer> ivf = new IntegerValueFactory(this.session.getPropertySet());

            try {
                NativePacketPayload resultPacket = this.session.sendCommand(getCommandBuilder().buildComQuery(null,
                        "select c.COLLATION_NAME, c.CHARACTER_SET_NAME, c.ID, cs.MAXLEN, c.IS_DEFAULT='Yes' from INFORMATION_SCHEMA.COLLATIONS as c left join"
                                + " INFORMATION_SCHEMA.CHARACTER_SETS as cs on cs.CHARACTER_SET_NAME=c.CHARACTER_SET_NAME"),
                        false, 0);
                Resultset rs = this.session.getProtocol().readAllResults(-1, false, resultPacket, false, null, new ResultsetFactory(Type.FORWARD_ONLY, null));
                ValueFactory<String> svf = new StringValueFactory(this.session.getPropertySet());
                Row r;
                while ((r = rs.getRows().next()) != null) {
                    String collationName = r.getValue(0, svf);
                    String charsetName = r.getValue(1, svf);
                    int collationIndex = ((Number) r.getValue(2, ivf)).intValue();
                    int maxlen = ((Number) r.getValue(3, ivf)).intValue();
                    boolean isDefault = ((Number) r.getValue(4, ivf)).intValue() > 0;

                    if (collationIndex >= MAP_SIZE //
                            || !collationName.equals(getStaticCollationNameForCollationIndex(collationIndex))
                            || !charsetName.equals(getStaticMysqlCharsetNameForCollationIndex(collationIndex))) {
                        customCollationIndexToCollationName.put(collationIndex, collationName);
                        customCollationNameToCollationIndex.put(collationName, collationIndex);
                        customCollationIndexToCharsetName.put(collationIndex, charsetName);
                        if (isDefault) {
                            customCharsetNameToCollationIndex.put(charsetName, collationIndex);
                        } else {
                            customCharsetNameToCollationIndex.putIfAbsent(charsetName, collationIndex);
                        }

                    }

                    // if no static map for charsetName adding to custom map
                    if (getStaticMysqlCharsetByName(charsetName) == null) {
                        customCharsetNameToMblen.put(charsetName, maxlen);
                        if (maxlen > 1) {
                            String enc = customCharsetNameToJavaEncoding.get(charsetName);
                            if (enc != null) {
                                customMultibyteEncodings.add(enc.toUpperCase(Locale.ENGLISH));
                            }
                        }
                    }

                }
            } catch (IOException e) {
                throw ExceptionFactory.createException(e.getMessage(), e, this.session.getExceptionInterceptor());
            }

            if (this.cacheServerConfiguration.getValue()) {
                synchronized (customCollationIndexToCharsetNameByUrl) {
                    customCollationIndexToCollationNameByUrl.put(databaseURL, Collections.unmodifiableMap(customCollationIndexToCollationName));
                    customCollationNameToCollationIndexByUrl.put(databaseURL, Collections.unmodifiableMap(customCollationNameToCollationIndex));
                    customCollationIndexToCharsetNameByUrl.put(databaseURL, Collections.unmodifiableMap(customCollationIndexToCharsetName));
                    customCharsetNameToMblenByUrl.put(databaseURL, Collections.unmodifiableMap(customCharsetNameToMblen));
                    customCharsetNameToJavaEncodingByUrl.put(databaseURL, Collections.unmodifiableMap(customCharsetNameToJavaEncoding));
                    customJavaEncodingUcToCharsetNameByUrl.put(databaseURL, Collections.unmodifiableMap(customJavaEncodingUcToCharsetName));
                    customCharsetNameToCollationIndexByUrl.put(databaseURL, Collections.unmodifiableMap(customCharsetNameToCollationIndex));
                    customMultibyteEncodingsByUrl.put(databaseURL, Collections.unmodifiableSet(customMultibyteEncodings));
                }
            }
        }

        if (customCollationIndexToCharsetName != null) {
            this.collationIndexToCollationName = customCollationIndexToCollationName;
            this.collationNameToCollationIndex = customCollationNameToCollationIndex;
            this.collationIndexToCharsetName = customCollationIndexToCharsetName;
            this.charsetNameToMblen = customCharsetNameToMblen;
            this.charsetNameToJavaEncoding = customCharsetNameToJavaEncoding;
            this.javaEncodingUcToCharsetName = customJavaEncodingUcToCharsetName;
            this.charsetNameToCollationIndex = customCharsetNameToCollationIndex;
            this.multibyteEncodings = customMultibyteEncodings;
        }
    }

    @Override
    public Integer getCollationIndexForCollationName(String collationName) {
        Integer collationIndex = null;
        if (this.collationNameToCollationIndex == null || (collationIndex = this.collationNameToCollationIndex.get(collationName)) == null) {
            collationIndex = getStaticCollationIndexForCollationName(collationName);
        }
        return collationIndex;
    }

    @Override
    public String getCollationNameForCollationIndex(Integer collationIndex) {
        String collationName = null;
        if (collationIndex != null
                && (this.collationIndexToCollationName == null || (collationName = this.collationIndexToCollationName.get(collationIndex)) == null)) {
            collationName = getStaticCollationNameForCollationIndex(collationIndex);
        }
        return collationName;
    }

    @Override
    public String getMysqlCharsetNameForCollationIndex(Integer collationIndex) {
        String charset = null;
        if (this.collationIndexToCharsetName == null || (charset = this.collationIndexToCharsetName.get(collationIndex)) == null) {
            charset = getStaticMysqlCharsetNameForCollationIndex(collationIndex);
        }
        return charset;
    }

    @Override
    public String getJavaEncodingForCollationIndex(int collationIndex) {
        return getJavaEncodingForCollationIndex(collationIndex, this.characterEncoding.getValue());
    }

    public String getJavaEncodingForCollationIndex(Integer collationIndex, String fallBackJavaEncoding) {
        String encoding = null;
        String charset = null;
        if (collationIndex != NativeConstants.NO_CHARSET_INFO) {
            if (this.collationIndexToCharsetName != null && (charset = this.collationIndexToCharsetName.get(collationIndex)) != null) {
                encoding = getJavaEncodingForMysqlCharset(charset, fallBackJavaEncoding);
            }
            if (encoding == null) {
                encoding = getStaticJavaEncodingForCollationIndex(collationIndex, fallBackJavaEncoding);
            }
        }
        return encoding != null ? encoding : fallBackJavaEncoding;
    }

    public int getCollationIndexForJavaEncoding(String javaEncoding, ServerVersion version) {
        return getCollationIndexForMysqlCharsetName(getMysqlCharsetForJavaEncoding(javaEncoding, version));
    }

    public int getCollationIndexForMysqlCharsetName(String charsetName) {
        Integer index = null;
        if (this.charsetNameToCollationIndex == null || (index = this.charsetNameToCollationIndex.get(charsetName)) == null) {
            index = getStaticCollationIndexForMysqlCharsetName(charsetName);
        }
        return index;
    }

    public String getJavaEncodingForMysqlCharset(String mysqlCharsetName) {
        String encoding = null;
        if (this.charsetNameToJavaEncoding == null || (encoding = this.charsetNameToJavaEncoding.get(mysqlCharsetName)) == null) {
            encoding = getStaticJavaEncodingForMysqlCharset(mysqlCharsetName);
        }
        return encoding;
    }

    public String getJavaEncodingForMysqlCharset(String mysqlCharsetName, String javaEncoding) {
        String encoding = null;
        if (this.charsetNameToJavaEncoding == null || (encoding = this.charsetNameToJavaEncoding.get(mysqlCharsetName)) == null) {
            encoding = getStaticJavaEncodingForMysqlCharset(mysqlCharsetName, javaEncoding);
        }
        return encoding;
    }

    public String getMysqlCharsetForJavaEncoding(String javaEncoding, ServerVersion version) {
        String charset = null;
        if (this.javaEncodingUcToCharsetName == null || (charset = this.javaEncodingUcToCharsetName.get(javaEncoding.toUpperCase(Locale.ENGLISH))) == null) {
            charset = getStaticMysqlCharsetForJavaEncoding(javaEncoding, version);
        }
        return charset;
    }

    public boolean isImpermissibleCollation(int collationIndex) {
        String charsetName = null;
        if (this.collationIndexToCharsetName != null && (charsetName = this.collationIndexToCharsetName.get(collationIndex)) != null) {
            if (charsetName.equals(MYSQL_CHARSET_NAME_ucs2) || charsetName.equals(MYSQL_CHARSET_NAME_utf16) || charsetName.equals(MYSQL_CHARSET_NAME_utf16le)
                    || charsetName.equals(MYSQL_CHARSET_NAME_utf32)) {
                return true;
            }
        }
        return isStaticImpermissibleCollation(collationIndex);
    }

    public boolean isMultibyteCharset(String javaEncodingName) {
        if (this.multibyteEncodings != null && this.multibyteEncodings.contains(javaEncodingName.toUpperCase(Locale.ENGLISH))) {
            return true;
        }
        return isStaticMultibyteCharset(javaEncodingName);
    }

    @Override
    public int getMaxBytesPerChar(String javaCharsetName) {
        return getMaxBytesPerChar(null, javaCharsetName);
    }

    @Override
    public int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName) {
        String charset = null;
        if ((charset = getMysqlCharsetNameForCollationIndex(charsetIndex)) == null) {
            // if we didn't find charset name by index
            charset = getStaticMysqlCharsetForJavaEncoding(javaCharsetName, this.serverSession.getServerVersion());
        }
        Integer mblen = null;
        if (this.charsetNameToMblen == null || (mblen = this.charsetNameToMblen.get(charset)) == null) {
            mblen = getStaticMblen(charset);
        }
        return mblen != null ? mblen.intValue() : 1;
    }
}
