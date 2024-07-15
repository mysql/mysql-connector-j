/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.protocol.a;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.mysql.cj.Constants;
import com.mysql.cj.Messages;
import com.mysql.cj.callback.MysqlCallbackHandler;
import com.mysql.cj.callback.UsernameCallback;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.UnableToConnectException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.AuthenticationPlugin;
import com.mysql.cj.protocol.AuthenticationProvider;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.authentication.AuthenticationKerberosClient;
import com.mysql.cj.protocol.a.authentication.AuthenticationLdapSaslClientPlugin;
import com.mysql.cj.protocol.a.authentication.AuthenticationOciClient;
import com.mysql.cj.protocol.a.authentication.AuthenticationWebAuthnClient;
import com.mysql.cj.protocol.a.authentication.CachingSha2PasswordPlugin;
import com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin;
import com.mysql.cj.protocol.a.authentication.MysqlNativePasswordPlugin;
import com.mysql.cj.protocol.a.authentication.MysqlOldPasswordPlugin;
import com.mysql.cj.protocol.a.authentication.Sha256PasswordPlugin;
import com.mysql.cj.protocol.a.result.OkPacket;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;

public class NativeAuthenticationProvider implements AuthenticationProvider<NativePacketPayload> {

    private static final int AUTH_411_OVERHEAD = 33;
    private static final String NONE = "none";

    private String seed;
    private String username;
    private String password;
    private String database;
    private boolean useConnectWithDb;

    private ExceptionInterceptor exceptionInterceptor;
    private PropertySet propertySet;

    private Protocol<NativePacketPayload> protocol;

    /**
     * Contains instances of authentication plugins that implements {@link AuthenticationPlugin} interface. Key values are MySQL protocol plugin names, for
     * example "mysql_native_password" and "mysql_old_password" for built-in plugins.
     */
    private Map<String, AuthenticationPlugin<NativePacketPayload>> authenticationPlugins = null;
    /**
     * Protocol name of default authentication plugin in client
     */
    private String clientDefaultAuthenticationPluginName = null;
    /**
     * Was the client default authentication plugin explicitly set?
     */
    private boolean clientDefaultAuthenticationPluginExplicitelySet = false;
    /**
     * Protocol name of default authentication plugin in server
     */
    private String serverDefaultAuthenticationPluginName = null;

    /**
     * A callback for updating the username from the authentication plugin.
     */
    private MysqlCallbackHandler callbackHandler = (cb) -> {
        if (cb instanceof UsernameCallback) {
            this.username = ((UsernameCallback) cb).getUsername();
        }
    };

    public NativeAuthenticationProvider() {
    }

    @Override
    public void init(Protocol<NativePacketPayload> prot, PropertySet propSet, ExceptionInterceptor excInterceptor) {
        this.protocol = prot;
        this.propertySet = propSet;
        this.exceptionInterceptor = excInterceptor;
    }

    /**
     * Initialize communications with the MySQL server. Handles logging on, and
     * handling initial connection errors.
     *
     * @param user
     *            user name
     * @param pass
     *            password
     * @param db
     *            database name
     */
    @Override
    public void connect(String user, String pass, String db) {
        ServerSession sessState = this.protocol.getServerSession();
        this.username = user;
        this.password = pass;
        this.database = db;

        NativeCapabilities capabilities = (NativeCapabilities) sessState.getCapabilities();
        NativePacketPayload buf = capabilities.getInitialHandshakePacket();

        SslMode sslMode = this.propertySet.<SslMode>getEnumProperty(PropertyKey.sslMode).getValue();
        int capabilityFlags = capabilities.getCapabilityFlags();
        if ((capabilityFlags & NativeServerSession.CLIENT_SSL) == 0 && sslMode != SslMode.DISABLED && sslMode != SslMode.PREFERRED) {
            // check SSL availability
            throw ExceptionFactory.createException(UnableToConnectException.class, Messages.getString("MysqlIO.15"), getExceptionInterceptor());
        } else if ((capabilityFlags & NativeServerSession.CLIENT_SECURE_CONNECTION) == 0) {
            // TODO: better messaging
            throw ExceptionFactory.createException(UnableToConnectException.class, "CLIENT_SECURE_CONNECTION is required", getExceptionInterceptor());
        } else if ((capabilityFlags & NativeServerSession.CLIENT_PLUGIN_AUTH) == 0) {
            // TODO: better messaging
            throw ExceptionFactory.createException(UnableToConnectException.class, "CLIENT_PLUGIN_AUTH is required", getExceptionInterceptor());
        }

        sessState.setStatusFlags(capabilities.getStatusFlags()); // read status flags (2 bytes)
        int authPluginDataLength = capabilities.getAuthPluginDataLength();

        StringBuilder fullSeed = new StringBuilder(authPluginDataLength > 0 ? authPluginDataLength : NativeConstants.SEED_LENGTH);
        fullSeed.append(capabilities.getSeed()); // read auth-plugin-data-part-1 (string[8])
        fullSeed.append(authPluginDataLength > 0 ?  // read string[$len] auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
                buf.readString(StringLengthDataType.STRING_FIXED, "ASCII", authPluginDataLength - 8) : buf.readString(StringSelfDataType.STRING_TERM, "ASCII"));
        this.seed = fullSeed.toString();

        this.useConnectWithDb = this.database != null && this.database.length() > 0
                && !this.propertySet.getBooleanProperty(PropertyKey.createDatabaseIfNotExist).getValue();

        long clientParam = capabilityFlags & NativeServerSession.CLIENT_LONG_PASSWORD //
                | (this.propertySet.getBooleanProperty(PropertyKey.useAffectedRows).getValue() ? //
                        0 : capabilityFlags & NativeServerSession.CLIENT_FOUND_ROWS) //
                | capabilityFlags & NativeServerSession.CLIENT_LONG_FLAG //
                | (this.useConnectWithDb ? capabilityFlags & NativeServerSession.CLIENT_CONNECT_WITH_DB : 0) //
                | (this.propertySet.getBooleanProperty(PropertyKey.useCompression).getValue() ? //
                        capabilityFlags & NativeServerSession.CLIENT_COMPRESS : 0) //
                | (this.propertySet.getBooleanProperty(PropertyKey.allowLoadLocalInfile).getValue()
                        || this.propertySet.getStringProperty(PropertyKey.allowLoadLocalInfileInPath).isExplicitlySet() ? //
                                capabilityFlags & NativeServerSession.CLIENT_LOCAL_FILES : 0) //
                | capabilityFlags & NativeServerSession.CLIENT_PROTOCOL_41 //
                | (this.propertySet.getBooleanProperty(PropertyKey.interactiveClient).getValue() ? //
                        capabilityFlags & NativeServerSession.CLIENT_INTERACTIVE : 0) //
                | (this.propertySet.<SslMode>getEnumProperty(PropertyKey.sslMode).getValue() != SslMode.DISABLED ? //
                        capabilityFlags & NativeServerSession.CLIENT_SSL : 0) //
                | capabilityFlags & NativeServerSession.CLIENT_TRANSACTIONS // Required to get server status values.
                | NativeServerSession.CLIENT_SECURE_CONNECTION //
                | (this.propertySet.getBooleanProperty(PropertyKey.allowMultiQueries).getValue() ? //
                        capabilityFlags & NativeServerSession.CLIENT_MULTI_STATEMENTS : 0) //
                | capabilityFlags & NativeServerSession.CLIENT_MULTI_RESULTS // Always allow multiple result sets.
                | capabilityFlags & NativeServerSession.CLIENT_PS_MULTI_RESULTS // Always allow multiple result sets for SSPS.
                | NativeServerSession.CLIENT_PLUGIN_AUTH //
                | (NONE.equals(this.propertySet.getStringProperty(PropertyKey.connectionAttributes).getValue()) ? //
                        0 : capabilityFlags & NativeServerSession.CLIENT_CONNECT_ATTRS) //
                | capabilityFlags & NativeServerSession.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA //
                | (this.propertySet.getBooleanProperty(PropertyKey.disconnectOnExpiredPasswords).getValue() ? //
                        0 : capabilityFlags & NativeServerSession.CLIENT_CAN_HANDLE_EXPIRED_PASSWORD) //
                | (this.propertySet.getBooleanProperty(PropertyKey.trackSessionState).getValue() ? //
                        capabilityFlags & NativeServerSession.CLIENT_SESSION_TRACK : 0) //
                | capabilityFlags & NativeServerSession.CLIENT_DEPRECATE_EOF //
                | capabilityFlags & NativeServerSession.CLIENT_QUERY_ATTRIBUTES //
                | capabilityFlags & NativeServerSession.CLIENT_MULTI_FACTOR_AUTHENTICATION;

        sessState.setClientParam(clientParam);

        /* First, negotiate SSL connection */
        if ((clientParam & NativeServerSession.CLIENT_SSL) != 0) {
            this.protocol.negotiateSSLConnection();
        }

        if (buf.isOKPacket()) {
            throw ExceptionFactory.createException(Messages.getString("AuthenticationProvider.UnexpectedAuthenticationApproval"), getExceptionInterceptor());
        }

        proceedHandshakeWithPluggableAuthentication(buf);

        this.password = null;
    }

    /**
     * Fill the authentication plugins map.
     *
     * Starts by filling the map with instances of the built-in authentication plugins. Then creates instances of plugins listed in the "authenticationPlugins"
     * connection property and adds them to the map too.
     *
     * The key for the map entry is got by {@link AuthenticationPlugin#getProtocolPluginName()} thus it is possible to replace built-in plugins with custom
     * implementations. To do it, the custom plugin should return one of the values "mysql_native_password", "mysql_clear_password", "sha256_password",
     * "caching_sha2_password", "mysql_old_password", "authentication_ldap_sasl_client" or "authentication_kerberos_client" from its own getProtocolPluginName()
     * method.
     */
    @SuppressWarnings("unchecked")
    private void loadAuthenticationPlugins() {
        // default plugin
        RuntimeProperty<String> defaultAuthenticationPluginProp = this.propertySet.getStringProperty(PropertyKey.defaultAuthenticationPlugin);
        String defaultAuthenticationPluginValue = defaultAuthenticationPluginProp.getValue();
        if (defaultAuthenticationPluginValue == null || "".equals(defaultAuthenticationPluginValue.trim())) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    Messages.getString("AuthenticationProvider.BadDefaultAuthenticationPlugin", new Object[] { defaultAuthenticationPluginValue }),
                    getExceptionInterceptor());
        }

        // disabled plugins
        String disabledPlugins = this.propertySet.getStringProperty(PropertyKey.disabledAuthenticationPlugins).getValue();
        List<String> disabledAuthenticationPlugins;
        if (disabledPlugins != null && !"".equals(disabledPlugins)) {
            disabledAuthenticationPlugins = StringUtils.split(disabledPlugins, ",", true);
        } else {
            disabledAuthenticationPlugins = Collections.EMPTY_LIST;
        }

        this.authenticationPlugins = new HashMap<>();
        List<AuthenticationPlugin<NativePacketPayload>> pluginsToInit = new LinkedList<>();

        // built-in plugins
        pluginsToInit.add(new MysqlNativePasswordPlugin());
        pluginsToInit.add(new MysqlClearPasswordPlugin());
        pluginsToInit.add(new Sha256PasswordPlugin());
        pluginsToInit.add(new CachingSha2PasswordPlugin());
        pluginsToInit.add(new MysqlOldPasswordPlugin());
        pluginsToInit.add(new AuthenticationLdapSaslClientPlugin());
        pluginsToInit.add(new AuthenticationKerberosClient());
        pluginsToInit.add(new AuthenticationOciClient());
        pluginsToInit.add(new AuthenticationWebAuthnClient());

        // plugins from authenticationPluginClasses connection parameter
        String authenticationPluginClasses = this.propertySet.getStringProperty(PropertyKey.authenticationPlugins).getValue();
        if (authenticationPluginClasses != null && !"".equals(authenticationPluginClasses.trim())) {
            List<String> pluginsToCreate = StringUtils.split(authenticationPluginClasses, ",", true);
            for (String className : pluginsToCreate) {
                pluginsToInit.add(Util.getInstance(AuthenticationPlugin.class, className, null, null, getExceptionInterceptor()));
            }
        }

        // add plugin instances
        boolean defaultFound = false;
        for (AuthenticationPlugin<NativePacketPayload> plugin : pluginsToInit) {
            String pluginProtocolName = plugin.getProtocolPluginName();
            String pluginClassName = plugin.getClass().getName();
            boolean disabledByProtocolName = disabledAuthenticationPlugins.contains(pluginProtocolName);
            boolean disabledByClassName = disabledAuthenticationPlugins.contains(pluginClassName);

            if (disabledByProtocolName || disabledByClassName) {
                // check if the default plugin is disabled
                if (!defaultFound
                        && (defaultAuthenticationPluginValue.equals(pluginProtocolName) || defaultAuthenticationPluginValue.equals(pluginClassName))) {
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("AuthenticationProvider.BadDisabledAuthenticationPlugin",
                                    new Object[] { disabledByClassName ? pluginClassName : pluginProtocolName }),
                            getExceptionInterceptor());
                }
            } else {
                this.authenticationPlugins.put(pluginProtocolName, plugin);
                if (!defaultFound
                        && (defaultAuthenticationPluginValue.equals(pluginProtocolName) || defaultAuthenticationPluginValue.equals(pluginClassName))) {
                    this.clientDefaultAuthenticationPluginName = pluginProtocolName;
                    this.clientDefaultAuthenticationPluginExplicitelySet = defaultAuthenticationPluginProp.isExplicitlySet();
                    defaultFound = true;
                }
            }
        }

        // check if the default plugin is listed
        if (!defaultFound) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    Messages.getString("AuthenticationProvider.DefaultAuthenticationPluginIsNotListed", new Object[] { defaultAuthenticationPluginValue }),
                    getExceptionInterceptor());
        }
    }

    /**
     * Get an authentication plugin instance from the authentication plugins map by pluginName key. If such plugin is found, its method
     * {@link AuthenticationPlugin#isReusable()} is called and if the value returned is <code>false</code> then a new instance of the plugin is returned
     * otherwise the instance that already exists is returned.
     *
     * If plugin is not found method returns null, in such case the subsequent behavior
     * of handshake process depends on type of last packet received from server:
     * if it was Auth Challenge Packet then handshake will proceed with default plugin,
     * if it was Auth Method Switch Request Packet then handshake will be interrupted with exception.
     *
     * @param pluginName
     *            mysql protocol plugin names, for example "mysql_native_password" and "mysql_old_password" for built-in plugins
     * @return null if plugin is not found or authentication plugin instance initialized with current connection properties
     */
    @SuppressWarnings("unchecked")
    private AuthenticationPlugin<NativePacketPayload> getAuthenticationPlugin(String pluginName) {
        AuthenticationPlugin<NativePacketPayload> plugin = this.authenticationPlugins.get(pluginName);

        if (plugin == null) {
            return null;
        }

        if (!plugin.isReusable()) {
            try {
                plugin = plugin.getClass().newInstance();
            } catch (Throwable t) {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("AuthenticationProvider.BadAuthenticationPlugin", new Object[] { plugin.getClass().getName() }), t,
                        getExceptionInterceptor());
            }
        }

        plugin.init(this.protocol, this.callbackHandler);
        return plugin;
    }

    /**
     * Check if given plugin requires confidentiality, but connection is without SSL
     *
     * @param plugin
     *            {@link AuthenticationPlugin}
     */
    private void checkConfidentiality(AuthenticationPlugin<?> plugin) {
        if (plugin.requiresConfidentiality() && !this.protocol.getSocketConnection().isSSLEstablished()) {
            throw ExceptionFactory.createException(
                    Messages.getString("AuthenticationProvider.AuthenticationPluginRequiresSSL", new Object[] { plugin.getProtocolPluginName() }),
                    getExceptionInterceptor());
        }
    }

    /**
     * Performs an authentication handshake to authorize connection to a given database as a given MySQL user.
     * This can happen upon initial connection to the server, after receiving Auth Challenge Packet, or
     * at any moment during the connection life-time via a Change User request.
     *
     * This method will use registered authentication plugins as requested by the server.
     *
     * @param challenge
     *            the Auth Challenge Packet received from server if
     *            this method is used during the initial connection.
     *            Otherwise null.
     */
    private void proceedHandshakeWithPluggableAuthentication(final NativePacketPayload challenge) {
        ServerSession serverSession = this.protocol.getServerSession();

        if (this.authenticationPlugins == null) {
            loadAuthenticationPlugins();
        }

        boolean forChangeUser = true;
        if (challenge != null) {
            this.serverDefaultAuthenticationPluginName = challenge.readString(StringSelfDataType.STRING_TERM, "ASCII");
            forChangeUser = false;
        }

        serverSession.getCharsetSettings().configurePreHandshake(forChangeUser);

        /*
         * Select the initial plugin:
         * Choose the client-side default authentication plugin, if explicitely specified, otherwise choose the server-side default authentication plugin.
         */
        String pluginName;
        if (this.clientDefaultAuthenticationPluginExplicitelySet) {
            pluginName = this.clientDefaultAuthenticationPluginName;
        } else {
            pluginName = this.serverDefaultAuthenticationPluginName != null ? this.serverDefaultAuthenticationPluginName
                    : this.clientDefaultAuthenticationPluginName;
        }
        AuthenticationPlugin<NativePacketPayload> plugin = getAuthenticationPlugin(pluginName);

        if (plugin == null) {
            /* Use default if there is no plugin for pluginName. */
            pluginName = this.clientDefaultAuthenticationPluginName;
            plugin = getAuthenticationPlugin(pluginName);
        }

        boolean skipPassword = false;
        if (pluginName.equals(Sha256PasswordPlugin.PLUGIN_NAME) && !pluginName.equals(this.clientDefaultAuthenticationPluginName)
                && !this.protocol.getSocketConnection().isSSLEstablished()
                && this.propertySet.getStringProperty(PropertyKey.serverRSAPublicKeyFile).getValue() == null
                && !this.propertySet.getBooleanProperty(PropertyKey.allowPublicKeyRetrieval).getValue()) {
            /*
             * Fall back to client default if plugin is 'sha256_password' but required conditions for this to work aren't met.
             * If client default is other than 'sha256_password', this will result in an immediate authentication switch request, allowing for other plugins to
             * authenticate successfully. If client default is 'sha256_password' then the authentication will fail as expected. In both cases user's password
             * won't be sent to avoid subjecting it to lesser security levels.
             */
            plugin = getAuthenticationPlugin(this.clientDefaultAuthenticationPluginName);
            skipPassword = true;
        }

        checkConfidentiality(plugin);

        // Servers not affected by Bug#70865 expect the Change User Request containing a correct answer to seed sent by the server during the initial handshake,
        // thus we reuse it here. Servers affected by Bug#70865 will just ignore it and send the Auth Switch.
        NativePacketPayload fromServer = new NativePacketPayload(StringUtils.getBytes(this.seed));
        String sourceOfAuthData = this.serverDefaultAuthenticationPluginName;

        NativePacketPayload lastSent = null;
        NativePacketPayload lastReceived = challenge;
        ArrayList<NativePacketPayload> toServer = new ArrayList<>();

        boolean firstPacket = true;

        // MFA authentication factor
        int mfaNthFactor = 1;

        /* Max iterations number */
        int counter = 100;
        while (0 < counter--) {
            /*
             * call plugin
             */
            plugin.setAuthenticationParameters(this.username, skipPassword ? null : getNthFactorPassword(mfaNthFactor));
            plugin.setSourceOfAuthData(sourceOfAuthData);
            plugin.nextAuthenticationStep(fromServer, toServer);

            /*
             * send response to server
             */
            if (firstPacket) {
                NativePacketPayload authData = toServer.isEmpty() ? new NativePacketPayload(0) : toServer.get(0);
                if (forChangeUser) {
                    // write COM_CHANGE_USER Packet
                    lastSent = createChangeUserPacket(serverSession, plugin.getProtocolPluginName(), authData);
                    this.protocol.send(lastSent, lastSent.getPosition());
                } else {
                    // write HandshakeResponse packet
                    lastSent = createHandshakeResponsePacket(serverSession, plugin.getProtocolPluginName(), authData);
                    this.protocol.send(lastSent, lastSent.getPosition());
                }
                firstPacket = false;
            } else if (!toServer.isEmpty()) {
                // write AuthSwitchResponse packet or raw packet(s)
                toServer.forEach(b -> this.protocol.send(b, b.getPayloadLength()));
            }

            /*
             * read packet from server
             */
            lastReceived = this.protocol.checkErrorMessage();

            if (lastReceived.isOKPacket()) {
                // read OK packet
                OkPacket ok = OkPacket.parse(lastReceived, serverSession);
                serverSession.setStatusFlags(ok.getStatusFlags(), true);
                serverSession.getServerSessionStateController().setSessionStateChanges(ok.getSessionStateChanges());

                // authentication complete
                plugin.destroy();
                break;

            } else if (lastReceived.isAuthMethodSwitchRequestPacket()) {
                // read AuthSwitchRequest Packet
                skipPassword = false;
                pluginName = lastReceived.readString(StringSelfDataType.STRING_TERM, "ASCII");
                if (plugin.getProtocolPluginName().equals(pluginName)) {
                    plugin.reset(); // just reset the current one
                } else {
                    // get new plugin
                    plugin.destroy();
                    plugin = getAuthenticationPlugin(pluginName);
                    if (plugin == null) {
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("AuthenticationProvider.BadAuthenticationPlugin", new Object[] { pluginName }), getExceptionInterceptor());
                    }
                }

                checkConfidentiality(plugin);
                fromServer = new NativePacketPayload(lastReceived.readBytes(StringSelfDataType.STRING_EOF));

            } else if (lastReceived.isAuthNextFactorPacket()) {
                // authentication not done yet, there's another MFA iteration
                mfaNthFactor++;
                skipPassword = false;
                pluginName = lastReceived.readString(StringSelfDataType.STRING_TERM, "ASCII");
                if (plugin.getProtocolPluginName().equals(pluginName)) {
                    plugin.reset(); // just reset the current one
                } else {
                    // get new plugin
                    plugin.destroy();
                    plugin = getAuthenticationPlugin(pluginName);
                    if (plugin == null) {
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("AuthenticationProvider.BadAuthenticationPlugin", new Object[] { pluginName }), getExceptionInterceptor());
                    }
                }

                checkConfidentiality(plugin);
                fromServer = new NativePacketPayload(lastReceived.readBytes(StringSelfDataType.STRING_EOF));

            } else {
                // read raw (from AuthMoreData) packet
                if (!this.protocol.versionMeetsMinimum(5, 5, 16)) {
                    lastReceived.setPosition(lastReceived.getPosition() - 1);
                }
                fromServer = new NativePacketPayload(lastReceived.readBytes(StringSelfDataType.STRING_EOF));
            }

            sourceOfAuthData = pluginName;
        }

        if (counter == 0) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    Messages.getString("CommunicationsException.TooManyAuthenticationPluginNegotiations"), getExceptionInterceptor());
        }

        this.protocol.afterHandshake();

        if (!this.useConnectWithDb) {
            this.protocol.changeDatabase(this.database);
        }
    }

    private String getNthFactorPassword(int nthFactor) {
        switch (nthFactor) {
            case 1:
                return this.password == null ? this.protocol.getPropertySet().getStringProperty(PropertyKey.password1).getValue() : this.password;
            case 2:
                return this.protocol.getPropertySet().getStringProperty(PropertyKey.password2).getValue();
            case 3:
                return this.protocol.getPropertySet().getStringProperty(PropertyKey.password3).getValue();
            default:
                return null;
        }
    }

    private Map<String, String> getConnectionAttributesMap(String attStr) {
        Map<String, String> attMap = new HashMap<>();

        if (attStr != null) {
            String[] pairs = attStr.split(",");
            for (String pair : pairs) {
                int keyEnd = pair.indexOf(":");
                if (keyEnd > 0 && keyEnd + 1 < pair.length()) {
                    attMap.put(pair.substring(0, keyEnd), pair.substring(keyEnd + 1));
                }
            }
        }

        // Leaving disabled until standard values are defined
        // props.setProperty("_os", NonRegisteringDriver.OS);
        // props.setProperty("_platform", NonRegisteringDriver.PLATFORM);
        attMap.put("_client_name", Constants.CJ_NAME);
        attMap.put("_client_version", Constants.CJ_VERSION);
        attMap.put("_runtime_vendor", Constants.JVM_VENDOR);
        attMap.put("_runtime_version", Constants.JVM_VERSION);
        attMap.put("_client_license", Constants.CJ_LICENSE);

        return attMap;
    }

    private void appendConnectionAttributes(NativePacketPayload buf, String attributes, String enc) {
        NativePacketPayload lb = new NativePacketPayload(100);
        Map<String, String> attMap = getConnectionAttributesMap(attributes);

        for (String key : attMap.keySet()) {
            lb.writeBytes(StringSelfDataType.STRING_LENENC, StringUtils.getBytes(key, enc));
            lb.writeBytes(StringSelfDataType.STRING_LENENC, StringUtils.getBytes(attMap.get(key), enc));
        }

        buf.writeInteger(IntegerDataType.INT_LENENC, lb.getPosition());
        buf.writeBytes(StringLengthDataType.STRING_FIXED, lb.getByteBuffer(), 0, lb.getPosition());
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    /**
     * Re-authenticates as the given user and password
     *
     * @param user
     *            user name
     * @param pass
     *            password
     * @param db
     *            database name
     */
    @Override
    public void changeUser(String user, String pass, String db) {
        this.username = user;
        this.password = pass;
        this.database = db;
        proceedHandshakeWithPluggableAuthentication(null);
        this.password = null;
    }

    private NativePacketPayload createHandshakeResponsePacket(ServerSession serverSession, String pluginName, NativePacketPayload authData) {
        long clientParam = serverSession.getClientParam();
        int collationIndex = serverSession.getCharsetSettings().configurePreHandshake(false);
        String enc = serverSession.getCharsetSettings().getPasswordCharacterEncoding();

        int userLength = this.username == null ? 0 : this.username.length();
        NativePacketPayload last_sent = new NativePacketPayload(AUTH_411_OVERHEAD + 7 //
                + 48                                                                  // passwordLength
                + 3 * userLength                                                    // userLength
                + (this.useConnectWithDb ? 3 * this.database.length() : 0)            // databaseLength
        );
        last_sent.writeInteger(IntegerDataType.INT4, clientParam);
        last_sent.writeInteger(IntegerDataType.INT4, NativeConstants.MAX_PACKET_SIZE);
        last_sent.writeInteger(IntegerDataType.INT1, collationIndex);
        last_sent.writeBytes(StringLengthDataType.STRING_FIXED, new byte[23]);   // Set of bytes reserved for future use.

        // User/Password data
        last_sent.writeBytes(StringSelfDataType.STRING_TERM, StringUtils.getBytes(this.username, enc));
        if ((clientParam & NativeServerSession.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
            // send lenenc-int length of auth-response and string[n] auth-response
            last_sent.writeBytes(StringSelfDataType.STRING_LENENC, authData.readBytes(StringSelfDataType.STRING_EOF));
        } else {
            // send 1 byte length of auth-response and string[n] auth-response
            last_sent.writeInteger(IntegerDataType.INT1, authData.getPayloadLength());
            last_sent.writeBytes(StringSelfDataType.STRING_EOF, authData.getByteBuffer(), 0, authData.getPayloadLength());
        }

        if (this.useConnectWithDb) {
            last_sent.writeBytes(StringSelfDataType.STRING_TERM, StringUtils.getBytes(this.database, enc));
        }

        last_sent.writeBytes(StringSelfDataType.STRING_TERM, StringUtils.getBytes(pluginName, enc));

        // connection attributes
        if ((clientParam & NativeServerSession.CLIENT_CONNECT_ATTRS) != 0) {
            appendConnectionAttributes(last_sent, this.propertySet.getStringProperty(PropertyKey.connectionAttributes).getValue(), enc);
        }
        return last_sent;
    }

    private NativePacketPayload createChangeUserPacket(ServerSession serverSession, String pluginName, NativePacketPayload authData) {
        // write Auth Response Packet
        long clientParam = serverSession.getClientParam();
        int collationIndex = serverSession.getCharsetSettings().configurePreHandshake(false);
        String enc = serverSession.getCharsetSettings().getPasswordCharacterEncoding();

        NativePacketPayload last_sent = new NativePacketPayload(AUTH_411_OVERHEAD + 7 //
                + 48                // passwordLength
                + 3 * this.username.length() // userLength
                + (this.useConnectWithDb ? 3 * this.database.length() : 1)   // databaseLength
                + 1);
        last_sent.writeInteger(IntegerDataType.INT1, NativeConstants.COM_CHANGE_USER);

        // User/Password data
        last_sent.writeBytes(StringSelfDataType.STRING_TERM, StringUtils.getBytes(this.username, enc));
        // 'auth-response-len' is limited to one Byte but, in case of success, COM_CHANGE_USER will be followed by an AuthSwitchRequest anyway
        if (authData.getPayloadLength() < 256) {
            // non-mysql servers may use this information to authenticate without requiring another round-trip
            last_sent.writeInteger(IntegerDataType.INT1, authData.getPayloadLength());
            last_sent.writeBytes(StringSelfDataType.STRING_EOF, authData.getByteBuffer(), 0, authData.getPayloadLength());
        } else {
            last_sent.writeInteger(IntegerDataType.INT1, 0);
        }

        if (this.useConnectWithDb) {
            last_sent.writeBytes(StringSelfDataType.STRING_TERM, StringUtils.getBytes(this.database, enc));
        } else {
            /* For empty database */
            last_sent.writeInteger(IntegerDataType.INT1, 0);
        }

        last_sent.writeInteger(IntegerDataType.INT1, collationIndex);
        last_sent.writeInteger(IntegerDataType.INT1, 0); // two (little-endian) bytes for charset in this packet

        // plugin name
        last_sent.writeBytes(StringSelfDataType.STRING_TERM, StringUtils.getBytes(pluginName, enc));

        // connection attributes
        if ((clientParam & NativeServerSession.CLIENT_CONNECT_ATTRS) != 0) {
            appendConnectionAttributes(last_sent, this.propertySet.getStringProperty(PropertyKey.connectionAttributes).getValue(), enc);
        }
        return last_sent;
    }

}
