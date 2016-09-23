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

package com.mysql.cj.mysqla.authentication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.mysql.cj.api.authentication.AuthenticationProvider;
import com.mysql.cj.api.conf.ModifiableProperty;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.api.io.ServerSession;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.api.mysqla.authentication.AuthenticationPlugin;
import com.mysql.cj.api.mysqla.io.NativeProtocol;
import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringLengthDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.core.Constants;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.io.Buffer;
import com.mysql.cj.mysqla.io.MysqlaCapabilities;
import com.mysql.cj.mysqla.io.MysqlaServerSession;

public class MysqlaAuthenticationProvider implements AuthenticationProvider {

    protected static final int AUTH_411_OVERHEAD = 33;
    private static final String NONE = "none";

    protected String seed;
    private boolean useConnectWithDb;

    private ExceptionInterceptor exceptionInterceptor;
    private PropertySet propertySet;

    private NativeProtocol protocol;

    private Log log;

    public MysqlaAuthenticationProvider(Log log) {
        this.log = log;
    }

    @Override
    public void init(Protocol prot, PropertySet propSet, ExceptionInterceptor excInterceptor) {
        this.protocol = (NativeProtocol) prot;
        this.propertySet = propSet;
        this.exceptionInterceptor = excInterceptor;
    }

    /**
     * Initialize communications with the MySQL server. Handles logging on, and
     * handling initial connection errors.
     * 
     * @param sessState
     *            The session state object. It's intended to be updated from the handshake
     * @param user
     * @param password
     * @param database
     */
    @Override
    public void connect(ServerSession sessState, String user, String password, String database) {
        long clientParam = sessState.getClientParam();

        MysqlaCapabilities capabilities = (MysqlaCapabilities) sessState.getCapabilities();

        PacketPayload buf = capabilities.getInitialHandshakePacket();

        // read auth-plugin-data-part-1 (string[8])
        this.seed = capabilities.getSeed();

        // read character set (1 byte)
        sessState.setServerDefaultCollationIndex(capabilities.getServerDefaultCollationIndex());
        // read status flags (2 bytes)
        sessState.setStatusFlags(capabilities.getStatusFlags());

        int capabilityFlags = capabilities.getCapabilityFlags();

        if ((capabilityFlags & MysqlaServerSession.CLIENT_SECURE_CONNECTION) != 0) {
            clientParam |= MysqlaServerSession.CLIENT_SECURE_CONNECTION;
            String seedPart2;
            StringBuilder newSeed;
            int authPluginDataLength = capabilities.getAuthPluginDataLength();

            // read string[$len] auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
            if (authPluginDataLength > 0) {
                // TODO: disabled the following check for further clarification
                //                  if (this.authPluginDataLength < 21) {
                //                      forceClose();
                //                      throw SQLError.createSQLException(Messages.getString("MysqlIO.103"), 
                //                          SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, getExceptionInterceptor());
                //                  }
                seedPart2 = buf.readString(StringLengthDataType.STRING_FIXED, "ASCII", authPluginDataLength - 8);
                newSeed = new StringBuilder(authPluginDataLength);
            } else {
                seedPart2 = buf.readString(StringSelfDataType.STRING_TERM, "ASCII");
                newSeed = new StringBuilder(MysqlaConstants.SEED_LENGTH);
            }
            newSeed.append(this.seed);
            newSeed.append(seedPart2);
            this.seed = newSeed.toString();
        } else {
            // TODO: better messaging
            this.protocol.rejectConnection("CLIENT_SECURE_CONNECTION is required");
        }

        if (((capabilityFlags & MysqlaServerSession.CLIENT_COMPRESS) != 0)
                && this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useCompression).getValue()) {
            clientParam |= MysqlaServerSession.CLIENT_COMPRESS;
        }

        this.useConnectWithDb = (database != null) && (database.length() > 0)
                && !this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_createDatabaseIfNotExist).getValue();

        if (this.useConnectWithDb) {
            clientParam |= MysqlaServerSession.CLIENT_CONNECT_WITH_DB;
        }

        // Changing SSL defaults for 5.7+ server: useSSL=true, requireSSL=false, verifyServerCertificate=false

        ModifiableProperty<Boolean> useSSL = this.propertySet.<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_useSSL);
        if (this.protocol.versionMeetsMinimum(5, 7, 0) && !useSSL.getValue() && !useSSL.isExplicitlySet()) {
            useSSL.setValue(true);
            this.propertySet.getModifiableProperty(PropertyDefinitions.PNAME_verifyServerCertificate).setValue(false);
            if (this.log != null) {
                this.log.logWarn(Messages.getString("MysqlIO.SSLWarning"));
            }
        }

        // check SSL availability
        if (((capabilityFlags & MysqlaServerSession.CLIENT_SSL) == 0) && useSSL.getValue()) {
            if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_requireSSL).getValue()) {
                this.protocol.rejectConnection(Messages.getString("MysqlIO.15"));
            }

            useSSL.setValue(false);
        }

        if ((capabilityFlags & MysqlaServerSession.CLIENT_LONG_FLAG) != 0) {
            clientParam |= MysqlaServerSession.CLIENT_LONG_FLAG;
            sessState.setHasLongColumnInfo(true);
        }

        // return FOUND rows
        if (!this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useAffectedRows).getValue()) {
            clientParam |= MysqlaServerSession.CLIENT_FOUND_ROWS;
        }

        if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_allowLoadLocalInfile).getValue()) {
            clientParam |= MysqlaServerSession.CLIENT_LOCAL_FILES;
        }

        if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_interactiveClient).getValue()) {
            clientParam |= MysqlaServerSession.CLIENT_INTERACTIVE;
        }

        if ((capabilityFlags & MysqlaServerSession.CLIENT_SESSION_TRACK) != 0) {
            // TODO MYSQLCONNJ-437
            // clientParam |= MysqlaServerSession.CLIENT_SESSION_TRACK;
        }

        if ((capabilityFlags & MysqlaServerSession.CLIENT_DEPRECATE_EOF) != 0) {
            clientParam |= MysqlaServerSession.CLIENT_DEPRECATE_EOF;
        }

        //
        // switch to pluggable authentication if available
        //
        if ((capabilityFlags & MysqlaServerSession.CLIENT_PLUGIN_AUTH) != 0) {
            sessState.setClientParam(clientParam);
            proceedHandshakeWithPluggableAuthentication(sessState, user, password, database, buf);
        } else {
            // TODO: better messaging
            this.protocol.rejectConnection("CLIENT_PLUGIN_AUTH is required");
        }

    }

    /**
     * Contains instances of authentication plugins which implements {@link AuthenticationPlugin} interface. Key values are mysql
     * protocol plugin names, for example "mysql_native_password" and
     * "mysql_old_password" for built-in plugins.
     */
    private Map<String, AuthenticationPlugin> authenticationPlugins = null;
    /**
     * Contains names of classes or mechanisms ("mysql_native_password"
     * for example) of authentication plugins which must be disabled.
     */
    private List<String> disabledAuthenticationPlugins = null;
    /**
     * Name of class for default authentication plugin in client
     */
    private String clientDefaultAuthenticationPlugin = null;
    /**
     * Protocol name of default authentication plugin in client
     */
    private String clientDefaultAuthenticationPluginName = null;
    /**
     * Protocol name of default authentication plugin in server
     */
    private String serverDefaultAuthenticationPluginName = null;

    /**
     * Fill the authentication plugins map.
     * First this method fill the map with instances of {@link MysqlNativePasswordPlugin}, {@link MysqlClearPasswordPlugin}, {@link Sha256PasswordPlugin}
     * and {@link MysqlOldPasswordPlugin}. Then it creates instances of plugins listed in "authenticationPlugins" connection property and adds them to the map
     * too.
     * 
     * The key for the map entry is got by {@link AuthenticationPlugin#getProtocolPluginName()} thus it is possible to replace built-in plugin with custom
     * implementation. To do it custom plugin should return value "mysql_native_password", "mysql_old_password", "mysql_clear_password" or "sha256_password"
     * from it's own getProtocolPluginName() method.
     * 
     */
    private void loadAuthenticationPlugins() {

        // default plugin
        this.clientDefaultAuthenticationPlugin = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_defaultAuthenticationPlugin).getValue();
        if (this.clientDefaultAuthenticationPlugin == null || "".equals(this.clientDefaultAuthenticationPlugin.trim())) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    Messages.getString("AuthenticationProvider.BadDefaultAuthenticationPlugin", new Object[] { this.clientDefaultAuthenticationPlugin }),
                    getExceptionInterceptor());
        }

        // disabled plugins
        String disabledPlugins = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_disabledAuthenticationPlugins).getValue();
        if (disabledPlugins != null && !"".equals(disabledPlugins)) {
            this.disabledAuthenticationPlugins = new ArrayList<String>();
            List<String> pluginsToDisable = StringUtils.split(disabledPlugins, ",", true);
            Iterator<String> iter = pluginsToDisable.iterator();
            while (iter.hasNext()) {
                this.disabledAuthenticationPlugins.add(iter.next());
            }
        }

        this.authenticationPlugins = new HashMap<String, AuthenticationPlugin>();
        boolean defaultIsFound = false;

        List<AuthenticationPlugin> pluginsToInit = new LinkedList<AuthenticationPlugin>();

        // embedded plugins
        pluginsToInit.add(new MysqlNativePasswordPlugin());
        pluginsToInit.add(new MysqlClearPasswordPlugin());
        pluginsToInit.add(new Sha256PasswordPlugin());
        pluginsToInit.add(new MysqlOldPasswordPlugin());

        // plugins from authenticationPluginClasses connection parameter
        String authenticationPluginClasses = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_authenticationPlugins).getValue();
        if (authenticationPluginClasses != null && !"".equals(authenticationPluginClasses)) {
            List<String> pluginsToCreate = StringUtils.split(authenticationPluginClasses, ",", true);
            String className = null;
            try {
                for (int i = 0, s = pluginsToCreate.size(); i < s; i++) {
                    className = pluginsToCreate.get(i);
                    pluginsToInit.add((AuthenticationPlugin) Class.forName(className).newInstance());
                }
            } catch (Throwable t) {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("AuthenticationProvider.BadAuthenticationPlugin", new Object[] { className }), t, this.exceptionInterceptor);
            }
        }

        // initialize plugin instances
        for (AuthenticationPlugin plugin : pluginsToInit) {
            plugin.init(this.protocol);
            if (addAuthenticationPlugin(plugin)) {
                defaultIsFound = true;
            }
        }

        // check if default plugin is listed
        if (!defaultIsFound) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages
                    .getString("AuthenticationProvider.DefaultAuthenticationPluginIsNotListed", new Object[] { this.clientDefaultAuthenticationPlugin }),
                    getExceptionInterceptor());
        }

    }

    /**
     * Add plugin to authentication plugins map if it is not disabled by
     * "disabledAuthenticationPlugins" property, check is it a default plugin.
     * 
     * @param plugin
     *            Instance of AuthenticationPlugin
     * @return True if plugin is default, false if plugin is not default.
     * @throws WrongArgumentException
     *             if plugin is default but disabled.
     */
    private boolean addAuthenticationPlugin(AuthenticationPlugin plugin) {
        boolean isDefault = false;
        String pluginClassName = plugin.getClass().getName();
        String pluginProtocolName = plugin.getProtocolPluginName();
        boolean disabledByClassName = this.disabledAuthenticationPlugins != null && this.disabledAuthenticationPlugins.contains(pluginClassName);
        boolean disabledByMechanism = this.disabledAuthenticationPlugins != null && this.disabledAuthenticationPlugins.contains(pluginProtocolName);

        if (disabledByClassName || disabledByMechanism) {
            // if disabled then check is it default
            if (this.clientDefaultAuthenticationPlugin.equals(pluginClassName)) {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("AuthenticationProvider.BadDisabledAuthenticationPlugin",
                                new Object[] { disabledByClassName ? pluginClassName : pluginProtocolName }),
                        getExceptionInterceptor());
            }
        } else {
            this.authenticationPlugins.put(pluginProtocolName, plugin);
            if (this.clientDefaultAuthenticationPlugin.equals(pluginClassName)) {
                this.clientDefaultAuthenticationPluginName = pluginProtocolName;
                isDefault = true;
            }
        }
        return isDefault;
    }

    /**
     * Get authentication plugin instance from authentication plugins map by
     * pluginName key. If such plugin is found it's {@link AuthenticationPlugin#isReusable()} method
     * is checked, when it's false this method returns a new instance of plugin
     * and the same instance otherwise.
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
    private AuthenticationPlugin getAuthenticationPlugin(String pluginName) {

        AuthenticationPlugin plugin = this.authenticationPlugins.get(pluginName);

        if (plugin != null && !plugin.isReusable()) {
            try {
                plugin = plugin.getClass().newInstance();
                plugin.init(this.protocol);
            } catch (Throwable t) {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("AuthenticationProvider.BadAuthenticationPlugin", new Object[] { plugin.getClass().getName() }), t,
                        getExceptionInterceptor());
            }
        }

        return plugin;
    }

    /**
     * Check if given plugin requires confidentiality, but connection is without SSL
     * 
     * @param plugin
     */
    private void checkConfidentiality(AuthenticationPlugin plugin) {
        if (plugin.requiresConfidentiality() && !this.protocol.getSocketConnection().isSSLEstablished()) {
            throw ExceptionFactory.createException(
                    Messages.getString("AuthenticationProvider.AuthenticationPluginRequiresSSL", new Object[] { plugin.getProtocolPluginName() }),
                    getExceptionInterceptor());
        }
    }

    /**
     * Performs an authentication handshake to authorize connection to a
     * given database as a given MySQL user. This can happen upon initial
     * connection to the server, after receiving Auth Challenge Packet, or
     * at any moment during the connection life-time via a Change User
     * request.
     * 
     * This method is aware of pluggable authentication and will use
     * registered authentication plugins as requested by the server.
     * 
     * @param sessState
     *            The current state of the session
     * @param user
     *            the MySQL user account to log into
     * @param password
     *            authentication data for the user account (depends
     *            on authentication method used - can be empty)
     * @param database
     *            database to connect to (can be empty)
     * @param challenge
     *            the Auth Challenge Packet received from server if
     *            this method is used during the initial connection.
     *            Otherwise null.
     */
    private void proceedHandshakeWithPluggableAuthentication(ServerSession sessState, String user, String password, String database, PacketPayload challenge) {
        if (this.authenticationPlugins == null) {
            loadAuthenticationPlugins();
        }

        boolean skipPassword = false;
        int passwordLength = 16;
        int userLength = (user != null) ? user.length() : 0;
        int databaseLength = (database != null) ? database.length() : 0;

        int packLength = ((userLength + passwordLength + databaseLength) * 3) + 7 + AUTH_411_OVERHEAD;

        long clientParam = sessState.getClientParam();
        int serverCapabilities = sessState.getCapabilities().getCapabilityFlags();

        AuthenticationPlugin plugin = null;
        PacketPayload fromServer = null;
        ArrayList<PacketPayload> toServer = new ArrayList<PacketPayload>();
        boolean done = false;
        PacketPayload last_sent = null;

        boolean old_raw_challenge = false;

        int counter = 100;

        while (0 < counter--) {

            if (!done) {

                if (challenge != null) {

                    if (challenge.isOKPacket()) {
                        throw ExceptionFactory.createException(
                                Messages.getString("AuthenticationProvider.UnexpectedAuthenticationApproval", new Object[] { plugin.getProtocolPluginName() }),
                                getExceptionInterceptor());
                    }

                    // read Auth Challenge Packet

                    clientParam |= MysqlaServerSession.CLIENT_PLUGIN_AUTH | MysqlaServerSession.CLIENT_LONG_PASSWORD | MysqlaServerSession.CLIENT_PROTOCOL_41
                            | MysqlaServerSession.CLIENT_TRANSACTIONS // Need this to get server status values
                            | MysqlaServerSession.CLIENT_MULTI_RESULTS // We always allow multiple result sets
                            | MysqlaServerSession.CLIENT_PS_MULTI_RESULTS  // We always allow multiple result sets for SSPS
                            | MysqlaServerSession.CLIENT_SECURE_CONNECTION; // protocol with pluggable authentication always support this

                    // We allow the user to configure whether or not they want to support multiple queries (by default, this is disabled).
                    if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_allowMultiQueries).getValue()) {
                        clientParam |= MysqlaServerSession.CLIENT_MULTI_STATEMENTS;
                    }

                    if (((serverCapabilities & MysqlaServerSession.CLIENT_CAN_HANDLE_EXPIRED_PASSWORD) != 0)
                            && !this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_disconnectOnExpiredPasswords).getValue()) {
                        clientParam |= MysqlaServerSession.CLIENT_CAN_HANDLE_EXPIRED_PASSWORD;
                    }
                    if (((serverCapabilities & MysqlaServerSession.CLIENT_CONNECT_ATTRS) != 0)
                            && !NONE.equals(this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_connectionAttributes).getValue())) {
                        clientParam |= MysqlaServerSession.CLIENT_CONNECT_ATTRS;
                    }
                    if ((serverCapabilities & MysqlaServerSession.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
                        clientParam |= MysqlaServerSession.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
                    }

                    sessState.setClientParam(clientParam);

                    if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useSSL).getValue()) {
                        negotiateSSLConnection(packLength);
                    }

                    String pluginName = null;
                    if ((serverCapabilities & MysqlaServerSession.CLIENT_PLUGIN_AUTH) != 0) {
                        // Due to Bug#59453 the auth-plugin-name is missing the terminating NUL-char in versions prior to 5.5.10 and 5.6.2.
                        if (!this.protocol.versionMeetsMinimum(5, 5, 10)
                                || this.protocol.versionMeetsMinimum(5, 6, 0) && !this.protocol.versionMeetsMinimum(5, 6, 2)) {
                            pluginName = challenge.readString(StringLengthDataType.STRING_FIXED, "ASCII",
                                    ((MysqlaCapabilities) sessState.getCapabilities()).getAuthPluginDataLength());
                        } else {
                            pluginName = challenge.readString(StringSelfDataType.STRING_TERM, "ASCII");
                        }
                    }

                    plugin = getAuthenticationPlugin(pluginName);
                    if (plugin == null) {
                        /*
                         * Use default if there is no plugin for pluginName.
                         */
                        plugin = getAuthenticationPlugin(this.clientDefaultAuthenticationPluginName);
                    } else if (pluginName.equals(Sha256PasswordPlugin.PLUGIN_NAME) && !this.protocol.getSocketConnection().isSSLEstablished()
                            && this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_serverRSAPublicKeyFile).getValue() == null
                            && !this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_allowPublicKeyRetrieval).getValue()) {
                        /*
                         * Fall back to default if plugin is 'sha256_password' but required conditions for this to work aren't met. If default is other than
                         * 'sha256_password' this will result in an immediate authentication switch request, allowing for other plugins to authenticate
                         * successfully. If default is 'sha256_password' then the authentication will fail as expected. In both cases user's password won't be
                         * sent to avoid subjecting it to lesser security levels.
                         */
                        plugin = getAuthenticationPlugin(this.clientDefaultAuthenticationPluginName);
                        skipPassword = !this.clientDefaultAuthenticationPluginName.equals(pluginName);
                    }

                    this.serverDefaultAuthenticationPluginName = plugin.getProtocolPluginName();

                    checkConfidentiality(plugin);
                    fromServer = new Buffer(StringUtils.getBytes(this.seed));
                } else {
                    // no challenge so this is a changeUser call
                    plugin = getAuthenticationPlugin(this.serverDefaultAuthenticationPluginName == null ? this.clientDefaultAuthenticationPluginName
                            : this.serverDefaultAuthenticationPluginName);

                    checkConfidentiality(plugin);

                    // Servers not affected by Bug#70865 expect the Change User Request containing a correct answer
                    // to seed sent by the server during the initial handshake, thus we reuse it here.
                    // Servers affected by Bug#70865 will just ignore it and send the Auth Switch.
                    fromServer = new Buffer(StringUtils.getBytes(this.seed));
                }

            } else {

                // read packet from server and check if it's an ERROR packet
                challenge = this.protocol.checkErrorPacket();
                old_raw_challenge = false;

                if (plugin == null) {
                    // this shouldn't happen in normal handshake packets exchange,
                    // we do it just to ensure that we don't get NPE in other case
                    plugin = getAuthenticationPlugin(this.serverDefaultAuthenticationPluginName == null ? this.clientDefaultAuthenticationPluginName
                            : this.serverDefaultAuthenticationPluginName);
                }

                if (challenge.isOKPacket()) {
                    // if OK packet then finish handshake
                    plugin.destroy();
                    break;

                } else if (challenge.isAuthMethodSwitchRequestPacket()) {
                    skipPassword = false;

                    // read Auth Method Switch Request Packet
                    String pluginName;
                    pluginName = challenge.readString(StringSelfDataType.STRING_TERM, "ASCII");

                    // get new plugin
                    if (!plugin.getProtocolPluginName().equals(pluginName)) {
                        plugin.destroy();
                        plugin = getAuthenticationPlugin(pluginName);
                        // if plugin is not found for pluginName throw exception
                        if (plugin == null) {
                            throw ExceptionFactory.createException(WrongArgumentException.class,
                                    Messages.getString("AuthenticationProvider.BadAuthenticationPlugin", new Object[] { pluginName }),
                                    getExceptionInterceptor());
                        }
                    }

                    checkConfidentiality(plugin);
                    fromServer = new Buffer(StringUtils.getBytes(challenge.readString(StringSelfDataType.STRING_TERM, "ASCII")));

                } else {
                    // read raw packet
                    if (!this.protocol.versionMeetsMinimum(5, 5, 16)) {
                        old_raw_challenge = true;
                        challenge.setPosition(challenge.getPosition() - 1);
                    }
                    fromServer = new Buffer(challenge.readBytes(StringSelfDataType.STRING_EOF));
                }

            }

            // call plugin
            plugin.setAuthenticationParameters(user, skipPassword ? null : password);
            done = plugin.nextAuthenticationStep(fromServer, toServer);

            // send response
            if (toServer.size() > 0) {
                if (challenge == null) {
                    String enc = getEncodingForHandshake();

                    // write COM_CHANGE_USER Packet
                    last_sent = new Buffer(packLength + 1);
                    last_sent.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_CHANGE_USER);

                    // User/Password data
                    last_sent.writeBytes(StringSelfDataType.STRING_TERM, StringUtils.getBytes(user, enc));

                    // 'auth-response-len' is limited to one Byte but, in case of success, COM_CHANGE_USER will be followed by an AuthSwitchRequest anyway
                    if (toServer.get(0).getPayloadLength() < 256) {
                        // non-mysql servers may use this information to authenticate without requiring another round-trip
                        last_sent.writeInteger(IntegerDataType.INT1, toServer.get(0).getPayloadLength());
                        last_sent.writeBytes(StringSelfDataType.STRING_EOF, toServer.get(0).getByteBuffer());
                    } else {
                        last_sent.writeInteger(IntegerDataType.INT1, 0);
                    }

                    if (this.useConnectWithDb) {
                        last_sent.writeBytes(StringSelfDataType.STRING_TERM, StringUtils.getBytes(database, enc));
                    } else {
                        /* For empty database */
                        last_sent.writeInteger(IntegerDataType.INT1, 0);
                    }

                    last_sent.writeInteger(IntegerDataType.INT1,
                            AuthenticationProvider.getCharsetForHandshake(enc, sessState.getCapabilities().getServerVersion()));
                    // two (little-endian) bytes for charset in this packet
                    last_sent.writeInteger(IntegerDataType.INT1, 0);

                    // plugin name
                    if ((serverCapabilities & MysqlaServerSession.CLIENT_PLUGIN_AUTH) != 0) {
                        last_sent.writeBytes(StringSelfDataType.STRING_TERM, StringUtils.getBytes(plugin.getProtocolPluginName(), enc));
                    }

                    // connection attributes
                    if ((clientParam & MysqlaServerSession.CLIENT_CONNECT_ATTRS) != 0) {
                        appendConnectionAttributes(last_sent,
                                this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_connectionAttributes).getValue(), enc);
                    }

                    this.protocol.send(last_sent, last_sent.getPosition());

                } else if (challenge.isAuthMethodSwitchRequestPacket()) {
                    // write Auth Method Switch Response Packet
                    this.protocol.send(toServer.get(0), toServer.get(0).getPayloadLength());

                } else if (challenge.isAuthMoreData() || old_raw_challenge) {
                    // write raw packet(s)
                    for (PacketPayload buffer : toServer) {
                        this.protocol.send(buffer, buffer.getPayloadLength());
                    }

                } else {
                    // write Auth Response Packet
                    String enc = getEncodingForHandshake();

                    last_sent = new Buffer(packLength);
                    last_sent.writeInteger(IntegerDataType.INT4, clientParam);
                    last_sent.writeInteger(IntegerDataType.INT4, MysqlaConstants.MAX_PACKET_SIZE);

                    last_sent.writeInteger(IntegerDataType.INT1,
                            AuthenticationProvider.getCharsetForHandshake(enc, sessState.getCapabilities().getServerVersion()));

                    last_sent.writeBytes(StringLengthDataType.STRING_FIXED, new byte[23]);   // Set of bytes reserved for future use.

                    // User/Password data
                    last_sent.writeBytes(StringSelfDataType.STRING_TERM, StringUtils.getBytes(user, enc));

                    if ((serverCapabilities & MysqlaServerSession.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
                        // send lenenc-int length of auth-response and string[n] auth-response
                        last_sent.writeBytes(StringSelfDataType.STRING_LENENC, toServer.get(0).readBytes(StringSelfDataType.STRING_EOF));
                    } else {
                        // send 1 byte length of auth-response and string[n] auth-response
                        last_sent.writeInteger(IntegerDataType.INT1, toServer.get(0).getPayloadLength());
                        last_sent.writeBytes(StringSelfDataType.STRING_EOF, toServer.get(0).getByteBuffer());
                    }

                    if (this.useConnectWithDb) {
                        last_sent.writeBytes(StringSelfDataType.STRING_TERM, StringUtils.getBytes(database, enc));
                    } else {
                        /* For empty database */
                        last_sent.writeInteger(IntegerDataType.INT1, 0);
                    }

                    if ((serverCapabilities & MysqlaServerSession.CLIENT_PLUGIN_AUTH) != 0) {
                        last_sent.writeBytes(StringSelfDataType.STRING_TERM, StringUtils.getBytes(plugin.getProtocolPluginName(), enc));
                    }

                    // connection attributes
                    if (((clientParam & MysqlaServerSession.CLIENT_CONNECT_ATTRS) != 0)) {
                        appendConnectionAttributes(last_sent,
                                this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_connectionAttributes).getValue(), enc);
                    }

                    this.protocol.send(last_sent, last_sent.getPosition());
                }

            }

        }

        if (counter == 0) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    Messages.getString("CommunicationsException.TooManyAuthenticationPluginNegotiations"), getExceptionInterceptor());
        }

        this.protocol.afterHandshake();

        if (!this.useConnectWithDb) {
            this.protocol.changeDatabase(database);
        }

    }

    private Properties getConnectionAttributesAsProperties(String atts) {

        Properties props = new Properties();

        if (atts != null) {
            String[] pairs = atts.split(",");
            for (String pair : pairs) {
                int keyEnd = pair.indexOf(":");
                if (keyEnd > 0 && (keyEnd + 1) < pair.length()) {
                    props.setProperty(pair.substring(0, keyEnd), pair.substring(keyEnd + 1));
                }
            }
        }

        // Leaving disabled until standard values are defined
        // props.setProperty("_os", NonRegisteringDriver.OS);
        // props.setProperty("_platform", NonRegisteringDriver.PLATFORM);
        props.setProperty("_client_name", Constants.CJ_NAME);
        props.setProperty("_client_version", Constants.CJ_VERSION);
        props.setProperty("_runtime_vendor", Constants.JVM_VENDOR);
        props.setProperty("_runtime_version", Constants.JVM_VERSION);
        props.setProperty("_client_license", Constants.CJ_LICENSE);

        return props;
    }

    private void appendConnectionAttributes(PacketPayload buf, String attributes, String enc) {

        PacketPayload lb = new Buffer(100);
        Properties props = getConnectionAttributesAsProperties(attributes);

        for (Object key : props.keySet()) {
            lb.writeBytes(StringSelfDataType.STRING_LENENC, StringUtils.getBytes((String) key, enc));
            lb.writeBytes(StringSelfDataType.STRING_LENENC, StringUtils.getBytes(props.getProperty((String) key), enc));
        }

        buf.writeInteger(IntegerDataType.INT_LENENC, lb.getPosition());
        buf.writeBytes(StringLengthDataType.STRING_FIXED, lb.getByteBuffer(), 0, lb.getPosition());
    }

    /**
     * Get the Java encoding to be used for the handshake
     * response. Defaults to UTF-8.
     */
    public String getEncodingForHandshake() {
        String enc = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue();
        if (enc == null) {
            enc = "UTF-8";
        }
        return enc;
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    /**
     * Negotiates the SSL communications channel used when connecting
     * to a MySQL server that understands SSL.
     * 
     * @param packLength
     */
    private void negotiateSSLConnection(int packLength) {
        this.protocol.negotiateSSLConnection(packLength);
    }

    /**
     * Re-authenticates as the given user and password
     * 
     * @param serverSession
     * @param userName
     * @param password
     * @param database
     */
    @Override
    public void changeUser(ServerSession serverSession, String userName, String password, String database) {
        proceedHandshakeWithPluggableAuthentication(serverSession, userName, password, database, null);
    }

}
