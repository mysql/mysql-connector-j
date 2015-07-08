/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqla.authentication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.mysql.cj.api.Extension;
import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.authentication.AuthenticationPlugin;
import com.mysql.cj.api.authentication.AuthenticationProvider;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.PacketBuffer;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.api.io.ServerSession;
import com.mysql.cj.core.Constants;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.core.util.Util;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.io.Buffer;
import com.mysql.cj.mysqla.io.MysqlaCapabilities;
import com.mysql.cj.mysqla.io.MysqlaServerSession;

public class MysqlaAuthenticationProvider implements AuthenticationProvider {

    protected static final int AUTH_411_OVERHEAD = 33;
    private static final String NONE = "none";

    protected String seed;
    private boolean useConnectWithDb;

    private MysqlConnection connection;
    private ExceptionInterceptor exceptionInterceptor;
    private PropertySet propertySet;

    private Protocol protocol;

    public MysqlaAuthenticationProvider() {
    }

    @Override
    public void init(MysqlConnection conn, Protocol prot, PropertySet propertySet, ExceptionInterceptor exceptionInterceptor) {
        this.connection = conn;
        this.protocol = prot;
        this.propertySet = propertySet;
        this.exceptionInterceptor = exceptionInterceptor;
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

        Buffer buf = capabilities.getInitialHandshakePacket();

        // read auth-plugin-data-part-1 (string[8])
        this.seed = capabilities.getSeed();

        // read character set (1 byte)
        sessState.setServerCharsetIndex(capabilities.getServerCharsetIndex());
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
                seedPart2 = buf.readString("ASCII", authPluginDataLength - 8);
                newSeed = new StringBuilder(authPluginDataLength);
            } else {
                seedPart2 = buf.readString("ASCII");
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

        if (((capabilityFlags & MysqlaServerSession.CLIENT_SSL) == 0)
                && this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useSSL).getValue()) {
            if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_requireSSL).getValue()) {
                this.protocol.rejectConnection(Messages.getString("MysqlIO.15"));
            }

            this.propertySet.<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_useSSL).setValue(false);
        }

        if ((capabilityFlags & MysqlaServerSession.CLIENT_LONG_FLAG) != 0) {
            // We understand other column flags, as well
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
     * Name of class for default authentication plugin
     */
    private String defaultAuthenticationPlugin = null;
    /**
     * Protocol name of default authentication plugin
     */
    private String defaultAuthenticationPluginProtocolName = null;

    /**
     * Fill the authentication plugins map.
     * First this method fill the map with instances of {@link MysqlNativePasswordPlugin}, {@link MysqlClearPasswordPlugin} and {@link Sha256PasswordPlugin}.
     * Then it gets instances of plugins listed in "authenticationPlugins" connection property by
     * {@link Util#loadExtensions(MysqlConnection, Properties, String, String, ExceptionInterceptor)} call and adds them to the map too.
     * 
     * The key for the map entry is getted by {@link AuthenticationPlugin#getProtocolPluginName()}.
     * Thus it is possible to replace built-in plugin with custom one, to do it custom plugin should return value
     * "mysql_native_password", "mysql_old_password", "mysql_clear_password" or "sha256_password" from it's own getProtocolPluginName() method.
     * 
     * All plugin instances in the map are initialized by {@link Extension#init(MysqlConnection, Properties)} call
     * with this.connection and this.connection.getProperties() values.
     * 
     */
    private void loadAuthenticationPlugins() {

        // default plugin
        this.defaultAuthenticationPlugin = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_defaultAuthenticationPlugin).getValue();
        if (this.defaultAuthenticationPlugin == null || "".equals(this.defaultAuthenticationPlugin.trim())) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    Messages.getString("Connection.BadDefaultAuthenticationPlugin", new Object[] { this.defaultAuthenticationPlugin }),
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
        AuthenticationPlugin plugin;
        boolean defaultIsFound = false;

        // embedded plugins
        plugin = new MysqlNativePasswordPlugin();
        plugin.init(this.connection, this.protocol, this.connection.getProperties());
        defaultIsFound = addAuthenticationPlugin(plugin);

        plugin = new MysqlClearPasswordPlugin();
        plugin.init(this.connection, this.protocol, this.connection.getProperties());
        if (addAuthenticationPlugin(plugin)) {
            defaultIsFound = true;
        }

        plugin = new Sha256PasswordPlugin();
        plugin.init(this.connection, this.protocol, this.connection.getProperties());
        if (addAuthenticationPlugin(plugin)) {
            defaultIsFound = true;
        }

        // plugins from authenticationPluginClasses connection parameter
        String authenticationPluginClasses = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_authenticationPlugins).getValue();
        if (authenticationPluginClasses != null && !"".equals(authenticationPluginClasses)) {

            List<Extension> plugins = Util.loadExtensions(this.connection, this.connection.getProperties(), authenticationPluginClasses,
                    "Connection.BadAuthenticationPlugin", getExceptionInterceptor());

            for (Extension object : plugins) {
                plugin = (AuthenticationPlugin) object;
                if (addAuthenticationPlugin(plugin)) {
                    defaultIsFound = true;
                }
            }

        }

        // check if default plugin is listed
        if (!defaultIsFound) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    Messages.getString("Connection.DefaultAuthenticationPluginIsNotListed", new Object[] { this.defaultAuthenticationPlugin }),
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
            if (this.defaultAuthenticationPlugin.equals(pluginClassName)) {
                throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("Connection.BadDisabledAuthenticationPlugin",
                        new Object[] { disabledByClassName ? pluginClassName : pluginProtocolName }), getExceptionInterceptor());
            }
        } else {
            this.authenticationPlugins.put(pluginProtocolName, plugin);
            if (this.defaultAuthenticationPlugin.equals(pluginClassName)) {
                this.defaultAuthenticationPluginProtocolName = pluginProtocolName;
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
                plugin.init(this.connection, this.protocol, this.connection.getProperties());
            } catch (Throwable t) {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("Connection.BadAuthenticationPlugin", new Object[] { plugin.getClass().getName() }), t, getExceptionInterceptor());
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
                    Messages.getString("Connection.AuthenticationPluginRequiresSSL", new Object[] { plugin.getProtocolPluginName() }),
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
    private void proceedHandshakeWithPluggableAuthentication(ServerSession sessState, String user, String password, String database, Buffer challenge) {
        if (this.authenticationPlugins == null) {
            loadAuthenticationPlugins();
        }

        int passwordLength = 16;
        int userLength = (user != null) ? user.length() : 0;
        int databaseLength = (database != null) ? database.length() : 0;

        int packLength = ((userLength + passwordLength + databaseLength) * 3) + 7 + MysqlaConstants.HEADER_LENGTH + AUTH_411_OVERHEAD;

        long clientParam = sessState.getClientParam();
        int serverCapabilities = sessState.getCapabilities().getCapabilityFlags();

        AuthenticationPlugin plugin = null;
        PacketBuffer fromServer = null;
        ArrayList<PacketBuffer> toServer = new ArrayList<PacketBuffer>();
        Boolean done = null;
        Buffer last_sent = null;

        boolean old_raw_challenge = false;

        int counter = 100;

        while (0 < counter--) {

            if (done == null) {

                if (challenge != null) {
                    // read Auth Challenge Packet

                    clientParam |= MysqlaServerSession.CLIENT_PLUGIN_AUTH | MysqlaServerSession.CLIENT_LONG_PASSWORD | MysqlaServerSession.CLIENT_PROTOCOL_41
                            | MysqlaServerSession.CLIENT_TRANSACTIONS // Need this to get server status values
                            | MysqlaServerSession.CLIENT_MULTI_RESULTS // We always allow multiple result sets
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
                        pluginName = challenge.readString("ASCII");
                    }

                    plugin = getAuthenticationPlugin(pluginName);
                    // if plugin is not found for pluginName get default instead 
                    if (plugin == null) {
                        plugin = getAuthenticationPlugin(this.defaultAuthenticationPluginProtocolName);
                    }

                    checkConfidentiality(plugin);
                    fromServer = new Buffer(StringUtils.getBytes(this.seed));
                } else {
                    // no challenge so this is a changeUser call
                    plugin = getAuthenticationPlugin(this.defaultAuthenticationPluginProtocolName);
                    checkConfidentiality(plugin);

                    // Servers not affected by Bug#70865 expect the Change User Request containing a correct answer
                    // to seed sent by the server during the initial handshake, thus we reuse it here.
                    // Servers affected by Bug#70865 will just ignore it and send the Auth Switch.
                    fromServer = new Buffer(StringUtils.getBytes(this.seed));
                }

            } else {

                // read packet from server and check if it's an ERROR packet
                challenge = this.protocol.readNextPacket();
                old_raw_challenge = false;

                if (challenge.isOKPacket()) {
                    // if OK packet then finish handshake
                    if (!done) {
                        throw ExceptionFactory.createException(
                                Messages.getString("Connection.UnexpectedAuthenticationApproval", new Object[] { plugin.getProtocolPluginName() }),
                                getExceptionInterceptor());
                    }
                    plugin.destroy();
                    break;

                } else if (challenge.isAuthMethodSwitchRequestPacket()) {
                    // read Auth Method Switch Request Packet
                    String pluginName;
                    pluginName = challenge.readString("ASCII");

                    // get new plugin
                    if (plugin != null && !plugin.getProtocolPluginName().equals(pluginName)) {
                        plugin.destroy();
                        plugin = getAuthenticationPlugin(pluginName);
                        // if plugin is not found for pluginName throw exception
                        if (plugin == null) {
                            throw ExceptionFactory.createException(WrongArgumentException.class,
                                    Messages.getString("Connection.BadAuthenticationPlugin", new Object[] { pluginName }), getExceptionInterceptor());
                        }
                    }

                    checkConfidentiality(plugin);
                    fromServer = new Buffer(StringUtils.getBytes(challenge.readString("ASCII")));

                } else {
                    // read raw packet
                    fromServer = new Buffer(challenge.getBytes(challenge.getPosition(), challenge.getBufLength() - challenge.getPosition()));
                }

            }

            // call plugin
            plugin.setAuthenticationParameters(user, password);
            done = plugin.nextAuthenticationStep(fromServer, toServer);

            // send response
            if (toServer.size() > 0) {
                if (challenge == null) {
                    String enc = getEncodingForHandshake();

                    // write COM_CHANGE_USER Packet
                    last_sent = new Buffer(packLength + 1);
                    last_sent.setPosition(0);
                    last_sent.writeByte((byte) MysqlaConstants.COM_CHANGE_USER);

                    // User/Password data
                    last_sent.writeString(user, enc);

                    // 'auth-response-len' is limited to one Byte but, in case of success, COM_CHANGE_USER will be followed by an AuthSwitchRequest anyway
                    if (toServer.get(0).getBufLength() < 256) {
                        // non-mysql servers may use this information to authenticate without requiring another round-trip
                        last_sent.writeByte((byte) toServer.get(0).getBufLength());
                        last_sent.writeBytesNoNull(toServer.get(0).getByteBuffer(), 0, toServer.get(0).getBufLength());
                    } else {
                        last_sent.writeByte((byte) 0);
                    }

                    if (this.useConnectWithDb) {
                        last_sent.writeString(database, enc);
                    } else {
                        /* For empty database */
                        last_sent.writeByte((byte) 0);
                    }

                    last_sent.writeByte(AuthenticationProvider.getCharsetForHandshake(enc, sessState.getCapabilities().getServerVersion()));
                    // two (little-endian) bytes for charset in this packet
                    last_sent.writeByte((byte) 0);

                    // plugin name
                    if ((serverCapabilities & MysqlaServerSession.CLIENT_PLUGIN_AUTH) != 0) {
                        last_sent.writeString(plugin.getProtocolPluginName(), enc);
                    }

                    // connection attributes
                    if ((clientParam & MysqlaServerSession.CLIENT_CONNECT_ATTRS) != 0) {
                        appendConnectionAttributes(last_sent,
                                this.connection.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_connectionAttributes).getValue(), enc);
                    }

                    this.protocol.send(last_sent, last_sent.getPosition());

                } else if (challenge.isAuthMethodSwitchRequestPacket()) {
                    // write Auth Method Switch Response Packet
                    this.protocol.send(toServer.get(0), toServer.get(0).getBufLength());

                } else if (challenge.isRawPacket() || old_raw_challenge) {
                    // write raw packet(s)
                    for (PacketBuffer buffer : toServer) {
                        this.protocol.send(buffer, buffer.getBufLength());
                    }

                } else {
                    // write Auth Response Packet
                    String enc = getEncodingForHandshake();

                    last_sent = new Buffer(packLength);
                    last_sent.setPosition(0);
                    last_sent.writeLong(clientParam);
                    last_sent.writeLong(MysqlaConstants.MAX_PACKET_SIZE);

                    last_sent.writeByte(AuthenticationProvider.getCharsetForHandshake(enc, sessState.getCapabilities().getServerVersion()));

                    last_sent.writeBytesNoNull(new byte[23]);   // Set of bytes reserved for future use.

                    // User/Password data
                    last_sent.writeString(user, enc);

                    if ((serverCapabilities & MysqlaServerSession.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
                        // send lenenc-int length of auth-response and string[n] auth-response
                        last_sent.writeLenBytes(toServer.get(0).getBytes(toServer.get(0).getBufLength()));
                    } else {
                        // send 1 byte length of auth-response and string[n] auth-response
                        last_sent.writeByte((byte) toServer.get(0).getBufLength());
                        last_sent.writeBytesNoNull(toServer.get(0).getByteBuffer(), 0, toServer.get(0).getBufLength());
                    }

                    if (this.useConnectWithDb) {
                        last_sent.writeString(database, enc);
                    } else {
                        /* For empty database */
                        last_sent.writeByte((byte) 0);
                    }

                    if ((serverCapabilities & MysqlaServerSession.CLIENT_PLUGIN_AUTH) != 0) {
                        last_sent.writeString(plugin.getProtocolPluginName(), enc);
                    }

                    // connection attributes
                    if (((clientParam & MysqlaServerSession.CLIENT_CONNECT_ATTRS) != 0)) {
                        appendConnectionAttributes(last_sent,
                                this.connection.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_connectionAttributes).getValue(), enc);
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

    private void appendConnectionAttributes(Buffer buf, String attributes, String enc) {

        Buffer lb = new Buffer(100);
        lb.setPosition(0);

        Properties props = getConnectionAttributesAsProperties(attributes);

        for (Object key : props.keySet()) {
            lb.writeLenString((String) key, enc);
            lb.writeLenString(props.getProperty((String) key), enc);
        }

        buf.writeByte((byte) lb.getPosition());
        buf.writeBytesNoNull(lb.getByteBuffer(), 0, lb.getPosition());
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
