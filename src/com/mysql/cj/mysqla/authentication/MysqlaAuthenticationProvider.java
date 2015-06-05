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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.mysql.cj.api.CharsetConverter;
import com.mysql.cj.api.Extension;
import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.Session;
import com.mysql.cj.api.SessionState;
import com.mysql.cj.api.authentication.AuthenticationPlugin;
import com.mysql.cj.api.authentication.AuthenticationProvider;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exception.ExceptionInterceptor;
import com.mysql.cj.api.io.PacketBuffer;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.core.CharsetMapping;
import com.mysql.cj.core.Constants;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exception.ClosedOnExpiredPasswordException;
import com.mysql.cj.core.exception.ExceptionFactory;
import com.mysql.cj.core.exception.MysqlErrorNumbers;
import com.mysql.cj.core.exception.PasswordExpiredException;
import com.mysql.cj.core.exception.WrongArgumentException;
import com.mysql.cj.core.io.Buffer;
import com.mysql.cj.core.io.ProtocolConstants;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.core.util.Util;
import com.mysql.jdbc.MysqlDefs;
import com.mysql.jdbc.MysqlIO;
import com.mysql.jdbc.exceptions.CommunicationsException;

public class MysqlaAuthenticationProvider implements AuthenticationProvider {

    protected static final int AUTH_411_OVERHEAD = 33;
    private static final String NONE = "none";

    private byte protocolVersion = 0;
    protected String seed;
    private int authPluginDataLength = 0;
    private boolean useConnectWithDb;

    private MysqlConnection connection;
    private ExceptionInterceptor exceptionInterceptor;
    private PropertySet propertySet;

    private Session session;
    private Protocol protocol;

    public MysqlaAuthenticationProvider() {
    }

    @Override
    public void init(MysqlConnection conn, Protocol prot, Session sess, PropertySet propertySet, ExceptionInterceptor exceptionInterceptor) {
        this.connection = conn;
        this.protocol = prot;
        this.session = sess;
        this.propertySet = propertySet;
        this.exceptionInterceptor = exceptionInterceptor;
    }

    @Override
    public Session connect(String userName, String password, String database) {
        doHandshake(userName, password, database);

        return this.session;
    }

    /**
     * Initialize communications with the MySQL server. Handles logging on, and
     * handling initial connection errors.
     * 
     * @param user
     * @param password
     * @param database
     * 
     * @throws SQLException
     * @throws CommunicationsException
     */
    void doHandshake(String user, String password, String database) {

        SessionState sessState = this.session.getSessionState();
        long clientParam = sessState.getClientParam();

        this.protocol.beforeHandshake();

        // Read the first packet
        Buffer buf;
        try {
            buf = this.protocol.readPacket();
        } catch (SQLException e) {
            throw ExceptionFactory.createException(e.getMessage(), e, getExceptionInterceptor());
        }

        // Get the protocol version
        this.protocolVersion = buf.readByte();

        // ERR packet instead of Initial Handshake
        if (this.protocolVersion == -1) {
            this.protocol.rejectProtocol(buf);
        }

        sessState.setServerVersion(ServerVersion.parseVersion(buf.readString("ASCII", getExceptionInterceptor())));

        // read connection id
        this.protocol.setThreadId(buf.readLong());

        // read auth-plugin-data-part-1 (string[8])
        this.seed = buf.readString("ASCII", getExceptionInterceptor(), 8);

        // read filler ([00])
        buf.readByte();

        int serverCapabilities = 0;

        // read capability flags (lower 2 bytes)
        if (buf.getPosition() < buf.getBufLength()) {
            serverCapabilities = buf.readInt();
        }

        // read character set (1 byte)
        sessState.setServerCharsetIndex(buf.readByte() & 0xff);
        // read status flags (2 bytes)
        sessState.setServerStatus(buf.readInt());

        // read capability flags (upper 2 bytes)
        serverCapabilities |= buf.readInt() << 16;

        sessState.setServerCapabilities(serverCapabilities);

        if ((serverCapabilities & SessionState.CLIENT_PLUGIN_AUTH) != 0) {
            // read length of auth-plugin-data (1 byte)
            this.authPluginDataLength = buf.readByte() & 0xff;
        } else {
            // read filler ([00])
            buf.readByte();
        }
        // next 10 bytes are reserved (all [00])
        buf.setPosition(buf.getPosition() + 10);

        if ((serverCapabilities & SessionState.CLIENT_SECURE_CONNECTION) != 0) {
            clientParam |= SessionState.CLIENT_SECURE_CONNECTION;
            String seedPart2;
            StringBuilder newSeed;
            // read string[$len] auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
            if (this.authPluginDataLength > 0) {
                // TODO: disabled the following check for further clarification
                //                  if (this.authPluginDataLength < 21) {
                //                      forceClose();
                //                      throw SQLError.createSQLException(Messages.getString("MysqlIO.103"), 
                //                          SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, getExceptionInterceptor());
                //                  }
                seedPart2 = buf.readString("ASCII", getExceptionInterceptor(), this.authPluginDataLength - 8);
                newSeed = new StringBuilder(this.authPluginDataLength);
            } else {
                seedPart2 = buf.readString("ASCII", getExceptionInterceptor());
                newSeed = new StringBuilder(ProtocolConstants.SEED_LENGTH);
            }
            newSeed.append(this.seed);
            newSeed.append(seedPart2);
            this.seed = newSeed.toString();
        } else {
            // TODO: better messaging
            this.protocol.rejectConnection("SessionState.CLIENT_SECURE_CONNECTION is required");
        }

        if (((serverCapabilities & SessionState.CLIENT_COMPRESS) != 0)
                && this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useCompression).getValue()) {
            clientParam |= SessionState.CLIENT_COMPRESS;
        }

        this.useConnectWithDb = (database != null) && (database.length() > 0)
                && !this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_createDatabaseIfNotExist).getValue();

        if (this.useConnectWithDb) {
            clientParam |= SessionState.CLIENT_CONNECT_WITH_DB;
        }

        if (((serverCapabilities & SessionState.CLIENT_SSL) == 0) && this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useSSL).getValue()) {
            if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_requireSSL).getValue()) {
                this.protocol.rejectConnection(Messages.getString("MysqlIO.15"));
            }

            this.propertySet.getBooleanModifiableProperty(PropertyDefinitions.PNAME_useSSL).setValue(false);
        }

        if ((serverCapabilities & SessionState.CLIENT_LONG_FLAG) != 0) {
            // We understand other column flags, as well
            clientParam |= SessionState.CLIENT_LONG_FLAG;
            sessState.setHasLongColumnInfo(true);
        }

        // return FOUND rows
        if (!this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useAffectedRows).getValue()) {
            clientParam |= SessionState.CLIENT_FOUND_ROWS;
        }

        if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_allowLoadLocalInfile).getValue()) {
            clientParam |= SessionState.CLIENT_LOCAL_FILES;
        }

        if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_interactiveClient).getValue()) {
            clientParam |= SessionState.CLIENT_INTERACTIVE;
        }

        //
        // switch to pluggable authentication if available
        //
        if ((serverCapabilities & SessionState.CLIENT_PLUGIN_AUTH) != 0) {
            sessState.setClientParam(clientParam);
            proceedHandshakeWithPluggableAuthentication(user, password, database, buf);
        } else {
            // TODO: better messaging
            this.protocol.rejectConnection("SessionState.CLIENT_PLUGIN_AUTH is required");
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
     * Fill the {@link MysqlIO#authenticationPlugins} map.
     * First this method fill the map with instances of {@link MysqlOldPasswordPlugin}, {@link MysqlNativePasswordPlugin}, {@link MysqlClearPasswordPlugin} and
     * {@link Sha256PasswordPlugin}.
     * Then it gets instances of plugins listed in "authenticationPlugins" connection property by
     * {@link Util#loadExtensions(Connection, Properties, String, String, ExceptionInterceptor)} call and adds them to the map too.
     * 
     * The key for the map entry is getted by {@link AuthenticationPlugin#getProtocolPluginName()}.
     * Thus it is possible to replace built-in plugin with custom one, to do it custom plugin should return value
     * "mysql_native_password", "mysql_old_password", "mysql_clear_password" or "sha256_password" from it's own getProtocolPluginName() method.
     * 
     * All plugin instances in the map are initialized by {@link Extension#init(Connection, Properties)} call
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
        plugin.init(this.connection, this.connection.getProperties());
        defaultIsFound = addAuthenticationPlugin(plugin);

        plugin = new MysqlClearPasswordPlugin();
        plugin.init(this.connection, this.connection.getProperties());
        if (addAuthenticationPlugin(plugin)) {
            defaultIsFound = true;
        }

        plugin = new Sha256PasswordPlugin();
        plugin.init(this.connection, this.connection.getProperties());
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
     * Add plugin to {@link MysqlIO#authenticationPlugins} if it is not disabled by
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
     * Get authentication plugin instance from {@link MysqlIO#authenticationPlugins} map by
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
                plugin.init(this.connection, this.connection.getProperties());
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
        if (plugin.requiresConfidentiality() && !this.protocol.getPhysicalConnection().isSSLEstablished()) {
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
    private void proceedHandshakeWithPluggableAuthentication(String user, String password, String database, Buffer challenge) {
        if (this.authenticationPlugins == null) {
            loadAuthenticationPlugins();
        }

        int passwordLength = 16;
        int userLength = (user != null) ? user.length() : 0;
        int databaseLength = (database != null) ? database.length() : 0;

        int packLength = ((userLength + passwordLength + databaseLength) * 3) + 7 + ProtocolConstants.HEADER_LENGTH + AUTH_411_OVERHEAD;

        long clientParam = this.session.getSessionState().getClientParam();
        int serverCapabilities = this.session.getSessionState().getServerCapabilities();

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

                    clientParam |= SessionState.CLIENT_PLUGIN_AUTH | SessionState.CLIENT_LONG_PASSWORD | SessionState.CLIENT_PROTOCOL_41
                            | SessionState.CLIENT_TRANSACTIONS // Need this to get server status values
                            | SessionState.CLIENT_MULTI_RESULTS // We always allow multiple result sets
                            | SessionState.CLIENT_SECURE_CONNECTION; // protocol with pluggable authentication always support this

                    // We allow the user to configure whether or not they want to support multiple queries (by default, this is disabled).
                    if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_allowMultiQueries).getValue()) {
                        clientParam |= SessionState.CLIENT_MULTI_STATEMENTS;
                    }

                    if (((serverCapabilities & SessionState.CLIENT_CAN_HANDLE_EXPIRED_PASSWORD) != 0)
                            && !this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_disconnectOnExpiredPasswords).getValue()) {
                        clientParam |= SessionState.CLIENT_CAN_HANDLE_EXPIRED_PASSWORD;
                    }
                    if (((serverCapabilities & SessionState.CLIENT_CONNECT_ATTRS) != 0)
                            && !NONE.equals(this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_connectionAttributes).getValue())) {
                        clientParam |= SessionState.CLIENT_CONNECT_ATTRS;
                    }
                    if ((serverCapabilities & SessionState.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
                        clientParam |= SessionState.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
                    }

                    this.session.getSessionState().setClientParam(clientParam);

                    if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useSSL).getValue()) {
                        negotiateSSLConnection(user, password, database, packLength);
                    }

                    String pluginName = null;
                    if ((serverCapabilities & SessionState.CLIENT_PLUGIN_AUTH) != 0) {
                        pluginName = challenge.readString("ASCII", getExceptionInterceptor());
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
                try {
                    challenge = this.protocol.readNextPacket();
                } catch (SQLException e) {
                    if (e.getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD) {
                        throw ExceptionFactory.createException(PasswordExpiredException.class, e.getMessage(), e, getExceptionInterceptor());
                    } else if (e.getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD_LOGIN) {
                        throw ExceptionFactory.createException(ClosedOnExpiredPasswordException.class, e.getMessage(), e, getExceptionInterceptor());
                    }
                    throw ExceptionFactory.createException(e.getMessage(), e, getExceptionInterceptor());
                }
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
                    pluginName = challenge.readString("ASCII", getExceptionInterceptor());

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
                    fromServer = new Buffer(StringUtils.getBytes(challenge.readString("ASCII", getExceptionInterceptor())));

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
                    last_sent.writeByte((byte) MysqlDefs.COM_CHANGE_USER);

                    // User/Password data
                    last_sent.writeString(user, enc, this.connection);

                    last_sent.writeByte((byte) toServer.get(0).getBufLength());
                    last_sent.writeBytesNoNull(toServer.get(0).getByteBuffer(), 0, toServer.get(0).getBufLength());

                    if (this.useConnectWithDb) {
                        last_sent.writeString(database, enc, this.connection);
                    } else {
                        /* For empty database */
                        last_sent.writeByte((byte) 0);
                    }

                    appendCharsetByteForHandshake(last_sent, enc);
                    // two (little-endian) bytes for charset in this packet
                    last_sent.writeByte((byte) 0);

                    // plugin name
                    if ((serverCapabilities & SessionState.CLIENT_PLUGIN_AUTH) != 0) {
                        last_sent.writeString(plugin.getProtocolPluginName(), enc, this.connection);
                    }

                    // connection attributes
                    if ((clientParam & SessionState.CLIENT_CONNECT_ATTRS) != 0) {
                        appendConnectionAttributes(last_sent,
                                this.connection.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_connectionAttributes).getValue(), enc,
                                this.connection.getCharsetConverter(enc));
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
                    last_sent.writeLong(ProtocolConstants.MAX_PACKET_SIZE);

                    appendCharsetByteForHandshake(last_sent, enc);

                    last_sent.writeBytesNoNull(new byte[23]);   // Set of bytes reserved for future use.

                    // User/Password data
                    last_sent.writeString(user, enc, this.connection);

                    if ((serverCapabilities & SessionState.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
                        // send lenenc-int length of auth-response and string[n] auth-response
                        last_sent.writeLenBytes(toServer.get(0).getBytes(toServer.get(0).getBufLength()));
                    } else {
                        // send 1 byte length of auth-response and string[n] auth-response
                        last_sent.writeByte((byte) toServer.get(0).getBufLength());
                        last_sent.writeBytesNoNull(toServer.get(0).getByteBuffer(), 0, toServer.get(0).getBufLength());
                    }

                    if (this.useConnectWithDb) {
                        last_sent.writeString(database, enc, this.connection);
                    } else {
                        /* For empty database */
                        last_sent.writeByte((byte) 0);
                    }

                    if ((serverCapabilities & SessionState.CLIENT_PLUGIN_AUTH) != 0) {
                        last_sent.writeString(plugin.getProtocolPluginName(), enc, this.connection);
                    }

                    // connection attributes
                    if (((clientParam & SessionState.CLIENT_CONNECT_ATTRS) != 0)) {
                        appendConnectionAttributes(last_sent,
                                this.connection.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_connectionAttributes).getValue(), enc,
                                this.connection.getCharsetConverter(enc));
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

    private void appendConnectionAttributes(Buffer buf, String attributes, String enc, CharsetConverter conv) {

        Buffer lb = new Buffer(100);
        lb.setPosition(0);

        Properties props = getConnectionAttributesAsProperties(attributes);

        for (Object key : props.keySet()) {
            lb.writeLenString((String) key, enc, conv, getExceptionInterceptor());
            lb.writeLenString(props.getProperty((String) key), enc, conv, getExceptionInterceptor());
        }

        buf.writeByte((byte) lb.getPosition());
        buf.writeBytesNoNull(lb.getByteBuffer(), 0, lb.getPosition());
    }

    /**
     * Get the Java encoding to be used for the handshake
     * response. Defaults to UTF-8.
     */
    public String getEncodingForHandshake() {
        String enc = this.connection.getCharacterEncoding();
        if (enc == null) {
            enc = "UTF-8";
        }
        return enc;
    }

    /**
     * Append the MySQL collation index to the handshake packet. A
     * single byte will be added to the packet corresponding to the
     * collation index found for the requested Java encoding name.
     * 
     * If the index is &gt; 255 which may be valid at some point in
     * the future, an exception will be thrown. At the time of this
     * implementation the index cannot be &gt; 255 and only the
     * COM_CHANGE_USER rpc, not the handshake response, can handle a
     * value &gt; 255.
     * 
     * @param packet
     *            to append to
     * @param end
     *            The Java encoding name used to lookup the collation index
     */
    public void appendCharsetByteForHandshake(Buffer packet, String enc) {
        int charsetIndex = 0;
        if (enc != null) {
            charsetIndex = CharsetMapping.getCollationIndexForJavaEncoding(enc, this.session.getSessionState().getServerVersion());
        }
        if (charsetIndex == 0) {
            charsetIndex = CharsetMapping.MYSQL_COLLATION_INDEX_utf8;
        }
        if (charsetIndex > 255) {
            throw ExceptionFactory.createException(Messages.getString("MysqlIO.113", new Object[] { enc }), getExceptionInterceptor());
        }
        packet.writeByte((byte) charsetIndex);
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    /**
     * Negotiates the SSL communications channel used when connecting
     * to a MySQL server that understands SSL.
     * 
     * @param user
     * @param password
     * @param database
     * @param packLength
     * @throws SQLException
     * @throws CommunicationsException
     */
    private void negotiateSSLConnection(String user, String password, String database, int packLength) {

        this.protocol.negotiateSSLConnection(user, password, database, packLength);

    }

    /**
     * Re-authenticates as the given user and password
     * 
     * @param userName
     * @param password
     * @param database
     * 
     */
    @Override
    public void changeUser(String userName, String password, String database) {
        proceedHandshakeWithPluggableAuthentication(userName, password, database, null);
    }

}
