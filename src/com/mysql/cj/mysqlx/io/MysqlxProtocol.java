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

package com.mysql.cj.mysqlx.io;

import java.io.IOException;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.google.protobuf.ByteString;

import static com.mysql.cj.mysqlx.protobuf.Mysqlx.Ok;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateFail;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateOk;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateStart;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.StmtExecute;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.Session;
import com.mysql.cj.api.authentication.AuthenticationProvider;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.PacketBuffer;
import com.mysql.cj.api.io.PacketSentTimeHolder;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.api.io.ResultsHandler;
import com.mysql.cj.api.io.ServerCapabilities;
import com.mysql.cj.api.io.ServerSession;
import com.mysql.cj.api.io.SocketConnection;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.mysqla.io.Buffer;
import com.mysql.cj.mysqlx.ExprUtil;
import com.mysql.cj.mysqlx.MysqlxSession;

/**
 * Low-level interface to communications with a MySQL-X server.
 */
public class MysqlxProtocol implements Protocol {
    // "xplugin" namespace for StmtExecute messages
    private static final String XPLUGIN_NAMESPACE = "xplugin";

    private static final String XPLUGIN_STMT_CREATE_COLLECTION = "create_collection";

    private MessageReader reader;
    private MessageWriter writer;

    public MysqlxProtocol(MessageReader reader, MessageWriter writer) {
        this.reader = reader;
        this.writer = writer;
    }

    public void init(MysqlConnection conn, int socketTimeout, SocketConnection socketConnection, PropertySet propertySet) {
        throw new NullPointerException("TODO: this implementation uses a constructor");
    }

    public PropertySet getPropertySet() {
        throw new NullPointerException("TODO");
    }

    public void setPropertySet(PropertySet propertySet) {
        throw new NullPointerException("TODO");
    }

    public ServerCapabilities readServerCapabilities() {
        throw new NullPointerException("TODO");
    }

    public ServerSession getServerSession() {
        throw new NullPointerException("TODO");
    }

    public MysqlConnection getConnection() {
        throw new NullPointerException("TODO");
    }

    public void setConnection(MysqlConnection connection) {
        throw new NullPointerException("TODO");
    }

    public SocketConnection getSocketConnection() {
        throw new NullPointerException("TODO");
    }

    public AuthenticationProvider getAuthenticationProvider() {
        throw new NullPointerException("TODO");
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        throw new NullPointerException("TODO");
    }

    public ResultsHandler getResultsHandler() {
        throw new NullPointerException("TODO");
    }

    public PacketSentTimeHolder getPacketSentTimeHolder() {
        throw new NullPointerException("TODO");
    }

    public void setPacketSentTimeHolder(PacketSentTimeHolder packetSentTimeHolder) {
        throw new NullPointerException("TODO");
    }

    public long getLastPacketReceivedTimeMs() {
        throw new NullPointerException("TODO");
    }

    /**
     * @todo docs?
     */
    public Session getSession(String user, String password, String database) {
        sendSaslAuthStart(user, password, database);
        // TODO: expired password handling
        readAuthenticateOk();
        return new MysqlxSession(this);
    }

    /**
     * @todo very MySQL-X specific method.
     */
    public void sendSaslAuthStart(String user, String password, String database) {
        // SASL requests information from the app through callbacks. We provide the username and password by these callbacks. This implementation works for
        // PLAIN and would also work for CRAM-MD5. Additional standardized methods may require additional callbacks. Non-standard methods such as MySQL auth
        // would require implementing a new mechanism to deal with the salt and hashing, etc.
        CallbackHandler callbackHandler = new CallbackHandler() {
                public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
                    for (Callback c : callbacks) {
                        if (NameCallback.class.isAssignableFrom(c.getClass())) {
                            // we get a name callback and provide the username
                            ((NameCallback) c).setName(user);
                        } else if (PasswordCallback.class.isAssignableFrom(c.getClass())) {
                            // we get  password callback and provide the password
                            ((PasswordCallback) c).setPassword(password.toCharArray());
                        } else {
                            // otherwise, thrown an exception
                            throw new UnsupportedCallbackException(c);
                        }
                    }
                }
            };
        try {
            // now we create the client object we use which can handle PLAIN mechanism for "MySQL-X" protocol to "serverName"
            String[] mechanisms = new String[] {"PLAIN"};
            String authorizationId = database; // as per protocol spec
            String protocol = "MySQL-X";
            Map<String, ?> props = null;
            // TODO: >> serverName. Is this of any use in our MySQL-X exchange? Should be defined to be blank or something.
            String serverName = "<unknown>";
            SaslClient saslClient = Sasl.createSaslClient(mechanisms, authorizationId, protocol, serverName, props, callbackHandler);

            // now just pass the details to the X-protocol auth start message
            AuthenticateStart.Builder authStartBuilder = AuthenticateStart.newBuilder();
            authStartBuilder.setMechName("PLAIN");
            // saslClient will build the SASL response message
            authStartBuilder.setAuthData(ByteString.copyFrom(saslClient.evaluateChallenge(null)));

            this.writer.write(authStartBuilder.build());
        } catch (SaslException ex) {
            // TODO: better exception, should introduce a new exception class for auth?
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new CJCommunicationsException(ex);
        }
    }

    public void negotiateSSLConnection(int packLength) {
        throw new NullPointerException("TODO: SSL is not yet supported in this MySQL-X client");
    }

    public void rejectConnection(String message) {
        throw new NullPointerException("TODO");
    }

    public void rejectProtocol(Buffer buf) {
        throw new NullPointerException("TODO");
    }

    public void beforeHandshake() {
        throw new NullPointerException("TODO");
    }

    public void afterHandshake() {
        throw new NullPointerException("TODO");
    }

    public void changeDatabase(String database) {
        throw new NullPointerException("TODO: Figure out how this is relevant for MySQL-X Session");
    }

    public void changeUser(String user, String password, String database) {
        // TODO: implement change user. Do we need to Close the session first?
        throw new NullPointerException("TODO");
    }

    public Buffer readPacket() {
        throw new NullPointerException("TODO: This shouldn't be a protocol method. MySQL-X doesn't use buffers");
    }

    public Buffer readNextPacket() {
        throw new NullPointerException("TODO: This shouldn't be a protocol method. MySQL-X doesn't use buffers");
    }

    public void send(PacketBuffer packet, int packetLen) {
        throw new NullPointerException("TODO: This shouldn't be a protocol method. MySQL-X doesn't use buffers");
    }

    public Buffer sendCommand(int command, String extraData, Buffer queryPacket, boolean skipCheck, String extraDataCharEncoding, int timeoutMillis) {
        throw new NullPointerException("TODO: This shouldn't be a protocol method. MySQL-X doesn't use buffers or command tags in the same way");
    }

    public String getPasswordCharacterEncoding() {
        throw new NullPointerException("TODO");
    }

    public boolean versionMeetsMinimum(int major, int minor, int subminor) {
        throw new NullPointerException("TODO: expose this via ServerVersion so calls look like x.getServerVersion().meetsMinimum(major, minor, subminor)");
    }

    /**
     * @todo not sure how to best expose this in a cross-protocol way. It should expose the state returned in an OK message.
     */
    public void readOk() {
        try {
            this.reader.read(Ok.class);
            // Ok msg = this.reader.read();
            // System.err.println(msg);
        } catch (IOException ex) {
            // TODO: move Comm exc down into reader/writer
            throw new RuntimeException(ex);
        }
    }

    public void readAuthenticateOk() {
        try {
            // if (this.reader.getNextMessageClass() == AuthenticateFail.class) {
            //     AuthenticateFail msg = this.reader.read(AuthenticateFail.class);
            // }
            this.reader.read(AuthenticateOk.class);
        } catch (IOException ex) {
            // TODO: move Comm exc down into reader/writer
            throw new RuntimeException(ex);
        }
    }

    // TODO: the follow methods should be expose via a different interface such as CrudProtocol
    public void sendCreateCollection(String schemaName, String collectionName) {
        StmtExecute.Builder builder = StmtExecute.newBuilder();

        builder.setNamespace(XPLUGIN_NAMESPACE);
        builder.setStmt(XPLUGIN_STMT_CREATE_COLLECTION);
        builder.addArgs(ExprUtil.buildAny(schemaName));
        builder.addArgs(ExprUtil.buildAny(collectionName));

        try {
            this.writer.write(builder.build());
        } catch (IOException ex) {
            // TODO: move Comm exc down into reader/writer
            throw new RuntimeException(ex);
        }
    }
}
