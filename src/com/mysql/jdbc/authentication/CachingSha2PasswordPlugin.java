/*
  Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc.authentication;

import java.io.UnsupportedEncodingException;
import java.security.DigestException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import com.mysql.jdbc.Buffer;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Messages;
import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.MysqlIO;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.Security;
import com.mysql.jdbc.StringUtils;

public class CachingSha2PasswordPlugin extends Sha256PasswordPlugin {
    public static String PLUGIN_NAME = "caching_sha2_password";

    public enum AuthStage {
        FAST_AUTH_SEND_SCRAMBLE, FAST_AUTH_READ_RESULT, FAST_AUTH_COMPLETE, FULL_AUTH;
    }

    private AuthStage stage = AuthStage.FAST_AUTH_SEND_SCRAMBLE;

    @Override
    public void init(Connection conn, Properties props) throws SQLException {
        super.init(conn, props);
        this.stage = AuthStage.FAST_AUTH_SEND_SCRAMBLE;
    }

    @Override
    public void destroy() {
        this.stage = AuthStage.FAST_AUTH_SEND_SCRAMBLE;
        super.destroy();
    }

    @Override
    public String getProtocolPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public boolean nextAuthenticationStep(Buffer fromServer, List<Buffer> toServer) throws SQLException {
        toServer.clear();

        if (this.password == null || this.password.length() == 0 || fromServer == null) {
            // no password
            Buffer bresp = new Buffer(new byte[] { 0 });
            toServer.add(bresp);

        } else {
            if (this.stage == AuthStage.FAST_AUTH_SEND_SCRAMBLE) {
                // send a scramble for fast auth
                this.seed = fromServer.readString();
                try {
                    toServer.add(new Buffer(Security.scrambleCachingSha2(StringUtils.getBytes(this.password, this.connection.getPasswordCharacterEncoding()),
                            this.seed.getBytes())));
                } catch (DigestException e) {
                    throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_GENERAL_ERROR, e, null);
                } catch (UnsupportedEncodingException e) {
                    throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_GENERAL_ERROR, e, null);
                }
                this.stage = AuthStage.FAST_AUTH_READ_RESULT;
                return true;

            } else if (this.stage == AuthStage.FAST_AUTH_READ_RESULT) {
                int fastAuthResult = fromServer.getByteBuffer()[0];
                switch (fastAuthResult) {
                    case 3:
                        this.stage = AuthStage.FAST_AUTH_COMPLETE;
                        return true;
                    case 4:
                        this.stage = AuthStage.FULL_AUTH;
                        break;
                    default:
                        throw SQLError.createSQLException("Unknown server response after fast auth.", SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE,
                                this.connection.getExceptionInterceptor());
                }
            }

            if (((MySQLConnection) this.connection).getIO().isSSLEstablished()) {
                // allow plain text over SSL
                Buffer bresp;
                try {
                    bresp = new Buffer(StringUtils.getBytes(this.password, this.connection.getPasswordCharacterEncoding()));
                } catch (UnsupportedEncodingException e) {
                    throw SQLError.createSQLException(
                            Messages.getString("Sha256PasswordPlugin.3", new Object[] { this.connection.getPasswordCharacterEncoding() }),
                            SQLError.SQL_STATE_GENERAL_ERROR, null);
                }
                bresp.setPosition(bresp.getBufLength());
                int oldBufLength = bresp.getBufLength();
                bresp.writeByte((byte) 0);
                bresp.setBufLength(oldBufLength + 1);
                bresp.setPosition(0);
                toServer.add(bresp);

            } else if (this.connection.getServerRSAPublicKeyFile() != null) {
                // encrypt with given key, don't use "Public Key Retrieval"
                Buffer bresp = new Buffer(encryptPassword());
                toServer.add(bresp);

            } else {
                if (!this.connection.getAllowPublicKeyRetrieval()) {
                    throw SQLError.createSQLException(Messages.getString("Sha256PasswordPlugin.2"), SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE,
                            this.connection.getExceptionInterceptor());
                }

                // We must request the public key from the server to encrypt the password
                if (this.publicKeyRequested && fromServer.getBufLength() > MysqlIO.SEED_LENGTH) {
                    // Servers affected by Bug#70865 could send Auth Switch instead of key after Public Key Retrieval,
                    // so we check payload length to detect that.

                    // read key response
                    this.publicKeyString = fromServer.readString();
                    Buffer bresp = new Buffer(encryptPassword());
                    toServer.add(bresp);
                    this.publicKeyRequested = false;
                } else {
                    // build and send Public Key Retrieval packet
                    Buffer bresp = new Buffer(new byte[] { 2 }); //was 1 in sha256_password
                    toServer.add(bresp);
                    this.publicKeyRequested = true;
                }
            }
        }
        return true;
    }

    @Override
    protected byte[] encryptPassword() throws SQLException {
        if (this.connection.versionMeetsMinimum(8, 0, 5)) {
            return super.encryptPassword();
        }
        return super.encryptPassword("RSA/ECB/PKCS1Padding");
    }

    @Override
    public void reset() {
        this.stage = AuthStage.FAST_AUTH_SEND_SCRAMBLE;
    }
}
