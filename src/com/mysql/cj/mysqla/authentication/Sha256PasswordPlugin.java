/*
  Copyright (c) 2012, 2015, Oracle and/or its affiliates. All rights reserved.

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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.authentication.AuthenticationPlugin;
import com.mysql.cj.api.io.PacketBuffer;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.authentication.Security;
import com.mysql.cj.core.exception.CJException;
import com.mysql.cj.core.exception.ExceptionFactory;
import com.mysql.cj.core.exception.UnableToConnectException;
import com.mysql.cj.core.exception.WrongArgumentException;
import com.mysql.cj.core.io.Buffer;
import com.mysql.cj.core.io.ExportControlled;
import com.mysql.cj.core.io.ProtocolConstants;
import com.mysql.cj.core.util.StringUtils;

/**
 * MySQL Clear Password Authentication Plugin
 */
public class Sha256PasswordPlugin implements AuthenticationPlugin {

    private MysqlConnection connection;
    private String password = null;
    private String seed = null;
    private boolean publicKeyRequested = false;
    private String publicKeyString = null;

    public void init(MysqlConnection conn, Properties props) {
        this.connection = conn;

        String pkURL = this.connection.getServerRSAPublicKeyFile();
        if (pkURL != null) {
            this.publicKeyString = readRSAKey(this.connection, pkURL);
        }
    }

    public void destroy() {
        this.password = null;
        this.seed = null;
        this.publicKeyRequested = false;
    }

    public String getProtocolPluginName() {
        return "sha256_password";
    }

    public boolean requiresConfidentiality() {
        return false;
    }

    public boolean isReusable() {
        return true;
    }

    public void setAuthenticationParameters(String user, String password) {
        this.password = password;
    }

    public boolean nextAuthenticationStep(PacketBuffer fromServer, List<PacketBuffer> toServer) {
        toServer.clear();

        if (this.password == null || this.password.length() == 0 || fromServer == null) {
            // no password or changeUser()
            Buffer bresp = new Buffer(new byte[] { 0 });
            toServer.add(bresp);

        } else {
            try {
                if (this.connection.getIO().getPhysicalConnection().isSSLEstablished()) {
                    // allow plain text over SSL
                    Buffer bresp;
                    try {
                        bresp = new Buffer(StringUtils.getBytes(this.password, this.connection.getPasswordCharacterEncoding()));
                    } catch (UnsupportedEncodingException e) {
                        throw ExceptionFactory.createException(
                                Messages.getString("Sha256PasswordPlugin.3", new Object[] { this.connection.getPasswordCharacterEncoding() }), e);
                    }
                    bresp.setPosition(bresp.getBufLength());
                    int oldBufLength = bresp.getBufLength();
                    bresp.writeByte((byte) 0);
                    bresp.setBufLength(oldBufLength + 1);
                    bresp.setPosition(0);
                    toServer.add(bresp);

                } else if (this.connection.getServerRSAPublicKeyFile() != null) {
                    // encrypt with given key, don't use "Public Key Retrieval"
                    this.seed = fromServer.readString();
                    Buffer bresp = new Buffer(encryptPassword(this.password, this.seed, this.publicKeyString, this.connection.getPasswordCharacterEncoding()));
                    toServer.add(bresp);

                } else {
                    if (!this.connection.getAllowPublicKeyRetrieval()) {
                        throw ExceptionFactory.createException(UnableToConnectException.class, Messages.getString("Sha256PasswordPlugin.2"),
                                this.connection.getExceptionInterceptor());

                    }

                    // We must request the public key from the server to encrypt the password
                    if (this.publicKeyRequested && fromServer.getBufLength() > ProtocolConstants.SEED_LENGTH) {
                        // Servers affected by Bug#70865 could send Auth Switch instead of key after Public Key Retrieval,
                        // so we check payload length to detect that.

                        // read key response
                        Buffer bresp = new Buffer(encryptPassword(this.password, this.seed, fromServer.readString(),
                                this.connection.getPasswordCharacterEncoding()));
                        toServer.add(bresp);
                        this.publicKeyRequested = false;
                    } else {
                        // build and send Public Key Retrieval packet
                        this.seed = fromServer.readString();
                        Buffer bresp = new Buffer(new byte[] { 1 });
                        toServer.add(bresp);
                        this.publicKeyRequested = true;
                    }
                }
            } catch (CJException e) {
                throw ExceptionFactory.createException(e.getMessage(), e, this.connection.getExceptionInterceptor());
            }
        }
        return true;
    }

    private static byte[] encryptPassword(String password, String seed, String key, String passwordCharacterEncoding) {
        byte[] input = null;
        try {
            input = password != null ? StringUtils.getBytesNullTerminated(password, passwordCharacterEncoding) : new byte[] { 0 };
        } catch (UnsupportedEncodingException e) {
            throw ExceptionFactory.createException(Messages.getString("Sha256PasswordPlugin.3", new Object[] { passwordCharacterEncoding }), e);
        }
        byte[] mysqlScrambleBuff = new byte[input.length];
        Security.xorString(input, mysqlScrambleBuff, seed.getBytes(), input.length);
        return ExportControlled.encryptWithRSAPublicKey(mysqlScrambleBuff, ExportControlled.decodeRSAPublicKey(key));
    }

    private static String readRSAKey(MysqlConnection connection, String pkPath) {
        String res = null;
        byte[] fileBuf = new byte[2048];

        BufferedInputStream fileIn = null;

        try {
            File f = new File(pkPath);
            String canonicalPath = f.getCanonicalPath();
            fileIn = new BufferedInputStream(new FileInputStream(canonicalPath));

            int bytesRead = 0;

            StringBuilder sb = new StringBuilder();
            while ((bytesRead = fileIn.read(fileBuf)) != -1) {
                sb.append(StringUtils.toAsciiString(fileBuf, 0, bytesRead));
            }
            res = sb.toString();

        } catch (IOException ioEx) {

            throw ExceptionFactory.createException(WrongArgumentException.class,
                    Messages.getString("Sha256PasswordPlugin.0", connection.getParanoid() ? new Object[] { "" } : new Object[] { "'" + pkPath + "'" }),
                    connection.getExceptionInterceptor());

        } finally {
            if (fileIn != null) {
                try {
                    fileIn.close();
                } catch (IOException e) {
                    throw ExceptionFactory.createException(Messages.getString("Sha256PasswordPlugin.1"), e, connection.getExceptionInterceptor());
                }
            }
        }

        return res;
    }

}
