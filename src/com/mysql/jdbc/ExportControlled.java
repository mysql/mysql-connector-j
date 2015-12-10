/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.crypto.Cipher;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.mysql.jdbc.util.Base64Decoder;

/**
 * Holds functionality that falls under export-control regulations.
 */
public class ExportControlled {
    private static final String SQL_STATE_BAD_SSL_PARAMS = "08000";

    protected static boolean enabled() {
        // we may wish to un-static-ify this class this static method call may be removed entirely by the compiler
        return true;
    }

    /**
     * Converts the socket being used in the given MysqlIO to an SSLSocket by
     * performing the SSL/TLS handshake.
     * 
     * @param mysqlIO
     *            the MysqlIO instance containing the socket to convert to an
     *            SSLSocket.
     * 
     * @throws CommunicationsException
     *             if the handshake fails, or if this distribution of
     *             Connector/J doesn't contain the SSL crypto hooks needed to
     *             perform the handshake.
     */
    protected static void transformSocketToSSLSocket(MysqlIO mysqlIO) throws SQLException {
        SocketFactory sslFact = new StandardSSLSocketFactory(getSSLSocketFactoryDefaultOrConfigured(mysqlIO), mysqlIO.socketFactory, mysqlIO.mysqlConnection);

        try {
            mysqlIO.mysqlConnection = sslFact.connect(mysqlIO.host, mysqlIO.port, null);

            List<String> allowedProtocols = new ArrayList<String>();
            List<String> supportedProtocols = Arrays.asList(((SSLSocket) mysqlIO.mysqlConnection).getSupportedProtocols());
            for (String protocol : (mysqlIO.versionMeetsMinimum(5, 6, 0) && Util.isEnterpriseEdition(mysqlIO.getServerVersion()) ? new String[] { "TLSv1.2",
                    "TLSv1.1", "TLSv1" } : new String[] { "TLSv1.1", "TLSv1" })) {
                if (supportedProtocols.contains(protocol)) {
                    allowedProtocols.add(protocol);
                }
            }
            ((SSLSocket) mysqlIO.mysqlConnection).setEnabledProtocols(allowedProtocols.toArray(new String[0]));

            // check allowed cipher suites
            String enabledSSLCipherSuites = mysqlIO.connection.getEnabledSSLCipherSuites();
            boolean overrideCiphers = enabledSSLCipherSuites != null && enabledSSLCipherSuites.length() > 0;

            List<String> allowedCiphers = null;
            if (overrideCiphers) {
                // If "enabledSSLCipherSuites" is set we just check that JVM allows provided values,
                // we don't disable DH algorithm, that allows c/J to deal with custom server builds with different security restrictions
                allowedCiphers = new ArrayList<String>();
                List<String> availableCiphers = Arrays.asList(((SSLSocket) mysqlIO.mysqlConnection).getEnabledCipherSuites());
                for (String cipher : enabledSSLCipherSuites.split("\\s*,\\s*")) {
                    if (availableCiphers.contains(cipher)) {
                        allowedCiphers.add(cipher);
                    }
                }

            } else {
                // If we don't override ciphers, then we check for known restrictions
                boolean disableDHAlgorithm = false;
                if (mysqlIO.versionMeetsMinimum(5, 5, 45) && !mysqlIO.versionMeetsMinimum(5, 6, 0) || mysqlIO.versionMeetsMinimum(5, 6, 26)
                        && !mysqlIO.versionMeetsMinimum(5, 7, 0) || mysqlIO.versionMeetsMinimum(5, 7, 6)) {
                    // Workaround for JVM bug http://bugs.java.com/bugdatabase/view_bug.do?bug_id=6521495
                    // Starting from 5.5.45, 5.6.26 and 5.7.6 server the key length used for creating Diffie-Hellman keys has been
                    // increased from 512 to 2048 bits, while JVMs affected by this bug allow only range from 512 to 1024 (inclusive).
                    // Bug is fixed in Java 8.
                    if (Util.getJVMVersion() < 8) {
                        disableDHAlgorithm = true;
                    }
                } else if (Util.getJVMVersion() >= 8) { // TODO check later for Java 9 behavior
                    // Java 8 default java.security contains jdk.tls.disabledAlgorithms=DH keySize < 768
                    // That causes handshake failures with older MySQL servers, eg 5.6.11. Thus we have to disable DH for them when running on Java 8+
                    disableDHAlgorithm = true;
                }

                if (disableDHAlgorithm) {
                    allowedCiphers = new ArrayList<String>();
                    for (String cipher : ((SSLSocket) mysqlIO.mysqlConnection).getEnabledCipherSuites()) {
                        if (!(disableDHAlgorithm && (cipher.indexOf("_DHE_") > -1 || cipher.indexOf("_DH_") > -1))) {
                            allowedCiphers.add(cipher);
                        }
                    }
                }
            }

            // if some ciphers were filtered into allowedCiphers 
            if (allowedCiphers != null) {
                ((SSLSocket) mysqlIO.mysqlConnection).setEnabledCipherSuites(allowedCiphers.toArray(new String[0]));
            }

            ((SSLSocket) mysqlIO.mysqlConnection).startHandshake();

            if (mysqlIO.connection.getUseUnbufferedInput()) {
                mysqlIO.mysqlInput = mysqlIO.mysqlConnection.getInputStream();
            } else {
                mysqlIO.mysqlInput = new BufferedInputStream(mysqlIO.mysqlConnection.getInputStream(), 16384);
            }

            mysqlIO.mysqlOutput = new BufferedOutputStream(mysqlIO.mysqlConnection.getOutputStream(), 16384);

            mysqlIO.mysqlOutput.flush();

            mysqlIO.socketFactory = sslFact;

        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(mysqlIO.connection, mysqlIO.getLastPacketSentTimeMs(), mysqlIO.getLastPacketReceivedTimeMs(), ioEx,
                    mysqlIO.getExceptionInterceptor());
        }
    }

    /**
     * Implementation of internal socket factory to wrap the SSL socket.
     */
    public static class StandardSSLSocketFactory implements SocketFactory, SocketMetadata {
        private SSLSocket rawSocket = null;
        private final SSLSocketFactory sslFact;
        private final SocketFactory existingSocketFactory;
        private final Socket existingSocket;

        public StandardSSLSocketFactory(SSLSocketFactory sslFact, SocketFactory existingSocketFactory, Socket existingSocket) {
            this.sslFact = sslFact;
            this.existingSocketFactory = existingSocketFactory;
            this.existingSocket = existingSocket;
        }

        public Socket afterHandshake() throws SocketException, IOException {
            this.existingSocketFactory.afterHandshake();
            return this.rawSocket;
        }

        public Socket beforeHandshake() throws SocketException, IOException {
            return this.rawSocket;
        }

        public Socket connect(String host, int portNumber, Properties props) throws SocketException, IOException {
            this.rawSocket = (SSLSocket) this.sslFact.createSocket(this.existingSocket, host, portNumber, true);
            return this.rawSocket;
        }

        public boolean isLocallyConnected(ConnectionImpl conn) throws SQLException {
            return SocketMetadata.Helper.isLocallyConnected(conn);
        }

    }

    private ExportControlled() { /* prevent instantiation */
    }

    private static SSLSocketFactory getSSLSocketFactoryDefaultOrConfigured(MysqlIO mysqlIO) throws SQLException {
        String clientCertificateKeyStoreUrl = mysqlIO.connection.getClientCertificateKeyStoreUrl();
        String trustCertificateKeyStoreUrl = mysqlIO.connection.getTrustCertificateKeyStoreUrl();
        String clientCertificateKeyStoreType = mysqlIO.connection.getClientCertificateKeyStoreType();
        String clientCertificateKeyStorePassword = mysqlIO.connection.getClientCertificateKeyStorePassword();
        String trustCertificateKeyStoreType = mysqlIO.connection.getTrustCertificateKeyStoreType();
        String trustCertificateKeyStorePassword = mysqlIO.connection.getTrustCertificateKeyStorePassword();

        if (StringUtils.isNullOrEmpty(clientCertificateKeyStoreUrl) && StringUtils.isNullOrEmpty(trustCertificateKeyStoreUrl)) {
            if (mysqlIO.connection.getVerifyServerCertificate()) {
                return (SSLSocketFactory) SSLSocketFactory.getDefault();
            }
        }

        TrustManagerFactory tmf = null;
        KeyManagerFactory kmf = null;

        try {
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        } catch (NoSuchAlgorithmException nsae) {
            throw SQLError.createSQLException(
                    "Default algorithm definitions for TrustManager and/or KeyManager are invalid.  Check java security properties file.",
                    SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
        }

        if (!StringUtils.isNullOrEmpty(clientCertificateKeyStoreUrl)) {
            InputStream ksIS = null;
            try {
                if (!StringUtils.isNullOrEmpty(clientCertificateKeyStoreType)) {
                    KeyStore clientKeyStore = KeyStore.getInstance(clientCertificateKeyStoreType);
                    URL ksURL = new URL(clientCertificateKeyStoreUrl);
                    char[] password = (clientCertificateKeyStorePassword == null) ? new char[0] : clientCertificateKeyStorePassword.toCharArray();
                    ksIS = ksURL.openStream();
                    clientKeyStore.load(ksIS, password);
                    kmf.init(clientKeyStore, password);
                }
            } catch (UnrecoverableKeyException uke) {
                throw SQLError.createSQLException("Could not recover keys from client keystore.  Check password?", SQL_STATE_BAD_SSL_PARAMS, 0, false,
                        mysqlIO.getExceptionInterceptor());
            } catch (NoSuchAlgorithmException nsae) {
                throw SQLError.createSQLException("Unsupported keystore algorithm [" + nsae.getMessage() + "]", SQL_STATE_BAD_SSL_PARAMS, 0, false,
                        mysqlIO.getExceptionInterceptor());
            } catch (KeyStoreException kse) {
                throw SQLError.createSQLException("Could not create KeyStore instance [" + kse.getMessage() + "]", SQL_STATE_BAD_SSL_PARAMS, 0, false,
                        mysqlIO.getExceptionInterceptor());
            } catch (CertificateException nsae) {
                throw SQLError.createSQLException("Could not load client" + clientCertificateKeyStoreType + " keystore from " + clientCertificateKeyStoreUrl,
                        mysqlIO.getExceptionInterceptor());
            } catch (MalformedURLException mue) {
                throw SQLError.createSQLException(clientCertificateKeyStoreUrl + " does not appear to be a valid URL.", SQL_STATE_BAD_SSL_PARAMS, 0, false,
                        mysqlIO.getExceptionInterceptor());
            } catch (IOException ioe) {
                SQLException sqlEx = SQLError.createSQLException("Cannot open " + clientCertificateKeyStoreUrl + " [" + ioe.getMessage() + "]",
                        SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
                sqlEx.initCause(ioe);

                throw sqlEx;
            } finally {
                if (ksIS != null) {
                    try {
                        ksIS.close();
                    } catch (IOException e) {
                        // can't close input stream, but keystore can be properly initialized so we shouldn't throw this exception
                    }
                }
            }
        }

        if (!StringUtils.isNullOrEmpty(trustCertificateKeyStoreUrl)) {

            InputStream ksIS = null;
            try {
                if (!StringUtils.isNullOrEmpty(trustCertificateKeyStoreType)) {
                    KeyStore trustKeyStore = KeyStore.getInstance(trustCertificateKeyStoreType);
                    URL ksURL = new URL(trustCertificateKeyStoreUrl);

                    char[] password = (trustCertificateKeyStorePassword == null) ? new char[0] : trustCertificateKeyStorePassword.toCharArray();
                    ksIS = ksURL.openStream();
                    trustKeyStore.load(ksIS, password);
                    tmf.init(trustKeyStore);
                }
            } catch (NoSuchAlgorithmException nsae) {
                throw SQLError.createSQLException("Unsupported keystore algorithm [" + nsae.getMessage() + "]", SQL_STATE_BAD_SSL_PARAMS, 0, false,
                        mysqlIO.getExceptionInterceptor());
            } catch (KeyStoreException kse) {
                throw SQLError.createSQLException("Could not create KeyStore instance [" + kse.getMessage() + "]", SQL_STATE_BAD_SSL_PARAMS, 0, false,
                        mysqlIO.getExceptionInterceptor());
            } catch (CertificateException nsae) {
                throw SQLError.createSQLException("Could not load trust" + trustCertificateKeyStoreType + " keystore from " + trustCertificateKeyStoreUrl,
                        SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
            } catch (MalformedURLException mue) {
                throw SQLError.createSQLException(trustCertificateKeyStoreUrl + " does not appear to be a valid URL.", SQL_STATE_BAD_SSL_PARAMS, 0, false,
                        mysqlIO.getExceptionInterceptor());
            } catch (IOException ioe) {
                SQLException sqlEx = SQLError.createSQLException("Cannot open " + trustCertificateKeyStoreUrl + " [" + ioe.getMessage() + "]",
                        SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());

                sqlEx.initCause(ioe);

                throw sqlEx;
            } finally {
                if (ksIS != null) {
                    try {
                        ksIS.close();
                    } catch (IOException e) {
                        // can't close input stream, but keystore can be properly initialized so we shouldn't throw this exception
                    }
                }
            }
        }

        SSLContext sslContext = null;

        try {
            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(StringUtils.isNullOrEmpty(clientCertificateKeyStoreUrl) ? null : kmf.getKeyManagers(),
                    mysqlIO.connection.getVerifyServerCertificate() ? tmf.getTrustManagers() : new X509TrustManager[] { new X509TrustManager() {
                        public void checkClientTrusted(X509Certificate[] chain, String authType) {
                            // return without complaint
                        }

                        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                            // return without complaint
                        }

                        public X509Certificate[] getAcceptedIssuers() {
                            return null;
                        }
                    } }, null);

            return sslContext.getSocketFactory();
        } catch (NoSuchAlgorithmException nsae) {
            throw SQLError.createSQLException("TLS is not a valid SSL protocol.", SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
        } catch (KeyManagementException kme) {
            throw SQLError.createSQLException("KeyManagementException: " + kme.getMessage(), SQL_STATE_BAD_SSL_PARAMS, 0, false,
                    mysqlIO.getExceptionInterceptor());
        }
    }

    public static boolean isSSLEstablished(MysqlIO mysqlIO) {
        return SSLSocket.class.isAssignableFrom(mysqlIO.mysqlConnection.getClass());
    }

    public static RSAPublicKey decodeRSAPublicKey(String key, ExceptionInterceptor interceptor) throws SQLException {

        try {
            if (key == null) {
                throw new SQLException("key parameter is null");
            }

            int offset = key.indexOf("\n") + 1;
            int len = key.indexOf("-----END PUBLIC KEY-----") - offset;

            // TODO: use standard decoders with Java 6+
            byte[] certificateData = Base64Decoder.decode(key.getBytes(), offset, len);

            X509EncodedKeySpec spec = new X509EncodedKeySpec(certificateData);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return (RSAPublicKey) kf.generatePublic(spec);
        } catch (Exception ex) {
            throw SQLError.createSQLException("Unable to decode public key", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex, interceptor);
        }
    }

    public static byte[] encryptWithRSAPublicKey(byte[] source, RSAPublicKey key, ExceptionInterceptor interceptor) throws SQLException {
        try {
            Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
            cipher.init(Cipher.ENCRYPT_MODE, key);
            return cipher.doFinal(source);
        } catch (Exception ex) {
            throw SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex, interceptor);
        }
    }

}