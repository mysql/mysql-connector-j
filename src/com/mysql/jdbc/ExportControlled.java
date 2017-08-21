/*
  Copyright (c) 2002, 2017, Oracle and/or its affiliates. All rights reserved.

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
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertPathValidatorResult;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXCertPathValidatorResult;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.crypto.Cipher;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.mysql.jdbc.util.Base64Decoder;

/**
 * Holds functionality that falls under export-control regulations.
 */
public class ExportControlled {
    private static final String SQL_STATE_BAD_SSL_PARAMS = "08000";
    private static final String TLSv1 = "TLSv1";
    private static final String TLSv1_1 = "TLSv1.1";
    private static final String TLSv1_2 = "TLSv1.2";
    private static final String[] TLS_PROTOCOLS = new String[] { TLSv1_2, TLSv1_1, TLSv1 };

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

            String[] tryProtocols = null;

            // If enabledTLSProtocols configuration option is set, overriding the default TLS version restrictions.
            // This allows enabling TLSv1.2 for self-compiled MySQL versions supporting it, as well as the ability
            // for users to restrict TLS connections to approved protocols (e.g., prohibiting TLSv1) on the client side.
            String enabledTLSProtocols = mysqlIO.connection.getEnabledTLSProtocols();
            if (enabledTLSProtocols != null && enabledTLSProtocols.length() > 0) {
                tryProtocols = enabledTLSProtocols.split("\\s*,\\s*");
            } else {
                // Note that it is problematic to enable TLSv1.2 on the client side when the server is compiled with yaSSL.
                // When client attempts to connect with TLSv1.2 yaSSL just closes the socket instead of re-attempting handshake with
                // lower TLS version.
                if (mysqlIO.versionMeetsMinimum(5, 6, 0) && Util.isEnterpriseEdition(mysqlIO.getServerVersion())) {
                    tryProtocols = new String[] { TLSv1_2, TLSv1_1, TLSv1 };
                }
            }
            // allow TLSv1 and TLSv1.1 for all server versions by default
            if (tryProtocols == null) {
                tryProtocols = new String[] { TLSv1_1, TLSv1 };
            }
            List<String> configuredProtocols = new ArrayList<String>(Arrays.asList(tryProtocols));
            List<String> jvmSupportedProtocols = Arrays.asList(((SSLSocket) mysqlIO.mysqlConnection).getSupportedProtocols());

            List<String> allowedProtocols = new ArrayList<String>();
            for (String protocol : TLS_PROTOCOLS) {
                if (jvmSupportedProtocols.contains(protocol) && configuredProtocols.contains(protocol)) {
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
                if (mysqlIO.versionMeetsMinimum(5, 5, 45) && !mysqlIO.versionMeetsMinimum(5, 6, 0)
                        || mysqlIO.versionMeetsMinimum(5, 6, 26) && !mysqlIO.versionMeetsMinimum(5, 7, 0) || mysqlIO.versionMeetsMinimum(5, 7, 6)) {
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

    /**
     * Implementation of X509TrustManager wrapping JVM X509TrustManagers to add expiration check
     */
    public static class X509TrustManagerWrapper implements X509TrustManager {

        private X509TrustManager origTm = null;
        private boolean verifyServerCert = false;
        private CertificateFactory certFactory = null;
        private PKIXParameters validatorParams = null;
        private CertPathValidator validator = null;

        public X509TrustManagerWrapper(X509TrustManager tm, boolean verifyServerCertificate) throws CertificateException {
            this.origTm = tm;
            this.verifyServerCert = verifyServerCertificate;

            if (verifyServerCertificate) {
                try {
                    Set<TrustAnchor> anch = new HashSet<TrustAnchor>();
                    for (X509Certificate cert : tm.getAcceptedIssuers()) {
                        anch.add(new TrustAnchor(cert, null));
                    }
                    this.validatorParams = new PKIXParameters(anch);
                    this.validatorParams.setRevocationEnabled(false);
                    this.validator = CertPathValidator.getInstance("PKIX");
                    this.certFactory = CertificateFactory.getInstance("X.509");
                } catch (Exception e) {
                    throw new CertificateException(e);
                }
            }
        }

        public X509TrustManagerWrapper() {
        }

        public X509Certificate[] getAcceptedIssuers() {
            return this.origTm != null ? this.origTm.getAcceptedIssuers() : new X509Certificate[0];
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            for (int i = 0; i < chain.length; i++) {
                chain[i].checkValidity();
            }

            if (this.validatorParams != null) {

                X509CertSelector certSelect = new X509CertSelector();
                certSelect.setSerialNumber(chain[0].getSerialNumber());

                try {
                    CertPath certPath = this.certFactory.generateCertPath(Arrays.asList(chain));
                    // Validate against truststore
                    CertPathValidatorResult result = this.validator.validate(certPath, this.validatorParams);
                    // Check expiration for the CA used to validate this path
                    ((PKIXCertPathValidatorResult) result).getTrustAnchor().getTrustedCert().checkValidity();

                } catch (InvalidAlgorithmParameterException e) {
                    throw new CertificateException(e);
                } catch (CertPathValidatorException e) {
                    throw new CertificateException(e);
                }
            }

            if (this.verifyServerCert) {
                this.origTm.checkServerTrusted(chain, authType);
            }
        }

        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            this.origTm.checkClientTrusted(chain, authType);
        }
    };

    private static SSLSocketFactory getSSLSocketFactoryDefaultOrConfigured(MysqlIO mysqlIO) throws SQLException {
        String clientCertificateKeyStoreUrl = mysqlIO.connection.getClientCertificateKeyStoreUrl();
        String clientCertificateKeyStorePassword = mysqlIO.connection.getClientCertificateKeyStorePassword();
        String clientCertificateKeyStoreType = mysqlIO.connection.getClientCertificateKeyStoreType();
        String trustCertificateKeyStoreUrl = mysqlIO.connection.getTrustCertificateKeyStoreUrl();
        String trustCertificateKeyStorePassword = mysqlIO.connection.getTrustCertificateKeyStorePassword();
        String trustCertificateKeyStoreType = mysqlIO.connection.getTrustCertificateKeyStoreType();

        if (StringUtils.isNullOrEmpty(clientCertificateKeyStoreUrl)) {
            clientCertificateKeyStoreUrl = System.getProperty("javax.net.ssl.keyStore");
            clientCertificateKeyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
            clientCertificateKeyStoreType = System.getProperty("javax.net.ssl.keyStoreType");
            if (StringUtils.isNullOrEmpty(clientCertificateKeyStoreType)) {
                clientCertificateKeyStoreType = "JKS";
            }
            // check URL
            if (!StringUtils.isNullOrEmpty(clientCertificateKeyStoreUrl)) {
                try {
                    new URL(clientCertificateKeyStoreUrl);
                } catch (MalformedURLException e) {
                    clientCertificateKeyStoreUrl = "file:" + clientCertificateKeyStoreUrl;
                }
            }
        }

        if (StringUtils.isNullOrEmpty(trustCertificateKeyStoreUrl)) {
            trustCertificateKeyStoreUrl = System.getProperty("javax.net.ssl.trustStore");
            trustCertificateKeyStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
            trustCertificateKeyStoreType = System.getProperty("javax.net.ssl.trustStoreType");
            if (StringUtils.isNullOrEmpty(trustCertificateKeyStoreType)) {
                trustCertificateKeyStoreType = "JKS";
            }
            // check URL
            if (!StringUtils.isNullOrEmpty(trustCertificateKeyStoreUrl)) {
                try {
                    new URL(trustCertificateKeyStoreUrl);
                } catch (MalformedURLException e) {
                    trustCertificateKeyStoreUrl = "file:" + trustCertificateKeyStoreUrl;
                }
            }
        }

        TrustManagerFactory tmf = null;
        KeyManagerFactory kmf = null;

        KeyManager[] kms = null;
        List<TrustManager> tms = new ArrayList<TrustManager>();

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
                    kms = kmf.getKeyManagers();
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

        InputStream trustStoreIS = null;
        try {
            KeyStore trustKeyStore = null;

            if (!StringUtils.isNullOrEmpty(trustCertificateKeyStoreUrl) && !StringUtils.isNullOrEmpty(trustCertificateKeyStoreType)) {
                trustStoreIS = new URL(trustCertificateKeyStoreUrl).openStream();
                char[] trustStorePassword = (trustCertificateKeyStorePassword == null) ? new char[0] : trustCertificateKeyStorePassword.toCharArray();

                trustKeyStore = KeyStore.getInstance(trustCertificateKeyStoreType);
                trustKeyStore.load(trustStoreIS, trustStorePassword);
            }

            tmf.init(trustKeyStore); // (trustKeyStore == null) initializes the TrustManagerFactory with the default truststore. 

            // building the customized list of TrustManagers from original one if it's available
            TrustManager[] origTms = tmf.getTrustManagers();
            final boolean verifyServerCert = mysqlIO.connection.getVerifyServerCertificate();

            for (TrustManager tm : origTms) {
                // wrap X509TrustManager or put original if non-X509 TrustManager
                tms.add(tm instanceof X509TrustManager ? new X509TrustManagerWrapper((X509TrustManager) tm, verifyServerCert) : tm);
            }

        } catch (MalformedURLException e) {
            throw SQLError.createSQLException(trustCertificateKeyStoreUrl + " does not appear to be a valid URL.", SQL_STATE_BAD_SSL_PARAMS, 0, false,
                    mysqlIO.getExceptionInterceptor());
        } catch (KeyStoreException e) {
            throw SQLError.createSQLException("Could not create KeyStore instance [" + e.getMessage() + "]", SQL_STATE_BAD_SSL_PARAMS, 0, false,
                    mysqlIO.getExceptionInterceptor());
        } catch (NoSuchAlgorithmException e) {
            throw SQLError.createSQLException("Unsupported keystore algorithm [" + e.getMessage() + "]", SQL_STATE_BAD_SSL_PARAMS, 0, false,
                    mysqlIO.getExceptionInterceptor());
        } catch (CertificateException e) {
            throw SQLError.createSQLException("Could not load trust" + trustCertificateKeyStoreType + " keystore from " + trustCertificateKeyStoreUrl,
                    SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
        } catch (IOException e) {
            SQLException sqlEx = SQLError.createSQLException("Cannot open " + trustCertificateKeyStoreType + " [" + e.getMessage() + "]",
                    SQL_STATE_BAD_SSL_PARAMS, 0, false, mysqlIO.getExceptionInterceptor());
            sqlEx.initCause(e);
            throw sqlEx;
        } finally {
            if (trustStoreIS != null) {
                try {
                    trustStoreIS.close();
                } catch (IOException e) {
                    // can't close input stream, but keystore can be properly initialized so we shouldn't throw this exception
                }
            }
        }

        // if original TrustManagers are not available then putting one X509TrustManagerWrapper which take care only about expiration check 
        if (tms.size() == 0) {
            tms.add(new X509TrustManagerWrapper());
        }

        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(kms, tms.toArray(new TrustManager[tms.size()]), null);
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