/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.SocketConnection;
import com.mysql.cj.api.io.SocketFactory;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.FeatureNotAvailableException;
import com.mysql.cj.core.exceptions.RSAException;
import com.mysql.cj.core.exceptions.SSLParamsException;
import com.mysql.cj.core.util.Base64Decoder;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.core.util.Util;

/**
 * Holds functionality that falls under export-control regulations.
 */
public class ExportControlled {

    public static boolean enabled() {
        // we may wish to un-static-ify this class this static method call may be removed entirely by the compiler
        return true;
    }

    /**
     * Converts the socket being used in the given CoreIO to an SSLSocket by
     * performing the SSL/TLS handshake.
     * 
     * @param socketConnection
     *            the Protocol instance containing the socket to convert to an
     *            SSLSocket.
     * @throws SSLParamsException
     * 
     *             if the handshake fails, or if this distribution of
     *             Connector/J doesn't contain the SSL crytpo hooks needed to
     *             perform the handshake.
     */
    public static void transformSocketToSSLSocket(SocketConnection socketConnection, ServerVersion serverVersion)
            throws IOException, SSLParamsException, FeatureNotAvailableException {
        SocketFactory sslFact = new StandardSSLSocketFactory(
                getSSLSocketFactoryDefaultOrConfigured(socketConnection.getPropertySet(), socketConnection.getExceptionInterceptor()),
                socketConnection.getSocketFactory(), socketConnection.getMysqlSocket());

        socketConnection.setMysqlSocket(sslFact.connect(socketConnection.getHost(), socketConnection.getPort(), null, 0));

        List<String> allowedProtocols = new ArrayList<String>();
        List<String> supportedProtocols = Arrays.asList(((SSLSocket) socketConnection.getMysqlSocket()).getSupportedProtocols());
        for (String protocol : (serverVersion.meetsMinimum(ServerVersion.parseVersion("5.6.0")) && Util.isEnterpriseEdition(serverVersion.toString())
                ? new String[] { "TLSv1.2", "TLSv1.1", "TLSv1" } : new String[] { "TLSv1.1", "TLSv1" })) {
            if (supportedProtocols.contains(protocol)) {
                allowedProtocols.add(protocol);
            }
        }
        ((SSLSocket) socketConnection.getMysqlSocket()).setEnabledProtocols(allowedProtocols.toArray(new String[0]));

        // check allowed cipher suites
        String enabledSSLCipherSuites = socketConnection.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_enabledSSLCipherSuites)
                .getValue();
        boolean overrideCiphers = enabledSSLCipherSuites != null && enabledSSLCipherSuites.length() > 0;

        if (overrideCiphers) {
            // If "enabledSSLCipherSuites" is set we just check that JVM allows provided values,
            // we don't disable DH algorithm, that allows c/J to deal with custom server builds with different security restrictions
            List<String> allowedCiphers = new ArrayList<String>();
            List<String> availableCiphers = Arrays.asList(((SSLSocket) socketConnection.getMysqlSocket()).getEnabledCipherSuites());
            for (String cipher : enabledSSLCipherSuites.split("\\s*,\\s*")) {
                if (availableCiphers.contains(cipher)) {
                    allowedCiphers.add(cipher);
                }
            }

            // if some ciphers were filtered into allowedCiphers 
            ((SSLSocket) socketConnection.getMysqlSocket()).setEnabledCipherSuites(allowedCiphers.toArray(new String[] {}));
        } else {
            // If we don't override ciphers, then we check for known restrictions

            // Java 8 default java.security contains jdk.tls.disabledAlgorithms=DH keySize < 768
            // That causes handshake failures with older MySQL servers, eg 5.6.11. Thus we have to disable DH for them when running on Java 8+
            // TODO check later for Java 9 behavior
            if (!(serverVersion.meetsMinimum(ServerVersion.parseVersion("5.7.6"))
                    || serverVersion.meetsMinimum(ServerVersion.parseVersion("5.6.26")) && !serverVersion.meetsMinimum(ServerVersion.parseVersion("5.7.0"))
                    || serverVersion.meetsMinimum(ServerVersion.parseVersion("5.5.45")) && !serverVersion.meetsMinimum(ServerVersion.parseVersion("5.6.0")))) {

                List<String> allowedCiphers = new ArrayList<String>();
                for (String cipher : ((SSLSocket) socketConnection.getMysqlSocket()).getEnabledCipherSuites()) {
                    if (cipher.indexOf("_DHE_") == -1 && cipher.indexOf("_DH_") == -1) {
                        allowedCiphers.add(cipher);
                    }
                }
                ((SSLSocket) socketConnection.getMysqlSocket()).setEnabledCipherSuites(allowedCiphers.toArray(new String[] {}));
            }
        }

        ((SSLSocket) socketConnection.getMysqlSocket()).startHandshake();

        if (socketConnection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useUnbufferedInput).getValue()) {
            socketConnection.setMysqlInput(socketConnection.getMysqlSocket().getInputStream());
        } else {
            socketConnection.setMysqlInput(new BufferedInputStream(socketConnection.getMysqlSocket().getInputStream(), 16384));
        }

        socketConnection.setMysqlOutput(new BufferedOutputStream(socketConnection.getMysqlSocket().getOutputStream(), 16384));

        socketConnection.getMysqlOutput().flush();

        socketConnection.setSocketFactory(sslFact);

    }

    /**
     * Implementation of internal socket factory to wrap the SSL socket.
     */
    public static class StandardSSLSocketFactory implements SocketFactory {
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

        public Socket connect(String host, int portNumber, Properties props, int loginTimeout) throws SocketException, IOException {
            this.rawSocket = (SSLSocket) this.sslFact.createSocket(this.existingSocket, host, portNumber, true);
            return this.rawSocket;
        }

        @Override
        public boolean isLocallyConnected(MysqlConnection conn) {
            return this.existingSocketFactory.isLocallyConnected(conn);
        }
    }

    private ExportControlled() { /* prevent instantiation */
    }

    private static SSLSocketFactory getSSLSocketFactoryDefaultOrConfigured(PropertySet propertySet, ExceptionInterceptor exceptionInterceptor)
            throws SSLParamsException {
        String clientCertificateKeyStoreUrl = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreUrl).getValue();
        String trustCertificateKeyStoreUrl = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreUrl).getValue();

        if (StringUtils.isNullOrEmpty(clientCertificateKeyStoreUrl) && StringUtils.isNullOrEmpty(trustCertificateKeyStoreUrl)) {
            if (propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_verifyServerCertificate).getValue()) {
                return (SSLSocketFactory) SSLSocketFactory.getDefault();
            }
        }

        return getSSLContext(propertySet, exceptionInterceptor).getSocketFactory();
    }

    /**
     * Configure the {@link SSLContext} based on the supplier property set.
     */
    public static SSLContext getSSLContext(PropertySet propertySet, ExceptionInterceptor exceptionInterceptor) throws SSLParamsException {
        String clientCertificateKeyStoreUrl = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreUrl).getValue();
        String trustCertificateKeyStoreUrl = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreUrl).getValue();
        String clientCertificateKeyStoreType = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreType).getValue();
        String clientCertificateKeyStorePassword = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStorePassword)
                .getValue();
        String trustCertificateKeyStoreType = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreType).getValue();
        String trustCertificateKeyStorePassword = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStorePassword).getValue();

        TrustManagerFactory tmf = null;
        KeyManagerFactory kmf = null;

        try {
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        } catch (NoSuchAlgorithmException nsae) {
            throw ExceptionFactory.createException(SSLParamsException.class,
                    "Default algorithm definitions for TrustManager and/or KeyManager are invalid.  Check java security properties file.", nsae,
                    exceptionInterceptor);
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
                throw ExceptionFactory.createException(SSLParamsException.class, "Could not recover keys from client keystore.  Check password?", uke,
                        exceptionInterceptor);
            } catch (NoSuchAlgorithmException nsae) {
                throw ExceptionFactory.createException(SSLParamsException.class, "Unsupported keystore algorithm [" + nsae.getMessage() + "]", nsae,
                        exceptionInterceptor);
            } catch (KeyStoreException kse) {
                throw ExceptionFactory.createException(SSLParamsException.class, "Could not create KeyStore instance [" + kse.getMessage() + "]", kse,
                        exceptionInterceptor);
            } catch (CertificateException nsae) {
                throw ExceptionFactory.createException(SSLParamsException.class,
                        "Could not load client" + clientCertificateKeyStoreType + " keystore from " + clientCertificateKeyStoreUrl, nsae, exceptionInterceptor);
            } catch (MalformedURLException mue) {
                throw ExceptionFactory.createException(SSLParamsException.class, clientCertificateKeyStoreUrl + " does not appear to be a valid URL.", mue,
                        exceptionInterceptor);
            } catch (IOException ioe) {
                throw ExceptionFactory.createException(SSLParamsException.class, "Cannot open " + clientCertificateKeyStoreUrl + " [" + ioe.getMessage() + "]",
                        ioe, exceptionInterceptor);
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
                throw ExceptionFactory.createException(SSLParamsException.class, "Unsupported keystore algorithm [" + nsae.getMessage() + "]", nsae,
                        exceptionInterceptor);
            } catch (KeyStoreException kse) {
                throw ExceptionFactory.createException(SSLParamsException.class, "Could not create KeyStore instance [" + kse.getMessage() + "]", kse,
                        exceptionInterceptor);
            } catch (CertificateException nsae) {
                throw ExceptionFactory.createException(SSLParamsException.class,
                        "Could not load trust" + trustCertificateKeyStoreType + " keystore from " + trustCertificateKeyStoreUrl, nsae, exceptionInterceptor);
            } catch (MalformedURLException mue) {
                throw ExceptionFactory.createException(SSLParamsException.class, trustCertificateKeyStoreUrl + " does not appear to be a valid URL.", mue,
                        exceptionInterceptor);
            } catch (IOException ioe) {
                throw ExceptionFactory.createException(SSLParamsException.class, "Cannot open " + trustCertificateKeyStoreUrl + " [" + ioe.getMessage() + "]",
                        ioe, exceptionInterceptor);
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
                    propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_verifyServerCertificate).getValue() ? tmf.getTrustManagers()
                            : new X509TrustManager[] { new X509TrustManager() {
                                public void checkClientTrusted(X509Certificate[] chain, String authType) {
                                    // return without complaint
                                }

                                public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                                    // return without complaint
                                }

                                public X509Certificate[] getAcceptedIssuers() {
                                    return null;
                                }
                            } },
                    null);

            return sslContext;
        } catch (NoSuchAlgorithmException nsae) {
            throw new SSLParamsException("TLS is not a valid SSL protocol.", nsae);
        } catch (KeyManagementException kme) {
            throw new SSLParamsException("KeyManagementException: " + kme.getMessage(), kme);
        }
    }

    public static boolean isSSLEstablished(Socket socket) {
        return SSLSocket.class.isAssignableFrom(socket.getClass());
    }

    public static RSAPublicKey decodeRSAPublicKey(String key) throws RSAException {

        if (key == null) {
            throw ExceptionFactory.createException(RSAException.class, "Key parameter is null");
        }

        int offset = key.indexOf("\n") + 1;
        int len = key.indexOf("-----END PUBLIC KEY-----") - offset;

        // TODO: use standard decoders with Java 6+
        byte[] certificateData = Base64Decoder.decode(key.getBytes(), offset, len);

        X509EncodedKeySpec spec = new X509EncodedKeySpec(certificateData);
        try {
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return (RSAPublicKey) kf.generatePublic(spec);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw ExceptionFactory.createException(RSAException.class, "Unable to decode public key", e);
        }
    }

    public static byte[] encryptWithRSAPublicKey(byte[] source, RSAPublicKey key) throws RSAException {
        try {
            Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
            cipher.init(Cipher.ENCRYPT_MODE, key);
            return cipher.doFinal(source);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | IllegalBlockSizeException | BadPaddingException e) {
            throw ExceptionFactory.createException(RSAException.class, e.getMessage(), e);
        }
    }

}
