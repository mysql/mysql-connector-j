/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

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

import com.mysql.cj.api.io.SocketFactory;
import com.mysql.cj.core.exception.ExceptionFactory;
import com.mysql.cj.core.exception.FeatureNotAvailableException;
import com.mysql.cj.core.exception.RSAException;
import com.mysql.cj.core.exception.SSLParamsException;
import com.mysql.cj.core.util.Base64Decoder;
import com.mysql.cj.core.util.StringUtils;

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
     * @param io
     *            the CoreIO instance containing the socket to convert to an
     *            SSLSocket.
     * @throws SSLParamsException
     * 
     *             if the handshake fails, or if this distribution of
     *             Connector/J doesn't contain the SSL crytpo hooks needed to
     *             perform the handshake.
     */
    public static void transformSocketToSSLSocket(CoreIO io) throws IOException, SSLParamsException, FeatureNotAvailableException {
        SocketFactory sslFact = new StandardSSLSocketFactory(getSSLSocketFactoryDefaultOrConfigured(io), io.getSocketFactory(), io.getMysqlSocket());

        io.setMysqlSocket(sslFact.connect(io.getHost(), io.getPort(), null, 0));

        // need to force TLSv1, or else JSSE tries to do a SSLv2 handshake which MySQL doesn't understand
        ((SSLSocket) io.getMysqlSocket()).setEnabledProtocols(new String[] { "TLSv1" });

        String enabledSSLCipherSuites = io.getConnection().getEnabledSSLCipherSuites();
        if (enabledSSLCipherSuites != null && enabledSSLCipherSuites.length() > 0) {
            ((SSLSocket) io.getMysqlSocket()).setEnabledCipherSuites(enabledSSLCipherSuites.split("\\s*,\\s*"));
        }

        ((SSLSocket) io.getMysqlSocket()).startHandshake();

        if (io.getConnection().getUseUnbufferedInput()) {
            io.setMysqlInput(io.getMysqlSocket().getInputStream());
        } else {
            io.setMysqlInput(new BufferedInputStream(io.getMysqlSocket().getInputStream(), 16384));
        }

        io.setMysqlOutput(new BufferedOutputStream(io.getMysqlSocket().getOutputStream(), 16384));

        io.getMysqlOutput().flush();

        io.setSocketFactory(sslFact);

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

    }

    private ExportControlled() { /* prevent instantiation */
    }

    private static SSLSocketFactory getSSLSocketFactoryDefaultOrConfigured(CoreIO io) throws SSLParamsException {
        String clientCertificateKeyStoreUrl = io.getConnection().getClientCertificateKeyStoreUrl();
        String trustCertificateKeyStoreUrl = io.getConnection().getTrustCertificateKeyStoreUrl();
        String clientCertificateKeyStoreType = io.getConnection().getClientCertificateKeyStoreType();
        String clientCertificateKeyStorePassword = io.getConnection().getClientCertificateKeyStorePassword();
        String trustCertificateKeyStoreType = io.getConnection().getTrustCertificateKeyStoreType();
        String trustCertificateKeyStorePassword = io.getConnection().getTrustCertificateKeyStorePassword();

        if (StringUtils.isNullOrEmpty(clientCertificateKeyStoreUrl) && StringUtils.isNullOrEmpty(trustCertificateKeyStoreUrl)) {
            if (io.getConnection().getVerifyServerCertificate()) {
                return (SSLSocketFactory) SSLSocketFactory.getDefault();
            }
        }

        TrustManagerFactory tmf = null;
        KeyManagerFactory kmf = null;

        try {
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        } catch (NoSuchAlgorithmException nsae) {
            throw ExceptionFactory.createException(SSLParamsException.class,
                    "Default algorithm definitions for TrustManager and/or KeyManager are invalid.  Check java security properties file.", nsae,
                    io.getExceptionInterceptor());
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
                        io.getExceptionInterceptor());
            } catch (NoSuchAlgorithmException nsae) {
                throw ExceptionFactory.createException(SSLParamsException.class, "Unsupported keystore algorithm [" + nsae.getMessage() + "]", nsae,
                        io.getExceptionInterceptor());
            } catch (KeyStoreException kse) {
                throw ExceptionFactory.createException(SSLParamsException.class, "Could not create KeyStore instance [" + kse.getMessage() + "]", kse,
                        io.getExceptionInterceptor());
            } catch (CertificateException nsae) {
                throw ExceptionFactory.createException(SSLParamsException.class, "Could not load client" + clientCertificateKeyStoreType + " keystore from "
                        + clientCertificateKeyStoreUrl, nsae, io.getExceptionInterceptor());
            } catch (MalformedURLException mue) {
                throw ExceptionFactory.createException(SSLParamsException.class, clientCertificateKeyStoreUrl + " does not appear to be a valid URL.", mue,
                        io.getExceptionInterceptor());
            } catch (IOException ioe) {
                throw ExceptionFactory.createException(SSLParamsException.class, "Cannot open " + clientCertificateKeyStoreUrl + " [" + ioe.getMessage() + "]",
                        ioe, io.getExceptionInterceptor());
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
                        io.getExceptionInterceptor());
            } catch (KeyStoreException kse) {
                throw ExceptionFactory.createException(SSLParamsException.class, "Could not create KeyStore instance [" + kse.getMessage() + "]", kse,
                        io.getExceptionInterceptor());
            } catch (CertificateException nsae) {
                throw ExceptionFactory.createException(SSLParamsException.class, "Could not load trust" + trustCertificateKeyStoreType + " keystore from "
                        + trustCertificateKeyStoreUrl, nsae, io.getExceptionInterceptor());
            } catch (MalformedURLException mue) {
                throw ExceptionFactory.createException(SSLParamsException.class, trustCertificateKeyStoreUrl + " does not appear to be a valid URL.", mue,
                        io.getExceptionInterceptor());
            } catch (IOException ioe) {
                throw ExceptionFactory.createException(SSLParamsException.class, "Cannot open " + trustCertificateKeyStoreUrl + " [" + ioe.getMessage() + "]",
                        ioe, io.getExceptionInterceptor());
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
            sslContext.init(StringUtils.isNullOrEmpty(clientCertificateKeyStoreUrl) ? null : kmf.getKeyManagers(), io.getConnection()
                    .getVerifyServerCertificate() ? tmf.getTrustManagers() : new X509TrustManager[] { new X509TrustManager() {
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
            throw new SSLParamsException("TLS is not a valid SSL protocol.", nsae);
        } catch (KeyManagementException kme) {
            throw new SSLParamsException("KeyManagementException: " + kme.getMessage(), kme);
        }
    }

    public static boolean isSSLEstablished(CoreIO io) {
        return SSLSocket.class.isAssignableFrom(io.getMysqlSocket().getClass());
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
