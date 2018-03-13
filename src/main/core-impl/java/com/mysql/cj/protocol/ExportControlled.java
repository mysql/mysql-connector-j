/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
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
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.RSAException;
import com.mysql.cj.exceptions.SSLParamsException;
import com.mysql.cj.util.Base64Decoder;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;

/**
 * Holds functionality that falls under export-control regulations.
 */
public class ExportControlled {

    private static final String TLSv1 = "TLSv1";
    private static final String TLSv1_1 = "TLSv1.1";
    private static final String TLSv1_2 = "TLSv1.2";
    private static final String[] TLS_PROTOCOLS = new String[] { TLSv1_2, TLSv1_1, TLSv1 };

    public static boolean enabled() {
        // we may wish to un-static-ify this class this static method call may be removed entirely by the compiler
        return true;
    }

    /**
     * Converts the socket being used in the given CoreIO to an SSLSocket by
     * performing the SSL/TLS handshake.
     * 
     * @param rawSocket
     *            original non-SSL socket
     * @param socketConnection
     *            the Protocol instance containing the socket to convert to an
     *            SSLSocket.
     * @param serverVersion
     *            ServerVersion object
     * @return SSL socket
     * @throws SSLParamsException
     *             if the handshake fails, or if this distribution of
     *             Connector/J doesn't contain the SSL crypto hooks needed to
     *             perform the handshake.
     */
    public static Socket performTlsHandshake(Socket rawSocket, SocketConnection socketConnection, ServerVersion serverVersion)
            throws IOException, SSLParamsException, FeatureNotAvailableException {

        PropertySet pset = socketConnection.getPropertySet();

        SSLSocket sslSocket = (SSLSocket) getSSLSocketFactoryDefaultOrConfigured(pset, socketConnection.getExceptionInterceptor()).createSocket(rawSocket,
                socketConnection.getHost(), socketConnection.getPort(), true);

        String[] tryProtocols = null;

        // If enabledTLSProtocols configuration option is set then override the default TLS version restrictions. This allows enabling TLSv1.2 for
        // self-compiled MySQL versions supporting it, as well as the ability for users to restrict TLS connections to approved protocols (e.g., prohibiting
        // TLSv1) on the client side.
        // Note that it is problematic to enable TLSv1.2 on the client side when the server is compiled with yaSSL. When client attempts to connect with
        // TLSv1.2 yaSSL just closes the socket instead of re-attempting handshake with lower TLS version.
        String enabledTLSProtocols = pset.getStringReadableProperty(PropertyDefinitions.PNAME_enabledTLSProtocols).getValue();
        if (enabledTLSProtocols != null && enabledTLSProtocols.length() > 0) {
            tryProtocols = enabledTLSProtocols.split("\\s*,\\s*");
        } else if (serverVersion.meetsMinimum(ServerVersion.parseVersion("8.0.4"))
                || serverVersion.meetsMinimum(ServerVersion.parseVersion("5.6.0")) && Util.isEnterpriseEdition(serverVersion.toString())) {
            // allow all known TLS versions for this subset of server versions by default
            tryProtocols = TLS_PROTOCOLS;
        } else {
            // allow TLSv1 and TLSv1.1 for all server versions by default
            tryProtocols = new String[] { TLSv1_1, TLSv1 };

        }

        List<String> configuredProtocols = new ArrayList<>(Arrays.asList(tryProtocols));
        List<String> jvmSupportedProtocols = Arrays.asList(sslSocket.getSupportedProtocols());
        List<String> allowedProtocols = new ArrayList<>();
        for (String protocol : TLS_PROTOCOLS) {
            if (jvmSupportedProtocols.contains(protocol) && configuredProtocols.contains(protocol)) {
                allowedProtocols.add(protocol);
            }
        }
        sslSocket.setEnabledProtocols(allowedProtocols.toArray(new String[0]));

        // check allowed cipher suites
        String enabledSSLCipherSuites = pset.getStringReadableProperty(PropertyDefinitions.PNAME_enabledSSLCipherSuites).getValue();
        boolean overrideCiphers = enabledSSLCipherSuites != null && enabledSSLCipherSuites.length() > 0;

        if (overrideCiphers) {
            // If "enabledSSLCipherSuites" is set we just check that JVM allows provided values,
            // we don't disable DH algorithm, that allows c/J to deal with custom server builds with different security restrictions
            List<String> allowedCiphers = new ArrayList<>();
            List<String> availableCiphers = Arrays.asList(sslSocket.getEnabledCipherSuites());
            for (String cipher : enabledSSLCipherSuites.split("\\s*,\\s*")) {
                if (availableCiphers.contains(cipher)) {
                    allowedCiphers.add(cipher);
                }
            }

            // if some ciphers were filtered into allowedCiphers 
            sslSocket.setEnabledCipherSuites(allowedCiphers.toArray(new String[] {}));
        } else {
            // If we don't override ciphers, then we check for known restrictions

            // Java 8 default java.security contains jdk.tls.disabledAlgorithms=DH keySize < 768
            // That causes handshake failures with older MySQL servers, eg 5.6.11. Thus we have to disable DH for them when running on Java 8+
            // TODO check later for Java 9 behavior
            if (!(serverVersion.meetsMinimum(ServerVersion.parseVersion("5.7.6"))
                    || serverVersion.meetsMinimum(ServerVersion.parseVersion("5.6.26")) && !serverVersion.meetsMinimum(ServerVersion.parseVersion("5.7.0"))
                    || serverVersion.meetsMinimum(ServerVersion.parseVersion("5.5.45")) && !serverVersion.meetsMinimum(ServerVersion.parseVersion("5.6.0")))) {

                List<String> allowedCiphers = new ArrayList<>();
                for (String cipher : sslSocket.getEnabledCipherSuites()) {
                    if (cipher.indexOf("_DHE_") == -1 && cipher.indexOf("_DH_") == -1) {
                        allowedCiphers.add(cipher);
                    }
                }
                sslSocket.setEnabledCipherSuites(allowedCiphers.toArray(new String[] {}));
            }
        }

        sslSocket.startHandshake();

        return sslSocket;
    }

    private ExportControlled() { /* prevent instantiation */
    }

    /**
     * Implementation of X509TrustManager wrapping JVM X509TrustManagers to add expiration and identity check
     */
    public static class X509TrustManagerWrapper implements X509TrustManager {

        private X509TrustManager origTm = null;
        private boolean verifyServerCert = false;
        private String hostName = null;
        private CertificateFactory certFactory = null;
        private PKIXParameters validatorParams = null;
        private CertPathValidator validator = null;

        public X509TrustManagerWrapper(X509TrustManager tm, boolean verifyServerCertificate, String hostName) throws CertificateException {
            this.origTm = tm;
            this.verifyServerCert = verifyServerCertificate;
            this.hostName = hostName;

            if (verifyServerCertificate) {
                try {
                    Set<TrustAnchor> anch = Arrays.stream(tm.getAcceptedIssuers()).map(c -> new TrustAnchor(c, null)).collect(Collectors.toSet());
                    this.validatorParams = new PKIXParameters(anch);
                    this.validatorParams.setRevocationEnabled(false);
                    this.validator = CertPathValidator.getInstance("PKIX");
                    this.certFactory = CertificateFactory.getInstance("X.509");
                } catch (Exception e) {
                    throw new CertificateException(e);
                }
            }

        }

        public X509TrustManagerWrapper(boolean verifyServerCertificate, String hostName) {
            this.verifyServerCert = verifyServerCertificate;
            this.hostName = hostName;
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
                if (this.origTm != null) {
                    this.origTm.checkServerTrusted(chain, authType);
                } else {
                    throw new CertificateException("Can't verify server certificate because no trust manager is found.");
                }

                // verify server certificate identity
                if (this.hostName != null) {
                    String dn = chain[0].getSubjectX500Principal().getName(X500Principal.RFC2253);
                    String cn = null;
                    try {
                        LdapName ldapDN = new LdapName(dn);
                        for (Rdn rdn : ldapDN.getRdns()) {
                            if (rdn.getType().equalsIgnoreCase("CN")) {
                                cn = rdn.getValue().toString();
                                break;
                            }
                        }
                    } catch (InvalidNameException e) {
                        throw new CertificateException("Failed to retrieve the Common Name (CN) from the server certificate.");
                    }

                    if (!this.hostName.equalsIgnoreCase(cn)) {
                        throw new CertificateException("Server certificate identity check failed. The certificate Common Name '" + cn
                                + "' does not match with '" + this.hostName + "'.");
                    }
                }
            }
        }

        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            this.origTm.checkClientTrusted(chain, authType);
        }
    }

    private static SSLSocketFactory getSSLSocketFactoryDefaultOrConfigured(PropertySet propertySet, ExceptionInterceptor exceptionInterceptor)
            throws SSLParamsException {
        String clientCertificateKeyStoreUrl = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreUrl).getValue();
        String clientCertificateKeyStorePassword = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStorePassword)
                .getValue();
        String clientCertificateKeyStoreType = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreType).getValue();
        String trustCertificateKeyStoreUrl = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreUrl).getValue();
        String trustCertificateKeyStorePassword = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStorePassword).getValue();
        String trustCertificateKeyStoreType = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_trustCertificateKeyStoreType).getValue();

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

        boolean verifyServerCert = propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_verifyServerCertificate).getValue();

        return getSSLContext(clientCertificateKeyStoreUrl, clientCertificateKeyStoreType, clientCertificateKeyStorePassword, trustCertificateKeyStoreUrl,
                trustCertificateKeyStoreType, trustCertificateKeyStorePassword, verifyServerCert, null, exceptionInterceptor).getSocketFactory();
    }

    /**
     * Configure the {@link SSLContext} based on the supplier property set.
     */
    public static SSLContext getSSLContext(String clientCertificateKeyStoreUrl, String clientCertificateKeyStoreType, String clientCertificateKeyStorePassword,
            String trustCertificateKeyStoreUrl, String trustCertificateKeyStoreType, String trustCertificateKeyStorePassword, boolean verifyServerCert,
            String hostName, ExceptionInterceptor exceptionInterceptor) throws SSLParamsException {
        return getSSLContext(clientCertificateKeyStoreUrl, clientCertificateKeyStoreType, clientCertificateKeyStorePassword, trustCertificateKeyStoreUrl,
                trustCertificateKeyStoreType, trustCertificateKeyStorePassword, true, verifyServerCert, hostName, exceptionInterceptor);
    }

    /**
     * Configure the {@link SSLContext} based on the supplier property set.
     */
    public static SSLContext getSSLContext(String clientCertificateKeyStoreUrl, String clientCertificateKeyStoreType, String clientCertificateKeyStorePassword,
            String trustCertificateKeyStoreUrl, String trustCertificateKeyStoreType, String trustCertificateKeyStorePassword,
            boolean fallbackToDefaultTrustStore, boolean verifyServerCert, String hostName, ExceptionInterceptor exceptionInterceptor)
            throws SSLParamsException {
        TrustManagerFactory tmf = null;
        KeyManagerFactory kmf = null;

        KeyManager[] kms = null;
        List<TrustManager> tms = new ArrayList<>();

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
                    kms = kmf.getKeyManagers();
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

        InputStream trustStoreIS = null;
        try {
            String trustStoreType = "";
            char[] trustStorePassword = null;
            KeyStore trustKeyStore = null;

            if (!StringUtils.isNullOrEmpty(trustCertificateKeyStoreUrl) && !StringUtils.isNullOrEmpty(trustCertificateKeyStoreType)) {
                trustStoreType = trustCertificateKeyStoreType;
                trustStorePassword = (trustCertificateKeyStorePassword == null) ? new char[0] : trustCertificateKeyStorePassword.toCharArray();
                trustStoreIS = new URL(trustCertificateKeyStoreUrl).openStream();

                trustKeyStore = KeyStore.getInstance(trustStoreType);
                trustKeyStore.load(trustStoreIS, trustStorePassword);
            }

            if (trustKeyStore != null || fallbackToDefaultTrustStore) {
                tmf.init(trustKeyStore); // (trustKeyStore == null) initializes the TrustManagerFactory with the default truststore.  

                // building the customized list of TrustManagers from original one if it's available
                TrustManager[] origTms = tmf.getTrustManagers();

                for (TrustManager tm : origTms) {
                    // wrap X509TrustManager or put original if non-X509 TrustManager
                    tms.add(tm instanceof X509TrustManager ? new X509TrustManagerWrapper((X509TrustManager) tm, verifyServerCert, hostName) : tm);
                }
            }

        } catch (MalformedURLException e) {
            throw ExceptionFactory.createException(SSLParamsException.class, trustCertificateKeyStoreUrl + " does not appear to be a valid URL.", e,
                    exceptionInterceptor);
        } catch (NoSuchAlgorithmException e) {
            throw ExceptionFactory.createException(SSLParamsException.class, "Unsupported keystore algorithm [" + e.getMessage() + "]", e,
                    exceptionInterceptor);
        } catch (KeyStoreException e) {
            throw ExceptionFactory.createException(SSLParamsException.class, "Could not create KeyStore instance [" + e.getMessage() + "]", e,
                    exceptionInterceptor);
        } catch (CertificateException e) {
            throw ExceptionFactory.createException(SSLParamsException.class,
                    "Could not load trust" + trustCertificateKeyStoreType + " keystore from " + trustCertificateKeyStoreUrl, e, exceptionInterceptor);
        } catch (IOException e) {
            throw ExceptionFactory.createException(SSLParamsException.class, "Cannot open " + trustCertificateKeyStoreUrl + " [" + e.getMessage() + "]", e,
                    exceptionInterceptor);
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
            tms.add(new X509TrustManagerWrapper(verifyServerCert, hostName));
        }

        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(kms, tms.toArray(new TrustManager[tms.size()]), null);
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

    public static byte[] encryptWithRSAPublicKey(byte[] source, RSAPublicKey key, String transformation) throws RSAException {
        try {
            Cipher cipher = Cipher.getInstance(transformation);
            cipher.init(Cipher.ENCRYPT_MODE, key);
            return cipher.doFinal(source);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | IllegalBlockSizeException | BadPaddingException e) {
            throw ExceptionFactory.createException(RSAException.class, e.getMessage(), e);
        }
    }

    public static byte[] encryptWithRSAPublicKey(byte[] source, RSAPublicKey key) throws RSAException {
        return encryptWithRSAPublicKey(source, key, "RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
    }

    public static AsynchronousSocketChannel startTlsOnAsynchronousChannel(AsynchronousSocketChannel channel, SocketConnection socketConnection)
            throws SSLException {

        PropertySet propertySet = socketConnection.getPropertySet();

        SslMode sslMode = propertySet.<SslMode> getEnumReadableProperty(PropertyDefinitions.PNAME_sslMode).getValue();

        boolean verifyServerCert = sslMode == SslMode.VERIFY_CA || sslMode == SslMode.VERIFY_IDENTITY;
        String trustStoreUrl = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl).getValue();

        String trustStoreType = null;
        String trustStorePassword = null;
        if (verifyServerCert) {
            trustStoreType = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_sslTrustStoreType).getValue();
            trustStorePassword = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_sslTrustStorePassword).getValue();

            if (StringUtils.isNullOrEmpty(trustStoreUrl)) {
                trustStoreUrl = System.getProperty("javax.net.ssl.trustStore");
                trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
                trustStoreType = System.getProperty("javax.net.ssl.trustStoreType");
                if (StringUtils.isNullOrEmpty(trustStoreType)) {
                    trustStoreType = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_sslTrustStoreType).getInitialValue();
                }
                // check URL
                if (!StringUtils.isNullOrEmpty(trustStoreUrl)) {
                    try {
                        new URL(trustStoreUrl);
                    } catch (MalformedURLException e) {
                        trustStoreUrl = "file:" + trustStoreUrl;
                    }
                }
            }

            if (StringUtils.isNullOrEmpty(trustStoreUrl)) {
                throw new CJCommunicationsException("No truststore provided to verify the Server certificate.");
            }
        }

        // TODO WL#9925 will redefine other SSL connection properties for X Protocol
        String keyStoreUrl = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreUrl).getValue();
        String keyStoreType = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreType).getValue();
        String keyStorePassword = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStorePassword).getValue();

        if (StringUtils.isNullOrEmpty(keyStoreUrl)) {
            keyStoreUrl = System.getProperty("javax.net.ssl.keyStore");
            keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
            keyStoreType = System.getProperty("javax.net.ssl.keyStoreType");
            if (StringUtils.isNullOrEmpty(keyStoreType)) {
                keyStoreType = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreType).getInitialValue();
            }
            // check URL
            if (!StringUtils.isNullOrEmpty(keyStoreUrl)) {
                try {
                    new URL(keyStoreUrl);
                } catch (MalformedURLException e) {
                    keyStoreUrl = "file:" + keyStoreUrl;
                }
            }
        }

        SSLContext sslContext = ExportControlled.getSSLContext(keyStoreUrl, keyStoreType, keyStorePassword, trustStoreUrl, trustStoreType, trustStorePassword,
                false, verifyServerCert, sslMode == PropertyDefinitions.SslMode.VERIFY_IDENTITY ? socketConnection.getHost() : null, null);
        SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(true);

        // check allowed cipher suites
        String enabledSSLCipherSuites = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_enabledSSLCipherSuites).getValue();
        boolean overrideCiphers = enabledSSLCipherSuites != null && enabledSSLCipherSuites.length() > 0;

        if (overrideCiphers) {
            // If "enabledSSLCipherSuites" is set we check that JVM allows provided values,
            List<String> allowedCiphers = new ArrayList<>();
            List<String> availableCiphers = Arrays.asList(sslEngine.getEnabledCipherSuites());
            for (String cipher : enabledSSLCipherSuites.split("\\s*,\\s*")) {
                if (availableCiphers.contains(cipher)) {
                    allowedCiphers.add(cipher);
                }
            }

            // if some ciphers were filtered into allowedCiphers 
            sslEngine.setEnabledCipherSuites(allowedCiphers.toArray(new String[] {}));
        }

        // If enabledTLSProtocols configuration option is set, overriding the default TLS version restrictions.
        // This allows enabling TLSv1.2 for self-compiled MySQL versions supporting it, as well as the ability
        // for users to restrict TLS connections to approved protocols (e.g., prohibiting TLSv1) on the client side.
        String enabledTLSProtocols = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_enabledTLSProtocols).getValue();
        String[] tryProtocols = enabledTLSProtocols != null && enabledTLSProtocols.length() > 0 ? enabledTLSProtocols.split("\\s*,\\s*")
                : new String[] { "TLSv1.1", "TLSv1" };
        List<String> configuredProtocols = new ArrayList<>(Arrays.asList(tryProtocols));
        List<String> jvmSupportedProtocols = Arrays.asList(sslEngine.getSupportedProtocols());

        List<String> allowedProtocols = new ArrayList<>();
        for (String p : TLS_PROTOCOLS) {
            if (jvmSupportedProtocols.contains(p) && configuredProtocols.contains(p)) {
                allowedProtocols.add(p);
            }
        }
        sslEngine.setEnabledProtocols(allowedProtocols.toArray(new String[0]));

        performTlsHandshake(sslEngine, channel);

        return new TlsAsynchronousSocketChannel(channel, sslEngine);
    }

    /**
     * Perform the handshaking step of the TLS connection. We use the `sslEngine' along with the `channel' to exchange messages with the server to setup an
     * encrypted channel.
     * 
     * @param sslEngine
     *            {@link SSLEngine}
     * @param channel
     *            {@link AsynchronousSocketChannel}
     * @throws SSLException
     *             in case of handshake error
     */
    private static void performTlsHandshake(SSLEngine sslEngine, AsynchronousSocketChannel channel) throws SSLException {
        sslEngine.beginHandshake();
        HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();

        // Create byte buffers to use for holding application data
        int packetBufferSize = sslEngine.getSession().getPacketBufferSize();
        ByteBuffer myNetData = ByteBuffer.allocate(packetBufferSize);
        ByteBuffer peerNetData = ByteBuffer.allocate(packetBufferSize);
        int appBufferSize = sslEngine.getSession().getApplicationBufferSize();
        ByteBuffer myAppData = ByteBuffer.allocate(appBufferSize);
        ByteBuffer peerAppData = ByteBuffer.allocate(appBufferSize);

        SSLEngineResult res = null;

        while (handshakeStatus != HandshakeStatus.FINISHED && handshakeStatus != HandshakeStatus.NOT_HANDSHAKING) {
            switch (handshakeStatus) {
                case NEED_WRAP:
                    myNetData.clear();
                    res = sslEngine.wrap(myAppData, myNetData);
                    handshakeStatus = res.getHandshakeStatus();
                    switch (res.getStatus()) {
                        case OK:
                            myNetData.flip();
                            write(channel, myNetData);
                            break;
                        case BUFFER_OVERFLOW:
                        case BUFFER_UNDERFLOW:
                        case CLOSED:
                            throw new CJCommunicationsException("Unacceptable SSLEngine result: " + res);
                    }
                    break;
                case NEED_UNWRAP:
                    peerNetData.flip(); // Process incoming handshaking data
                    res = sslEngine.unwrap(peerNetData, peerAppData);
                    handshakeStatus = res.getHandshakeStatus();
                    switch (res.getStatus()) {
                        case OK:
                            peerNetData.compact();
                            break;
                        case BUFFER_OVERFLOW:
                            // Check if we need to enlarge the peer application data buffer.
                            final int newPeerAppDataSize = sslEngine.getSession().getApplicationBufferSize();
                            if (newPeerAppDataSize > peerAppData.capacity()) {
                                // enlarge the peer application data buffer
                                ByteBuffer newPeerAppData = ByteBuffer.allocate(newPeerAppDataSize);
                                newPeerAppData.put(peerAppData);
                                newPeerAppData.flip();
                                peerAppData = newPeerAppData;
                            } else {
                                peerAppData.compact();
                            }
                            break;
                        case BUFFER_UNDERFLOW:
                            // Check if we need to enlarge the peer network packet buffer
                            final int newPeerNetDataSize = sslEngine.getSession().getPacketBufferSize();
                            if (newPeerNetDataSize > peerNetData.capacity()) {
                                // enlarge the peer network packet buffer
                                ByteBuffer newPeerNetData = ByteBuffer.allocate(newPeerNetDataSize);
                                newPeerNetData.put(peerNetData);
                                newPeerNetData.flip();
                                peerNetData = newPeerNetData;
                            } else {
                                peerNetData.compact();
                            }
                            // obtain more inbound network data and then retry the operation
                            if (read(channel, peerNetData) < 0) {
                                throw new CJCommunicationsException("Server does not provide enough data to proceed with SSL handshake.");
                            }
                            break;
                        case CLOSED:
                            throw new CJCommunicationsException("Unacceptable SSLEngine result: " + res);
                    }
                    break;

                case NEED_TASK:
                    sslEngine.getDelegatedTask().run();
                    handshakeStatus = sslEngine.getHandshakeStatus();
                    break;
                case FINISHED:
                case NOT_HANDSHAKING:
                    break;
            }
        }
    }

    /**
     * Synchronously send data to the server. (Needed here for TLS handshake)
     * 
     * @param channel
     *            {@link AsynchronousSocketChannel}
     * @param data
     *            {@link ByteBuffer}
     */
    private static void write(AsynchronousSocketChannel channel, ByteBuffer data) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        int bytesToWrite = data.limit();
        CompletionHandler<Integer, Void> handler = new CompletionHandler<Integer, Void>() {
            public void completed(Integer bytesWritten, Void nothing) {
                if (bytesWritten < bytesToWrite) {
                    channel.write(data, null, this);
                } else {
                    f.complete(null);
                }
            }

            public void failed(Throwable exc, Void nothing) {
                f.completeExceptionally(exc);
            }
        };
        channel.write(data, null, handler);
        try {
            f.get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new CJCommunicationsException(ex);
        }
    }

    /**
     * Synchronously read data from the server. (Needed here for TLS handshake)
     * 
     * @param channel
     *            {@link AsynchronousSocketChannel}
     * @param data
     *            {@link ByteBuffer}
     * @return the number of bytes read
     */
    private static Integer read(AsynchronousSocketChannel channel, ByteBuffer data) {
        Future<Integer> f = channel.read(data);
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new CJCommunicationsException(ex);
        }
    }

}
