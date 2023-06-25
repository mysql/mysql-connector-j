/*
 * Copyright (c) 2002, 2023, Oracle and/or its affiliates.
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
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Signature;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertPathValidatorResult;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXCertPathValidatorResult;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.RSAException;
import com.mysql.cj.exceptions.SSLParamsException;
import com.mysql.cj.log.Log;
import com.mysql.cj.util.Base64Decoder;
import com.mysql.cj.util.StringUtils;

/**
 * Holds functionality that falls under export-control regulations.
 */
public class ExportControlled {
    private static final String SSL_CONTEXT_PROTOCOL = "TLS";

    private static final String TLSv1 = "TLSv1";
    private static final String TLSv1_1 = "TLSv1.1";
    private static final String TLSv1_2 = "TLSv1.2";
    private static final String TLSv1_3 = "TLSv1.3";
    private static final String[] KNOWN_TLS_PROTOCOLS = new String[] { TLSv1_3, TLSv1_2, TLSv1_1, TLSv1 };
    private static final String[] APPROVED_TLS_PROTOCOLS = new String[] { TLSv1_3, TLSv1_2 };

    private static final String TLS_SETTINGS_RESOURCE = "/com/mysql/cj/TlsSettings.properties";
    private static final List<String> ALLOWED_CIPHERS = new ArrayList<>();
    private static final List<String> UNACCEPTABLE_CIPHER_SUBSTR = new ArrayList<>();

    static {
        try {
            Properties tlsSettings = new Properties();
            tlsSettings.load(ExportControlled.class.getResourceAsStream(TLS_SETTINGS_RESOURCE));
            // Ciphers prefixed with "TLS_" are used by Oracle Java while the ones prefixed with "SSL_" are used by IBM Java
            Arrays.stream(tlsSettings.getProperty("TLSCiphers.Mandatory").split("\\s*,\\s*")).forEach(s -> {
                ALLOWED_CIPHERS.add("TLS_" + s.trim());
                ALLOWED_CIPHERS.add("SSL_" + s.trim());
            });
            Arrays.stream(tlsSettings.getProperty("TLSCiphers.Approved").split("\\s*,\\s*")).forEach(s -> {
                ALLOWED_CIPHERS.add("TLS_" + s.trim());
                ALLOWED_CIPHERS.add("SSL_" + s.trim());
            });
            Arrays.stream(tlsSettings.getProperty("TLSCiphers.Deprecated").split("\\s*,\\s*")).forEach(s -> {
                ALLOWED_CIPHERS.add("TLS_" + s.trim());
                ALLOWED_CIPHERS.add("SSL_" + s.trim());
            });
            Arrays.stream(tlsSettings.getProperty("TLSCiphers.Unacceptable.Mask").split("\\s*,\\s*")).forEach(s -> UNACCEPTABLE_CIPHER_SUBSTR.add(s.trim()));
        } catch (IOException e) {
            throw ExceptionFactory.createException("Unable to load TlsSettings.properties");
        }
    }

    private ExportControlled() { // Prevent instantiation.
    }

    public static boolean enabled() {
        return true;
    }

    /**
     * Converts the socket being used in the given SocketConnection to an SSLSocket by performing the TLS handshake.
     * 
     * @param rawSocket
     *            original non-SSL socket
     * @param socketConnection
     *            the Protocol instance containing the socket to convert into an SSLSocket.
     * @param serverVersion
     *            ServerVersion object
     * @param log
     *            Logger
     * 
     * @return SSL socket
     * 
     * @throws IOException
     *             if an I/O exception occurs
     * @throws SSLParamsException
     *             if the handshake fails
     * @throws FeatureNotAvailableException
     *             if TLS is not supported
     */
    public static Socket performTlsHandshake(Socket rawSocket, SocketConnection socketConnection, ServerVersion serverVersion, Log log)
            throws IOException, SSLParamsException, FeatureNotAvailableException {
        PropertySet pset = socketConnection.getPropertySet();

        SslMode sslMode = pset.<SslMode>getEnumProperty(PropertyKey.sslMode).getValue();
        boolean fipsCompliantJsse = pset.getBooleanProperty(PropertyKey.fipsCompliantJsse).getValue();
        boolean verifyServerCert = fipsCompliantJsse || sslMode == SslMode.VERIFY_CA || sslMode == SslMode.VERIFY_IDENTITY;
        boolean fallbackToSystemTrustStore = pset.getBooleanProperty(PropertyKey.fallbackToSystemTrustStore).getValue();

        KeyStoreConfigurations trustStoreConfigs = !verifyServerCert ? new KeyStoreConfigurations() : getTrustStoreConfigurations(pset);
        if (serverVersion == null && verifyServerCert && !fallbackToSystemTrustStore && StringUtils.isNullOrEmpty(trustStoreConfigs.keyStoreUrl)) {
            // If serverVersion == null then this was called from the X DevAPI.
            throw new CJCommunicationsException("No truststore provided to verify the Server certificate.");
        }

        SslContextBuilder sslContextBuilder = new SslContextBuilder();
        sslContextBuilder.setKeyStoreSettings(getKeyStoreConfigurations(pset));
        sslContextBuilder.setTrustStoreSettings(trustStoreConfigs);
        sslContextBuilder.setVerifyServerCertificate(verifyServerCert);
        sslContextBuilder.setFallbackToSystemTrustStore(fallbackToSystemTrustStore);
        sslContextBuilder.setFipsCompliantJsse(fipsCompliantJsse);
        sslContextBuilder.setKeyManagerFactoryProvider(pset.getStringProperty(PropertyKey.keyManagerFactoryProvider).getValue());
        sslContextBuilder.setTrustManagerFactoryProvider(pset.getStringProperty(PropertyKey.trustManagerFactoryProvider).getValue());
        sslContextBuilder.setKeyStoreProvider(pset.getStringProperty(PropertyKey.keyStoreProvider).getValue());
        sslContextBuilder.setSslContextProvider(pset.getStringProperty(PropertyKey.sslContextProvider).getValue());
        sslContextBuilder.setExceptionInterceptor(socketConnection.getExceptionInterceptor());

        SSLContext sslContext = sslContextBuilder.build();
        SSLSocketFactory socketFactory = sslContext.getSocketFactory();
        SSLSocket sslSocket = (SSLSocket) socketFactory.createSocket(rawSocket, socketConnection.getHost(), socketConnection.getPort(), true);

        String[] allowedProtocols = getAllowedProtocols(pset, sslSocket.getSupportedProtocols());
        sslSocket.setEnabledProtocols(allowedProtocols);

        String[] allowedCiphers = getAllowedCiphers(pset, Arrays.asList(sslSocket.getEnabledCipherSuites()));
        if (allowedCiphers != null) {
            sslSocket.setEnabledCipherSuites(allowedCiphers);
        }

        sslSocket.startHandshake();

        // Verify server identity post TLS handshake.
        if (sslMode == SslMode.VERIFY_IDENTITY) {
            String hostname = socketConnection.getHost();
            if (!StringUtils.isNullOrEmpty(hostname)) {
                HostnameChecker hostnameChecker = new HostnameChecker(socketConnection.getHost());

                SSLSession session = sslSocket.getSession();
                Certificate[] peerCerts = session.getPeerCertificates();

                X509Certificate peerCert;
                if (peerCerts[0] instanceof java.security.cert.X509Certificate) {
                    peerCert = (java.security.cert.X509Certificate) peerCerts[0];
                } else {
                    throw ExceptionFactory.createException(SSLParamsException.class,
                            "Server identity verification failed. Could not read Server's X.509 Certificate.");

                }
                try {
                    hostnameChecker.match(peerCert);
                } catch (CertificateException e) {
                    throw new IOException(e);
                }
            }
        }

        return sslSocket;
    }

    public static boolean isSSLEstablished(Socket socket) {
        return socket == null ? false : SSLSocket.class.isAssignableFrom(socket.getClass());
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

    public static RSAPrivateKey decodeRSAPrivateKey(String key) throws RSAException {
        if (key == null) {
            throw ExceptionFactory.createException(RSAException.class, "Key parameter is null");
        }

        String keyData = key.replace("-----BEGIN PRIVATE KEY-----", "").replaceAll("\\R", "").replace("-----END PRIVATE KEY-----", "");
        byte[] decodedKeyData = Base64.getDecoder().decode(keyData);

        try {
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            return (RSAPrivateKey) keyFactory.generatePrivate(new PKCS8EncodedKeySpec(decodedKeyData));
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw ExceptionFactory.createException(RSAException.class, "Unable to decode private key", e);
        }
    }

    public static byte[] sign(byte[] source, RSAPrivateKey privateKey) throws RSAException {
        try {
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initSign(privateKey);
            signature.update(source);
            return signature.sign();
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            throw ExceptionFactory.createException(RSAException.class, e.getMessage(), e);
        }
    }

    private static KeyStoreConfigurations getKeyStoreConfigurations(PropertySet propertySet) {
        String keyStoreUrl = propertySet.getStringProperty(PropertyKey.clientCertificateKeyStoreUrl).getValue();
        String keyStorePassword = propertySet.getStringProperty(PropertyKey.clientCertificateKeyStorePassword).getValue();
        String keyStoreType = propertySet.getStringProperty(PropertyKey.clientCertificateKeyStoreType).getValue();
        boolean fallbackToSystemKeyStore = propertySet.getBooleanProperty(PropertyKey.fallbackToSystemKeyStore).getValue();

        if (fallbackToSystemKeyStore && StringUtils.isNullOrEmpty(keyStoreUrl)) {
            keyStoreUrl = System.getProperty("javax.net.ssl.keyStore");
            keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
            keyStoreType = System.getProperty("javax.net.ssl.keyStoreType");
            if (StringUtils.isNullOrEmpty(keyStoreType)) {
                keyStoreType = propertySet.getStringProperty(PropertyKey.clientCertificateKeyStoreType).getInitialValue();
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

        return new KeyStoreConfigurations(keyStoreUrl, keyStorePassword, keyStoreType);
    }

    private static KeyStoreConfigurations getTrustStoreConfigurations(PropertySet propertySet) {
        String trustStoreUrl = propertySet.getStringProperty(PropertyKey.trustCertificateKeyStoreUrl).getValue();
        String trustStorePassword = propertySet.getStringProperty(PropertyKey.trustCertificateKeyStorePassword).getValue();
        String trustStoreType = propertySet.getStringProperty(PropertyKey.trustCertificateKeyStoreType).getValue();
        boolean fallbackToSystemTrustStore = propertySet.getBooleanProperty(PropertyKey.fallbackToSystemTrustStore).getValue();

        if (fallbackToSystemTrustStore && StringUtils.isNullOrEmpty(trustStoreUrl)) {
            trustStoreUrl = System.getProperty("javax.net.ssl.trustStore");
            trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
            trustStoreType = System.getProperty("javax.net.ssl.trustStoreType");
            if (StringUtils.isNullOrEmpty(trustStoreType)) {
                trustStoreType = propertySet.getStringProperty(PropertyKey.trustCertificateKeyStoreType).getInitialValue();
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

        return new KeyStoreConfigurations(trustStoreUrl, trustStorePassword, trustStoreType);
    }

    private static String[] getAllowedProtocols(PropertySet pset, String[] socketProtocols) {
        List<String> tryProtocols = null;

        RuntimeProperty<String> tlsVersions = pset.getStringProperty(PropertyKey.tlsVersions);
        if (tlsVersions != null && tlsVersions.isExplicitlySet()) {
            // If tlsVersions configuration option is set then override the default TLS versions restriction.
            if (tlsVersions.getValue() == null) {
                throw ExceptionFactory.createException(SSLParamsException.class,
                        "Specified list of TLS versions is empty. Accepted values are TLSv1.2 and TLSv1.3.");
            }
            tryProtocols = getValidProtocols(tlsVersions.getValue().split("\\s*,\\s*"));
        } else {
            tryProtocols = new ArrayList<>(Arrays.asList(APPROVED_TLS_PROTOCOLS));
        }

        List<String> jvmSupportedProtocols = Arrays.asList(socketProtocols);
        List<String> allowedProtocols = new ArrayList<>();
        for (String protocol : tryProtocols) {
            if (jvmSupportedProtocols.contains(protocol)) {
                allowedProtocols.add(protocol);
            }
        }
        return allowedProtocols.toArray(new String[0]);
    }

    public static void checkValidProtocols(List<String> protocols) {
        getValidProtocols(protocols.toArray(new String[0]));
    }

    private static List<String> getValidProtocols(String[] protocols) {
        List<String> requestedProtocols = Arrays.stream(protocols).filter(p -> !StringUtils.isNullOrEmpty(p.trim())).collect(Collectors.toList());
        if (requestedProtocols.size() == 0) {
            throw ExceptionFactory.createException(SSLParamsException.class,
                    "Specified list of TLS versions is empty. Accepted values are TLSv1.2 and TLSv1.3.");
        }
        List<String> sanitizedProtocols = Stream.of(KNOWN_TLS_PROTOCOLS).filter(requestedProtocols::contains).collect(Collectors.toList());
        if (sanitizedProtocols.size() == 0) {
            throw ExceptionFactory.createException(SSLParamsException.class,
                    "Specified list of TLS versions only contains non valid TLS protocols. Accepted values are TLSv1.2 and TLSv1.3.");
        }
        List<String> validProtocols = Stream.of(APPROVED_TLS_PROTOCOLS).filter(sanitizedProtocols::contains).collect(Collectors.toList());
        if (validProtocols.size() == 0) {
            throw ExceptionFactory.createException(SSLParamsException.class,
                    "TLS protocols TLSv1 and TLSv1.1 are not supported. Accepted values are TLSv1.2 and TLSv1.3.");
        }
        return validProtocols;
    }

    private static String[] getAllowedCiphers(PropertySet pset, List<String> socketCipherSuites) {
        String enabledSSLCipherSuites = pset.getStringProperty(PropertyKey.tlsCiphersuites).getValue();
        Stream<String> filterStream = StringUtils.isNullOrEmpty(enabledSSLCipherSuites) ? socketCipherSuites.stream()
                : Arrays.stream(enabledSSLCipherSuites.split("\\s*,\\s*")).filter(socketCipherSuites::contains);
        List<String> allowedCiphers = filterStream // 
                .filter(ALLOWED_CIPHERS::contains) // filter by mandatory, approved and deprecated ciphers
                .filter(c -> !UNACCEPTABLE_CIPHER_SUBSTR.stream().filter(c::contains).findFirst().isPresent()) // unacceptable ciphers
                .collect(Collectors.toList());

        return allowedCiphers.toArray(new String[] {});
    }

    private static class SslContextBuilder {
        private KeyStoreConfigurations keyStoreSettings = null;
        private KeyStoreConfigurations trustStoreSettings = null;
        private boolean verifyServerCertificate = true;
        private boolean fallbackToSystemTrustStore = true;
        private boolean fipsCompliantJsse = false;
        private String KeyManagerFactoryProvider = null;
        private String trustManagerFactoryProvider = null;
        private String keyStoreProvider = null;
        private String sslContextProvider = null;
        private ExceptionInterceptor exceptionInterceptor = null;

        public SslContextBuilder() {
        }

        public void setKeyStoreSettings(KeyStoreConfigurations keyStoreSettings) {
            this.keyStoreSettings = keyStoreSettings;
        }

        public void setTrustStoreSettings(KeyStoreConfigurations keyStoreSettings) {
            this.trustStoreSettings = keyStoreSettings;
        }

        public void setVerifyServerCertificate(boolean verifyServerCertificate) {
            this.verifyServerCertificate = verifyServerCertificate;
        }

        public void setFallbackToSystemTrustStore(boolean fallbackToDefaultTrustStore) {
            this.fallbackToSystemTrustStore = fallbackToDefaultTrustStore;
        }

        public void setFipsCompliantJsse(boolean fipsCompliantJsse) {
            this.fipsCompliantJsse = fipsCompliantJsse;
        }

        public void setKeyManagerFactoryProvider(String keyManagerFactoryProvider) {
            this.KeyManagerFactoryProvider = keyManagerFactoryProvider;
        }

        public void setTrustManagerFactoryProvider(String trustManagerFactoryProvider) {
            this.trustManagerFactoryProvider = trustManagerFactoryProvider;
        }

        public void setKeyStoreProvider(String keyStoreProvider) {
            this.keyStoreProvider = keyStoreProvider;
        }

        public void setSslContextProvider(String sslContextProvider) {
            this.sslContextProvider = sslContextProvider;
        }

        public void setExceptionInterceptor(ExceptionInterceptor exceptionInterceptor) {
            this.exceptionInterceptor = exceptionInterceptor;
        }

        public SSLContext build() {
            KeyManagerFactory kmf = null;
            KeyManager[] kms = null;

            TrustManagerFactory tmf = null;
            TrustManager[] tms = new TrustManager[0];

            try {
                kmf = StringUtils.isNullOrEmpty(this.KeyManagerFactoryProvider) ? KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
                        : KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm(), this.KeyManagerFactoryProvider);
                tmf = StringUtils.isNullOrEmpty(this.trustManagerFactoryProvider) ? TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                        : TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm(), this.trustManagerFactoryProvider);
            } catch (NoSuchAlgorithmException e) {
                throw ExceptionFactory.createException(SSLParamsException.class,
                        "Default algorithm for TrustManager or KeyManager is invalid. Check java security properties file.", e, this.exceptionInterceptor);
            } catch (NoSuchProviderException e) {
                throw ExceptionFactory.createException(SSLParamsException.class,
                        "Specified TrustManager or KeyManager Provider is invalid. Ensure it is property registered.", e, this.exceptionInterceptor);
            }

            if (!StringUtils.isNullOrEmpty(this.keyStoreSettings.keyStoreUrl)) {
                InputStream ksIS = null;
                try {
                    if (!StringUtils.isNullOrEmpty(this.keyStoreSettings.keyStoreType)) {
                        KeyStore clientKeyStore = StringUtils.isNullOrEmpty(this.keyStoreProvider) ? KeyStore.getInstance(this.keyStoreSettings.keyStoreType)
                                : KeyStore.getInstance(this.keyStoreSettings.keyStoreType, this.keyStoreProvider);
                        URL ksURL = new URL(this.keyStoreSettings.keyStoreUrl);
                        char[] password = (this.keyStoreSettings.keyStorePassword == null) ? new char[0] : this.keyStoreSettings.keyStorePassword.toCharArray();
                        ksIS = ksURL.openStream();
                        clientKeyStore.load(ksIS, password);
                        kmf.init(clientKeyStore, password);
                        kms = kmf.getKeyManagers();
                    }
                } catch (UnrecoverableKeyException e) {
                    throw ExceptionFactory.createException(SSLParamsException.class, "Could not recover keys from client keystore.  Check password?", e,
                            this.exceptionInterceptor);
                } catch (NoSuchAlgorithmException e) {
                    throw ExceptionFactory.createException(SSLParamsException.class, "Unsupported keystore algorithm [" + e.getMessage() + "]", e,
                            this.exceptionInterceptor);
                } catch (NoSuchProviderException e) {
                    throw ExceptionFactory.createException(SSLParamsException.class,
                            "Specified KeyStore Provider is invalid. Ensure it is property registered.", e, this.exceptionInterceptor);
                } catch (KeyStoreException e) {
                    throw ExceptionFactory.createException(SSLParamsException.class, "Could not create KeyStore instance [" + e.getMessage() + "]", e,
                            this.exceptionInterceptor);
                } catch (CertificateException e) {
                    throw ExceptionFactory.createException(SSLParamsException.class,
                            "Could not load client" + this.keyStoreSettings.keyStoreType + " keystore from " + this.keyStoreSettings.keyStoreUrl, e,
                            this.exceptionInterceptor);
                } catch (MalformedURLException e) {
                    throw ExceptionFactory.createException(SSLParamsException.class, this.keyStoreSettings.keyStoreUrl + " does not appear to be a valid URL.",
                            e, this.exceptionInterceptor);
                } catch (IOException e) {
                    throw ExceptionFactory.createException(SSLParamsException.class,
                            "Cannot open " + this.keyStoreSettings.keyStoreUrl + " [" + e.getMessage() + "]", e, this.exceptionInterceptor);
                } finally {
                    if (ksIS != null) {
                        try {
                            ksIS.close();
                        } catch (IOException e) {
                            // Can't close input stream, but the keystore can be properly initialized so there's no need to throw this exception.
                        }
                    }
                }
            }

            InputStream trustStoreIS = null;
            boolean x509TrustManagerFound = false;
            try {
                if (this.verifyServerCertificate) {
                    KeyStore trustKeyStore = null;
                    if (!StringUtils.isNullOrEmpty(this.trustStoreSettings.keyStoreUrl) && !StringUtils.isNullOrEmpty(this.trustStoreSettings.keyStoreType)) {
                        char[] trustStorePassword = (this.trustStoreSettings.keyStorePassword == null) ? new char[0]
                                : this.trustStoreSettings.keyStorePassword.toCharArray();
                        trustStoreIS = new URL(this.trustStoreSettings.keyStoreUrl).openStream();
                        trustKeyStore = StringUtils.isNullOrEmpty(this.keyStoreProvider) ? KeyStore.getInstance(this.trustStoreSettings.keyStoreType)
                                : KeyStore.getInstance(this.trustStoreSettings.keyStoreType, this.keyStoreProvider);
                        trustKeyStore.load(trustStoreIS, trustStorePassword);
                    }

                    if (trustKeyStore != null || this.fallbackToSystemTrustStore) {
                        tmf.init(trustKeyStore); // If trustKeyStore == null then the TrustManagerFactory is initialized with the system-wide truststore.  
                        tms = tmf.getTrustManagers();

                        // Check if there are any X509TrustManagers and wrap original if not operating in FIPS compliant mode.
                        for (int i = 0; i < tms.length; i++) {
                            if (tms[i] instanceof X509TrustManager) {
                                x509TrustManagerFound = true;
                                if (!this.fipsCompliantJsse) {
                                    tms[i] = new X509TrustManagerWrapper((X509TrustManager) tms[i]);
                                }
                            }
                        }

                    }
                }

                // If no other TrustManagers were found then add a single X509TrustManagerWrapper that just takes care of certificate expiration check. 
                if (tms.length == 0 && !this.fipsCompliantJsse) {
                    tms = new TrustManager[] { new X509TrustManagerWrapper() };
                }
            } catch (MalformedURLException e) {
                throw ExceptionFactory.createException(SSLParamsException.class, this.trustStoreSettings.keyStoreUrl + " does not appear to be a valid URL.", e,
                        this.exceptionInterceptor);
            } catch (NoSuchAlgorithmException e) {
                throw ExceptionFactory.createException(SSLParamsException.class, "Unsupported keystore algorithm [" + e.getMessage() + "]", e,
                        this.exceptionInterceptor);
            } catch (NoSuchProviderException e) {
                throw ExceptionFactory.createException(SSLParamsException.class, "Specified KeyStore Provider is invalid. Ensure it is property registered.", e,
                        this.exceptionInterceptor);
            } catch (KeyStoreException e) {
                throw ExceptionFactory.createException(SSLParamsException.class, "Could not create KeyStore instance [" + e.getMessage() + "]", e,
                        this.exceptionInterceptor);
            } catch (CertificateException e) {
                throw ExceptionFactory.createException(SSLParamsException.class,
                        "Could not load trust" + this.trustStoreSettings.keyStoreType + " keystore from " + this.trustStoreSettings.keyStoreUrl, e,
                        this.exceptionInterceptor);
            } catch (IOException e) {
                throw ExceptionFactory.createException(SSLParamsException.class,
                        "Cannot open " + this.trustStoreSettings.keyStoreUrl + " [" + e.getMessage() + "]", e, this.exceptionInterceptor);
            } finally {
                if (trustStoreIS != null) {
                    try {
                        trustStoreIS.close();
                    } catch (IOException e) {
                        // Can't close input stream, but the keystore can be properly initialized so there's no need to throw this exception.
                    }
                }
            }

            if (this.verifyServerCertificate && !x509TrustManagerFound) {
                throw ExceptionFactory.createException(SSLParamsException.class,
                        "Failed setting up server certificate validation because no X.509 Trust Manager was found.", this.exceptionInterceptor);
            }

            try {
                SSLContext sslContext = StringUtils.isNullOrEmpty(this.sslContextProvider) ? SSLContext.getInstance(SSL_CONTEXT_PROTOCOL)
                        : SSLContext.getInstance(SSL_CONTEXT_PROTOCOL, this.sslContextProvider);
                sslContext.init(kms, tms, null);
                return sslContext;
            } catch (NoSuchAlgorithmException nsae) {
                throw new SSLParamsException(SSL_CONTEXT_PROTOCOL + " is not a valid protocol.", nsae);
            } catch (NoSuchProviderException e) {
                throw ExceptionFactory.createException(SSLParamsException.class, "Specified SSLContext Provider is invalid. Ensure it is property registered.",
                        e, this.exceptionInterceptor);
            } catch (KeyManagementException kme) {
                throw new SSLParamsException("KeyManagementException: " + kme.getMessage(), kme);
            }
        }
    }

    /**
     * X509TrustManager wrapper that allows skipping server certificate validation and adds certificate expiration check.
     */
    public static class X509TrustManagerWrapper implements X509TrustManager {
        private static final String CERT_PATH_VALIDATOR_ALGORITHM = "PKIX";
        private static final String CERT_FACTORY_TYPE = "X.509";

        private boolean validateServerCert = false;

        private X509TrustManager originalTrustManager = null;
        private CertificateFactory certFactory = null;
        private PKIXParameters pkixParams = null;
        private CertPathValidator certPathValidator = null;

        /**
         * Constructor for enabling server certificate validation and certificate expiration check.
         * 
         * @param tm
         *            if null then enables just certificate expiration check.
         * @throws CertificateException
         */
        X509TrustManagerWrapper(X509TrustManager tm) throws CertificateException {
            this.originalTrustManager = tm;
            this.validateServerCert = this.originalTrustManager != null;

            if (this.validateServerCert) {
                try {
                    Set<TrustAnchor> anch = Arrays.stream(tm.getAcceptedIssuers()).map(c -> new TrustAnchor(c, null)).collect(Collectors.toSet());
                    this.pkixParams = new PKIXParameters(anch);
                    this.pkixParams.setRevocationEnabled(false);
                    this.certPathValidator = CertPathValidator.getInstance(CERT_PATH_VALIDATOR_ALGORITHM);
                    this.certFactory = CertificateFactory.getInstance(CERT_FACTORY_TYPE);
                } catch (Exception e) {
                    throw new CertificateException(e);
                }
            }
        }

        /**
         * Constructor for enabling just certificate expiration check.
         * 
         * @throws CertificateException
         */
        X509TrustManagerWrapper() throws CertificateException {
            this(null);
        }

        public X509Certificate[] getAcceptedIssuers() {
            return this.originalTrustManager != null ? this.originalTrustManager.getAcceptedIssuers() : new X509Certificate[0];
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            for (int i = 0; i < chain.length; i++) {
                chain[i].checkValidity();
            }

            if (this.validateServerCert) {
                X509Certificate serverCert = chain[0];
                X509CertSelector certSelect = new X509CertSelector();
                certSelect.setSerialNumber(serverCert.getSerialNumber());

                try {
                    CertPath certPath = this.certFactory.generateCertPath(Arrays.asList(chain));
                    // Validate against the truststore.
                    CertPathValidatorResult result = this.certPathValidator.validate(certPath, this.pkixParams);
                    // Check expiration for the CA used to validate this path.
                    ((PKIXCertPathValidatorResult) result).getTrustAnchor().getTrustedCert().checkValidity();
                } catch (InvalidAlgorithmParameterException e) {
                    throw new CertificateException(e);
                } catch (CertPathValidatorException e) {
                    throw new CertificateException(e);
                }

                this.originalTrustManager.checkServerTrusted(chain, authType);
            }
        }

        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            this.originalTrustManager.checkClientTrusted(chain, authType);
        }
    }

    private static class KeyStoreConfigurations {
        public String keyStoreUrl = null;
        public String keyStorePassword = null;
        public String keyStoreType = KeyStore.getDefaultType();

        public KeyStoreConfigurations() {
        }

        public KeyStoreConfigurations(String keyStoreUrl, String keyStorePassword, String keyStoreType) {
            this.keyStoreUrl = keyStoreUrl;
            this.keyStorePassword = keyStorePassword;
            this.keyStoreType = keyStoreType;
        }
    }

    private static class HostnameChecker {
        private String hostname;

        public HostnameChecker(String hostname) {
            this.hostname = hostname;
        }

        public void match(X509Certificate certificate) throws CertificateException {
            boolean hostNameVerified = false;

            // Check each one of the DNS-ID (or IP) entries from the certificate 'subjectAltName' field.
            // See https://tools.ietf.org/html/rfc6125#section-6.4 and https://tools.ietf.org/html/rfc2818#section-3.1
            Collection<List<?>> subjectAltNames = null;
            subjectAltNames = certificate.getSubjectAlternativeNames();
            if (subjectAltNames != null) {
                boolean sanVerification = false;
                for (final List<?> san : subjectAltNames) {
                    final Integer nameType = (Integer) san.get(0);
                    // dNSName   [2] IA5String
                    // iPAddress [7] OCTET STRING
                    if (nameType == 2) {
                        sanVerification = true;
                        if (verifyHostName((String) san.get(1))) {  // May contain a wildcard char.
                            // Host name is valid.
                            hostNameVerified = true;
                            break;
                        }
                    } else if (nameType == 7) {
                        sanVerification = true;
                        if (this.hostname.equalsIgnoreCase((String) san.get(1))) {
                            // Host name (IP) is valid.
                            hostNameVerified = true;
                            break;
                        }
                    }
                }
                if (sanVerification && !hostNameVerified) {
                    throw new CertificateException("Server identity verification failed. None of the certificate's DNS or IP Subject Alternative Name entries "
                            + "matched the server hostname/IP '" + this.hostname + "'.");
                }
            }

            if (!hostNameVerified) {
                // Fall-back to checking the Relative Distinguished Name CN-ID (Common Name/CN) from the certificate 'subject' field.   
                // https://tools.ietf.org/html/rfc6125#section-6.4.4
                final String dn = certificate.getSubjectX500Principal().getName(X500Principal.RFC2253);
                String cn = null;
                try {
                    LdapName ldapDN = new LdapName(dn);
                    cn = ldapDN.getRdns().stream().filter(r -> r.getType().equalsIgnoreCase("CN")).findFirst().get().getValue().toString();
                } catch (InvalidNameException e) {
                    throw new CertificateException("Failed to retrieve the Common Name (CN) from the server certificate.", e);
                }

                if (!verifyHostName(cn)) {
                    throw new CertificateException(
                            "Server identity verification failed. The certificate's Common Name '" + cn + "' does not match '" + this.hostname + "'.");
                }
            }
        }

        /**
         * Verify the host name against the given pattern, using the rules specified in <a href="https://tools.ietf.org/html/rfc6125#section-6.4.3">RFC 6125,
         * Section 6.4.3</a>. Support wildcard character as defined in the RFC.
         * 
         * @param ptn
         *            the pattern to match with the host name.
         * @return
         *         <code>true</code> if the host name matches the pattern, <code>false</code> otherwise.
         */
        private boolean verifyHostName(String ptn) {
            final int indexOfStar = ptn.indexOf('*');
            if (indexOfStar >= 0 && indexOfStar < ptn.indexOf('.')) {
                final String head = ptn.substring(0, indexOfStar);
                final String tail = ptn.substring(indexOfStar + 1);

                return StringUtils.startsWithIgnoreCase(this.hostname, head) && StringUtils.endsWithIgnoreCase(this.hostname.substring(head.length()), tail)
                        && this.hostname.substring(head.length(), this.hostname.length() - tail.length()).indexOf('.') == -1;
            }
            return this.hostname.equalsIgnoreCase(ptn);
        }
    }
}
