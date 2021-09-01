/*
 * Copyright (c) 2002, 2021, Oracle and/or its affiliates.
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
import java.security.Signature;
import java.security.SignatureException;
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
import javax.naming.ldap.Rdn;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.RSAException;
import com.mysql.cj.exceptions.SSLParamsException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.log.Log;
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
    private static final String TLSv1_3 = "TLSv1.3";
    private static final String[] TLS_PROTOCOLS = new String[] { TLSv1_3, TLSv1_2, TLSv1_1, TLSv1 };

    private static final String TLS_SETTINGS_RESOURCE = "/com/mysql/cj/TlsSettings.properties";
    private static final List<String> ALLOWED_CIPHERS = new ArrayList<>();
    private static final List<String> RESTRICTED_CIPHER_SUBSTR = new ArrayList<>();

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
            Arrays.stream(tlsSettings.getProperty("TLSCiphers.Unacceptable.Mask").split("\\s*,\\s*")).forEach(s -> RESTRICTED_CIPHER_SUBSTR.add(s.trim()));
        } catch (IOException e) {
            throw ExceptionFactory.createException("Unable to load TlsSettings.properties");
        }
    }

    private ExportControlled() { /* prevent instantiation */
    }

    public static boolean enabled() {
        // we may wish to un-static-ify this class this static method call may be removed entirely by the compiler
        return true;
    }

    private static String[] getAllowedCiphers(PropertySet pset, List<String> socketCipherSuites) {
        String enabledSSLCipherSuites = pset.getStringProperty(PropertyKey.enabledSSLCipherSuites).getValue();
        Stream<String> filterStream = StringUtils.isNullOrEmpty(enabledSSLCipherSuites) ? socketCipherSuites.stream()
                : Arrays.stream(enabledSSLCipherSuites.split("\\s*,\\s*")).filter(socketCipherSuites::contains);

        List<String> allowedCiphers = filterStream
                // mandatory, approved and deprecated ciphers
                .filter(ALLOWED_CIPHERS::contains)
                // unacceptable ciphers
                .filter(c -> !RESTRICTED_CIPHER_SUBSTR.stream().filter(r -> c.contains(r)).findFirst().isPresent())
                //
                .collect(Collectors.toList());

        return allowedCiphers.toArray(new String[] {});
    }

    private static String[] getAllowedProtocols(PropertySet pset, ServerVersion serverVersion, String[] socketProtocols) {
        String[] tryProtocols = null;

        // If enabledTLSProtocols configuration option is set, overriding the default TLS version restrictions.
        // This allows enabling TLSv1.2 for self-compiled MySQL versions supporting it, as well as the ability
        // for users to restrict TLS connections to approved protocols (e.g., prohibiting TLSv1) on the client side.
        String enabledTLSProtocols = pset.getStringProperty(PropertyKey.enabledTLSProtocols).getValue();
        if (enabledTLSProtocols != null && enabledTLSProtocols.length() > 0) {
            tryProtocols = enabledTLSProtocols.split("\\s*,\\s*");
        }
        // It is problematic to enable TLSv1.2 on the client side when the server is compiled with yaSSL. When client attempts to connect with
        // TLSv1.2 yaSSL just closes the socket instead of re-attempting handshake with lower TLS version. So here we allow all protocols only
        // for server versions which are known to be compiled with OpenSSL.
        else if (serverVersion == null) {
            // X Protocol doesn't provide server version, but we prefer to use most recent TLS version, though it also means that X Protocol
            // connection to old MySQL 5.7 GPL releases will fail by default, user must use enabledTLSProtocols=TLSv1.1 to connect them.
            tryProtocols = TLS_PROTOCOLS;
        } else if (serverVersion.meetsMinimum(new ServerVersion(5, 7, 28))
                || serverVersion.meetsMinimum(new ServerVersion(5, 6, 46)) && !serverVersion.meetsMinimum(new ServerVersion(5, 7, 0))
                || serverVersion.meetsMinimum(new ServerVersion(5, 6, 0)) && Util.isEnterpriseEdition(serverVersion.toString())) {
            tryProtocols = TLS_PROTOCOLS;
        } else {
            // allow only TLSv1 and TLSv1.1 for other server versions by default
            tryProtocols = new String[] { TLSv1_1, TLSv1 };
        }

        List<String> configuredProtocols = new ArrayList<>(Arrays.asList(tryProtocols));
        List<String> jvmSupportedProtocols = Arrays.asList(socketProtocols);

        List<String> allowedProtocols = new ArrayList<>();
        for (String protocol : TLS_PROTOCOLS) {
            if (jvmSupportedProtocols.contains(protocol) && configuredProtocols.contains(protocol)) {
                allowedProtocols.add(protocol);
            }
        }
        return allowedProtocols.toArray(new String[0]);

    }

    public static void checkValidProtocols(List<String> protocols) {
        List<String> validProtocols = Arrays.asList(TLS_PROTOCOLS);
        for (String protocol : protocols) {
            if (!validProtocols.contains(protocol)) {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        "'" + protocol + "' not recognized as a valid TLS protocol version (should be one of "
                                + Arrays.stream(TLS_PROTOCOLS).collect(Collectors.joining(", ")) + ").");
            }
        }
    }

    private static class KeyStoreConf {
        public String keyStoreUrl = null;
        public String keyStorePassword = null;
        public String keyStoreType = "JKS";

        public KeyStoreConf() {
        }

        public KeyStoreConf(String keyStoreUrl, String keyStorePassword, String keyStoreType) {
            this.keyStoreUrl = keyStoreUrl;
            this.keyStorePassword = keyStorePassword;
            this.keyStoreType = keyStoreType;
        }
    }

    private static KeyStoreConf getTrustStoreConf(PropertySet propertySet, boolean required) {
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

        if (required && StringUtils.isNullOrEmpty(trustStoreUrl)) {
            throw new CJCommunicationsException("No truststore provided to verify the Server certificate.");
        }

        return new KeyStoreConf(trustStoreUrl, trustStorePassword, trustStoreType);
    }

    private static KeyStoreConf getKeyStoreConf(PropertySet propertySet) {
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

        return new KeyStoreConf(keyStoreUrl, keyStorePassword, keyStoreType);
    }

    /**
     * Converts the socket being used in the given SocketConnection to an SSLSocket by performing the SSL/TLS handshake.
     * 
     * @param rawSocket
     *            original non-SSL socket
     * @param socketConnection
     *            the Protocol instance containing the socket to convert to an SSLSocket.
     * @param serverVersion
     *            ServerVersion object
     * @param log
     *            Logger
     * @return SSL socket
     * @throws IOException
     *             if i/o exception occurs
     * @throws SSLParamsException
     *             if the handshake fails, or if this distribution of Connector/J doesn't contain the SSL crypto hooks needed to perform the handshake.
     * @throws FeatureNotAvailableException
     *             if TLS is not supported
     */
    public static Socket performTlsHandshake(Socket rawSocket, SocketConnection socketConnection, ServerVersion serverVersion, Log log)
            throws IOException, SSLParamsException, FeatureNotAvailableException {
        PropertySet pset = socketConnection.getPropertySet();

        SslMode sslMode = pset.<SslMode>getEnumProperty(PropertyKey.sslMode).getValue();
        boolean verifyServerCert = sslMode == SslMode.VERIFY_CA || sslMode == SslMode.VERIFY_IDENTITY;
        boolean fallbackToSystemTrustStore = pset.getBooleanProperty(PropertyKey.fallbackToSystemTrustStore).getValue();

        // (serverVersion == null) means that it was called from the X DevAPI.
        KeyStoreConf trustStore = !verifyServerCert ? new KeyStoreConf()
                : getTrustStoreConf(pset, serverVersion == null && verifyServerCert && !fallbackToSystemTrustStore);
        KeyStoreConf keyStore = getKeyStoreConf(pset);

        SSLSocketFactory socketFactory = getSSLContext(keyStore, trustStore, fallbackToSystemTrustStore, verifyServerCert,
                sslMode == PropertyDefinitions.SslMode.VERIFY_IDENTITY ? socketConnection.getHost() : null, socketConnection.getExceptionInterceptor())
                        .getSocketFactory();

        SSLSocket sslSocket = (SSLSocket) socketFactory.createSocket(rawSocket, socketConnection.getHost(), socketConnection.getPort(), true);

        String[] allowedProtocols = getAllowedProtocols(pset, serverVersion, sslSocket.getSupportedProtocols());
        sslSocket.setEnabledProtocols(allowedProtocols);

        String[] allowedCiphers = getAllowedCiphers(pset, Arrays.asList(sslSocket.getEnabledCipherSuites()));
        if (allowedCiphers != null) {
            sslSocket.setEnabledCipherSuites(allowedCiphers);
        }

        sslSocket.startHandshake();

        if (log != null) {
            String tlsVersion = sslSocket.getSession().getProtocol();
            if (TLSv1.equalsIgnoreCase(tlsVersion) || TLSv1_1.equalsIgnoreCase(tlsVersion)) {
                log.logWarn("This connection is using " + tlsVersion + " which is now deprecated and will be removed in a future release of Connector/J.");
            }
        }

        return sslSocket;
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
                    // Validate against the truststore.
                    CertPathValidatorResult result = this.validator.validate(certPath, this.validatorParams);
                    // Check expiration for the CA used to validate this path.
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

                // Validate server identity.
                if (this.hostName != null) {
                    boolean hostNameVerified = false;

                    // Check each one of the DNS-ID (or IP) entries from the certificate 'subjectAltName' field.
                    // See https://tools.ietf.org/html/rfc6125#section-6.4 and https://tools.ietf.org/html/rfc2818#section-3.1
                    final Collection<List<?>> subjectAltNames = chain[0].getSubjectAlternativeNames();
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
                                if (this.hostName.equalsIgnoreCase((String) san.get(1))) {
                                    // Host name (IP) is valid.
                                    hostNameVerified = true;
                                    break;
                                }
                            }
                        }
                        if (sanVerification && !hostNameVerified) {
                            throw new CertificateException("Server identity verification failed. "
                                    + "None of the DNS or IP Subject Alternative Name entries matched the server hostname/IP '" + this.hostName + "'.");
                        }
                    }

                    if (!hostNameVerified) {
                        // Fall-back to checking the Relative Distinguished Name CN-ID (Common Name/CN) from the certificate 'subject' field.   
                        // https://tools.ietf.org/html/rfc6125#section-6.4.4
                        final String dn = chain[0].getSubjectX500Principal().getName(X500Principal.RFC2253);
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

                        if (!verifyHostName(cn)) {
                            throw new CertificateException(
                                    "Server identity verification failed. The certificate Common Name '" + cn + "' does not match '" + this.hostName + "'.");
                        }
                    }
                }
            }

            // Nothing else to validate.
        }

        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            this.origTm.checkClientTrusted(chain, authType);
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

                return StringUtils.startsWithIgnoreCase(this.hostName, head) && StringUtils.endsWithIgnoreCase(this.hostName, tail)
                        && this.hostName.substring(head.length(), this.hostName.length() - tail.length()).indexOf('.') == -1;
            }
            return this.hostName.equalsIgnoreCase(ptn);
        }
    }

    /**
     * Configure the {@link SSLContext} based on the supplier property set.
     * 
     * @param clientCertificateKeyStore
     *            clientCertificateKeyStore
     * @param trustCertificateKeyStore
     *            trustCertificateKeyStore
     * @param fallbackToDefaultTrustStore
     *            fallbackToDefaultTrustStore
     * @param verifyServerCert
     *            verifyServerCert
     * @param hostName
     *            host name
     * @param exceptionInterceptor
     *            exception interceptor
     * @return SSLContext
     * @throws SSLParamsException
     *             if an error occurs
     */
    public static SSLContext getSSLContext(KeyStoreConf clientCertificateKeyStore, KeyStoreConf trustCertificateKeyStore, boolean fallbackToDefaultTrustStore,
            boolean verifyServerCert, String hostName, ExceptionInterceptor exceptionInterceptor) throws SSLParamsException {
        String clientCertificateKeyStoreUrl = clientCertificateKeyStore.keyStoreUrl;
        String clientCertificateKeyStoreType = clientCertificateKeyStore.keyStoreType;
        String clientCertificateKeyStorePassword = clientCertificateKeyStore.keyStorePassword;
        String trustCertificateKeyStoreUrl = trustCertificateKeyStore.keyStoreUrl;
        String trustCertificateKeyStoreType = trustCertificateKeyStore.keyStoreType;
        String trustCertificateKeyStorePassword = trustCertificateKeyStore.keyStorePassword;

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

            if (trustKeyStore != null || verifyServerCert && fallbackToDefaultTrustStore) {
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
}
