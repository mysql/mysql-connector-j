/*
 * Copyright (c) 2020, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.sasl;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.mysql.cj.util.SaslPrep;
import com.mysql.cj.util.SaslPrep.StringType;
import com.mysql.cj.util.StringUtils;

/**
 * A {@link SaslClient} implementation for SCRAM-SHA mechanisms as specified in <a href="https://tools.ietf.org/html/rfc5802">RFC 5802</a>.
 * Subclasses of this class must implement the hashing algorithms for the corresponding authentication mechanism.
 */
public abstract class ScramShaSaslClient implements SaslClient {

    protected enum ScramExchangeStage {

        TERMINATED(null), SERVER_FINAL(TERMINATED), SERVER_FIRST_CLIENT_FINAL(SERVER_FINAL), CLIENT_FIRST(SERVER_FIRST_CLIENT_FINAL);

        private ScramExchangeStage next;

        private ScramExchangeStage(ScramExchangeStage next) {
            this.next = next;
        }

        public ScramExchangeStage getNext() {
            return this.next == null ? this : this.next;
        }

    }

    protected static final int MINIMUM_ITERATIONS = 4096;
    protected static final String GS2_CBIND_FLAG = "n";
    protected static final byte[] CLIENT_KEY = "Client Key".getBytes();
    protected static final byte[] SERVER_KEY = "Server Key".getBytes();

    protected String authorizationId;
    protected String authenticationId;
    protected String password;

    protected ScramExchangeStage scramStage = ScramExchangeStage.CLIENT_FIRST;
    protected String cNonce;
    protected String gs2Header;
    protected String clientFirstMessageBare;
    protected byte[] serverSignature;

    public ScramShaSaslClient(String authorizationId, String authenticationId, String password) throws SaslException {
        this.authorizationId = StringUtils.isNullOrEmpty(authorizationId) ? "" : authorizationId;
        this.authenticationId = StringUtils.isNullOrEmpty(authenticationId) ? this.authorizationId : authenticationId;
        if (StringUtils.isNullOrEmpty(this.authenticationId)) {
            throw new SaslException("The authenticationId cannot be null or empty.");
        }
        this.password = StringUtils.isNullOrEmpty(password) ? "" : password;
        this.scramStage = ScramExchangeStage.CLIENT_FIRST;
    }

    /**
     * Returns the real IANA-registered mechanism name of this SASL client. This is the same as {@link SaslClient#getMechanismName()} except that subclasses may
     * use custom mechanism names to avoid future name clashes.
     *
     * @return
     *         a non-null string representing the IANA-registered mechanism name.
     */
    abstract String getIanaMechanismName();

    @Override
    public boolean hasInitialResponse() {
        return true;
    }

    @Override
    public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
        try {
            switch (this.scramStage) {
                case CLIENT_FIRST: // Process client-first-message.
                    // client-first-message      = gs2-header client-first-message-bare
                    // gs2-header                = gs2-cbind-flag "," [ authzid ] ","
                    // gs2-cbind-flag            = "n"
                    // authzid                   = "a=" saslname
                    // client-first-message-bare = username "," nonce
                    // username                  = "n=" saslname
                    // saslname                  = 1*(value-safe-char / "=2C" / "=3D")
                    // value-safe-char           = UTF8
                    //                             ;; UTF8 except NUL, "=", and ",".
                    // nonce                     = "r=" c-nonce [s-nonce]
                    //                             ;; Second part provided by server.
                    // c-nonce                   = printable
                    // printable                 = %x21-2B / %x2D-7E
                    //                             ;; Printable ASCII except ",".
                    this.gs2Header = GS2_CBIND_FLAG + "," + (StringUtils.isNullOrEmpty(this.authorizationId) ? "" : "a=" + prepUserName(this.authorizationId))
                            + ",";
                    this.cNonce = generateRandomPrintableAsciiString(32);
                    this.clientFirstMessageBare = "n=" + prepUserName(this.authenticationId) + ",r=" + this.cNonce;
                    String clientFirstMessage = this.gs2Header + this.clientFirstMessageBare;

                    return StringUtils.getBytes(clientFirstMessage, "UTF-8");

                case SERVER_FIRST_CLIENT_FINAL: // Process server-first-message & client-final-message.
                    // 1st part: server-first-message.
                    String serverFirstMessage = StringUtils.toString(challenge, StandardCharsets.UTF_8);
                    Map<String, String> serverFirstAttributes = parseChallenge(serverFirstMessage);

                    if (!serverFirstAttributes.containsKey("r") || !serverFirstAttributes.containsKey("s") || !serverFirstAttributes.containsKey("i")) {
                        throw new SaslException("Missing required SCRAM attribute from server first message.");
                    }

                    String sNonce = serverFirstAttributes.get("r");
                    if (!sNonce.startsWith(this.cNonce)) {
                        throw new SaslException("Invalid server nonce for " + getIanaMechanismName() + " authentication.");
                    }
                    byte[] salt = Base64.getDecoder().decode(serverFirstAttributes.get("s"));
                    int iterations = Integer.parseInt(serverFirstAttributes.get("i"));
                    if (iterations < MINIMUM_ITERATIONS) {
                        throw new SaslException("Announced " + getIanaMechanismName() + " iteration count is too low.");
                    }

                    // 2nd part: client-final-message.

                    // client-final-message-without-proof = channel-binding "," nonce
                    // channel-binding                    = "c=" base64
                    //                                      ;; base64 encoding of cbind-input.
                    // cbind-input                        = gs2-header
                    String clientFinalMessageWithoutProof = "c=" + Base64.getEncoder().encodeToString(StringUtils.getBytes(this.gs2Header, "UTF-8")) + ",r="
                            + sNonce;

                    // Compute ClientProof:
                    //   SaltedPassword  := Hi(Normalize(password), salt, i)
                    //   ClientKey       := HMAC(SaltedPassword, "Client Key")
                    //   StoredKey       := H(ClientKey)
                    //   AuthMessage     := client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
                    //   ClientSignature := HMAC(StoredKey, AuthMessage)
                    //   ClientProof     := ClientKey XOR ClientSignature
                    byte[] saltedPassword = hi(SaslPrep.prepare(this.password, StringType.STORED), salt, iterations);
                    byte[] clientKey = hmac(saltedPassword, CLIENT_KEY);
                    byte[] storedKey = h(clientKey);
                    String authMessage = this.clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
                    byte[] clientSignature = hmac(storedKey, StringUtils.getBytes(authMessage, "UTF-8"));
                    byte[] clientProof = clientKey.clone();
                    xorInPlace(clientProof, clientSignature);

                    // client-final-message               = client-final-message-without-proof "," proof
                    // proof                              = "p=" base64
                    String clientFinalMessage = clientFinalMessageWithoutProof + ",p=" + Base64.getEncoder().encodeToString(clientProof);

                    // Compute ServerSignature (for future verification):
                    //   ServerKey       := HMAC(SaltedPassword, "Server Key")
                    //   ServerSignature := HMAC(ServerKey, AuthMessage)
                    byte[] serverKey = hmac(saltedPassword, SERVER_KEY);
                    this.serverSignature = hmac(serverKey, StringUtils.getBytes(authMessage, "UTF-8"));

                    return StringUtils.getBytes(clientFinalMessage, "UTF-8");

                case SERVER_FINAL: // Process server-final-message.
                    String serverFinalMessage = StringUtils.toString(challenge, "UTF-8");
                    Map<String, String> serverFinalAttributes = parseChallenge(serverFinalMessage);

                    if (serverFinalAttributes.containsKey("e")) {
                        throw new SaslException("Authentication failed due to server error '" + serverFinalAttributes.get("e") + "'.");
                    }

                    if (!serverFinalAttributes.containsKey("v")) {
                        throw new SaslException("Missing required SCRAM attribute from server final message.");
                    }

                    // verifier = "v=" base64
                    //            ;; base-64 encoded ServerSignature.
                    byte[] verifier = Base64.getDecoder().decode(serverFinalAttributes.get("v"));

                    if (!MessageDigest.isEqual(this.serverSignature, verifier)) {
                        throw new SaslException(getIanaMechanismName() + " server signature could not be verified.");
                    }
                    break;

                default:
                    throw new SaslException("Unexpected SCRAM authentication message.");
            }

            return null;
        } catch (Throwable e) {
            this.scramStage = ScramExchangeStage.TERMINATED;
            throw e;
        } finally {
            this.scramStage = this.scramStage.getNext();
        }
    }

    @Override
    public boolean isComplete() {
        return this.scramStage == ScramExchangeStage.TERMINATED;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
        throw new IllegalStateException("Integrity and/or privacy has not been negotiated.");
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
        throw new IllegalStateException("Integrity and/or privacy has not been negotiated.");
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
        return null;
    }

    @Override
    public void dispose() throws SaslException {
    }

    private String prepUserName(String userName) {
        return SaslPrep.prepare(userName, StringType.QUERY).replace("=", "=2D").replace(",", "=2C");
    }

    /**
     * Parses a SASL challenge.
     *
     * @param challenge
     *            the server message (challenge) to parse.
     * @return
     *         a {@link Map} with the key/value pairs obtained from the server challenge.
     */
    private Map<String, String> parseChallenge(String challenge) {
        Map<String, String> attributesMap = new HashMap<>();
        for (String attribute : challenge.split(",")) {
            String[] keyValue = attribute.split("=", 2);
            attributesMap.put(keyValue[0], keyValue[1]);
        }
        return attributesMap;
    }

    /**
     * Generates a RFC 5802 safe nonce: "a sequence of random printable ASCII characters excluding ','"
     *
     * @param length
     *            the length of the nonce.
     * @return
     *         a randomly generated string formed by printable ASCII characters except comma.
     */
    private String generateRandomPrintableAsciiString(int length) {
        final int first = 0x21; // First printable ASCII character: exclamation mark (!).
        final int last = 0x7E; // Last printable ASCII character: tilde (~).
        final int excl = 0x2C; // Comma (,) is excluded as per RFC 5802 (https://tools.ietf.org/html/rfc5802#section-5.1).
        final int bound = last - first;
        Random random = new SecureRandom();
        char[] result = new char[length];

        for (int i = 0; i < length;) {
            int randomValue = random.nextInt(bound) + first;
            if (randomValue != excl) {
                result[i++] = (char) randomValue;
            }
        }
        return new String(result);
    }

    /**
     * The "H(str)" cryptographic hash function as described in <a href="https://tools.ietf.org/html/rfc5802#section-2.2">RFC 5802, Section 2.2</a>.
     *
     * @param str
     *            the string to hash.
     * @return
     *         the hash value of the given string.
     */
    abstract byte[] h(byte[] str);

    /**
     * The "HMAC(key, str)" HMAC keyed hash algorithm as described in <a href="https://tools.ietf.org/html/rfc5802#section-2.2">RFC 5802, Section 2.2</a>.
     *
     * @param key
     *            the hash key.
     * @param str
     *            the input string.
     * @return
     *         the hashed value of the given params.
     */
    abstract byte[] hmac(byte[] key, byte[] str);

    /**
     * The "Hi(str, salt, i)" PBKDF2 function as described in <a href="https://tools.ietf.org/html/rfc5802#section-2.2">RFC 5802, Section 2.2</a>.
     *
     * @param str
     *            the string value to use as the internal HMAC key.
     * @param salt
     *            the input string to hash in the initial iteration.
     * @param iterations
     *            the number of iterations to run the algorithm.
     *
     * @return
     *         an hash value with an output length equal to the length of H(str).
     */
    abstract byte[] hi(String str, byte[] salt, int iterations);

    /**
     * Combines the two byte arrays in a XOR operation, changing the contents of the first.
     *
     * @param inOut
     *            the left operand of the XOR operation and the destination of the result.
     * @param other
     *            the right operand of the XOR operation.
     * @return
     *         the same as the param <code>inOut</code>, after being updated.
     */
    byte[] xorInPlace(byte[] inOut, byte[] other) {
        for (int i = 0; i < inOut.length; i++) {
            inOut[i] ^= other[i];
        }
        return inOut;
    }

}
