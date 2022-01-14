/*
 * Copyright (c) 2022, Oracle and/or its affiliates.
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

package com.mysql.cj.callback;

/**
 * The callback object used by the authentication plugin AuthenticationFidoClient to exchange authenticator data between the driver and the client application.
 * 
 * Applications must implement a MysqlCallbackHandler to manage the interaction with authenticator devices and provide the data required to proceed with FIDO
 * authentication. Such handler receives an instance of this class, which must then be used to obtain the data to send to the device and to submit the produced
 * authenticator data and signature into the driver.
 */
public class FidoAuthenticationCallback implements MysqlCallback {
    // FIDO inputs.
    private byte[] scramble;
    private String relyingPartyId;
    private byte[] credentialId;

    // FIDO outputs.
    private byte[] authenticatorData;
    private byte[] signature;

    /**
     * Instances of this object are used to exchange FIDO data between the client application and the driver and is responsible for managing all the
     * interactions with the FIDO authenticator devices.
     * 
     * @param scramble
     * @param relyingPartyId
     * @param credentialId
     */
    public FidoAuthenticationCallback(byte[] scramble, String relyingPartyId, byte[] credentialId) {
        this.scramble = scramble;
        this.relyingPartyId = relyingPartyId;
        this.credentialId = credentialId;
    }

    /**
     * Returns the FIDO Client Data Hash (scramble) for the coming authenticator interaction.
     * 
     * @return
     *         the scramble
     */
    public byte[] getScramble() {
        return this.scramble;
    }

    /**
     * Returns the FIDO Relying Party ID for the coming authenticator interaction.
     * 
     * @return
     *         the relying party id
     */
    public String getRelyingPartyId() {
        return this.relyingPartyId;
    }

    /**
     * Returns the FIDO Credential ID for the coming authenticator interaction.
     * 
     * @return
     *         the credential id
     */
    public byte[] getCredentialId() {
        return this.credentialId;
    }

    /**
     * Sets the FIDO Authenticator Data produced by the authenticator interaction.
     * 
     * @param authenticatorData
     *            the authenticator data
     */
    public void setAuthenticatorData(byte[] authenticatorData) {
        this.authenticatorData = authenticatorData;
    }

    /**
     * Returns the FIDO Authenticator Data produced by the authenticator interaction.
     * 
     * @return
     *         the authenticator data
     */
    public byte[] getAuthenticatorData() {
        return this.authenticatorData;
    }

    /**
     * Sets the FIDO Signature produced by the authenticator interaction.
     * 
     * @param signature
     *            the signature
     */
    public void setSignature(byte[] signature) {
        this.signature = signature;
    }

    /**
     * Returns the FIDO Signature produced by the authenticator interaction
     * 
     * @return
     *         the signature
     */
    public byte[] getSignature() {
        return this.signature;
    }
}
