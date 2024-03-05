/*
 * Copyright (c) 2023, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.callback;

import java.util.ArrayList;
import java.util.List;

/**
 * The callback object used by the authentication plugin AuthenticationWebAuthnClient to exchange authenticator data between the driver and the client
 * application.
 *
 * Applications must implement a MysqlCallbackHandler to manage the interaction with authenticator devices and provide the data required to proceed with
 * FIDO2/WebAuthn authentication. Such handler receives an instance of this class, which must then be used to obtain the data to send to the device and to
 * submit the produced authenticator data and signature into the driver.
 */
public class WebAuthnAuthenticationCallback implements MysqlCallback {

    // FIDO inputs.
    private byte[] clientDataHash;
    private String relyingPartyId;
    private byte[] credentialId;

    // FIDO outputs.
    private boolean supportsCredentialManagement;
    private List<byte[]> authenticatorDataEntries;
    private List<byte[]> signatureEntries;

    /**
     * Instances of this object are used to exchange FIDO data between the client application and the driver and are responsible for managing all the
     * interactions with the FIDO authenticator devices.
     *
     * @param clientDataHash
     *            the client data hash
     * @param relyingPartyId
     *            the relying party id
     * @param credentialId
     *            the credential id
     */
    public WebAuthnAuthenticationCallback(byte[] clientDataHash, String relyingPartyId, byte[] credentialId) {
        this.clientDataHash = clientDataHash;
        this.relyingPartyId = relyingPartyId;
        this.credentialId = credentialId;
        this.authenticatorDataEntries = new ArrayList<>();
        this.signatureEntries = new ArrayList<>();
    }

    /**
     * Returns the FIDO Client Data Hash (an SHA-256 hash computed from the Client Data JSON) for the upcoming authenticator interaction.
     *
     * @return
     *         the client data hash
     */
    public byte[] getClientDataHash() {
        return this.clientDataHash;
    }

    /**
     * Returns the FIDO Relying Party Id for the upcoming authenticator interaction.
     *
     * @return
     *         the relying party id
     */
    public String getRelyingPartyId() {
        return this.relyingPartyId;
    }

    /**
     * Returns the FIDO Credential Id for the upcoming authenticator interaction.
     *
     * @return
     *         the credential id
     */
    public byte[] getCredentialId() {
        return this.credentialId;
    }

    /**
     * Sets whether this FIDO Authenticator device supports Credential Management.
     *
     * @param supportsCredMan
     *            is credential management supported?
     */
    public void setSupportsCredentialManagement(boolean supportsCredMan) {
        this.supportsCredentialManagement = supportsCredMan;
    }

    /**
     * Returns whether this FIDO Authenticator device supports Credential Management.
     *
     * @return
     *         is credential management supported?
     */
    public boolean getSupportsCredentialManagement() {
        return this.supportsCredentialManagement;
    }

    /**
     * Adds a FIDO Authenticator Data produced by the authenticator interaction.
     *
     * @param authenticatorData
     *            the authenticator data
     */
    public void addAuthenticatorData(byte[] authenticatorData) {
        this.authenticatorDataEntries.add(authenticatorData);
    }

    /**
     * Returns one of FIDO Authenticator Data produced by the authenticator interaction.
     *
     * @param idx
     *            the index of the Authenticator Data to return
     *
     * @return
     *         the authenticator data
     */
    public byte[] getAuthenticatorData(int idx) {
        if (idx >= this.authenticatorDataEntries.size()) {
            return null;
        }
        return this.authenticatorDataEntries.get(idx);
    }

    /**
     * Adds a FIDO Signature produced by the authenticator interaction.
     *
     * @param signature
     *            the signature
     */
    public void addSignature(byte[] signature) {
        this.signatureEntries.add(signature);
    }

    /**
     * Returns one of the FIDO Signatures produced by the authenticator interaction
     *
     * @param idx
     *            the index of the Signature to return
     *
     * @return
     *         the signature
     */
    public byte[] getSignature(int idx) {
        if (idx >= this.signatureEntries.size()) {
            return null;
        }
        return this.signatureEntries.get(idx);
    }

    /**
     * Returns the number of assertions produced by the authenticator interaction
     *
     * @return
     *         the number of assertions
     */
    public int getAssertCount() {
        return Math.min(this.authenticatorDataEntries.size(), this.signatureEntries.size());
    }

}
