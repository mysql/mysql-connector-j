/*
 * Copyright (c) 2024, Oracle and/or its affiliates.
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

import java.util.function.Function;

import com.mysql.cj.conf.PropertyKey;

/**
 * The callback object used by the authentication plugin AuthenticationOpenidConnectClient to let the client application supply Identity Tokens to the driver.
 *
 * In OpenID, user authentication is outsourced to an IdP. This Callback is required to trigger some sort of user interaction by performing a login into an
 * external system at the time the connection is established but not prior to it, because the success of the authentication depends on the MySQL user being
 * authenticated and how the user was created on the MySQL Server.
 */
public class OpenidConnectAuthenticationCallback implements MysqlCallback {

    private String user;
    Function<PropertyKey, String> connPropSupplier;

    public OpenidConnectAuthenticationCallback(Function<PropertyKey, String> connPropSupplier) {
        this.connPropSupplier = connPropSupplier;
    }

    /**
     * Provides access to the connection properties where this authentication is being performed.
     *
     * @param propKey
     *            a {@link PropertyKey} element.
     * @return
     *         the value of the specified {@link PropertyKey} as a String.
     */
    public String getConnProperty(PropertyKey propKey) {
        return this.connPropSupplier.apply(propKey);
    }

    /**
     * Returns the user name.
     *
     * @return
     *         the user name.
     */
    public String getUser() {
        return this.user;
    }

    /**
     * Sets the user name.
     *
     * @param user
     *            the user name.
     */
    public void setUser(String user) {
        this.user = user;
    }

    private byte[] identityToken;

    /**
     * Returns the OpenID Identity Token.
     *
     * @return
     *         the OpenID Identity Token.
     */
    public byte[] getIdentityToken() {
        return this.identityToken;
    }

    /**
     * Sets the OpenID Identity Token.
     *
     * @param identityToken
     *            the OpenID Identity Token.
     */
    public void setIdentityToken(byte[] identityToken) {
        this.identityToken = identityToken;
    }

}
