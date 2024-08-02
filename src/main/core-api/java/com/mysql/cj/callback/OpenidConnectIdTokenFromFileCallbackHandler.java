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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.util.StringUtils;

/**
 * A callback handler that reads an OpenID Identity Token from a file.
 *
 * The file containing the Identity Token must exist and not be larger than 10KB.
 */
public class OpenidConnectIdTokenFromFileCallbackHandler implements MysqlCallbackHandler {

    private static final int ID_TOKEN_SIZE_LIMIT = 10 * 1024;

    public OpenidConnectIdTokenFromFileCallbackHandler() {
    }

    @Override
    public void handle(MysqlCallback cb) {
        if (!OpenidConnectAuthenticationCallback.class.isAssignableFrom(cb.getClass())) {
            return;
        }

        OpenidConnectAuthenticationCallback openidConnectAuthCallback = (OpenidConnectAuthenticationCallback) cb;

        String idTokenFileName = openidConnectAuthCallback.getConnProperty(PropertyKey.idTokenFile);
        if (StringUtils.isNullOrEmpty(idTokenFileName)) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("AuthenticationOpenidConnect.MissingIdTokenFileOption"));
        }

        File idTokenFile = new File(idTokenFileName);

        if (!idTokenFile.exists() || !idTokenFile.isFile()) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("AuthenticationOpenidConnect.FailReadIdTokenFile"));
        }

        if (idTokenFile.length() > ID_TOKEN_SIZE_LIMIT) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("AuthenticationOpenidConnect.InvalidIdTokenFile"));
        }

        try {
            byte[] idToken = Files.readAllBytes(idTokenFile.toPath());
            openidConnectAuthCallback.setIdentityToken(idToken);
        } catch (IOException e) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("AuthenticationOpenidConnect.FailReadIdTokenFile"));
        }
    }

}
