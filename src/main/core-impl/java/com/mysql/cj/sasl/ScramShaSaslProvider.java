/*
 * Copyright (c) 2020, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.sasl;

import java.security.AccessController;
import java.security.InvalidParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.security.Provider;
import java.security.ProviderException;

/**
 * A SASL provider for the authentication mechanisms SCRAM-SHA-1 and SCRAM-SHA-256, here renamed to MYSQLCJ-SCRAM-SHA-1 and MYSQL-SRAM-SHA-256 respectively to
 * avoid conflicts with future default implementations.
 */
public final class ScramShaSaslProvider extends Provider {

    private static final long serialVersionUID = 866717063477857937L;

    private static final String INFO = "MySQL Connector/J SASL provider (implements client mechanisms for " + ScramSha1SaslClient.MECHANISM_NAME + " and "
            + ScramSha256SaslClient.MECHANISM_NAME + ")";

    private static final class ProviderService extends Provider.Service {

        public ProviderService(Provider provider, String type, String algorithm, String className) {
            super(provider, type, algorithm, className, null, null);
        }

        @Override
        public Object newInstance(Object constructorParameter) throws NoSuchAlgorithmException {
            String type = getType();
            if (constructorParameter != null) {
                throw new InvalidParameterException("constructorParameter not used with " + type + " engines");
            }

            String algorithm = getAlgorithm();
            if (type.equals("SaslClientFactory")) {
                if (algorithm.equals(ScramSha1SaslClient.MECHANISM_NAME) || algorithm.equals(ScramSha256SaslClient.MECHANISM_NAME)) {
                    return new ScramShaSaslClientFactory();
                }
            }
            throw new ProviderException("No implementation for " + algorithm + " " + type);
        }

    }

    public ScramShaSaslProvider() {
        super("MySQLScramShaSasl", 1.0, INFO);

        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            putService(new ProviderService(ScramShaSaslProvider.this, "SaslClientFactory", ScramSha1SaslClient.MECHANISM_NAME,
                    ScramShaSaslClientFactory.class.getName()));
            putService(new ProviderService(ScramShaSaslProvider.this, "SaslClientFactory", ScramSha256SaslClient.MECHANISM_NAME,
                    ScramShaSaslClientFactory.class.getName()));
            return null;
        });
    }

}
