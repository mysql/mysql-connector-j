/*
  Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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

package com.mysql.cj.api.xdevapi;

import com.mysql.cj.core.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.xdevapi.SessionConfig;
import com.mysql.cj.xdevapi.SessionConfigManager;

/**
 * A handler for password persistence. Implementations of this interface can be registered in
 * {@link SessionConfigManager#setPasswordHandler(PasswordHandler)} so that passwords can be securely saved and loaded back to {@link SessionConfig}
 * structures.
 */
public interface PasswordHandler {
    /**
     * Saves the given password in some persistent, desirably secure, storage. The password must be indexed by the given key and service values.
     * 
     * @param key
     *            the key component of the index for the given password; corresponds to the user name.
     * @param service
     *            the service component of the index for the given password; corresponds to the string composed of the "host:port" pair.
     * @param password
     *            the password to save.
     */
    default void save(String key, String service, String password) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "This operation is not supported by default.");
    }

    /**
     * Retrieves the password saved under the index composed by the given key and service components.
     * 
     * @param key
     *            the key component of the index for the given password; corresponds to the user name.
     * @param service
     *            the service component of the index for the given password; corresponds to the string composed of the "host:port" pair.
     * @return
     *         the retrieved password.
     */
    default String load(String key, String service) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "This operation is not supported by default.");
    }
}
