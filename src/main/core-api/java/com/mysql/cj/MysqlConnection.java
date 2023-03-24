/*
 * Copyright (c) 2015, 2021, Oracle and/or its affiliates.
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

package com.mysql.cj;

import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.protocol.ServerSessionStateController;

public interface MysqlConnection {

    PropertySet getPropertySet();

    /**
     * Creates an IO channel to the server.
     * 
     * @param isForReconnect
     *            is this request for a re-connect
     */
    void createNewIO(boolean isForReconnect);

    long getId();

    /**
     * Returns the parsed and passed in properties for this connection.
     * 
     * @return {@link Properties}
     */
    Properties getProperties();

    ReentrantLock getConnectionMutex();

    Session getSession();

    String getURL();

    String getUser();

    ExceptionInterceptor getExceptionInterceptor();

    void checkClosed();

    void normalClose();

    /**
     * Destroys this connection and any underlying resources.
     * 
     * @param whyCleanedUp
     *            exception caused the connection clean up
     */
    void cleanup(Throwable whyCleanedUp);

    ServerSessionStateController getServerSessionStateController();
}
