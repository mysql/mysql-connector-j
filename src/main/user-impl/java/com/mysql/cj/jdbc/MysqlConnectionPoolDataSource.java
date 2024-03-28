/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

/**
 * This class is used to obtain a physical connection and instantiate and return a MysqlPooledConnection. J2EE application servers map client calls to
 * dataSource.getConnection to this class based upon mapping set within deployment descriptor. This class extends MysqlDataSource.
 */
public class MysqlConnectionPoolDataSource extends MysqlDataSource implements ConnectionPoolDataSource {

    static final long serialVersionUID = -7767325445592304961L;

    private final Lock lock = new ReentrantLock();

    @Override
    public PooledConnection getPooledConnection() throws SQLException {
        this.lock.lock();
        try {
            Connection connection = getConnection();
            MysqlPooledConnection mysqlPooledConnection = MysqlPooledConnection.getInstance((JdbcConnection) connection);

            return mysqlPooledConnection;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public PooledConnection getPooledConnection(String u, String p) throws SQLException {
        this.lock.lock();
        try {
            Connection connection = getConnection(u, p);
            MysqlPooledConnection mysqlPooledConnection = MysqlPooledConnection.getInstance((JdbcConnection) connection);

            return mysqlPooledConnection;
        } finally {
            this.lock.unlock();
        }
    }

}
