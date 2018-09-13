/*
 * Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.xdevapi;

/**
 * X DevAPI class encapsulating a Session pooling functionality.
 * <p>
 * The Client object is obtained via {@link ClientFactory#getClient(String, java.util.Properties)} or {@link ClientFactory#getClient(String, String)} methods.
 */
public interface Client {

    /**
     * Get <code>Session</code> from pool or the new one.
     * 
     * @return {@link Session}
     */
    public Session getSession();

    /**
     * Close <code>Client</code>.
     * Closes all Sessions it had created, and destroys the managed pool.
     * <p>
     * Calling the method <code>close</code> on a <code>Client</code>
     * object that is already closed is a no-op.
     */
    public void close();

    public enum ClientProperty {
        POOLING_ENABLED("pooling.enabled"), POOLING_MAX_SIZE("pooling.maxSize"), POOLING_MAX_IDLE_TIME("pooling.maxIdleTime"),
        POOLING_QUEUE_TIMEOUT("pooling.queueTimeout");

        private String keyName = "";

        ClientProperty(String keyName) {
            this.keyName = keyName;
        }

        public String getKeyName() {
            return this.keyName;
        }
    }

}
