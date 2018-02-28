/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.protocol;

import com.mysql.cj.CharsetMapping;
import com.mysql.cj.Messages;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;

public interface AuthenticationProvider<M extends Message> {

    void init(Protocol<M> prot, PropertySet propertySet, ExceptionInterceptor exceptionInterceptor);

    void connect(ServerSession serverSession, String userName, String password, String database);

    /**
     * Re-authenticates as the given user and password
     * 
     * @param serverSession
     *            {@link ServerSession} object
     * @param userName
     *            user name
     * @param password
     *            password
     * @param database
     *            db name
     */
    void changeUser(ServerSession serverSession, String userName, String password, String database);

    String getEncodingForHandshake();

    /**
     * Get the MySQL collation index for the handshake packet. A single byte will be added to the packet corresponding to the collation index
     * found for the requested Java encoding name.
     * 
     * If the index is &gt; 255 which may be valid at some point in the future, an exception will be thrown. At the time of this implementation
     * the index cannot be &gt; 255 and only the COM_CHANGE_USER rpc, not the handshake response, can handle a value &gt; 255.
     * 
     * @param enc
     *            The Java encoding name used to lookup the collation index
     * @param sv
     *            server version
     * @return collation index
     */
    static byte getCharsetForHandshake(String enc, ServerVersion sv) {
        int charsetIndex = 0;
        if (enc != null) {
            charsetIndex = CharsetMapping.getCollationIndexForJavaEncoding(enc, sv);
        }
        if (charsetIndex == 0) {
            charsetIndex = CharsetMapping.MYSQL_COLLATION_INDEX_utf8;
        }
        if (charsetIndex > 255) {
            throw ExceptionFactory.createException(Messages.getString("MysqlIO.113", new Object[] { enc }));
        }
        return (byte) charsetIndex;
    }

}
