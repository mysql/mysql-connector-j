/*
 * Copyright (c) 2011, 2019, Oracle and/or its affiliates. All rights reserved.
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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import com.mysql.cj.Messages;
import com.mysql.cj.Session;

public interface SocketMetadata {

    default boolean isLocallyConnected(Session sess) {
        String processHost = sess.getProcessHost();
        return isLocallyConnected(sess, processHost);
    }

    default boolean isLocallyConnected(Session sess, String processHost) {
        if (processHost != null) {
            sess.getLog().logDebug(Messages.getString("SocketMetadata.0", new Object[] { processHost }));

            int endIndex = processHost.lastIndexOf(":");
            if (endIndex != -1) {
                processHost = processHost.substring(0, endIndex);
            }

            try {

                InetAddress[] whereMysqlThinksIConnectedFrom = InetAddress.getAllByName(processHost);

                SocketAddress remoteSocketAddr = sess.getRemoteSocketAddress();

                if (remoteSocketAddr instanceof InetSocketAddress) {
                    InetAddress whereIConnectedTo = ((InetSocketAddress) remoteSocketAddr).getAddress();

                    for (InetAddress hostAddr : whereMysqlThinksIConnectedFrom) {
                        if (hostAddr.equals(whereIConnectedTo)) {
                            sess.getLog().logDebug(Messages.getString("SocketMetadata.1", new Object[] { hostAddr, whereIConnectedTo }));
                            return true;
                        }
                        sess.getLog().logDebug(Messages.getString("SocketMetadata.2", new Object[] { hostAddr, whereIConnectedTo }));
                    }

                } else {
                    sess.getLog().logDebug(Messages.getString("SocketMetadata.3", new Object[] { remoteSocketAddr }));
                }

                return false;
            } catch (UnknownHostException e) {
                sess.getLog().logWarn(Messages.getString("Connection.CantDetectLocalConnect", new Object[] { processHost }), e);

                return false;
            }

        }

        return false;
    }

}
