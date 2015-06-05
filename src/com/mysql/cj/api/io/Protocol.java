/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package com.mysql.cj.api.io;

import java.sql.SQLException;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.Session;
import com.mysql.cj.api.exception.ExceptionInterceptor;
import com.mysql.cj.core.io.Buffer;
import com.mysql.jdbc.exceptions.CommunicationsException;

public interface Protocol {

    void init(MysqlConnection conn, int socketTimeout, PhysicalConnection physicalConnection);

    public MysqlConnection getConnection();

    public void setConnection(MysqlConnection connection);

    public PhysicalConnection getPhysicalConnection();

    public ExceptionInterceptor getExceptionInterceptor();

    /**
     * @return Returns the lastPacketSentTimeMs.
     */
    public long getLastPacketSentTimeMs();

    public long getLastPacketReceivedTimeMs();

    public Session getSession();

    void setSession(Session session);

    void negotiateSSLConnection(String user, String password, String database, int packLength);

    void rejectConnection(String message);

    void rejectProtocol(Buffer buf);

    void beforeHandshake();

    void afterHandshake();

    void changeDatabase(String database);

    /**
     * Read one packet from the MySQL server
     * 
     * @return the packet from the server.
     * 
     * @throws SQLException
     * @throws CommunicationsException
     */
    Buffer readPacket() throws SQLException;

    /**
     * Read next packet in sequence from the MySQL server,
     * incrementing sequence counter.
     * 
     * @return the packet from the server.
     * 
     * @throws SQLException
     * @throws CommunicationsException
     */
    Buffer readNextPacket() throws SQLException;

    /**
     * @param packet
     * @param packetLen
     *            length of header + payload
     */
    void send(PacketBuffer packet, int packetLen);

    /**
     * Send a command to the MySQL server If data is to be sent with command,
     * it should be put in extraData.
     * 
     * Raw packets can be sent by setting queryPacket to something other
     * than null.
     * 
     * @param command
     *            the MySQL protocol 'command' from MysqlDefs
     * @param extraData
     *            any 'string' data for the command
     * @param queryPacket
     *            a packet pre-loaded with data for the protocol (i.e.
     *            from a client-side prepared statement).
     * @param skipCheck
     *            do not call checkErrorPacket() if true
     * @param extraDataCharEncoding
     *            the character encoding of the extraData
     *            parameter.
     * 
     * @return the response packet from the server
     * 
     * @throws SQLException
     *             if an I/O error or SQL error occurs
     */

    Buffer sendCommand(int command, String extraData, Buffer queryPacket, boolean skipCheck, String extraDataCharEncoding, int timeoutMillis)
            throws SQLException;

    void setThreadId(long threadId);
}
