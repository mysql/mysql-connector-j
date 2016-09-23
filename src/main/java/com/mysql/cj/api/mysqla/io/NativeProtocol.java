/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.mysqla.io;

import java.io.IOException;
import java.io.InputStream;

import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.mysqla.result.ProtocolEntity;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.CJException;

/**
 * Extends {@link Protocol} with methods specific to native MySQL protocol.
 *
 */
public interface NativeProtocol extends Protocol {

    void rejectProtocol(PacketPayload buf);

    PacketReader getPacketReader();

    /**
     * Read one packet from the MySQL server into the reusable Buffer if provided or into the new one.
     * 
     * @param reuse
     * @return the packet from the server.
     * 
     * @throws CJCommunicationsException
     */
    PacketPayload readPacket(PacketPayload reuse);

    /**
     * Read one packet from the MySQL server, checks for errors in it, and if none,
     * returns the packet, ready for reading
     * 
     * @return a packet ready for reading.
     * 
     * @throws CJException
     *             is the packet is an error packet
     */
    PacketPayload checkErrorPacket();

    /**
     * @param packet
     * @param packetLen
     *            length of header + payload
     */
    void send(PacketPayload packet, int packetLen);

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
     * @throws CJException
     *             if an I/O error or SQL error occurs
     */

    PacketPayload sendCommand(int command, String extraData, PacketPayload queryPacket, boolean skipCheck, String extraDataCharEncoding, int timeoutMillis);

    /**
     * Basic protocol data types as they are defined in http://dev.mysql.com/doc/internals/en/integer.html
     *
     */
    public enum IntegerDataType {

        /**
         * 1 byte Protocol::FixedLengthInteger
         */
        INT1,

        /**
         * 2 byte Protocol::FixedLengthInteger
         */
        INT2,

        /**
         * 3 byte Protocol::FixedLengthInteger
         */
        INT3,

        /**
         * 4 byte Protocol::FixedLengthInteger
         */
        INT4,

        /**
         * 6 byte Protocol::FixedLengthInteger
         */
        INT6,

        /**
         * 8 byte Protocol::FixedLengthInteger
         */
        INT8,

        /**
         * Length-Encoded Integer Type
         */
        INT_LENENC;
    }

    /**
     * Basic protocol data types as they are defined in http://dev.mysql.com/doc/internals/en/string.html
     * which require explicit length specification.
     *
     */
    public static enum StringLengthDataType {

        /**
         * Protocol::FixedLengthString
         * Fixed-length strings have a known, hardcoded length.
         */
        STRING_FIXED,

        /**
         * Protocol::VariableLengthString
         * The length of the string is determined by another field or is calculated at runtime
         */
        STRING_VAR;
    }

    /**
     * Basic self-describing protocol data types as they are defined in http://dev.mysql.com/doc/internals/en/string.html
     *
     */
    public static enum StringSelfDataType {

        /**
         * Protocol::NulTerminatedString
         * Strings that are terminated by a [00] byte.
         */
        STRING_TERM,

        /**
         * Protocol::LengthEncodedString
         * A length encoded string is a string that is prefixed with length encoded integer describing the length of the string.
         * It is a special case of Protocol::VariableLengthString
         */
        STRING_LENENC,

        /**
         * Protocol::RestOfPacketString
         * If a string is the last component of a packet, its length can be calculated from the overall packet length minus the current position.
         */
        STRING_EOF;
    }

    <T extends ProtocolEntity> T read(Class<T> requiredClass, ProtocolEntityFactory<T> protocolEntityFactory) throws IOException;

    /**
     * 
     * @param requiredClass
     * @param maxRows
     * @param streamResults
     * @param resultPacket
     * @param isBinaryEncoded
     * @param metadata
     *            use this metadata instead of the one provided on wire
     * @param protocolEntityFactory
     * @return
     * @throws IOException
     */
    <T extends ProtocolEntity> T read(Class<Resultset> requiredClass, int maxRows, boolean streamResults, PacketPayload resultPacket, boolean isBinaryEncoded,
            ColumnDefinition metadata, ProtocolEntityFactory<T> protocolEntityFactory) throws IOException;

    /**
     * Sets an InputStream instance that will be used to send data
     * to the MySQL server for a "LOAD DATA LOCAL INFILE" statement
     * rather than a FileInputStream or URLInputStream that represents
     * the path given as an argument to the statement.
     * 
     * This stream will be read to completion upon execution of a
     * "LOAD DATA LOCAL INFILE" statement, and will automatically
     * be closed by the driver, so it needs to be reset
     * before each call to execute*() that would cause the MySQL
     * server to request data to fulfill the request for
     * "LOAD DATA LOCAL INFILE".
     * 
     * If this value is set to NULL, the driver will revert to using
     * a FileInputStream or URLInputStream as required.
     */
    void setLocalInfileInputStream(InputStream stream);

    /**
     * Returns the InputStream instance that will be used to send
     * data in response to a "LOAD DATA LOCAL INFILE" statement.
     * 
     * This method returns NULL if no such stream has been set
     * via setLocalInfileInputStream().
     */
    InputStream getLocalInfileInputStream();
}
