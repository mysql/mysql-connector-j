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
import java.sql.SQLException;

import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.mysqla.result.ProtocolEntity;
import com.mysql.cj.core.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.core.exceptions.ExceptionFactory;

public interface ProtocolEntityReader<T extends ProtocolEntity> {

    /**
     * 
     * @param sf
     * @return
     * @throws IOException
     */
    default T read(ProtocolEntityFactory<T> sf) throws IOException {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not allowed");
    }

    /**
     * Reads one result set off of the wire, if the result is actually an
     * update count, creates an update-count only result set.
     * 
     * @param maxRows
     *            the maximum number of rows to read (-1 means all rows)
     * @param streamResults
     *            should the driver leave the results on the wire,
     *            and read them only when needed?
     * @param resultPacket
     *            the first packet of information in the result set
     * @param metadata
     *            use this metadata instead of the one provided on wire
     * @param protocolEntityFactory
     * 
     * @return a result set that either represents the rows, or an update count
     * 
     * @throws SQLException
     *             if an error occurs while reading the rows
     */
    default T read(int maxRows, boolean streamResults, PacketPayload resultPacket, ColumnDefinition metadata, ProtocolEntityFactory<T> protocolEntityFactory)
            throws IOException {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not allowed");
    }

}
